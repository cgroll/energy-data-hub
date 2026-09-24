"""PECD (Pan-European Climate Database) official capacity-factor product,
plus the MaStR<->PECD region/technology linking and CF x capacity weighting
that turns it into a Germany-wide potential-generation series.

Ported and consolidated (2026-09-23 design conversation) from three sibling
repos, replacing their sibling-repo-read shortcut with hub-native inputs:
- `pecd-power-validity-DE/pipeline/01+02` (raw CDS download + region-wide
  parquet) and `pkg/potential.py` (CF x capacity weighting).
- `mastr-power-capacities-germany/pipeline/07_build_wind_zone_panel.py`
  (wind's PEON/PEOF fractional zone weighting) and `mpg/panels.py`
  (the monthly-snapshot cumulative-delta panel builder).
- `pecd-replication/pecdr/solar_technology.py` (MaStR -> PECD technology
  classification) and `pipeline/24_build_solar_capacity_by_nuts2_month.py`
  (the exact `is_tracked` rule and NUTS2-from-NUTS3-prefix shortcut).

**Two capacity-weighting variants, both built from these same pieces:**
- *Historic* (`capacity_monthly_panel` -> `compute_potential_monthly`):
  each hour weighted by that calendar month's actual installed capacity --
  comparable against true historic generation (e.g. SMARD), since it never
  applies a fleet size that didn't exist yet.
- *Current fleet* (`capacity_snapshot` -> `compute_potential_fixed`): one
  fixed capacity snapshot (today, or any `as_of` date) broadcast uniformly
  across the whole 2015-2025 weather record -- "what would today's fleet
  have faced under every past hour's weather", meant to be divided by its
  own total capacity into a capacity factor and then rescaled by any
  hypothetical installed capacity.

**Wind's onshore/offshore split** uses `mastr_units_wind`'s own
`wind_onshore_or_offshore` column directly (`"Windkraft an Land"` /
`"Windkraft auf See"`) -- simpler than deriving it from a region-code
detour, and confirmed as the real signal by inspecting actual values.
**Solar's region assignment** goes through `edh.region_geo`'s LAU->NUTS3
crosswalk (municipality_key -> NUTS3 -> NUTS2, truncating to 4 characters --
NUTS3 codes are a strict prefix of their parent NUTS2 code in Eurostat's
scheme). Solar mostly lacks coordinates, so it never uses the raster-mask
path wind does.

See `docs/pecd_data_availability.md` for research notes on what CDS
actually offers beyond what this module currently downloads -- country-
level (nuts_0) availability, wind zone-scheme granularity (peon/p2on/
peof/p2of), technology code meanings, historical range, and geographic
domain -- gathered while scoping a not-yet-built full-Europe/longer-
history pull.
"""

import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from edh.paths import (
    lau_nuts_correspondence_file,
    mastr_solar_technical_detail_file,
    mastr_units_file,
    pecd_capacity_factor_zip,
    pecd_capacity_factors_file,
    pecd_europe_decade_capacity_factors_file,
    pecd_europe_decade_zip,
    pecd_mask_file,
)

EXPORT_START_PERIOD = pd.Period("2015-01", freq="M")  # this hub's own chosen window (matches MaStR/SMARD availability) -- NOT a PECD limit, PECD's CF product goes back to at least 1980, confirmed real not just catalogue metadata; see docs/pecd_data_availability.md

# --- Raw PECD download (official product, taken as given -- not re-derived
# from weather data) -------------------------------------------------------

YEARS = [str(y) for y in range(2015, 2026)]
MONTHS = [f"{m:02d}" for m in range(1, 13)]

CF_REQUESTS = {
    "solar_tech60": ("solar", "60"),
    "solar_tech61": ("solar", "61"),
    "solar_tech62": ("solar", "62"),
    "solar_tech63": ("solar", "63"),
    "solar_country_tech60": ("solar_country", "60"),
    "solar_country_tech61": ("solar_country", "61"),
    "solar_country_tech62": ("solar_country", "62"),
    "solar_country_tech63": ("solar_country", "63"),
    "wind_onshore_tech30": ("wind_onshore", "30"),
    "wind_offshore_tech20": ("wind_offshore", "20"),
}
CF_VARIABLE_NAMES = {
    "solar": "solar_photovoltaic_generation_capacity_factor",
    "solar_country": "solar_photovoltaic_generation_capacity_factor",
    "wind_onshore": "wind_power_onshore_capacity_factor",
    "wind_offshore": "wind_power_offshore_capacity_factor",
}
# "solar_country" is PECD's true nuts_0 (country-level) product for the same
# variable/technologies as "solar" (NUTS2) -- confirmed available via CDS's
# own constraint solver (unlike wind, which PECD doesn't expose at nuts_0 at
# all -- confirmed against the live CDS API, see docs/pecd_data_availability.md).
CF_SPATIAL_RESOLUTION = {"solar": "nuts_2", "solar_country": "nuts_0", "wind_onshore": "peon", "wind_offshore": "peof"}
SOLAR_TECHNOLOGIES = ["60", "61", "62", "63"]


def retrieve_with_retries(client, dataset: str, request: dict, target: str, max_attempts: int = 4, backoff_seconds: float = 30.0) -> None:
    """`client.retrieve()`, retrying on any exception with linear backoff --
    cds.climate.copernicus.eu is intermittently flaky at the TLS layer, not
    something a request-payload change fixes. A failed attempt never leaves
    a partial `target` behind, so a retry is always a clean re-attempt."""
    import time

    for attempt in range(1, max_attempts + 1):
        try:
            client.retrieve(dataset, request, target)
            return
        except Exception as exc:
            if attempt == max_attempts:
                raise
            wait = backoff_seconds * attempt
            print(f"  Attempt {attempt}/{max_attempts} failed ({exc!r}), retrying in {wait:.0f}s...")
            time.sleep(wait)


def download_capacity_factor_zip(label: str) -> Path:
    """Fetch one (kind, technology) PECD capacity-factor zip -- all of
    Europe, 2015-2025, hourly. Requires `~/.cdsapirc`."""
    import cdsapi

    kind, technology = CF_REQUESTS[label]
    output_file = pecd_capacity_factor_zip(kind, technology)
    request = {
        "pecd_version": "pecd4_2",
        "temporal_period": "historical",
        "origin": "era5_reanalysis",
        "variable": CF_VARIABLE_NAMES[kind],
        "technology": technology,
        "spatial_resolution": CF_SPATIAL_RESOLUTION[kind],
        "year": YEARS,
        "month": MONTHS,
        "file_version": "fv1",
    }
    if kind not in ("solar", "solar_country"):
        request["energy_scenario"] = "resource_grade_b"

    client = cdsapi.Client()
    retrieve_with_retries(client, "sis-energy-pecd", request, str(output_file))
    return output_file


def load_region_timeseries_zip(zip_path: Path, region_prefix: str = "DE") -> pd.DataFrame:
    """Read a PECD region-aggregated-timeseries zip (one CSV per year
    inside) into a wide, hourly-indexed DataFrame. Each member CSV has a
    few metadata header rows before the real `Date,...` header, and one
    column per European region -- `region_prefix` filters down to one
    country's zones."""
    parts = []
    with zipfile.ZipFile(zip_path) as z:
        for csv_name in z.namelist():
            with z.open(csv_name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                header_idx = next(i for i, line in enumerate(text) if line.startswith("Date,"))
            with z.open(csv_name) as f:
                df = pd.read_csv(
                    f, skiprows=header_idx, parse_dates=["Date"], index_col="Date",
                    usecols=lambda c: c == "Date" or c.startswith(region_prefix),
                )
            parts.append(df)
    combined = pd.concat(parts).sort_index()
    combined.index.name = "timestamp"
    if combined.index.duplicated().any():
        combined = combined[~combined.index.duplicated(keep="first")]
    return combined


def process_solar_capacity_factors() -> pd.DataFrame:
    """Combine the 4 solar-technology zips into one Germany-only, hourly,
    MultiIndex (technology, region) frame. PECD's solar timestamps run 1h
    ahead of true UTC (confirmed empirically in pecd-replication against
    SMARD) -- corrected here."""
    parts = {tech: load_region_timeseries_zip(pecd_capacity_factor_zip("solar", tech)) for tech in SOLAR_TECHNOLOGIES}
    solar = pd.concat(parts, axis=1, names=["technology", "region"])
    solar.index = solar.index - pd.Timedelta(hours=1)
    return solar


def process_solar_country_capacity_factors() -> pd.DataFrame:
    """Combine the 4 solar-technology nuts_0 zips into one Germany-only,
    hourly, technology-columned frame -- PECD's own true country-level
    aggregation (no region dimension left: nuts_0 already *is* the whole
    country), as opposed to `process_solar_capacity_factors`'s NUTS2
    product. Same 1h timestamp correction as that function (same
    variable, same empirically-confirmed PECD solar timestamp offset)."""
    parts = {tech: load_region_timeseries_zip(pecd_capacity_factor_zip("solar_country", tech))["DE"] for tech in SOLAR_TECHNOLOGIES}
    solar = pd.DataFrame(parts)
    solar.columns.name = "technology"
    solar.index = solar.index - pd.Timedelta(hours=1)
    return solar


def process_wind_capacity_factors(kind: str, technology: str) -> pd.DataFrame:
    """Combine one wind (onshore or offshore) zip into a Germany-only,
    hourly, zone-columned frame."""
    return load_region_timeseries_zip(pecd_capacity_factor_zip(kind, technology))


# --- Full-Europe pull (all zone/country columns, no DE filter, 1980-2025)
# -- see docs/pecd_data_availability.md for the scoping research this
# executes: wind's `peon` onshore (technology 30, the real fleet) and
# `p2of` offshore (technology 20, the coarser of the two offshore zone
# schemes, chosen here since the goal is "as aggregated as possible"
# continent-wide); solar's true `nuts_0` country-level product, all 4
# technologies. Split into decade-sized CDS requests -- the only combined
# multi-year request size actually proven to work so far is the existing
# 11-year (2015-2025) DE pull; a single 46-year request is untested and,
# per `retrieve_with_retries`, a failure means redoing the *whole* request,
# so decades bound that risk to a tenth of the work instead of all of it.
# Each decade's raw zip is parsed straight to parquet and deleted -- see
# `pecd_europe_decade_zip`'s docstring for why the zip isn't worth keeping.

EUROPE_DECADES = [
    ("1980-1989", [str(y) for y in range(1980, 1990)]),
    ("1990-1999", [str(y) for y in range(1990, 2000)]),
    ("2000-2009", [str(y) for y in range(2000, 2010)]),
    ("2010-2019", [str(y) for y in range(2010, 2020)]),
    ("2020-2025", [str(y) for y in range(2020, 2026)]),
]
EUROPE_SPATIAL_RESOLUTION = {"wind_onshore": "peon", "wind_offshore": "p2of", "solar_country": "nuts_0"}


def download_and_convert_europe_decade(kind: str, technology: str, decade_label: str, years: list[str]) -> Path:
    """One full-Europe, one-decade PECD capacity-factor pull: download the
    raw CDS zip (every column CDS returns -- all of Europe, North Africa,
    and the Middle East, not just DE), parse it straight into that decade's
    parquet, then delete the raw zip. No-op (returns the existing file) if
    that decade's parquet is already on disk -- this is the
    incremental-download checkpoint, so a re-run only fetches decades still
    missing. `kind` is "wind_onshore", "wind_offshore", or "solar_country"
    (technology one of that kind's PECD technology codes)."""
    import cdsapi

    output_file = pecd_europe_decade_capacity_factors_file(kind, technology, decade_label)
    if output_file.exists():
        return output_file

    zip_path = pecd_europe_decade_zip(kind, technology, decade_label)
    request = {
        "pecd_version": "pecd4_2",
        "temporal_period": "historical",
        "origin": "era5_reanalysis",
        "variable": CF_VARIABLE_NAMES[kind],
        "technology": technology,
        "spatial_resolution": EUROPE_SPATIAL_RESOLUTION[kind],
        "year": years,
        "month": MONTHS,
        "file_version": "fv1",
    }
    if kind not in ("solar", "solar_country"):
        request["energy_scenario"] = "resource_grade_b"

    client = cdsapi.Client()
    retrieve_with_retries(client, "sis-energy-pecd", request, str(zip_path))

    df = load_region_timeseries_zip(zip_path, region_prefix="")  # empty prefix -- keep every column, not just DE
    df.to_parquet(output_file)
    zip_path.unlink()
    return output_file


def load_europe_capacity_factors(kind: str, technology: str) -> pd.DataFrame:
    """Concatenate every full-Europe decade parquet currently on disk for
    one (kind, technology) into one continuous hourly, zone/country-columned
    frame. Decades are independent and immutable once written, so this is
    just a stack along the time axis -- decades not yet downloaded are
    silently skipped rather than erroring, so this always reflects
    whatever's been fetched so far."""
    parts = [
        pd.read_parquet(pecd_europe_decade_capacity_factors_file(kind, technology, label))
        for label, _ in EUROPE_DECADES
        if pecd_europe_decade_capacity_factors_file(kind, technology, label).exists()
    ]
    return pd.concat(parts).sort_index() if parts else pd.DataFrame()


def load_europe_solar_capacity_factors() -> pd.DataFrame:
    """`load_europe_capacity_factors("solar_country", tech)` for each of
    solar's 4 PECD technologies, combined into one MultiIndex
    (technology, country) column frame -- same column shape as
    `process_solar_capacity_factors`'s NUTS2 product, just at true
    country-level (`nuts_0`) resolution and full-Europe instead of DE-only."""
    parts = {tech: load_europe_capacity_factors("solar_country", tech) for tech in SOLAR_TECHNOLOGIES}
    non_empty = {tech: df for tech, df in parts.items() if not df.empty}
    if not non_empty:
        return pd.DataFrame()
    return pd.concat(non_empty, axis=1, names=["technology", "region"])


# --- Simplified, MaStR-free country-level capacity factor (2026-09-24) ----
# Generalizes `energy-insights`' `06_pecd_simple_vs_mastr_weighted.py`
# DE-only prototype to every PECD country now that the full-Europe pulls
# above exist. Solar: blended across its 4 technologies with per-country
# weights from `SOLAR_COUNTRY_WEIGHT_OVERRIDES`, falling back to
# `DEFAULT_SOLAR_COUNTRY_WEIGHTS` (DE-market-derived) for every country
# without its own entry -- i.e. every country today, since no real
# per-country technology-mix data has been sourced yet (2026-09-24: found
# that SolarPower Europe's country-level rooftop/utility segment tables
# are member-only, and no source at all gives PECD's finer fixed-tilt vs.
# tracking utility split by country). The override dict exists so real
# figures can be added country-by-country later without touching the
# blending logic -- see `solar_country_weights`. Wind onshore:
# area-weighted mean per country using PECD's own `peon` zone mask, which
# already covers the full domain (no extra download needed). Wind
# offshore: **unweighted** mean per country, not area-weighted -- the
# full-Europe pull uses the coarser `p2of` zone scheme (see
# docs/pecd_data_availability.md) whose zone codes don't match the
# existing `peof` mask's zones, and the DE notebook itself found
# unweighted vs. area-weighted barely differs (~0.975 vs ~0.977 hourly
# correlation against the MaStR-weighted truth), so the accuracy given up
# is minor next to needing a whole new CDS mask download.

DEFAULT_SOLAR_COUNTRY_WEIGHTS = {"62": 0.31, "63": 0.02, "61": 0.39, "60": 0.28}
# DE-market weights, ported verbatim from `06_pecd_simple_vs_mastr_weighted.py`
# (~2025 BSW-Solar/BNetzA/pv-magazine figures) -- see that notebook for the
# per-technology sourcing. Used as every country's weights until that
# country gets its own entry in `SOLAR_COUNTRY_WEIGHT_OVERRIDES` below.

SOLAR_COUNTRY_WEIGHT_OVERRIDES: dict[str, dict[str, float]] = {
    # "ES": {"60": ..., "61": ..., "62": ..., "63": ...},  -- add here once a
    # country's real rooftop/utility (and, ideally, fixed-tilt/tracking)
    # technology mix is sourced. Empty for now -- see module docstring.
}


def solar_country_weights(country: str) -> dict[str, float]:
    """Technology-mix weights for one country: `SOLAR_COUNTRY_WEIGHT_OVERRIDES[country]`
    if one has been sourced, else `DEFAULT_SOLAR_COUNTRY_WEIGHTS` (DE's, unvalidated
    for every other country -- see module docstring)."""
    return SOLAR_COUNTRY_WEIGHT_OVERRIDES.get(country, DEFAULT_SOLAR_COUNTRY_WEIGHTS)


def country_solar_capacity_factor_simple(solar_wide: pd.DataFrame) -> pd.DataFrame:
    """Blend solar's 4 PECD technologies into one series per country, with
    `solar_country_weights(country)`'s weights. `solar_wide` is
    `load_europe_solar_capacity_factors()`'s MultiIndex (technology,
    region) frame; output has one plain column per country.

    A country missing one of the 4 technologies entirely (all-NaN for that
    (technology, country) pair -- PECD doesn't model every technology
    everywhere) has its remaining technologies' weights renormalized to
    sum to 1, rather than propagating NaN into the whole blended series --
    same treatment as the wind country blends' NaN-zone handling below."""
    countries = solar_wide.columns.get_level_values("region").unique()
    blended = {}
    for country in countries:
        modeled = {
            tech: weight
            for tech, weight in solar_country_weights(country).items()
            if (tech, country) in solar_wide.columns and solar_wide[(tech, country)].notna().any()
        }
        if not modeled:
            continue
        total_weight = sum(modeled.values())
        blended[country] = sum(solar_wide[(tech, country)] * (weight / total_weight) for tech, weight in modeled.items())
    return pd.DataFrame(blended)


def country_zone_area_weights(mask_file: Path, cf_columns: pd.Index) -> pd.Series:
    """Area weight for every zone in `cf_columns` that PECD's mask actually
    knows about, normalized to sum to 1 *within each zone's own country*
    (its 2-letter code prefix) -- the multi-country generalization of
    `06_pecd_simple_vs_mastr_weighted.py`'s DE-only `zone_area_weights`."""
    import xarray as xr

    ds = xr.open_dataset(mask_file)
    zones = [z for z in cf_columns if z in ds["region"].values]
    mask_values = ds["mask"].sel(region=zones).values  # (zone, lat, lon)
    lat_weight = np.cos(np.radians(ds["latitude"].values))[None, :, None]
    ds.close()

    area = pd.Series((mask_values * lat_weight).sum(axis=(1, 2)), index=zones)
    country = pd.Series(zones, index=zones).str[:2]
    return area / area.groupby(country).transform("sum")


def country_area_weighted_wind_cf(cf: pd.DataFrame, mask_file: Path) -> pd.DataFrame:
    """Area-weighted mean capacity factor per country, from a zone-columned
    wind CF frame (`load_europe_capacity_factors`'s output) and PECD's own
    zone mask. Zones the mask doesn't cover, or PECD never modeled (100%
    NaN throughout), are dropped and each country's remaining zone weights
    renormalized -- same NaN handling as the DE notebook, just grouped by
    country instead of assumed-DE."""
    modeled = cf.columns[cf.notna().any()]
    weights = country_zone_area_weights(mask_file, modeled)
    weighted = cf[weights.index] * weights
    country = weights.index.str[:2]
    return weighted.T.groupby(country).sum().T


def country_unweighted_wind_cf(cf: pd.DataFrame) -> pd.DataFrame:
    """Plain (unweighted) mean capacity factor per country, from a
    zone-columned wind CF frame -- used for offshore since the full-Europe
    pull's `p2of` zone scheme has no matching area mask (see module
    comment above). `.mean` already skips a zone that's 100% NaN
    throughout rather than treating it as zero."""
    country = cf.columns.str[:2]
    return cf.T.groupby(country).mean().T


# --- MaStR -> PECD technology/region classification -----------------------

GROUND_MOUNTED_INSTALLATION_TYPE = "Freiflächensolaranlage"
HOUSEHOLD_SECTOR = "Haushalt"
TECH_INDUSTRIAL_ROOFTOP, TECH_RESIDENTIAL_ROOFTOP = "60", "61"
TECH_UTILITY_FIXED, TECH_UTILITY_TRACKING = "62", "63"

ONSHORE_LABEL = "Windkraft an Land"
OFFSHORE_LABEL = "Windkraft auf See"


def classify_pecd_technology(installation_type: pd.Series, usage_sector: pd.Series, is_tracked: pd.Series) -> np.ndarray:
    """Vectorized classification into "60"/"61"/"62"/"63": ground-mounted
    (`installation_type`) -> utility segment, split by tracking; everything
    else (rooftop/balcony) -> rooftop segment, split by `usage_sector`
    ("Haushalt" -> residential; any other named sector -> industrial; a
    missing sector defaults to residential -- dominated by balcony units,
    almost always small household installations even when unreported)."""
    is_missing_sector = usage_sector.isna().to_numpy()
    is_ground_mounted = installation_type.eq(GROUND_MOUNTED_INSTALLATION_TYPE).fillna(False).to_numpy()
    is_household = usage_sector.eq(HOUSEHOLD_SECTOR).fillna(False).to_numpy()

    rooftop_tech = np.where(is_household | is_missing_sector, TECH_RESIDENTIAL_ROOFTOP, TECH_INDUSTRIAL_ROOFTOP)
    ground_tech = np.where(is_tracked.to_numpy(), TECH_UTILITY_TRACKING, TECH_UTILITY_FIXED)
    return np.where(is_ground_mounted, ground_tech, rooftop_tech)


def prep_solar_units(units: pd.DataFrame, technical_detail: pd.DataFrame, lau_nuts: pd.DataFrame) -> pd.DataFrame:
    """Filter + enrich `mastr_units_solar` into one row per plant with
    `nuts2_region`, `pecd_technology`, `capacity_mw`, and the
    commissioning/shutdown dates needed downstream. Drops units with no
    commissioning date, non-positive capacity, or an unmatched municipality
    key (~1% of rows in practice, per the equivalent MaStR-side filter)."""
    df = units.merge(technical_detail, on="unit_id", how="left")
    df["capacity_mw"] = df["net_capacity_kw"].astype(float) / 1000.0
    df["commissioning_date"] = pd.to_datetime(df["commissioning_date"], errors="coerce")
    df["final_shutdown_date"] = pd.to_datetime(df["final_shutdown_date"], errors="coerce")
    df = df[df["commissioning_date"].notna() & (df["capacity_mw"] > 0)].copy()

    df["municipality_key"] = df["municipality_key"].astype(str).str.extract(r"(\d+)")[0].str.zfill(8)
    df = df.merge(lau_nuts, on="municipality_key", how="left")
    df = df[df["nuts3_code"].notna()].copy()
    df["nuts2_region"] = df["nuts3_code"].str[:4]

    is_tracked = df["main_orientation"].eq("nachgeführt").fillna(False) | df["main_orientation_tilt_bucket"].eq("Nachgeführt").fillna(False)
    df["pecd_technology"] = classify_pecd_technology(df["installation_type"], df["usage_sector"], is_tracked)

    return df[["unit_id", "capacity_mw", "nuts2_region", "pecd_technology", "commissioning_date", "final_shutdown_date"]]


def prep_wind_units(units: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter `mastr_units_wind` into (onshore, offshore) plant tables,
    each with `capacity_mw` and the columns `fractional_zone_weights`
    needs. Drops units with no coordinates, no commissioning date, or
    non-positive capacity."""
    df = units.copy()
    df["capacity_mw"] = df["net_capacity_kw"].astype(float) / 1000.0
    df["commissioning_date"] = pd.to_datetime(df["commissioning_date"], errors="coerce")
    df["final_shutdown_date"] = pd.to_datetime(df["final_shutdown_date"], errors="coerce")
    has_coords = df["longitude"].notna() & df["latitude"].notna()
    df = df[has_coords & df["commissioning_date"].notna() & (df["capacity_mw"] > 0)].copy()

    cols = ["unit_id", "capacity_mw", "longitude", "latitude", "commissioning_date", "final_shutdown_date"]
    onshore = df.loc[df["wind_onshore_or_offshore"] == ONSHORE_LABEL, cols].copy()
    offshore = df.loc[df["wind_onshore_or_offshore"] == OFFSHORE_LABEL, cols].copy()
    return onshore, offshore


# --- Capacity aggregation: monthly (time-varying) + as-of-date snapshot ---

def monthly_snapshot_panel(
    df: pd.DataFrame,
    group_cols: list[str],
    month_range: pd.PeriodIndex,
    export_start_period: pd.Period,
    capacity_col: str = "capacity_mw",
) -> pd.DataFrame:
    """Installed capacity per group, for every month in `month_range`.
    Vectorized cumulative-delta approach (ported from
    `mastr-power-capacities-germany/mpg/panels.py`): +capacity at a unit's
    commissioning month, -capacity at its shutdown month, cumulative sum
    along the month axis per group. `capacity_col` should already be
    pre-weighted for a fractionally-split input (see
    `expand_wind_zone_weights`)."""
    start_period = df["commissioning_date"].dt.to_period("M")
    end_period = df["final_shutdown_date"].dt.to_period("M")
    has_end = end_period.notna()

    adds = df[group_cols].copy()
    adds["period"] = start_period
    adds["capacity_delta"] = df[capacity_col]

    removes = df.loc[has_end, group_cols].copy()
    removes["period"] = end_period[has_end]
    removes["capacity_delta"] = -df.loc[has_end, capacity_col]

    net = pd.concat([adds, removes], ignore_index=True).groupby(group_cols + ["period"])["capacity_delta"].sum()
    capacity = net.unstack("period").reindex(columns=month_range, fill_value=0.0).fillna(0.0).cumsum(axis=1)

    panel = capacity.stack().rename("capacity_mw").reset_index()
    panel = panel.rename(columns={"period": "month"})
    panel = panel[panel["month"] >= export_start_period].copy()
    panel["month"] = panel["month"].dt.to_timestamp() + pd.offsets.MonthEnd(0)
    return panel.reset_index(drop=True)


def active_units_at(df: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    """Rows active as of `as_of`: commissioned by then, not yet shut down."""
    is_active = (df["commissioning_date"] <= as_of) & (df["final_shutdown_date"].isna() | (df["final_shutdown_date"] > as_of))
    return df.loc[is_active].copy()


def capacity_snapshot(df: pd.DataFrame, group_cols: list[str], as_of: pd.Timestamp, capacity_col: str = "capacity_mw") -> pd.Series:
    """Installed capacity per group, as of a single fixed date."""
    return active_units_at(df, as_of).groupby(group_cols)[capacity_col].sum()


def expand_wind_zone_weights(units: pd.DataFrame, mask_file: Path) -> pd.DataFrame:
    """One row per (unit_id, zone_id), each unit's capacity pre-split
    across its fractional PEON/PEOF zone weights, ready for
    `monthly_snapshot_panel` or `capacity_snapshot`."""
    from edh.region_geo import fractional_zone_weights

    weights = fractional_zone_weights(units, mask_file)
    expanded = weights.merge(units[["unit_id", "capacity_mw", "commissioning_date", "final_shutdown_date"]], on="unit_id", how="left")
    expanded["capacity_mw"] = expanded["capacity_mw"] * expanded["weight"]
    return expanded


# --- CF x capacity weighting ----------------------------------------------

def broadcast_capacity_to_hourly(capacity_wide: pd.DataFrame, hourly_index: pd.DatetimeIndex) -> pd.DataFrame:
    """Repeat each month's capacity row across every hour in that calendar
    month -- capacity only changes monthly, the capacity factor moves hourly."""
    monthly = capacity_wide.copy()
    monthly.index = monthly.index.to_period("M")
    monthly = monthly[~monthly.index.duplicated(keep="last")].sort_index()
    hour_periods = hourly_index.to_period("M")
    broadcast = monthly.reindex(hour_periods)
    broadcast.index = hourly_index
    return broadcast


def compute_potential_monthly(capacity_factors: pd.DataFrame, capacity_wide: pd.DataFrame) -> pd.Series:
    """National hourly potential (MW): `sum_over_columns(CF x capacity)`,
    capacity varying month to month. A region/technology PECD never
    modeled (100% NaN CF) contributes zero rather than poisoning the row."""
    capacity_wide = capacity_wide.reindex(columns=capacity_factors.columns, fill_value=0.0)
    capacity_hourly = broadcast_capacity_to_hourly(capacity_wide, capacity_factors.index)
    product = capacity_factors.to_numpy() * capacity_hourly.to_numpy()
    return pd.Series(np.nansum(product, axis=1), index=capacity_factors.index, name="potential_mw")


def compute_potential_fixed(capacity_factors: pd.DataFrame, capacity_now: pd.Series) -> pd.Series:
    """Same as `compute_potential_monthly`, but with one fixed capacity
    vector broadcast uniformly across every hour (the current-fleet
    variant -- see module docstring)."""
    capacity_now = capacity_now.reindex(capacity_factors.columns, fill_value=0.0)
    product = capacity_factors.to_numpy() * capacity_now.to_numpy()[None, :]
    return pd.Series(np.nansum(product, axis=1), index=capacity_factors.index, name="potential_mw")


def unmodeled_capacity_share(capacity_factors: pd.DataFrame, capacity_now: pd.Series) -> pd.Series:
    """Mean/current installed capacity (MW) sitting in columns PECD never
    modeled (100% NaN CF throughout) -- diagnostic for the NaN-as-zero
    handling above; empty if none."""
    capacity_now = capacity_now.reindex(capacity_factors.columns, fill_value=0.0)
    unmodeled_columns = capacity_factors.columns[capacity_factors.isna().all()]
    return capacity_now[unmodeled_columns] if len(unmodeled_columns) else pd.Series(dtype=float)


# --- Orchestration: load + prep everything both derived assets need ------

def load_prepared_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load and prep all MaStR/PECD inputs shared by both the historic and
    current-fleet potential/CF assets. Returns (onshore_expanded,
    offshore_expanded, solar_prepped, cf_solar, cf_wind_onshore,
    cf_wind_offshore)."""
    wind = pd.read_parquet(mastr_units_file("wind"))
    solar = pd.read_parquet(mastr_units_file("solar"))
    detail = pd.read_parquet(mastr_solar_technical_detail_file())
    lau_nuts = pd.read_parquet(lau_nuts_correspondence_file())

    onshore, offshore = prep_wind_units(wind)
    solar_prepped = prep_solar_units(solar, detail, lau_nuts)

    onshore_expanded = expand_wind_zone_weights(onshore, pecd_mask_file("peon"))
    offshore_expanded = expand_wind_zone_weights(offshore, pecd_mask_file("peof"))

    cf_solar = pd.read_parquet(pecd_capacity_factors_file("solar"))
    cf_wind_onshore = pd.read_parquet(pecd_capacity_factors_file("wind_onshore"))
    cf_wind_offshore = pd.read_parquet(pecd_capacity_factors_file("wind_offshore"))

    return onshore_expanded, offshore_expanded, solar_prepped, cf_solar, cf_wind_onshore, cf_wind_offshore


def full_month_range(*prepped_frames: pd.DataFrame) -> pd.PeriodIndex:
    """Month range from the earliest commissioning date across all given
    frames through the current month -- `monthly_snapshot_panel`'s
    cumulative sum needs the full history to be correct from
    `EXPORT_START_PERIOD` onward, not just 2015 on."""
    start = min(df["commissioning_date"].min() for df in prepped_frames).to_period("M")
    end = pd.Timestamp.today().to_period("M")
    return pd.period_range(start=start, end=end, freq="M", name="period")


def wind_zone_wide(monthly_panel: pd.DataFrame) -> pd.DataFrame:
    """Long (zone_id, month, capacity_mw) panel -> wide (month index, zone
    columns), matching a wind CF frame's column space."""
    return monthly_panel.pivot(index="month", columns="zone_id", values="capacity_mw")


def solar_nuts2_wide(monthly_panel: pd.DataFrame, column_names: list[str]) -> pd.DataFrame:
    """Long (pecd_technology, nuts2_region, month, capacity_mw) panel ->
    wide (month index, MultiIndex (technology, region) columns), matching
    `cf_solar`'s column space exactly (`column_names` should be
    `cf_solar.columns.names`)."""
    wide = monthly_panel.pivot_table(index="month", columns=["pecd_technology", "nuts2_region"], values="capacity_mw", fill_value=0.0)
    wide.columns = wide.columns.set_names(column_names)
    return wide
