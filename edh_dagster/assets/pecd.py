"""PECD official capacity factors + DE aggregate potential/capacity-factor
series for solar PV, wind onshore, and wind offshore.

Migrated 2026-09-23 (design conversation) from three sibling repos
(`pecd-power-validity-DE`, `mastr-power-capacities-germany`,
`pecd-replication`) -- see `edh/pecd.py` and `edh/region_geo.py` module
docstrings for exactly which piece of logic came from where. Everything
here runs off the hub's own `mastr_units_wind`/`mastr_units_solar`/
`mastr_solar_technical_detail`, not a sibling repo's already-built output.

Two groups:
- **`pecd`** -- the raw/reference layer, taken as given: `lau_nuts_correspondence`,
  `peon_region_mask`, `peof_region_mask` (generic Eurostat/PECD reference
  data, not MaStR- or capacity-factor-specific), and
  `pecd_solar_capacity_factors` / `pecd_wind_onshore_capacity_factors` /
  `pecd_wind_offshore_capacity_factors` (PECD's official capacity-factor
  product, not re-derived from weather data). Also
  `pecd_wind_onshore_europe_capacity_factors` /
  `pecd_wind_offshore_europe_capacity_factors` /
  `pecd_solar_europe_capacity_factors` (2026-09-24) -- the full-domain (not
  DE-filtered), 1980-2025 counterparts, downloaded/cached per decade
  (per-technology for solar's 4 sub-types) rather than DE's single
  2015-2025 request; see `docs/pecd_data_availability.md` and
  `edh/pecd.py`'s `download_and_convert_europe_decade` /
  `load_europe_capacity_factors` / `load_europe_solar_capacity_factors`.
- **`de_potential`** -- this hub's own derived logic on top of the above
  plus `mastr_units_wind`/`mastr_units_solar`/`mastr_solar_technical_detail`,
  not something PECD or MaStR publish directly:
  - `de_potential_historic` -- Germany-wide hourly potential MW, weighted
    by each month's *actual* installed capacity, comparable against true
    historic generation (SMARD etc.).
  - `de_capacity_factor_current_fleet` -- Germany-wide hourly capacity
    factor (0-1), today's fleet held fixed across the whole 2015-2025
    weather record -- multiply by any hypothetical installed capacity to
    explore "would today's fleet have avoided past Dunkelflauten".

**No schedule, `full_refresh`:** the raw CDS downloads (reference data +
capacity factors) are effectively static once fetched -- PECD's historical
product doesn't change hour to hour. Materialize by hand; `de_potential`
rebuilds quickly against whatever's already downloaded/materialized in `pecd`.
"""

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from edh.paths import (
    de_capacity_factor_current_fleet_file,
    de_potential_historic_file,
    lau_nuts_correspondence_file,
    pecd_capacity_factors_file,
    pecd_europe_decade_capacity_factors_file,
    pecd_solar_country_capacity_factors_file,
)
from edh.pecd import (
    CF_REQUESTS,
    EUROPE_DECADES,
    SOLAR_TECHNOLOGIES,
    capacity_snapshot,
    compute_potential_fixed,
    compute_potential_monthly,
    download_and_convert_europe_decade,
    download_capacity_factor_zip,
    load_europe_capacity_factors,
    load_europe_solar_capacity_factors,
    process_solar_capacity_factors,
    process_solar_country_capacity_factors,
    process_wind_capacity_factors,
    EXPORT_START_PERIOD,
    full_month_range,
    load_prepared_inputs,
    monthly_snapshot_panel,
    solar_nuts2_wide,
    unmodeled_capacity_share,
    wind_zone_wide,
)
from edh.region_geo import download_lau_nuts_correspondence, download_pecd_mask

_PECD_TAGS = {"load_pattern": "full_refresh"}
CDS_SOURCE_URL = "https://cds.climate.copernicus.eu/datasets/sis-energy-pecd"


@asset(
    group_name="pecd",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description="Eurostat LAU (municipality) -> NUTS3 crosswalk, Germany only -- used to assign solar units to NUTS2 zones.",
    metadata={
        "source": "Eurostat/GISCO",
        "source_url": MetadataValue.url("https://ec.europa.eu/eurostat/web/nuts"),
        "region": "DE",
        "resolution": "n/a (static reference table)",
        "unit": "n/a",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, static reference data",
    },
)
def lau_nuts_correspondence(context: AssetExecutionContext) -> None:
    output_file = lau_nuts_correspondence_file()
    if output_file.exists():
        row_count = len(pd.read_parquet(output_file, columns=["municipality_key"]))
    else:
        row_count, output_file = download_lau_nuts_correspondence()
    context.add_output_metadata({"dagster/row_count": row_count, "path": MetadataValue.path(str(output_file))})


def _make_mask_asset(scheme: str, human_name: str):
    @asset(
        name=f"{scheme}_region_mask",
        group_name="pecd",
        kinds={"netcdf"},
        tags=_PECD_TAGS,
        description=f"PECD v4.2 {human_name} wind-zone rasterized region mask (0.25-degree fractional coverage per zone).",
        metadata={
            "source": "PECD v4.2 (C3S/ENTSO-E), via CDS",
            "source_url": MetadataValue.url(CDS_SOURCE_URL),
            "region": "Europe (full domain, not DE-cropped -- needed so a unit near the domain edge always has a matching cell)",
            "resolution": "0.25 degree",
            "unit": "fractional area coverage (0-1)",
            "timestamp_timezone": "n/a",
            "update_pattern": "full_refresh, static reference data",
        },
    )
    def _mask_asset(context: AssetExecutionContext) -> None:
        from edh.paths import pecd_mask_file

        output_file = pecd_mask_file(scheme)
        if not output_file.exists():
            output_file = download_pecd_mask(scheme)
        context.add_output_metadata({"path": MetadataValue.path(str(output_file))})

    return _mask_asset


peon_region_mask = _make_mask_asset("peon", "PEON (onshore)")
peof_region_mask = _make_mask_asset("peof", "PEOF (offshore)")


def _cf_metadata(kind: str, resolution: str) -> dict:
    return {
        "source": "PECD v4.2 (C3S/ENTSO-E), existing-fleet technology, resource_grade_b",
        "source_url": MetadataValue.url(CDS_SOURCE_URL),
        "region": "DE",
        "resolution": f"hourly, 2015-2025, by {resolution}",
        "unit": "capacity factor (0-1, dimensionless)",
        "timestamp_timezone": (
            "naive, represents UTC minus 1h correction for solar (PECD's solar timestamps run 1h ahead of "
            "true UTC, confirmed empirically against SMARD)" if kind == "solar" else "naive, represents UTC"
        ),
        "update_pattern": "full_refresh, static historical reference data",
    }


@asset(
    group_name="pecd",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description="PECD official solar PV capacity factor, Germany-only, hourly, 2015-2025, MultiIndex (technology, NUTS2 region) columns.",
    metadata=_cf_metadata("solar", "PECD technology (4 sub-types) x NUTS2 region"),
)
def pecd_solar_capacity_factors(context: AssetExecutionContext) -> None:
    output_file = pecd_capacity_factors_file("solar")
    if output_file.exists():
        df = pd.read_parquet(output_file)
    else:
        from edh.paths import pecd_capacity_factor_zip

        for label, (kind, technology) in CF_REQUESTS.items():
            if kind != "solar":
                continue
            if not pecd_capacity_factor_zip(kind, technology).exists():
                download_capacity_factor_zip(label)

        df = process_solar_capacity_factors()
        df.to_parquet(output_file)
    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(df.tail(3).to_markdown()) if len(df) else MetadataValue.md("*empty*"),
        }
    )


@asset(
    group_name="pecd",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description=(
        "PECD's own nuts_0 (country-level) solar PV capacity factor, Germany-only, hourly, 2015-2025, one column "
        "per PECD technology (60/61/62/63) -- PECD's true country-level product, downloaded directly, not a "
        "hub-side NUTS2 aggregation like pecd_solar_capacity_factors. (PECD doesn't offer nuts_0 for wind at all -- "
        "confirmed against the live CDS API, see docs/pecd_data_availability.md.)"
    ),
    metadata=_cf_metadata("solar", "PECD technology (4 sub-types), country-level (nuts_0)"),
)
def pecd_solar_country_capacity_factors(context: AssetExecutionContext) -> None:
    output_file = pecd_solar_country_capacity_factors_file()
    if output_file.exists():
        df = pd.read_parquet(output_file)
    else:
        from concurrent.futures import ThreadPoolExecutor

        from edh.paths import pecd_capacity_factor_zip

        missing_labels = [
            f"solar_country_tech{tech}" for tech in SOLAR_TECHNOLOGIES
            if not pecd_capacity_factor_zip("solar_country", tech).exists()
        ]
        if missing_labels:
            with ThreadPoolExecutor(max_workers=len(missing_labels)) as pool:
                list(pool.map(download_capacity_factor_zip, missing_labels))

        df = process_solar_country_capacity_factors()
        df.to_parquet(output_file)
    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(df.tail(3).to_markdown()) if len(df) else MetadataValue.md("*empty*"),
        }
    )


def _make_wind_cf_asset(kind: str, label: str, technology: str, human_name: str):
    @asset(
        name=f"pecd_{kind}_capacity_factors",
        group_name="pecd",
        kinds={"parquet"},
        tags=_PECD_TAGS,
        description=f"PECD official {human_name} capacity factor, Germany-only, hourly, 2015-2025, one column per zone.",
        metadata=_cf_metadata(kind, "zone" if kind == "wind_onshore" else "zone (3 of 6 PEOF zones are 100% NaN -- see unmodeled_capacity_share)"),
    )
    def _wind_cf_asset(context: AssetExecutionContext) -> None:
        output_file = pecd_capacity_factors_file(kind)
        if output_file.exists():
            df = pd.read_parquet(output_file)
        else:
            from edh.paths import pecd_capacity_factor_zip

            if not pecd_capacity_factor_zip(kind, technology).exists():
                download_capacity_factor_zip(label)

            df = process_wind_capacity_factors(kind, technology)
            df.to_parquet(output_file)
        context.add_output_metadata(
            {
                "dagster/row_count": len(df),
                "path": MetadataValue.path(str(output_file)),
                "preview": MetadataValue.md(df.tail(3).to_markdown()) if len(df) else MetadataValue.md("*empty*"),
            }
        )

    return _wind_cf_asset


pecd_wind_onshore_capacity_factors = _make_wind_cf_asset("wind_onshore", "wind_onshore_tech30", "30", "wind-onshore (PEON)")
pecd_wind_offshore_capacity_factors = _make_wind_cf_asset("wind_offshore", "wind_offshore_tech20", "20", "wind-offshore (PEOF)")


_EUROPE_WIND_TECHNOLOGY = {"wind_onshore": "30", "wind_offshore": "20"}
_EUROPE_WIND_ZONE_SCHEME = {"wind_onshore": "PEON", "wind_offshore": "P2OF"}


def _make_europe_wind_cf_asset(kind: str, human_name: str):
    technology = _EUROPE_WIND_TECHNOLOGY[kind]
    zone_scheme = _EUROPE_WIND_ZONE_SCHEME[kind]

    @asset(
        name=f"pecd_{kind}_europe_capacity_factors",
        group_name="pecd",
        kinds={"parquet"},
        tags=_PECD_TAGS,
        description=(
            f"PECD official {human_name} capacity factor, full domain (all of Europe + North Africa + Middle "
            f"East, not DE-filtered), hourly, 1980-2025, one column per {zone_scheme} zone. Downloaded/cached "
            "per decade -- each decade's raw CDS zip is parsed to parquet and deleted immediately (see "
            "docs/pecd_data_availability.md); this asset itself only ensures every decade is on disk, the "
            "combined series is assembled on demand by edh.pecd.load_europe_capacity_factors rather than "
            "materialized as one file."
        ),
        metadata={
            "source": f"PECD v4.2 (C3S/ENTSO-E), existing-fleet technology {technology}, resource_grade_b, {zone_scheme} zones",
            "source_url": MetadataValue.url(CDS_SOURCE_URL),
            "region": "Europe + North Africa + Middle East (full CDS domain)",
            "resolution": f"hourly, 1980-2025, by {zone_scheme} zone",
            "unit": "capacity factor (0-1, dimensionless)",
            "timestamp_timezone": "naive, represents UTC",
            "update_pattern": "full_refresh per missing decade, static historical reference data",
        },
    )
    def _europe_wind_cf_asset(context: AssetExecutionContext) -> None:
        missing = [(label, years) for label, years in EUROPE_DECADES if not pecd_europe_decade_capacity_factors_file(kind, technology, label).exists()]
        if missing:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=len(missing)) as pool:
                list(pool.map(lambda item: download_and_convert_europe_decade(kind, technology, item[0], item[1]), missing))

        df = load_europe_capacity_factors(kind, technology)
        decades_on_disk = [label for label, _ in EUROPE_DECADES if pecd_europe_decade_capacity_factors_file(kind, technology, label).exists()]
        context.add_output_metadata(
            {
                "dagster/row_count": len(df),
                "decades_on_disk": MetadataValue.text(", ".join(decades_on_disk) if decades_on_disk else "none"),
                "zone_count": len(df.columns),
                "preview": MetadataValue.md(df.tail(3).to_markdown()) if len(df) else MetadataValue.md("*empty*"),
            }
        )

    return _europe_wind_cf_asset


pecd_wind_onshore_europe_capacity_factors = _make_europe_wind_cf_asset("wind_onshore", "wind-onshore (PEON)")
pecd_wind_offshore_europe_capacity_factors = _make_europe_wind_cf_asset("wind_offshore", "wind-offshore (P2OF)")


@asset(
    name="pecd_solar_europe_capacity_factors",
    group_name="pecd",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description=(
        "PECD's own nuts_0 (country-level) solar PV capacity factor, full domain (all of Europe + North Africa "
        "+ Middle East, not DE-filtered), hourly, 1980-2025, MultiIndex (technology, country) columns -- the "
        "full-Europe/longer-history counterpart to pecd_solar_country_capacity_factors. Downloaded/cached per "
        "(technology, decade) -- 4 solar technologies x 5 decades = 20 downloads, each parsed to parquet and its "
        "raw CDS zip deleted immediately (see docs/pecd_data_availability.md); this asset itself only ensures "
        "every (technology, decade) is on disk, the combined series is assembled on demand by "
        "edh.pecd.load_europe_solar_capacity_factors rather than materialized as one file."
    ),
    metadata={
        "source": "PECD v4.2 (C3S/ENTSO-E), all 4 technologies (60/61/62/63), true country-level (nuts_0)",
        "source_url": MetadataValue.url(CDS_SOURCE_URL),
        "region": "Europe + North Africa + Middle East (full CDS domain)",
        "resolution": "hourly, 1980-2025, by PECD technology (4 sub-types) x country",
        "unit": "capacity factor (0-1, dimensionless)",
        "timestamp_timezone": (
            "naive, represents UTC minus 1h correction for solar (PECD's solar timestamps run 1h ahead of true "
            "UTC, confirmed empirically against SMARD). The raw per-decade parquet cache on disk keeps PECD's "
            "original, uncorrected timestamps; edh.pecd.load_europe_solar_capacity_factors applies the shift on "
            "every load, same as process_solar_country_capacity_factors -- any other code reading the raw decade "
            "files directly must apply it too (fixed 2026-09-24, see that function's docstring for how the "
            "miss was caught)."
        ),
        "update_pattern": "full_refresh per missing (technology, decade), static historical reference data",
    },
)
def pecd_solar_europe_capacity_factors(context: AssetExecutionContext) -> None:
    combos = [(tech, label, years) for tech in SOLAR_TECHNOLOGIES for label, years in EUROPE_DECADES]
    missing = [(tech, label, years) for tech, label, years in combos if not pecd_europe_decade_capacity_factors_file("solar_country", tech, label).exists()]
    if missing:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=len(missing)) as pool:
            list(pool.map(lambda item: download_and_convert_europe_decade("solar_country", item[0], item[1], item[2]), missing))

    df = load_europe_solar_capacity_factors()
    decades_on_disk = {
        tech: [label for label, _ in EUROPE_DECADES if pecd_europe_decade_capacity_factors_file("solar_country", tech, label).exists()]
        for tech in SOLAR_TECHNOLOGIES
    }
    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "decades_on_disk": MetadataValue.md("\n".join(f"- `{tech}`: {', '.join(labels) if labels else 'none'}" for tech, labels in decades_on_disk.items())),
            "country_count": len(df.columns.get_level_values("region").unique()) if len(df.columns) else 0,
            "preview": MetadataValue.md(df.tail(3).to_markdown()) if len(df) else MetadataValue.md("*empty*"),
        }
    )


@asset(
    name="pecd_country_capacity_factors_simple",
    deps=[
        "pecd_wind_onshore_europe_capacity_factors",
        "pecd_wind_offshore_europe_capacity_factors",
        "pecd_solar_europe_capacity_factors",
        "peon_region_mask",
    ],
    group_name="pecd_country",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description=(
        "Simplified, MaStR-free capacity factor per PECD country and technology "
        "(solar/wind_onshore/wind_offshore), hourly, full downloaded history -- "
        "generalizes energy-insights' 06_pecd_simple_vs_mastr_weighted.py DE-only "
        "prototype (fixed solar technology-mix weights + area-weighted wind zone "
        "means) to every country in the full-Europe pull above.\n\n"
        "**⚠️ DATA QUALITY CAVEAT -- solar only:** the 4 PECD solar "
        "technologies are blended per country using Germany's own market-derived "
        "weights (`edh/pecd.py::DEFAULT_SOLAR_COUNTRY_WEIGHTS`) for EVERY country "
        "unless that country has a real entry in `SOLAR_COUNTRY_WEIGHT_OVERRIDES` "
        "-- none do yet, as of 2026-09-24. This is not each country's real "
        "technology mix, just a placeholder that at least gives an apples-to-apples "
        "number; it can meaningfully distort a country whose solar market looks "
        "very different from Germany's (e.g. much more utility-scale, or almost no "
        "rooftop). Treat every country's solar series here as illustrative, not "
        "authoritative, until it gets a sourced override -- see README.md's 'Known "
        "data-quality caveats' section. Wind onshore/offshore have NO such caveat: "
        "both are computed from PECD's own zone geometry per country, not a "
        "borrowed default.\n\n"
        "Wind onshore: area-weighted mean of PEON zones per country. Wind "
        "offshore: unweighted mean of P2OF zones per country (no matching area "
        "mask at that zone scheme -- see edh/pecd.py module comment). MultiIndex "
        "(technology, country) columns."
    ),
    metadata={
        "source": "Derived from the three PECD europe capacity-factor assets above",
        "source_url": MetadataValue.url(CDS_SOURCE_URL),
        "region": "Every PECD country (Europe + North Africa + Middle East)",
        "resolution": "hourly, full downloaded history, by technology x country",
        "unit": "capacity factor (0-1, dimensionless)",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": "full_refresh, rebuilt from whatever decades are currently on disk",
        "data_quality_warning": MetadataValue.md(
            "**Solar only:** every country currently uses Germany's technology-mix "
            "weights as a placeholder (`SOLAR_COUNTRY_WEIGHT_OVERRIDES` is empty) -- "
            "a known, potentially large distortion for countries unlike Germany's "
            "solar market. See `edh/pecd.py` module docstring and README.md. Wind "
            "is unaffected (real per-country geometry, no borrowed weights)."
        ),
    },
)
def pecd_country_capacity_factors_simple(context: AssetExecutionContext) -> None:
    from edh.paths import pecd_country_capacity_factors_simple_file, pecd_mask_file
    from edh.pecd import (
        SOLAR_COUNTRY_WEIGHT_OVERRIDES,
        country_area_weighted_wind_cf,
        country_solar_capacity_factor_simple,
        country_unweighted_wind_cf,
    )

    onshore_cf = load_europe_capacity_factors("wind_onshore", "30")
    offshore_cf = load_europe_capacity_factors("wind_offshore", "20")
    solar_wide = load_europe_solar_capacity_factors()

    onshore_country = country_area_weighted_wind_cf(onshore_cf, pecd_mask_file("peon"))
    offshore_country = country_unweighted_wind_cf(offshore_cf)
    solar_country = country_solar_capacity_factor_simple(solar_wide)

    solar_countries = solar_wide.columns.get_level_values("region").unique()
    countries_on_default_weights = sorted(set(solar_countries) - SOLAR_COUNTRY_WEIGHT_OVERRIDES.keys())

    combined = pd.concat(
        {"wind_onshore": onshore_country, "wind_offshore": offshore_country, "solar": solar_country},
        axis=1,
        names=["technology", "country"],
    ).sort_index()

    output_file = pecd_country_capacity_factors_simple_file()
    combined.to_parquet(output_file)

    context.add_output_metadata(
        {
            "dagster/row_count": len(combined),
            "path": MetadataValue.path(str(output_file)),
            "country_count": len(combined.columns.get_level_values("country").unique()),
            "min_timestamp": MetadataValue.text(str(combined.index.min())) if len(combined) else MetadataValue.text("n/a"),
            "max_timestamp": MetadataValue.text(str(combined.index.max())) if len(combined) else MetadataValue.text("n/a"),
            "solar_countries_on_default_de_weights": MetadataValue.int(len(countries_on_default_weights)),
            "solar_countries_with_sourced_weights": MetadataValue.text(
                ", ".join(sorted(SOLAR_COUNTRY_WEIGHT_OVERRIDES)) if SOLAR_COUNTRY_WEIGHT_OVERRIDES else "none -- see data_quality_warning"
            ),
            "preview": MetadataValue.md(combined.tail(3).to_markdown()) if len(combined) else MetadataValue.md("*empty*"),
        }
    )


_DE_DEPS = [
    "mastr_units_wind",
    "mastr_units_solar",
    "mastr_solar_technical_detail",
    "lau_nuts_correspondence",
    "peon_region_mask",
    "peof_region_mask",
    "pecd_solar_capacity_factors",
    "pecd_wind_onshore_capacity_factors",
    "pecd_wind_offshore_capacity_factors",
]


@asset(
    deps=_DE_DEPS,
    group_name="de_potential",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description=(
        "Germany-wide hourly potential generation (MW), time-varying fleet -- each hour weighted by that "
        "calendar month's actual installed capacity, so comparable against true historic generation (e.g. SMARD)."
    ),
    metadata={
        "source": "Derived: PECD official capacity factors x MaStR-derived monthly installed capacity",
        "source_url": MetadataValue.url(CDS_SOURCE_URL),
        "region": "DE",
        "resolution": "hourly, 2015-2025",
        "unit": "MW",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": "full_refresh, rebuilt from whatever the upstream MaStR/PECD assets currently hold",
    },
)
def de_potential_historic(context: AssetExecutionContext) -> None:
    onshore, offshore, solar, cf_solar, cf_wind_onshore, cf_wind_offshore = load_prepared_inputs()
    month_range = full_month_range(onshore, offshore, solar)

    peon_wide = wind_zone_wide(monthly_snapshot_panel(onshore, ["zone_id"], month_range, EXPORT_START_PERIOD))
    peof_wide = wind_zone_wide(monthly_snapshot_panel(offshore, ["zone_id"], month_range, EXPORT_START_PERIOD))
    solar_wide = solar_nuts2_wide(
        monthly_snapshot_panel(solar, ["pecd_technology", "nuts2_region"], month_range, EXPORT_START_PERIOD),
        cf_solar.columns.names,
    )

    potential_solar = compute_potential_monthly(cf_solar, solar_wide)
    potential_wind_onshore = compute_potential_monthly(cf_wind_onshore, peon_wide)
    potential_wind_offshore = compute_potential_monthly(cf_wind_offshore, peof_wide)

    panel = pd.DataFrame(
        {
            "potential_solar_mw": potential_solar,
            "potential_wind_onshore_mw": potential_wind_onshore,
            "potential_wind_offshore_mw": potential_wind_offshore,
        }
    )
    panel.index.name = "timestamp"

    output_file = de_potential_historic_file()
    panel.to_parquet(output_file)

    unmodeled = unmodeled_capacity_share(cf_wind_offshore, peof_wide.iloc[-1])
    context.add_output_metadata(
        {
            "dagster/row_count": len(panel),
            "path": MetadataValue.path(str(output_file)),
            "min_timestamp": MetadataValue.text(str(panel.index.min())),
            "max_timestamp": MetadataValue.text(str(panel.index.max())),
            "unmodeled_peof_capacity_mw": MetadataValue.md(unmodeled.to_markdown()) if len(unmodeled) else MetadataValue.text("none"),
            "preview": MetadataValue.md(panel.tail(5).to_markdown()),
        }
    )


@asset(
    deps=_DE_DEPS,
    group_name="de_potential",
    kinds={"parquet"},
    tags=_PECD_TAGS,
    description=(
        "Germany-wide hourly capacity factor (0-1), today's fleet held fixed across the full 2015-2025 weather "
        "record -- 'what would today's fleet have faced under past weather'. Multiply by any hypothetical "
        "installed capacity (MW) to get a scenario generation series."
    ),
    metadata={
        "source": "Derived: PECD official capacity factors x MaStR-derived current installed capacity",
        "source_url": MetadataValue.url(CDS_SOURCE_URL),
        "region": "DE",
        "resolution": "hourly, 2015-2025",
        "unit": "capacity factor (0-1, dimensionless) -- multiply by an assumed MW to get generation",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": "full_refresh, rebuilt from whatever the upstream MaStR/PECD assets currently hold; fleet snapshot dated in as_of_date metadata",
    },
)
def de_capacity_factor_current_fleet(context: AssetExecutionContext) -> None:
    onshore, offshore, solar, cf_solar, cf_wind_onshore, cf_wind_offshore = load_prepared_inputs()
    as_of = pd.Timestamp.today().normalize()

    peon_now = capacity_snapshot(onshore, ["zone_id"], as_of, capacity_col="capacity_mw")
    peof_now = capacity_snapshot(offshore, ["zone_id"], as_of, capacity_col="capacity_mw")
    solar_now = capacity_snapshot(solar, ["pecd_technology", "nuts2_region"], as_of, capacity_col="capacity_mw")

    total_solar_mw = float(solar_now.sum())
    total_onshore_mw = float(peon_now.sum())
    total_offshore_mw = float(peof_now.sum())

    potential_solar = compute_potential_fixed(cf_solar, solar_now)
    potential_wind_onshore = compute_potential_fixed(cf_wind_onshore, peon_now)
    potential_wind_offshore = compute_potential_fixed(cf_wind_offshore, peof_now)

    panel = pd.DataFrame(
        {
            "capacity_factor_solar": potential_solar / total_solar_mw,
            "capacity_factor_wind_onshore": potential_wind_onshore / total_onshore_mw,
            "capacity_factor_wind_offshore": potential_wind_offshore / total_offshore_mw,
        }
    )
    panel.index.name = "timestamp"

    output_file = de_capacity_factor_current_fleet_file()
    panel.to_parquet(output_file)

    unmodeled = unmodeled_capacity_share(cf_wind_offshore, peof_now)
    context.add_output_metadata(
        {
            "dagster/row_count": len(panel),
            "path": MetadataValue.path(str(output_file)),
            "as_of_date": MetadataValue.text(str(as_of.date())),
            "current_capacity_mw": MetadataValue.md(
                f"| technology | MW |\n|---|---|\n| solar | {total_solar_mw:,.0f} |\n"
                f"| wind_onshore | {total_onshore_mw:,.0f} |\n| wind_offshore | {total_offshore_mw:,.0f} |"
            ),
            "unmodeled_peof_capacity_mw": MetadataValue.md(unmodeled.to_markdown()) if len(unmodeled) else MetadataValue.text("none"),
            "preview": MetadataValue.md(panel.tail(5).to_markdown()),
        }
    )


pecd_assets = [
    lau_nuts_correspondence,
    peon_region_mask,
    peof_region_mask,
    pecd_solar_capacity_factors,
    pecd_solar_country_capacity_factors,
    pecd_wind_onshore_capacity_factors,
    pecd_wind_offshore_capacity_factors,
    pecd_wind_onshore_europe_capacity_factors,
    pecd_wind_offshore_europe_capacity_factors,
    pecd_solar_europe_capacity_factors,
    pecd_country_capacity_factors_simple,
    de_potential_historic,
    de_capacity_factor_current_fleet,
]
