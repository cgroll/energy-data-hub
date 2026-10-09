"""Output paths for hub data.

No DVC / versioning here on purpose (see README.md): every dataset lives at
one fixed, current-state path that downstream consumers (book repos,
`dvc import`-free) read directly. Dagster's own materialization history
(timestamps, row counts as metadata) is the audit trail, not the file
system.
"""

from pathlib import Path

HUB_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = HUB_ROOT / "data"
SMARD_DIR = DATA_ROOT / "smard"

# open-mastr's own working area (its XML zip cache + the SQLite db it parses
# into) -- not itself a tracked hub output, just where the raw/intermediate
# state lives on disk. Kept separate from MASTR_DIR so a `find data/mastr`
# by a downstream consumer never sees the raw cache.
MASTR_HOME_DIR = DATA_ROOT / "mastr_home"
MASTR_DIR = DATA_ROOT / "mastr"

# Generic Eurostat/PECD reference data (region geometries, zone masks) --
# not MaStR- or PECD-capacity-factor-specific, so its own directory.
REGIONS_DIR = DATA_ROOT / "regions"
PECD_DIR = DATA_ROOT / "pecd"
PECD_CF_DOWNLOADS_DIR = PECD_DIR / "capacity_factors"
PECD_EUROPE_DIR = PECD_DIR / "capacity_factors_europe"

# Region-assigned wind+solar capacity events/panels -- derived from the
# mastr_units_* + region_geo assets above, not raw MaStR or PECD data itself.
CAPACITY_DIR = DATA_ROOT / "capacity"

# Fuel/commodity market prices (currently just TTF gas) -- not a power-market
# price like smard_price_*, so kept in its own directory rather than under
# smard/.
GAS_DIR = DATA_ROOT / "gas"

# Kelmarsh wind farm data (Zenodo record 5841834) -- a single named UK wind
# farm's own real metered generation, used to validate PECD's onshore wind
# capacity factors against ground truth. Not a national aggregate like
# smard/ or pecd/, so kept in its own directory.
KELMARSH_DIR = DATA_ROOT / "kelmarsh"

# Balancing-market prices: reBAP (netztransparenz.de) + FCR/aFRR capacity
# prices (regelleistung.net) -- a different mechanism from smard_price_*
# (day-ahead auction) or gas/ (a commodity future), so kept in its own
# directory rather than folded into smard/.
BALANCING_MARKET_DIR = DATA_ROOT / "balancing_market"


def smard_file(name: str) -> Path:
    """Path for one SMARD series, e.g. `smard_file("load")` ->
    `data/smard/load.parquet`."""
    SMARD_DIR.mkdir(parents=True, exist_ok=True)
    return SMARD_DIR / f"{name}.parquet"


def mastr_units_file(name: str) -> Path:
    """Path for one MaStR technology's harmonized unit table, e.g.
    `mastr_units_file("wind")` -> `data/mastr/wind.parquet`."""
    MASTR_DIR.mkdir(parents=True, exist_ok=True)
    return MASTR_DIR / f"{name}.parquet"


def mastr_solar_technical_detail_file() -> Path:
    """Per-solar-unit orientation/tracking detail, keyed by unit_id -- joins
    against `mastr_units_file("solar")`. See `edh/mastr.py`."""
    MASTR_DIR.mkdir(parents=True, exist_ok=True)
    return MASTR_DIR / "solar_technical_detail.parquet"


def mastr_storage_location_ids_file() -> Path:
    """Per-storage-unit `location_id` only (not the full harmonized unit
    table) -- used for the solar<->storage co-location join. See
    `edh/mastr.py`."""
    MASTR_DIR.mkdir(parents=True, exist_ok=True)
    return MASTR_DIR / "storage_location_ids.parquet"


def lau_nuts_correspondence_file() -> Path:
    """Eurostat LAU (municipality) -> NUTS3 crosswalk, Germany only. See
    `edh/region_geo.py`."""
    REGIONS_DIR.mkdir(parents=True, exist_ok=True)
    return REGIONS_DIR / "lau_nuts_correspondence.parquet"


def nuts_regions_file() -> Path:
    """German NUTS region geometries (levels 0-3), GeoJSON. See
    `edh/region_geo.py`."""
    REGIONS_DIR.mkdir(parents=True, exist_ok=True)
    return REGIONS_DIR / "nuts_regions.geojson"


def country_borders_file() -> Path:
    """Country-level outlines for Germany and its North/Baltic Sea
    neighbors (map context around offshore wind zones). See
    `edh/region_geo.py`."""
    REGIONS_DIR.mkdir(parents=True, exist_ok=True)
    return REGIONS_DIR / "country_borders.geojson"


def pecd_mask_file(scheme: str) -> Path:
    """PECD v4.2 wind-zone rasterized region mask. `scheme` is "peon"
    (onshore) or "peof" (offshore). See `edh/region_geo.py`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / f"{scheme}_region_mask.nc"


def pecd_capacity_factor_zip(kind: str, technology: str) -> Path:
    """Raw PECD official capacity-factor download, all of Europe, 2015-2025,
    one file per (kind, technology). `kind` is "solar", "wind_onshore", or
    "wind_offshore"; `technology` is PECD's technology code. See
    `edh/pecd.py`."""
    PECD_CF_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_CF_DOWNLOADS_DIR / f"{kind}_tech{technology}.zip"


def pecd_europe_decade_zip(kind: str, technology: str, decade_label: str) -> Path:
    """Raw PECD wind capacity-factor CDS zip for one decade, full Europe +
    North Africa + Middle East (not DE-filtered) -- a disposable
    intermediate, deleted by `edh.pecd.download_and_convert_europe_decade`
    right after it's parsed into that decade's parquet. Never kept as a
    cache: the parsing logic (CSV header-skip, column selection) is stable
    enough that re-downloading from CDS on the rare occasion it needs
    reprocessing is cheaper than permanently storing ~150MB of raw zips.
    See `docs/pecd_data_availability.md`."""
    PECD_EUROPE_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_EUROPE_DIR / f"_tmp_{kind}_tech{technology}_{decade_label}.zip"


def pecd_europe_decade_capacity_factors_file(kind: str, technology: str, decade_label: str) -> Path:
    """Full-Europe (every zone/country column CDS returns, not DE-filtered)
    PECD wind capacity-factor parquet for one decade, e.g.
    `decade_label="1990-1999"`. This file's existence is the incremental
    checkpoint: a decade already on disk is never re-downloaded. See
    `edh.pecd.load_europe_capacity_factors` to concatenate every decade
    currently on disk into one continuous series, and
    `docs/pecd_data_availability.md` for the download plan."""
    PECD_EUROPE_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_EUROPE_DIR / f"pecd_{kind}_tech{technology}_{decade_label}.parquet"


def pecd_capacity_factors_file(kind: str) -> Path:
    """PECD official capacity factor, Germany-only, hourly, 2015-2025.
    `kind` is "solar" (MultiIndex technology x NUTS2 columns), "wind_onshore"
    (PEON zone columns), or "wind_offshore" (PEOF zone columns). See
    `edh/pecd.py`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / f"pecd_{kind}_capacity_factors.parquet"


def pecd_solar_country_capacity_factors_file() -> Path:
    """PECD's own nuts_0 (country-level) solar capacity factor, Germany
    only, hourly, 2015-2025, one column per PECD technology (60/61/62/63).
    Unlike `pecd_capacity_factors_file("solar")`'s NUTS2 product, this is
    PECD's true country-level aggregation, downloaded directly at
    `spatial_resolution=nuts_0` -- PECD doesn't expose nuts_0 for wind at
    all (confirmed against the live CDS API, see
    docs/pecd_data_availability.md). See `edh/pecd.py`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "pecd_solar_country_capacity_factors.parquet"


def ttf_gas_prices_file() -> Path:
    """Daily TTF (Title Transfer Facility) natural gas futures OHLCV, from
    Yahoo Finance. See `edh/ttf_gas.py`. Migrated from
    ~/research/world-of-energy/pipeline/02_download_ttf_gas.py
    (2026-09-24), which keeps working on this hub's own copy."""
    GAS_DIR.mkdir(parents=True, exist_ok=True)
    return GAS_DIR / "ttf_gas_prices.parquet"


def de_potential_historic_file() -> Path:
    """Germany-wide hourly potential generation (MW), time-varying fleet --
    each hour weighted by that month's actual installed capacity, so
    comparable against true historic generation (e.g. SMARD). Columns:
    `potential_solar_mw`, `potential_wind_onshore_mw`,
    `potential_wind_offshore_mw`. See `edh/pecd.py`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "de_potential_historic.parquet"


def capacity_events_file() -> Path:
    """Region-assigned wind+solar unit-level table (one row per plant):
    `region_code` (NUTS3, or a synthetic offshore pseudo-region), capacity,
    commissioning/shutdown dates, coordinates, and (solar only) behind-the-
    meter `pv_category`. See `edh/capacity_panel.py`."""
    CAPACITY_DIR.mkdir(parents=True, exist_ok=True)
    return CAPACITY_DIR / "capacity_events.parquet"


def capacity_by_region_year_file() -> Path:
    """Annual (region_code, technology, year) installed-capacity snapshot
    panel, year-end. See `edh/capacity_panel.py`."""
    CAPACITY_DIR.mkdir(parents=True, exist_ok=True)
    return CAPACITY_DIR / "capacity_by_region_year.parquet"


def capacity_by_region_year_pv_category_file() -> Path:
    """Annual (region_code, pv_category, year) solar-only installed-capacity
    snapshot panel, year-end. See `edh/capacity_panel.py`."""
    CAPACITY_DIR.mkdir(parents=True, exist_ok=True)
    return CAPACITY_DIR / "capacity_by_region_year_pv_category.parquet"


def offshore_regions_file() -> Path:
    """Offshore wind footprint polygons (convex hull of currently-installed
    turbine coordinates), one row per pseudo-region. See
    `edh/capacity_panel.py`."""
    CAPACITY_DIR.mkdir(parents=True, exist_ok=True)
    return CAPACITY_DIR / "offshore_regions.geojson"


def pecd_country_capacity_factors_simple_file() -> Path:
    """Simplified, MaStR-free capacity factor per PECD country and
    technology (solar/wind_onshore/wind_offshore), hourly, full downloaded
    history -- MultiIndex (technology, country) columns. See
    `edh/pecd.py`'s `country_solar_capacity_factor_simple` /
    `country_area_weighted_wind_cf` / `country_unweighted_wind_cf`, and
    `energy-insights`' `06_pecd_simple_vs_mastr_weighted` notebook (the
    DE-only prototype this generalizes to every PECD country)."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "pecd_country_capacity_factors_simple.parquet"


def pecd_country_capacity_factors_simple_de_file() -> Path:
    """Just the `DE` columns of `pecd_country_capacity_factors_simple`
    (solar/wind_onshore/wind_offshore, plain columns, no MultiIndex),
    exported standalone so a consumer that only wants Germany doesn't need
    to load the full ~230MB all-country file. Not to be confused with
    `de_capacity_factor_current_fleet` below -- that's the real,
    MaStR-weighted DE product; this is DE's slice of the simplified,
    every-country approximation. See `edh_dagster/assets/pecd.py`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "pecd_country_capacity_factors_simple_de.parquet"


def de_capacity_factor_current_fleet_file() -> Path:
    """Germany-wide hourly capacity factor (0-1), current fleet held fixed
    across the full 2015-2025 weather record -- answers "what would today's
    fleet have faced under past weather", and is meant to be multiplied by
    any hypothetical installed capacity. Columns: `capacity_factor_solar`,
    `capacity_factor_wind_onshore`, `capacity_factor_wind_offshore`. See
    `edh/pecd.py`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "de_capacity_factor_current_fleet.parquet"


def de_technology_capacity_factors_file() -> Path:
    """Germany-wide hourly capacity factor (0-1), six columns, full
    downloaded PECD history (1980-2025), zero MaStR dependency -- the
    stable "base" layer `de_capacity_factors_fleet_weighted` is built on
    top of. Columns: `solar_60`/`solar_61`/`solar_62`/`solar_63` (PECD's
    four official country-level solar technologies, unblended),
    `wind_onshore` (area-weighted PEON zone mean), `wind_offshore`
    (area-weighted P2OF zone mean). Deliberately kept separate from any
    fleet-weight snapshot -- see `edh/pecd.py::de_technology_capacity_factors`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "de_technology_capacity_factors.parquet"


def de_fleet_weights_snapshot_file() -> Path:
    """One-row snapshot of today's real MaStR fleet: the four solar
    technology-mix weights (sum to 1) and total installed MW for solar/
    wind-onshore/wind-offshore, tagged with the `as_of` date it was
    computed from -- kept separate from the long capacity-factor series on
    purpose, so a different fleet vintage can be substituted later without
    recomputing the PECD side. See `edh/pecd.py::de_fleet_weights_snapshot`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "de_fleet_weights_snapshot.parquet"


def de_capacity_factors_fleet_weighted_file() -> Path:
    """`de_technology_capacity_factors` + `de_fleet_weights_snapshot`
    combined: solar's four technologies blended by today's real MaStR
    technology mix (not NUTS2/zone-fraction detail -- see
    `de_capacity_factor_current_fleet` for that finer product), wind
    columns passed through unchanged. Columns: `capacity_factor_solar`/
    `capacity_factor_wind_onshore`/`capacity_factor_wind_offshore` (0-1),
    `power_mw_solar`/`power_mw_wind_onshore`/`power_mw_wind_offshore`
    (capacity factor x today's real MaStR MW), `power_mw_total` (sum of
    the three power columns). See `edh/pecd.py::blend_fleet_weighted_cf`."""
    PECD_DIR.mkdir(parents=True, exist_ok=True)
    return PECD_DIR / "de_capacity_factors_fleet_weighted.parquet"


ERA5_DIR = DATA_ROOT / "era5_climatology"


def era5_de_temperature_hourly_file() -> Path:
    """ERA5-Land hourly Germany spatial-average 2 m temperature, one row per
    real UTC hour, 1991-01-01 to 2021-01-01 (262,992 hours). Columns
    `timestamp` + `avg_de_2m_temp` (deg C). Spatial averaging only, no
    temporal/climatology aggregation -- that's a downstream step from this
    base series, see `edh/era5_climatology.py`."""
    ERA5_DIR.mkdir(parents=True, exist_ok=True)
    return ERA5_DIR / "era5_de_temperature_hourly_1991_2020.parquet"


def era5_de_wind_speed_hourly_file() -> Path:
    """Same base-data pattern as the temperature file above, but for 10 m
    wind speed (m/s): one row per real UTC hour, 1991-2020, columns
    `timestamp` + `avg_de_wind_speed_10m`. Computed from ERA5-Land's
    `u_component_of_wind_10m` / `v_component_of_wind_10m`, combined into
    speed *per pixel* before the Germany spatial average."""
    ERA5_DIR.mkdir(parents=True, exist_ok=True)
    return ERA5_DIR / "era5_de_wind_speed_hourly_1991_2020.parquet"


def era5_de_precipitation_hourly_file() -> Path:
    """Same base-data pattern again, for precipitation (mm): one row per
    real UTC hour, 1991-2020, columns `timestamp` + `avg_de_precipitation_mm`.
    Straight from ERA5-Land's `total_precipitation_hourly` band."""
    ERA5_DIR.mkdir(parents=True, exist_ok=True)
    return ERA5_DIR / "era5_de_precipitation_hourly_1991_2020.parquet"


ERA5_GRID_DIR = DATA_ROOT / "era5_grid"


def era5_grid_month_file(year: int, month: int) -> Path:
    """One month of the full ERA5 reanalysis grid (Germany+offshore bbox,
    0.25 deg, hourly) -- columns `cell_lon`, `cell_lat`, `valid_time`,
    `u100`, `v100`, `u10`, `v10`, `t2m`, `ssrd`. See `edh/era5_grid.py`."""
    ERA5_GRID_DIR.mkdir(parents=True, exist_ok=True)
    return ERA5_GRID_DIR / f"era5_grid_{year}_{month:02d}.parquet"


def kelmarsh_wt_static_file() -> Path:
    """Per-turbine static specs for Kelmarsh wind farm's 6 Senvion MM92
    units: coordinates, rated power, hub height, rotor diameter,
    commercial operations date. See `edh/kelmarsh.py`."""
    KELMARSH_DIR.mkdir(parents=True, exist_ok=True)
    return KELMARSH_DIR / "kelmarsh_wt_static.parquet"


def kelmarsh_grid_meter_file() -> Path:
    """Kelmarsh wind farm's 10-minute site grid meter export: real metered
    generation at the grid connection point, plus Greenbyte's own
    availability flags. See `edh/kelmarsh.py`."""
    KELMARSH_DIR.mkdir(parents=True, exist_ok=True)
    return KELMARSH_DIR / "kelmarsh_grid_meter.parquet"


def kelmarsh_turbine_scada_file() -> Path:
    """Kelmarsh wind farm's 10-minute per-turbine SCADA: real nacelle wind
    speed (plain + density-adjusted), each turbine's own metered power,
    and data availability, long format (one row per turbine per
    timestamp). See `edh/kelmarsh.py`."""
    KELMARSH_DIR.mkdir(parents=True, exist_ok=True)
    return KELMARSH_DIR / "kelmarsh_turbine_scada.parquet"


def rebap_price_file() -> Path:
    """German reBAP (balancing energy price), quarter-hourly, naive UTC,
    columns `rebap_eur_mwh` / `rebap_ueberdeckt_eur_mwh`. See
    `edh/rebap.py`."""
    BALANCING_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    return BALANCING_MARKET_DIR / "rebap_price.parquet"


def fcr_capacity_price_file() -> Path:
    """FCR (PRL) settlement capacity price, Germany, daily (one row per
    delivery day), one column per 4-hour block (`negpos_00_04` ...
    `negpos_20_24`), EUR/MW/h, naive UTC-midnight index. **The block
    columns themselves are CET/CEST, not UTC** -- see `edh/regelleistung.py`
    module docstring before using them."""
    BALANCING_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    return BALANCING_MARKET_DIR / "fcr_capacity_price.parquet"


def afrr_capacity_price_file() -> Path:
    """aFRR (SRL) marginal capacity price, Germany, daily (one row per
    delivery day), one column per direction x 4-hour block (`neg_00_04`
    ... `pos_20_24`), EUR/MW/h, naive UTC-midnight index. **The block
    columns themselves are CET/CEST, not UTC** -- see `edh/regelleistung.py`
    module docstring before using them."""
    BALANCING_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    return BALANCING_MARKET_DIR / "afrr_capacity_price.parquet"


def nrv_saldo_file() -> Path:
    """Germany-wide NRV-Saldo (Netzregelverbund-Saldo), quarter-hourly,
    naive UTC, single `nrv_saldo_mw` column (positive = under-supplied,
    negative = over-supplied). Known real gaps 2014-2022 -- see
    `edh/nrv_saldo.py`."""
    BALANCING_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    return BALANCING_MARKET_DIR / "nrv_saldo.parquet"


def id_aep_file() -> Path:
    """Germany-wide ID-AEP (Index Ausgleichsenergiepreis / "IP-Index"),
    quarter-hourly, naive UTC, single `id_aep_eur_mwh` column (NaN where
    undefined -- too little intraday trading volume). See `edh/id_aep.py`."""
    BALANCING_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    return BALANCING_MARKET_DIR / "id_aep.parquet"


def aep_modules_file() -> Path:
    """Germany-wide AEP Module 1/2/3 (the three components reBAP is the
    max/min of), quarter-hourly, naive UTC, columns
    `aep_module1_eur_mwh`/`aep_module2_eur_mwh`/`aep_module3_eur_mwh` (NaN
    where a module doesn't apply that quarter-hour). See
    `edh/aep_modules.py`."""
    BALANCING_MARKET_DIR.mkdir(parents=True, exist_ok=True)
    return BALANCING_MARKET_DIR / "aep_modules.parquet"
