"""SMARD (Bundesnetzagentur) series as Dagster software-defined assets.

Full catalog (47 series), migrated 2026-09-23 from ~/research/smard-data
(first commit 2025-04, predates this hub) to retire that repo -- see
edh/smard.py's Variable docstring for the exact provenance. One asset per
series, grouped by category (`smard_generation`, `smard_consumption`,
`smard_price`, `smard_forecast`, `smard_capacity`) so a book that only
needs `smard_load` doesn't show a dependency on `smard_price_at`.

**Resolution:** hourly, always -- SMARD's `resolution="hour"` endpoint,
hardcoded (see `edh/smard.py::download_series`). smard-data used
quarter-hour; deliberately not matched here for the migration -- hourly
is what every current consumer of this hub actually needs, and the
gap-check/watermark machinery below is hourly-shaped throughout.
Revisit if a real quarter-hour need shows up (see BEST_PRACTICES.md-style
"don't build it until something needs it").

**Timestamps:** naive pandas timestamps that represent UTC, not
Europe/Berlin wall-clock -- `edh/smard.py` parses SMARD's epoch-ms values
as UTC-aware, then strips the tz label (`tz_localize(None)`), by design
(reproducible across machines, unlike `datetime.fromtimestamp()`). Treat
the index as UTC when joining against anything else.

**Region, verified empirically per category (2026-09-23), not assumed:**
- `smard_generation`, `smard_consumption`, `smard_forecast`: `DE-LU`.
  Confirmed to matter -- `DE` vs `DE-LU` give genuinely different values
  for e.g. total load (~1% apart) and forecast_total; smard-data's
  blanket use of `DE` for everything was a real inaccuracy for these,
  not replicated here.
- `smard_price`: `DE-LU`, though verified the region path segment is
  actually irrelevant for these variable IDs (AT/DE-LU/DE all returned
  identical values for e.g. `PRICE_AT`) -- `DE-LU` used purely for
  consistency with `smard_price_de_lu`, not because it's required.
- `smard_capacity`: `DE`, matching the pre-existing
  `capacity_solar`/`capacity_wind_*` convention (this hub cares about
  capacity physically installed in Germany, not the DE-LU market area --
  confirmed `DE` vs `DE-LU` differ here too, e.g. hydro capacity,
  since Luxembourg has some of its own).

**Units:** MW throughout except `smard_price_*` (EUR/MWh) -- verified by
magnitude for the original 8 series against known real-world figures
(see git history); the 39 series added in the smard-data migration
inherit the same unit by category by construction (same kind of physical
quantity, same API), not independently re-verified one by one. Flag if
any single one looks implausible.

**Load pattern: `data_derived_watermark`** (see docs/load_patterns.md
and BEST_PRACTICES.md). Idempotent, unpartitioned: each run reads its
own current output file back, resumes from
`existing.index.max() + 1h` (or SMARD's earliest available timestamp on
a first run), and appends only what's missing -- always catching up to
"now", never to a fixed partition boundary. Running it twice in a row is
safe: the second run just fetches 0 new rows. A series SMARD has stopped
publishing (e.g. `smard_generation_nuclear` post phase-out) behaves
correctly under this scheme too -- new_data comes back empty, the
watermark simply stops advancing, not an error and not a gap (the
expected range in hourly_gap_check only ever extends to the last real
timestamp).

**Overwrite-in-place, no version history:** each asset owns exactly one
output file that gets read, appended to, and rewritten every run -- past
contents aren't retained anywhere once overwritten (see edh/paths.py).

**Gap check:** paired with an `hourly_gap_check` asset check
(edh_dagster/checks/smard.py) -- see BEST_PRACTICES.md for why every
`data_derived_watermark` asset needs one.

**Caveat inherited from the original per-repo scripts this consolidates
(pecd-replication, pecd-power-validity-DE, ...):** incremental-by-append
assumes SMARD doesn't revise already-fetched hours after the fact. This
was never explicitly verified against SMARD's actual revision behavior --
if that assumption turns out to be wrong for some series, this would
silently miss revisions rather than error. `smard_redispatch_by_source`
(edh_dagster/assets/redispatch.py) is the one series known to be revised
after publication, which is exactly why *that* one is `full_refresh`
instead.
"""

from datetime import timedelta

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from edh.paths import smard_file
from edh.smard import BASE_URL, DEFAULT_START_DATE, Variable, download_series
from edh.known_gaps import KNOWN_GAPS

# asset_name -> (variable, region, group, human description, unit)
SMARD_SERIES: dict[str, tuple[Variable, str, str, str, str]] = {
    # --- Generation, by fuel type (DE-LU) ---------------------------------
    "smard_generation_solar": (Variable.SOLAR, "DE-LU", "smard_generation", "Solar (PV) generation", "MW"),
    "smard_generation_wind_onshore": (Variable.WIND_ONSHORE, "DE-LU", "smard_generation", "Onshore wind generation", "MW"),
    "smard_generation_wind_offshore": (Variable.WIND_OFFSHORE, "DE-LU", "smard_generation", "Offshore wind generation", "MW"),
    "smard_generation_brown_coal": (Variable.BROWN_COAL, "DE-LU", "smard_generation", "Brown coal (lignite) generation", "MW"),
    "smard_generation_hard_coal": (Variable.HARD_COAL, "DE-LU", "smard_generation", "Hard coal generation", "MW"),
    "smard_generation_nuclear": (Variable.NUCLEAR, "DE-LU", "smard_generation", "Nuclear generation (zero since Germany's 2023 phase-out)", "MW"),
    "smard_generation_natural_gas": (Variable.NATURAL_GAS, "DE-LU", "smard_generation", "Natural gas generation", "MW"),
    "smard_generation_hydro": (Variable.HYDRO, "DE-LU", "smard_generation", "Hydro generation", "MW"),
    "smard_generation_biomass": (Variable.BIOMASS, "DE-LU", "smard_generation", "Biomass generation", "MW"),
    "smard_generation_pumped_storage": (Variable.PUMPED_STORAGE, "DE-LU", "smard_generation", "Pumped-storage generation (discharge)", "MW"),
    "smard_generation_other_conventional": (Variable.OTHER_CONVENTIONAL, "DE-LU", "smard_generation", "Other conventional generation", "MW"),
    "smard_generation_other_renewable": (Variable.OTHER_RENEWABLE, "DE-LU", "smard_generation", "Other renewable generation", "MW"),
    # --- Consumption (DE-LU) -----------------------------------------------
    "smard_load": (Variable.TOTAL_LOAD, "DE-LU", "smard_consumption", "Total grid load", "MW"),
    "smard_consumption_residual_load": (Variable.RESIDUAL_LOAD, "DE-LU", "smard_consumption", "Residual load (load minus renewable generation)", "MW"),
    "smard_consumption_pumped_storage_load": (Variable.PUMPED_STORAGE_LOAD, "DE-LU", "smard_consumption", "Pumped-storage load (charging)", "MW"),
    # --- Day-ahead prices (region verified irrelevant -- see module docstring) ---
    "smard_price_de_lu": (Variable.PRICE_DE_LU, "DE-LU", "smard_price", "Day-ahead auction price, DE-LU", "EUR/MWh"),
    "smard_price_de_lu_neighbors": (Variable.PRICE_DE_LU_NEIGHBORS, "DE-LU", "smard_price", "Day-ahead price, DE-LU neighboring-zone comparison series", "EUR/MWh"),
    "smard_price_at": (Variable.PRICE_AT, "DE-LU", "smard_price", "Day-ahead auction price, Austria", "EUR/MWh"),
    "smard_price_be": (Variable.PRICE_BE, "DE-LU", "smard_price", "Day-ahead auction price, Belgium", "EUR/MWh"),
    "smard_price_ch": (Variable.PRICE_CH, "DE-LU", "smard_price", "Day-ahead auction price, Switzerland", "EUR/MWh"),
    "smard_price_cz": (Variable.PRICE_CZ, "DE-LU", "smard_price", "Day-ahead auction price, Czechia", "EUR/MWh"),
    "smard_price_dk1": (Variable.PRICE_DK1, "DE-LU", "smard_price", "Day-ahead auction price, Denmark DK1", "EUR/MWh"),
    "smard_price_dk2": (Variable.PRICE_DK2, "DE-LU", "smard_price", "Day-ahead auction price, Denmark DK2", "EUR/MWh"),
    "smard_price_fr": (Variable.PRICE_FR, "DE-LU", "smard_price", "Day-ahead auction price, France", "EUR/MWh"),
    "smard_price_hu": (Variable.PRICE_HU, "DE-LU", "smard_price", "Day-ahead auction price, Hungary", "EUR/MWh"),
    "smard_price_it_north": (Variable.PRICE_IT_NORTH, "DE-LU", "smard_price", "Day-ahead auction price, Italy North", "EUR/MWh"),
    "smard_price_nl": (Variable.PRICE_NL, "DE-LU", "smard_price", "Day-ahead auction price, Netherlands", "EUR/MWh"),
    "smard_price_no2": (Variable.PRICE_NO2, "DE-LU", "smard_price", "Day-ahead auction price, Norway NO2", "EUR/MWh"),
    "smard_price_pl": (Variable.PRICE_PL, "DE-LU", "smard_price", "Day-ahead auction price, Poland", "EUR/MWh"),
    "smard_price_pl2": (Variable.PRICE_PL2, "DE-LU", "smard_price", "Day-ahead auction price, Poland (2nd series)", "EUR/MWh"),
    "smard_price_si": (Variable.PRICE_SI, "DE-LU", "smard_price", "Day-ahead auction price, Slovenia", "EUR/MWh"),
    # --- Forecasts (DE-LU) ---------------------------------------------------
    "smard_forecast_total": (Variable.FORECAST_TOTAL, "DE-LU", "smard_forecast", "Total generation forecast", "MW"),
    "smard_forecast_onshore": (Variable.FORECAST_ONSHORE, "DE-LU", "smard_forecast", "Onshore wind generation forecast", "MW"),
    "smard_forecast_offshore": (Variable.FORECAST_OFFSHORE, "DE-LU", "smard_forecast", "Offshore wind generation forecast", "MW"),
    "smard_forecast_solar": (Variable.FORECAST_SOLAR, "DE-LU", "smard_forecast", "Solar generation forecast", "MW"),
    "smard_forecast_wind_solar": (Variable.FORECAST_WIND_SOLAR, "DE-LU", "smard_forecast", "Combined wind+solar generation forecast", "MW"),
    "smard_forecast_other": (Variable.FORECAST_OTHER, "DE-LU", "smard_forecast", "Other generation forecast", "MW"),
    # --- Installed capacity, by fuel type (DE) ------------------------------
    "smard_capacity_solar": (Variable.CAPACITY_SOLAR, "DE", "smard_capacity", "SMARD's own installed solar capacity", "MW"),
    "smard_capacity_wind_onshore": (Variable.CAPACITY_WIND_ONSHORE, "DE", "smard_capacity", "SMARD's own installed onshore wind capacity", "MW"),
    "smard_capacity_wind_offshore": (Variable.CAPACITY_WIND_OFFSHORE, "DE", "smard_capacity", "SMARD's own installed offshore wind capacity", "MW"),
    "smard_capacity_brown_coal": (Variable.CAPACITY_BROWN_COAL, "DE", "smard_capacity", "Installed brown coal (lignite) capacity", "MW"),
    "smard_capacity_hard_coal": (Variable.CAPACITY_HARD_COAL, "DE", "smard_capacity", "Installed hard coal capacity", "MW"),
    "smard_capacity_natural_gas": (Variable.CAPACITY_NATURAL_GAS, "DE", "smard_capacity", "Installed natural gas capacity", "MW"),
    "smard_capacity_hydro": (Variable.CAPACITY_HYDRO, "DE", "smard_capacity", "Installed hydro capacity", "MW"),
    "smard_capacity_biomass": (Variable.CAPACITY_BIOMASS, "DE", "smard_capacity", "Installed biomass capacity", "MW"),
    "smard_capacity_pumped_storage": (Variable.CAPACITY_PUMPED_STORAGE, "DE", "smard_capacity", "Installed pumped-storage capacity", "MW"),
    "smard_capacity_other_renewable": (Variable.CAPACITY_OTHER_RENEWABLE, "DE", "smard_capacity", "Installed other-renewable capacity", "MW"),
}


def _make_smard_asset(asset_name: str, variable: Variable, region: str, group: str, human_name: str, unit: str):
    source_url = f"{BASE_URL}/chart_data/{variable.value}/{region}/index_hour.json"
    known_gaps = KNOWN_GAPS.get(asset_name)

    static_metadata = {
        "source": "SMARD (Bundesnetzagentur)",
        "source_url": MetadataValue.url(source_url),
        "smard_variable_id": variable.value,
        "smard_variable_name": variable.name,
        "region": region,
        "resolution": "hourly",
        "unit": unit,
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": "data_derived_watermark: idempotent append, no partitions -- backfills to now every run",
    }
    if known_gaps:
        # Visible right on the asset's own overview page, not just buried
        # in hourly_gap_check's pass/fail history -- see known_gaps.py for
        # why these are allowlisted rather than chased or hidden.
        gap_table = "\n".join(f"| `{ts}` | {reason} |" for ts, reason in known_gaps.items())
        static_metadata["known_data_issues"] = MetadataValue.md(
            f"| timestamp | reason |\n|---|---|\n{gap_table}\n\n"
            f"See `edh/known_gaps.py` for the investigation. "
            f"`hourly_gap_check` allowlists these -- it only fails for gaps "
            f"*not* listed here."
        )

    @asset(
        name=asset_name,
        group_name=group,
        kinds={"api", "parquet"},
        tags={"load_pattern": "data_derived_watermark"},
        description=(
            f"{human_name} ({region}), hourly, from SMARD (Bundesnetzagentur). "
            f"Idempotent, unpartitioned: each run fills in whatever hours are "
            f"missing since the last materialization, from SMARD's earliest "
            f"available data through now -- see module docstring for the full "
            f"incremental-append and no-partition rationale."
            + (" Has known, allowlisted data gaps -- see 'known_data_issues' metadata below." if known_gaps else "")
        ),
        metadata=static_metadata,
    )
    def _smard_asset(context: AssetExecutionContext) -> None:
        output_file = smard_file(asset_name.removeprefix("smard_"))
        existing = pd.read_parquet(output_file) if output_file.exists() else None

        if existing is not None and not existing.empty:
            start_time = existing.index.max().to_pydatetime() + timedelta(hours=1)
        else:
            start_time = DEFAULT_START_DATE

        new_data = download_series(variable, region=region, start_time=start_time)

        if existing is not None and not existing.empty:
            combined = pd.concat([existing, new_data])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        else:
            combined = new_data

        combined.to_parquet(output_file)

        context.add_output_metadata(
            {
                "dagster/row_count": len(combined),
                "new_rows": len(new_data),
                "min_timestamp": MetadataValue.text(str(combined.index.min()) if len(combined) else "n/a"),
                "max_timestamp": MetadataValue.text(str(combined.index.max()) if len(combined) else "n/a"),
                "path": MetadataValue.path(str(output_file)),
                "preview": MetadataValue.md(combined.tail(5).to_markdown()) if len(combined) else MetadataValue.md("*empty*"),
            }
        )

    return _smard_asset


smard_assets = [
    _make_smard_asset(name, variable, region, group, human_name, unit)
    for name, (variable, region, group, human_name, unit) in SMARD_SERIES.items()
]
