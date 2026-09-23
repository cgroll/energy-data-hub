"""SMARD (Bundesnetzagentur) series as Dagster software-defined assets.

One asset per series, all in the `smard` group -- gives each series its own
node in the Asset Graph (a book that only needs `smard_load` shouldn't show
a dependency on `smard_price_de_lu`).

**Resolution:** hourly, always -- SMARD's `resolution="hour"` endpoint,
hardcoded (see `edh/smard.py::download_series`). None of these series are
downloaded at any other granularity anywhere in this hub.

**Timestamps:** naive pandas timestamps that represent UTC, not
Europe/Berlin wall-clock -- `edh/smard.py` parses SMARD's epoch-ms values
as UTC-aware, then strips the tz label (`tz_localize(None)`), by design
(reproducible across machines, unlike `datetime.fromtimestamp()`). Treat
the index as UTC when joining against anything else.

**Units:** MW for generation/load/capacity, EUR/MWh for price -- verified
by magnitude against known real-world figures (DE grid load ~31-82 GW
range, DE solar capacity ~37-103 GW 2015-2026, EPEX's known -500 EUR/MWh
price floor), not read off an explicit unit label on SMARD's site (a JS
SPA, not scriptable) -- see conversation/commit history for the exact
check. Re-verify directly against SMARD if exactness beyond
order-of-magnitude ever matters (MW and MWh are numerically identical at
hourly resolution regardless).

**Load pattern: `data_derived_watermark`** (see docs/load_patterns.md
and BEST_PRACTICES.md). Idempotent, unpartitioned: each run reads its
own current output file back, resumes from
`existing.index.max() + 1h` (or SMARD's earliest available timestamp on
a first run), and appends only what's missing -- always catching up to
"now", never to a fixed partition boundary. Running it twice in a row is
safe: the second run just fetches 0 new rows. This is a deliberate
choice over Dagster's partitioned-asset pattern -- there's no need to
track per-day/per-week materialization status or support
partition-scoped backfills here; the series' own timestamp index is
already the single source of truth for "what's missing".

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

# name -> (SMARD variable, region, human description, unit). Region
# "DE-LU" for the market-area series (generation, load, price); "DE" for
# SMARD's own national capacity series -- see
# pecd-power-validity-DE/pipeline/04_download_smard_capacities.py.
SMARD_SERIES: dict[str, tuple[Variable, str, str, str]] = {
    "smard_generation_solar": (Variable.SOLAR, "DE-LU", "Solar (PV) generation", "MW"),
    "smard_generation_wind_onshore": (Variable.WIND_ONSHORE, "DE-LU", "Onshore wind generation", "MW"),
    "smard_generation_wind_offshore": (Variable.WIND_OFFSHORE, "DE-LU", "Offshore wind generation", "MW"),
    "smard_load": (Variable.TOTAL_LOAD, "DE-LU", "Total grid load", "MW"),
    "smard_price_de_lu": (Variable.PRICE_DE_LU, "DE-LU", "Day-ahead auction price", "EUR/MWh"),
    "smard_capacity_solar": (Variable.CAPACITY_SOLAR, "DE", "SMARD's own installed solar capacity", "MW"),
    "smard_capacity_wind_onshore": (Variable.CAPACITY_WIND_ONSHORE, "DE", "SMARD's own installed onshore wind capacity", "MW"),
    "smard_capacity_wind_offshore": (Variable.CAPACITY_WIND_OFFSHORE, "DE", "SMARD's own installed offshore wind capacity", "MW"),
}


def _make_smard_asset(asset_name: str, variable: Variable, region: str, human_name: str, unit: str):
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
        group_name="smard",
        kinds={"api", "parquet"},
        tags={"load_pattern": "data_derived_watermark"},
        description=(
            f"{human_name} ({region} market area), hourly, from SMARD "
            f"(Bundesnetzagentur). Idempotent, unpartitioned: each run "
            f"fills in whatever hours are missing since the last "
            f"materialization, from SMARD's earliest available data "
            f"through now -- see module docstring for the full "
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
    _make_smard_asset(name, variable, region, human_name, unit)
    for name, (variable, region, human_name, unit) in SMARD_SERIES.items()
]
