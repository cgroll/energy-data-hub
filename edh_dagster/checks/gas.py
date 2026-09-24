"""Gap-detection check for the `ttf_gas_price` `data_derived_watermark`
asset (edh_dagster/assets/gas.py).

Same underlying risk as `hourly_gap_check` (checks/smard.py): a bad run
producing a wrongly/future-dated row would silently poison the watermark
and skip real data on every later run, with no other symptom. But
`ttf_gas_price` is trading-day data, not an hourly grid -- ordinary
weekends and exchange holidays are expected, benign gaps, not the
failure mode this guards against. A literal "every calendar day between
min and max must be present" check (the `hourly_gap_check` shape) would
be permanently red from ordinary weekends alone, training you to ignore
it -- exactly the failure BEST_PRACTICES.md warns against.

Instead: flag only gaps longer than what a long holiday weekend could
plausibly explain (5 calendar days -- covers a normal weekend plus one
adjacent holiday). Not a per-day allowlist like `known_gaps.py`; that
approach doesn't fit an asset whose "gaps" are the majority-normal case
rather than named exceptions.
"""

import pandas as pd
from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from edh.paths import ttf_gas_prices_file

MAX_PLAUSIBLE_GAP_DAYS = 5


@asset_check(
    asset="ttf_gas_price",
    name="ttf_gas_no_large_gap_check",
    description=(
        "No stretch longer than 5 calendar days with zero rows, between "
        "this series' min and max date -- catches a poisoned "
        "data_derived_watermark without flagging ordinary weekends/"
        "exchange holidays as failures. See module docstring."
    ),
)
def ttf_gas_no_large_gap_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
    output_file = ttf_gas_prices_file()
    if not output_file.exists():
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Never materialized -- nothing to check yet.",
        )

    df = pd.read_parquet(output_file)
    if df.empty:
        return AssetCheckResult(passed=True, description="Empty series -- vacuously no gaps.")

    gap_days = df.index.to_series().diff().dt.days
    large_gaps = gap_days[gap_days > MAX_PLAUSIBLE_GAP_DAYS]

    return AssetCheckResult(
        passed=len(large_gaps) == 0,
        severity=AssetCheckSeverity.ERROR,
        description=(
            "No unexplained gaps." if len(large_gaps) == 0
            else f"{len(large_gaps)} gap(s) longer than {MAX_PLAUSIBLE_GAP_DAYS} days -- investigate."
        ),
        metadata={
            "max_gap_days": int(gap_days.max()) if len(gap_days.dropna()) else 0,
            "large_gap_count": len(large_gaps),
            "first_large_gap_end": str(large_gaps.index[0].date()) if len(large_gaps) else "n/a",
        },
    )


gas_checks = [ttf_gas_no_large_gap_check]
