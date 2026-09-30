"""Gap-detection checks for the `balancing_market` group's
`data_derived_watermark` assets (edh_dagster/assets/balancing_market.py).

Same underlying risk as `hourly_gap_check` (checks/smard.py): a bad run
producing a wrongly/future-dated row would silently poison the watermark
and skip real data on every later run. None of these three assets are
hourly-gridded, though, so none reuse `hourly_gap_check` directly:

- `rebap_price` is a genuine continuous 15-min grid (unlike gas trading
  days, every quarter-hour of every day is expected) -- `rebap_gap_check`
  is the direct 15-min-frequency analogue of `hourly_gap_check`, no
  allowlist yet (add one via a `known_gaps`-style registry if a real,
  investigated gap ever turns up).
- `fcr_capacity_price` / `afrr_capacity_price` are one row per *calendar*
  day (not hourly) -- delivery is required every day of the year once a
  product exists (no "weekend" concept the way gas trading days have), so
  `regelleistung_daily_gap_check` checks every calendar day between min
  and max is present, same strict shape as `hourly_gap_check` but daily.
"""

import pandas as pd
from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from edh.paths import afrr_capacity_price_file, fcr_capacity_price_file, rebap_price_file


@asset_check(
    asset="rebap_price",
    name="rebap_gap_check",
    description=(
        "Every 15-minute interval between this series' min and max timestamp is present -- no unexplained "
        "gaps from a mis-derived watermark or other cause. See docs/load_patterns.md."
    ),
)
def rebap_gap_check(context: AssetCheckExecutionContext) -> AssetCheckResult:
    output_file = rebap_price_file()
    if not output_file.exists():
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN, description="Never materialized -- nothing to check yet.")

    df = pd.read_parquet(output_file)
    if df.empty:
        return AssetCheckResult(passed=True, description="Empty series -- vacuously no gaps.")

    expected = pd.date_range(df.index.min(), df.index.max(), freq="15min")
    missing = expected.difference(df.index)

    return AssetCheckResult(
        passed=len(missing) == 0,
        severity=AssetCheckSeverity.ERROR,
        description="No gaps." if len(missing) == 0 else f"{len(missing)} missing 15-min interval(s) -- investigate.",
        metadata={
            "missing_intervals": len(missing),
            "expected_intervals": len(expected),
            "actual_intervals": len(df),
            "first_missing": str(missing[0]) if len(missing) else "n/a",
            "last_missing": str(missing[-1]) if len(missing) else "n/a",
        },
    )


# Investigated 2026-09-30: `fcr_capacity_price`'s regelleistung.net source
# file itself (RESULT_OVERVIEW_CAPACITY_MARKET_FCR_2021-10-01_2021-10-31.xlsx)
# has no row at all for 2021-10-03 (German Unity Day, a Sunday that year) --
# confirmed by re-querying the raw file directly, not a parsing bug on our
# side. A real, permanent gap in the source; allowlisted rather than chased.
KNOWN_REGELLEISTUNG_GAPS: dict[str, dict[str, str]] = {
    "fcr_capacity_price": {"2021-10-03": "Missing from regelleistung.net's own monthly file (German Unity Day) -- confirmed at source, not a parsing issue."},
}


def _make_daily_gap_check(asset_name: str, output_file_fn):
    known_gap_index = pd.DatetimeIndex(sorted(KNOWN_REGELLEISTUNG_GAPS.get(asset_name, {})))

    @asset_check(
        asset=asset_name,
        name="regelleistung_daily_gap_check",
        description=(
            "Every calendar day between this series' min and max date is present -- delivery is required "
            "daily once the product exists, so unlike gas trading days there's no benign 'weekend' gap to "
            "tolerate. Known, investigated gaps are allowlisted (see KNOWN_REGELLEISTUNG_GAPS above) -- this "
            "only fails for gaps *not* in that registry. See docs/load_patterns.md."
        ),
    )
    def _check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        output_file = output_file_fn()
        if not output_file.exists():
            return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN, description="Never materialized -- nothing to check yet.")

        df = pd.read_parquet(output_file)
        if df.empty:
            return AssetCheckResult(passed=True, description="Empty series -- vacuously no gaps.")

        expected = pd.date_range(df.index.min(), df.index.max(), freq="D")
        missing = expected.difference(df.index)
        unexpected = missing.difference(known_gap_index)
        known = missing.intersection(known_gap_index)

        return AssetCheckResult(
            passed=len(unexpected) == 0,
            severity=AssetCheckSeverity.ERROR,
            description=(
                f"{len(known)} known/allowlisted gap(s), {len(unexpected)} new/unexpected gap(s)."
                if len(unexpected) == 0
                else f"{len(unexpected)} NEW, unallowlisted missing day(s) -- investigate before allowlisting."
            ),
            metadata={
                "unexpected_missing_days": len(unexpected),
                "known_allowlisted_days": len(known),
                "expected_days": len(expected),
                "actual_days": len(df),
                "first_unexpected": str(unexpected[0].date()) if len(unexpected) else "n/a",
                "last_unexpected": str(unexpected[-1].date()) if len(unexpected) else "n/a",
            },
        )

    return _check


fcr_daily_gap_check = _make_daily_gap_check("fcr_capacity_price", fcr_capacity_price_file)
afrr_daily_gap_check = _make_daily_gap_check("afrr_capacity_price", afrr_capacity_price_file)

balancing_market_checks = [rebap_gap_check, fcr_daily_gap_check, afrr_daily_gap_check]
