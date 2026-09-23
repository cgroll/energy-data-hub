"""Gap-detection checks for the `data_derived_watermark` assets in
edh_dagster/assets/smard.py.

See BEST_PRACTICES.md's "Quality checks" section for why this exists:
it guards specifically against the `data_derived_watermark` failure mode
documented in docs/load_patterns.md -- a bad run once producing a
wrongly-timestamped row silently causes every later incremental run to
skip real data in between, and a normally-succeeding run never surfaces
that on its own (the append logic only ever looks forward from its own
last timestamp, never back). This check makes an existing gap visible;
it can't prevent one from being created in the first place.

Not applied to the `redispatch` group's `full_refresh` assets -- a bad
run there doesn't compound silently across future runs the same way,
since the next run re-fetches everything regardless of what's on disk.

**By design, this check does not auto-heal anything it finds.** A found
gap is a signal for a deliberate, manual, one-off backfill decision, not
something a routine run retries on its own -- an automatic retry-forever
would be indistinguishable, for a permanently-missing source range, from
a broken loop hammering an API for data that will never appear. See the
2026-09-23 conversation/commit for the reasoning.

**Known gaps are allowlisted, not perpetually red.** Every timestamp in
`known_gaps.KNOWN_GAPS` is excluded from what this check flags -- see
that file for the investigation behind each entry and why a check that
stays red forever for an understood issue is actively harmful (it
trains you to ignore red, which then hides a genuinely new gap). This
check only fails for gaps *not* in that registry -- a red result here
always means something new, never "yeah, that old thing again".
"""

import pandas as pd
from dagster import AssetCheckExecutionContext, AssetCheckResult, AssetCheckSeverity, asset_check

from edh.paths import smard_file
from edh_dagster.assets.smard import SMARD_SERIES
from edh.known_gaps import KNOWN_GAPS


def _make_hourly_gap_check(asset_name: str):
    file_name = asset_name.removeprefix("smard_")
    known_gap_index = pd.DatetimeIndex(sorted(KNOWN_GAPS.get(asset_name, {})))

    @asset_check(
        asset=asset_name,
        name="hourly_gap_check",
        description=(
            "Every hour between this series' min and max timestamp is "
            "present in the output, except gaps allowlisted in "
            "known_gaps.py -- no *new, unexplained* gaps from a "
            "historically mis-derived watermark or other cause. See "
            "docs/load_patterns.md and known_gaps.py."
        ),
    )
    def _check(context: AssetCheckExecutionContext) -> AssetCheckResult:
        output_file = smard_file(file_name)
        if not output_file.exists():
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.WARN,
                description="Never materialized -- nothing to check yet.",
            )

        df = pd.read_parquet(output_file)
        if df.empty:
            return AssetCheckResult(passed=True, description="Empty series -- vacuously no gaps.")

        expected = pd.date_range(df.index.min(), df.index.max(), freq="h")
        missing = expected.difference(df.index)
        unexpected = missing.difference(known_gap_index)
        known = missing.intersection(known_gap_index)

        return AssetCheckResult(
            passed=len(unexpected) == 0,
            severity=AssetCheckSeverity.ERROR,
            description=(
                f"{len(known)} known/allowlisted gap(s), {len(unexpected)} new/unexpected gap(s)."
                if len(unexpected) == 0
                else f"{len(unexpected)} NEW, unallowlisted gap(s) found -- investigate before adding to known_gaps.py."
            ),
            metadata={
                "unexpected_missing_hours": len(unexpected),
                "known_allowlisted_hours": len(known),
                "expected_hours": len(expected),
                "actual_hours": len(df),
                "first_unexpected": str(unexpected[0]) if len(unexpected) else "n/a",
                "last_unexpected": str(unexpected[-1]) if len(unexpected) else "n/a",
            },
        )

    return _check


hourly_gap_checks = [_make_hourly_gap_check(name) for name in SMARD_SERIES]
