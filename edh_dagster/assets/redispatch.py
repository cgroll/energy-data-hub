"""Redispatch curtailment data as Dagster software-defined assets.

Two independent, complementary sources (see edh/smard_redispatch.py and
edh/redispatch_measures.py docstrings for why both exist): SMARD's coarse
monthly by-energy-source series, and netztransparenz's richer per-measure
export.

**Not hourly, unlike the `smard` group:** `smard_redispatch_by_source` is
monthly; `redispatch_measures` has real sub-hourly (down to 15-minute)
start/end timestamps per measure, not a regular grid at all.

**Timestamps:** `smard_redispatch_by_source`'s `month` column is a bare
calendar date (no time-of-day, no timezone concept applies).
`redispatch_measures`'s `start`/`end` are parsed directly from
netztransparenz's CSV date+time fields with no explicit UTC conversion --
assumed to be Europe/Berlin wall-clock (a German regulatory publication),
but this has **not** been verified against DST-transition behavior in the
data. Flagging as an open caveat rather than a confirmed fact.

**Units:** `gwh` (smard_redispatch_by_source) and `mwh`
(redispatch_measures) are self-documenting via their own column names --
no separate unit lookup needed, unlike the `smard` group's bare series
names.

**Load pattern: `full_refresh`, not `data_derived_watermark` like the
`smard` group** -- both assets re-download their *entire* series every
run and overwrite the output in full, rather than resuming from a last
timestamp. This is a real, deliberate difference, not an oversight: SMARD
does restate recent months of the by-source series after publication (an
incremental-append would freeze a restated month at whatever value was
first observed), and netztransparenz's per-measure export has no
delta/incremental endpoint at all. Both are cheap enough (a few hundred
rows / a few MB) that re-fetching in full every run is simply the correct
approach here -- see docs/load_patterns.md and BEST_PRACTICES.md.

**No `hourly_gap_check` here** (unlike the `smard` group): that check
exists specifically for the `data_derived_watermark` failure mode (a
poisoned watermark silently skipping data forever); `full_refresh` assets
re-fetch everything every run regardless of what's on disk, so they
can't accumulate a silent gap the same way.

**Idempotent, unpartitioned** in the same sense as `smard_*`: safe to
re-run any time, always produces the current full series -- just without
an append step, since there's nothing to append to.
"""

from dagster import AssetExecutionContext, MetadataValue, asset

from edh.paths import DATA_ROOT
from edh.redispatch_measures import REDISPATCH_PAGE_URL, download_redispatch_measures, load_redispatch_measures
from edh.smard_redispatch import TOPIC_ARTICLE_URL, download_redispatch_by_source

REDISPATCH_DIR = DATA_ROOT / "redispatch"


@asset(
    group_name="redispatch",
    kinds={"api", "parquet"},
    tags={"load_pattern": "full_refresh"},
    description=(
        "Monthly Redispatch 2.0 volume by energy source (SMARD), since "
        "2022-07 -- national totals only, no per-TSO/per-plant detail. "
        "Always re-fetched in full (not incremental-append) since SMARD "
        "restates recent months after publication -- see module "
        "docstring."
    ),
    metadata={
        "source": "SMARD (Bundesnetzagentur)",
        "source_url": MetadataValue.url(TOPIC_ARTICLE_URL),
        "resolution": "monthly",
        "region": "DE",
        "unit": "GWh (see 'gwh' column)",
        "timestamp_timezone": "bare calendar date, no time-of-day/timezone",
        "update_pattern": "full_refresh: always re-fetches the full series (SMARD restates recent months)",
    },
)
def smard_redispatch_by_source(context: AssetExecutionContext) -> None:
    REDISPATCH_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REDISPATCH_DIR / "smard_redispatch_by_source.parquet"

    redispatch = download_redispatch_by_source()
    redispatch.to_parquet(out_path)

    context.add_output_metadata(
        {
            "dagster/row_count": len(redispatch),
            "min_month": MetadataValue.text(str(redispatch["month"].min().date())),
            "max_month": MetadataValue.text(str(redispatch["month"].max().date())),
            "path": MetadataValue.path(str(out_path)),
            "preview": MetadataValue.md(redispatch.tail(5).to_markdown()) if len(redispatch) else MetadataValue.md("*empty*"),
        }
    )


@asset(
    group_name="redispatch",
    kinds={"api", "parquet"},
    tags={"load_pattern": "full_refresh"},
    description=(
        "Per-measure Redispatch 2.0 export (netztransparenz.de, Format "
        "5): real start/end timestamps (down to 15-minute steps), "
        "direction, total energy (mwh) plus average/max power (avg_mw, "
        "max_mw), affected plant/cluster name, coarse primary-energy-type "
        "flag, since 2021-01-01. Always re-fetched in full -- "
        "netztransparenz has no delta/incremental endpoint. Fetched via "
        "a simulated ASP.NET WebForms postback against the page's own "
        "'Download CSV' button, not a documented API -- see "
        "edh/redispatch_measures.py."
    ),
    metadata={
        "source": "netztransparenz.de",
        "source_url": MetadataValue.url(REDISPATCH_PAGE_URL),
        "resolution": "irregular (real start/end per measure, ~15min steps)",
        "region": "DE",
        "unit": "mwh: MWh; avg_mw/max_mw: MW",
        "timestamp_timezone": (
            "Europe/Berlin wall-clock, CONFIRMED via the raw CSV's own "
            "ZEITZONE_VON/ZEITZONE_BIS (CEST/CET) columns -- see "
            "load_redispatch_measures() docstring for the ~2h/year DST-"
            "ambiguity caveat this doesn't resolve"
        ),
        "update_pattern": "full_refresh: always re-fetches the full series (no delta endpoint)",
    },
)
def redispatch_measures(context: AssetExecutionContext) -> None:
    REDISPATCH_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = REDISPATCH_DIR / "redispatch_measures_raw.csv"
    tidy_path = REDISPATCH_DIR / "redispatch_measures.parquet"

    csv_bytes = download_redispatch_measures()
    raw_path.write_bytes(csv_bytes)

    tidy = load_redispatch_measures(raw_path)
    tidy.to_parquet(tidy_path)

    context.add_output_metadata(
        {
            "dagster/row_count": len(tidy),
            "raw_bytes": len(csv_bytes),
            "min_start": MetadataValue.text(str(tidy["start"].min())),
            "max_start": MetadataValue.text(str(tidy["start"].max())),
            "path": MetadataValue.path(str(tidy_path)),
            "preview": MetadataValue.md(tidy.tail(5).to_markdown()) if len(tidy) else MetadataValue.md("*empty*"),
        }
    )


redispatch_assets = [smard_redispatch_by_source, redispatch_measures]
