"""TTF natural gas futures price as a Dagster software-defined asset.

See `edh/ttf_gas.py` for the source (Yahoo Finance's `TTF=F`, via
`yfinance`) and provenance (migrated from ~/research/world-of-energy,
2026-09-24).

**Load pattern: `data_derived_watermark`**, same shape as the `smard`
group: each run reads its own current output back, resumes from
`existing.index.max() + 1 day`, and appends only what's newer -- see
`docs/load_patterns.md`. Chosen over `full_refresh` because Yahoo
Finance's historical daily closes for a futures contract aren't expected
to be retroactively revised (unlike e.g. SMARD's redispatch data) --
not independently verified beyond ordinary observation, flagged here as
an assumption rather than a confirmed fact.

**Deliberately no schedule.** Every other asset in this hub also has no
Dagster `ScheduleDefinition` wired up today (materialization is manual
via the UI/CLI) -- this asset doesn't change that; explicitly requested
to stay manual-trigger-only rather than added to some future daily cron.

**Resolution:** daily, trading days only -- Yahoo Finance simply has no
rows for weekends/exchange holidays, not a gap in the usual sense. This
is why there's no `hourly_gap_check`-style strict per-calendar-day check
here (see `edh_dagster/checks/gas.py`): a naive "every day between min
and max" check would permanently flag every weekend as a gap. Instead
`ttf_gas_no_large_gap_check` only flags stretches with no data at all
longer than what a long holiday weekend could plausibly explain -- aimed
squarely at the real risk (a poisoned/future-dated watermark silently
skipping real data), not at benign market closures.

**Timestamps:** naive daily `date`, no time-of-day -- yfinance's
tz-aware exchange-local index is stripped to a bare calendar date in
`edh/ttf_gas.py::download_prices`.

**Units:** EUR/MWh for `open`/`high`/`low`/`close` -- read directly from
Yahoo Finance's own quote for `TTF=F` (denominated in EUR/MWh), not
independently cross-checked against another source. `volume` is
contracts traded, not an energy unit.
"""

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from edh.paths import ttf_gas_prices_file
from edh.ttf_gas import TTF_TICKER, download_prices


@asset(
    group_name="gas",
    kinds={"api", "parquet"},
    tags={"load_pattern": "data_derived_watermark"},
    description=(
        "Daily TTF (Title Transfer Facility) natural gas futures OHLCV, "
        "from Yahoo Finance (`TTF=F`). Idempotent, unpartitioned: each "
        "run fills in whatever trading days are missing since the last "
        "materialization, through the latest available close -- see "
        "module docstring."
    ),
    metadata={
        "source": "Yahoo Finance",
        "source_url": MetadataValue.url(f"https://finance.yahoo.com/quote/{TTF_TICKER}"),
        "region": "NL (TTF hub, EU benchmark gas price)",
        "resolution": "daily (trading days only)",
        "unit": "EUR/MWh (open/high/low/close); volume: contracts traded",
        "timestamp_timezone": "naive calendar date, no time-of-day",
        "update_pattern": "data_derived_watermark: idempotent append, no partitions -- backfills to latest close every run",
    },
)
def ttf_gas_price(context: AssetExecutionContext) -> None:
    output_file = ttf_gas_prices_file()
    existing = pd.read_parquet(output_file) if output_file.exists() else None

    start_date = None
    if existing is not None and not existing.empty:
        start_date = (existing.index.max() + pd.Timedelta(days=1)).to_pydatetime()

    new_data = download_prices(start_date=start_date)

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
            "min_date": MetadataValue.text(str(combined.index.min().date()) if len(combined) else "n/a"),
            "max_date": MetadataValue.text(str(combined.index.max().date()) if len(combined) else "n/a"),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(combined.tail(5).to_markdown()) if len(combined) else MetadataValue.md("*empty*"),
        }
    )


gas_assets = [ttf_gas_price]
