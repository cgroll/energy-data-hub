"""Balancing-market prices as Dagster software-defined assets: reBAP
(netztransparenz.de) and FCR/aFRR capacity prices (regelleistung.net).

Migrated 2026-09-30 from ~/research/vpp-learning (which already had these
downloaded and validated -- see `edh/rebap.py` / `edh/regelleistung.py` for
the source-mechanism provenance). All three are physically distinct from
`smard_price_*` (SMARD's day-ahead auction price) -- reBAP is the imbalance
settlement price, FCR/aFRR are capacity procurement prices from a separate
market entirely -- hence their own asset group rather than folding into
`smard`.

**Load pattern: `data_derived_watermark`** for all three, same shape as
the `smard`/`gas` groups (see `docs/load_patterns.md`): each run reads its
own current output back, derives a watermark from it, and appends only
what's newer.

- `rebap_price`: watermark = `existing.index.max() - REBAP_REFRESH_WINDOW_DAYS`
  (14 days), **not** `existing.index.max() + 15min` -- reBAP's most recent
  days often come back as `N.A.` ("quality-assured price not yet
  published"), and a plain "append what's newer" watermark would trust
  and permanently keep whatever was seen first. Caught empirically
  2026-09-30: a batch of days (2026-07-29..07-31) had come back `N.A.` on
  an earlier run, but had real values on request ~2 months later --
  *while newer days in between already had real values*, i.e. the
  publication lag isn't strictly monotonic. A plain trailing-`NaN` trim
  (this asset's first design) would have missed that gap forever, since
  it wasn't at the tail by the time anything re-checked it. The 14-day
  rolling re-fetch is cheap (one extra small request every run) and
  self-heals this whole class of gap, not just an always-NaN tail; any
  `NaN` still left at the very end after that re-fetch (data for "today"
  genuinely not published yet) is trimmed before writing, so the file
  itself never contains a placeholder value.
- `fcr_capacity_price` / `afrr_capacity_price`: watermark = the **first
  day of** the month containing `existing.index.max()`, not the day
  after it -- regelleistung.net's monthly RESULT_OVERVIEW file for the
  current month keeps being republished as more of that month's tenders
  clear, so the whole current month is always re-fetched and overwritten
  rather than treated as final the moment any day in it first appears.
  Only fully-past months are treated as immutable.

**Resolution:** reBAP is 15-min; FCR/aFRR capacity prices are one row per
calendar day (each column already a 4-hour delivery block within that
day) -- not hourly, so none of these get the `smard`-style
`hourly_gap_check`; see `edh_dagster/checks/balancing_market.py` for their
own gap checks instead.

**Timestamps:** naive, representing true UTC throughout (same hub-wide
convention as `smard`/PECD/etc.) -- reBAP's source rows carry an explicit
CET/CEST label per row, converted exactly (no DST-transition ambiguity,
same approach as `edh/redispatch_measures.py`); FCR/aFRR's `delivery_date`
is UTC midnight.

**Units:** reBAP in EUR/MWh (an energy price); FCR/aFRR in EUR/MW/h (a
capacity price, per hour of the 4h block) -- these are not
directly comparable to each other or to `smard_price_*` without knowing
which market mechanism each belongs to, see each asset's description.

**Deliberately no schedule** -- consistent with every other asset in this
hub (materialization is manual via the UI/CLI); see README.md's "Running
it" section.
"""

from datetime import date, timedelta

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from edh.paths import afrr_capacity_price_file, fcr_capacity_price_file, rebap_price_file
from edh.rebap import REBAP_START_DATE, download_rebap
from edh.regelleistung import AFRR_START_DATE, FCR_START_DATE, download_afrr_capacity_prices, download_fcr_prices

REBAP_REFRESH_WINDOW_DAYS = 14  # see module docstring -- publication lag isn't strictly monotonic


@asset(
    group_name="balancing_market",
    kinds={"api", "parquet"},
    tags={"load_pattern": "data_derived_watermark"},
    description=(
        "German reBAP (Ausgleichsenergiepreis / balancing energy price), quarter-hourly, from "
        "netztransparenz.de. Symmetric by design (same price applies whether a balancing group was short or "
        "long), except in rare capacity-reserve (KapRes) activation quarter-hours -- see "
        "`rebap_ueberdeckt_eur_mwh` for the counterpart series. Idempotent, unpartitioned: each run always "
        "re-fetches a trailing 14-day window (publication isn't strictly monotonic -- see module docstring), "
        "then fills in everything missing since then through today, dropping any still-not-yet-published rows "
        "at the very end rather than persisting them as NaN."
    ),
    metadata={
        "source": "netztransparenz.de (joint German TSO transparency platform)",
        "source_url": MetadataValue.url("https://www.netztransparenz.de/de-de/Regelenergie/Ausgleichsenergiepreis/reBAP"),
        "region": "DE",
        "resolution": "15 min (quarter-hourly)",
        "unit": "EUR/MWh",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": (
            "data_derived_watermark: idempotent append, no partitions -- always re-fetches a trailing 14-day "
            "window every run (publication lag isn't strictly monotonic, see module docstring), and drops any "
            "still-NaN rows at the very end before writing rather than persisting a placeholder"
        ),
    },
)
def rebap_price(context: AssetExecutionContext) -> None:
    output_file = rebap_price_file()
    existing = pd.read_parquet(output_file) if output_file.exists() else None

    if existing is not None and not existing.empty:
        # Always re-request a trailing REBAP_REFRESH_WINDOW_DAYS window, not
        # just "the day after the last stored row" -- caught empirically
        # 2026-09-30: a batch of days (2026-07-29..07-31) came back `N.A.`
        # on one run, but had real values a couple of months later on
        # request, *while newer days in between already had real values*
        # -- i.e. the publication lag isn't strictly monotonic, a plain
        # trailing-NaN trim would have permanently missed this specific
        # gap since it wasn't at the tail by the time it was checked. A 14
        # day rolling re-fetch is cheap (one extra small request) and
        # self-heals this whole class of gap, not just the always-NaN tail.
        start_date = existing.index.max().date() - timedelta(days=REBAP_REFRESH_WINDOW_DAYS)
        start_date = max(start_date, REBAP_START_DATE)
    else:
        start_date = REBAP_START_DATE
    end_date = date.today() + timedelta(days=1)  # download_rebap's date_to is exclusive -- +1 to include today

    new_data = download_rebap(start_date, end_date)
    combined = pd.concat([existing, new_data]) if existing is not None and not existing.empty else new_data
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()

    n_before_trim = len(combined)
    last_valid = combined["rebap_eur_mwh"].last_valid_index()
    combined = combined.loc[:last_valid] if last_valid is not None else combined.iloc[0:0]
    n_trimmed = n_before_trim - len(combined)

    combined.to_parquet(output_file)

    context.add_output_metadata(
        {
            "dagster/row_count": len(combined),
            "new_rows": len(new_data),
            "trailing_not_yet_published_rows_dropped": n_trimmed,
            "min_timestamp": MetadataValue.text(str(combined.index.min()) if len(combined) else "n/a"),
            "max_timestamp": MetadataValue.text(str(combined.index.max()) if len(combined) else "n/a"),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(combined.tail(5).to_markdown()) if len(combined) else MetadataValue.md("*empty*"),
        }
    )


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _make_regelleistung_asset(asset_name: str, human_name: str, output_file_fn, download_fn, start_date: date, columns_desc: str):
    @asset(
        name=asset_name,
        group_name="balancing_market",
        kinds={"api", "parquet"},
        tags={"load_pattern": "data_derived_watermark"},
        description=(
            f"{human_name}, Germany, one row per delivery day, {columns_desc}, from regelleistung.net's public "
            "CRDS API (monthly RESULT_OVERVIEW Excel files). Idempotent, unpartitioned: each run always "
            "re-downloads and overwrites the current (potentially still-updating) month in full, then fills in "
            "any fully-past months missing since the last materialization -- see module docstring."
        ),
        metadata={
            "source": "regelleistung.net CRDS API",
            "source_url": MetadataValue.url("https://www.regelleistung.net/apps/crds"),
            "region": "DE",
            "resolution": "daily (one row per delivery day; each column is a 4-hour delivery block)",
            "unit": "EUR/MW/h",
            "timestamp_timezone": "naive, represents UTC midnight",
            "update_pattern": (
                "data_derived_watermark: idempotent append, no partitions -- watermark is the first day of the "
                "month containing the existing max date, so the current month is always re-fetched in full "
                "rather than frozen the moment it first appears"
            ),
        },
    )
    def _asset(context: AssetExecutionContext) -> None:
        output_file = output_file_fn()
        existing = pd.read_parquet(output_file) if output_file.exists() else None

        range_start = _month_start(existing.index.max().date()) if existing is not None and not existing.empty else start_date
        range_end = date.today()

        new_data = download_fn(range_start, range_end)
        combined = pd.concat([existing, new_data]) if existing is not None and not existing.empty else new_data
        if not combined.empty:
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()

        combined.to_parquet(output_file)

        context.add_output_metadata(
            {
                "dagster/row_count": len(combined),
                "new_or_refreshed_rows": len(new_data),
                "min_date": MetadataValue.text(str(combined.index.min().date()) if len(combined) else "n/a"),
                "max_date": MetadataValue.text(str(combined.index.max().date()) if len(combined) else "n/a"),
                "path": MetadataValue.path(str(output_file)),
                "preview": MetadataValue.md(combined.tail(5).to_markdown()) if len(combined) else MetadataValue.md("*empty*"),
            }
        )

    return _asset


fcr_capacity_price = _make_regelleistung_asset(
    "fcr_capacity_price",
    "FCR (Frequency Containment Reserve / PRL) settlement capacity price",
    fcr_capacity_price_file,
    download_fcr_prices,
    FCR_START_DATE,
    "one column per 4-hour block (`negpos_00_04` ... `negpos_20_24`)",
)

afrr_capacity_price = _make_regelleistung_asset(
    "afrr_capacity_price",
    "aFRR (automatic Frequency Restoration Reserve / SRL) marginal capacity price",
    afrr_capacity_price_file,
    download_afrr_capacity_prices,
    AFRR_START_DATE,
    "one column per direction x 4-hour block (`neg_00_04` ... `pos_20_24`)",
)


balancing_market_assets = [rebap_price, fcr_capacity_price, afrr_capacity_price]
