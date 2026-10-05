"""Balancing-market prices as Dagster software-defined assets: reBAP,
NRV-Saldo, ID-AEP, and AEP Module 1/2/3 (all netztransparenz.de), plus
FCR/aFRR capacity prices (regelleistung.net).

reBAP/FCR/aFRR migrated 2026-09-30 from ~/research/vpp-learning (which
already had these downloaded and validated -- see `edh/rebap.py` /
`edh/regelleistung.py` for the source-mechanism provenance). NRV-Saldo,
ID-AEP, and AEP Module 1/2/3 migrated 2026-10-05 from
`~/research/energy-research` (`pipeline/08_download_nrv_saldo.py`,
`15_download_id_aep.py`, `19_download_aep_modules.py`), which discovered
each endpoint and validated the full picture: `max`/`min(Module1, Module2,
Module3)` reconstructs real reBAP to 99.99% exact match (see
`energy-research`'s `20_rebap_exact_reconstruction.py`) -- so this hub now
has every piece of reBAP's own published calculation formula, not just the
final price. All physically distinct from `smard_price_*` (SMARD's
day-ahead auction price) -- these are the imbalance-settlement side of the
market -- hence their own asset group rather than folding into `smard`.

**Load pattern: `data_derived_watermark`** for all six, same shape as
the `smard`/`gas` groups (see `docs/load_patterns.md`): each run reads its
own current output back, derives a watermark from it, and appends only
what's newer.

- `rebap_price` / `nrv_saldo` / `aep_modules`: watermark =
  `existing.index.max() - REBAP_REFRESH_WINDOW_DAYS` (14 days), **not**
  `existing.index.max() + 15min` -- all three share reBAP's own
  "qualitätsgesichert" settlement pipeline and its ~2-week publication lag,
  and the most recent days often come back as `N.A.`/`N.E.` before that.
  Caught empirically 2026-09-30 for `rebap_price`: a batch of days
  (2026-07-29..07-31) had come back `N.A.` on an earlier run, but had real
  values on request ~2 months later -- *while newer days in between
  already had real values*, i.e. the publication lag isn't strictly
  monotonic. A plain trailing-`NaN` trim would have missed that gap
  forever, since it wasn't at the tail by the time anything re-checked it.
  The 14-day rolling re-fetch is cheap (one extra small request every run)
  and self-heals this whole class of gap; any `NaN` still left at the very
  end after that re-fetch (data genuinely not published yet) is trimmed
  before writing, so the file itself never contains a placeholder value.
  `nrv_saldo`/`aep_modules` reuse the exact same window constant, same
  underlying settlement timing.
- `id_aep`: much shorter watermark window (`ID_AEP_REFRESH_WINDOW_DAYS`,
  3 days) -- ID-AEP has effectively no publication lag (confirmed
  empirically: a same-day request already returns real values through the
  most recent completed quarter-hour), so there's no reBAP-style "long tail
  of N.A." to chase, just a small safety margin against any late
  correction.
- `fcr_capacity_price` / `afrr_capacity_price`: watermark = the **first
  day of** the month containing `existing.index.max()`, not the day
  after it -- regelleistung.net's monthly RESULT_OVERVIEW file for the
  current month keeps being republished as more of that month's tenders
  clear, so the whole current month is always re-fetched and overwritten
  rather than treated as final the moment any day in it first appears.
  Only fully-past months are treated as immutable.

**Resolution:** reBAP/NRV-Saldo/ID-AEP/AEP-Module are all 15-min; FCR/aFRR
capacity prices are one row per calendar day (each column already a
4-hour delivery block within that day) -- not hourly, so none of these get
the `smard`-style `hourly_gap_check`; see
`edh_dagster/checks/balancing_market.py` for their own gap checks instead.

**Timestamps:** naive, representing true UTC throughout (same hub-wide
convention as `smard`/PECD/etc.) -- every netztransparenz.de source row
here carries an explicit CET/CEST label per row, converted exactly (no
DST-transition ambiguity, same approach as `edh/redispatch_measures.py`);
FCR/aFRR's `delivery_date` is UTC midnight -- but that's the *index* only,
see the block-columns caveat directly below, it does not apply to
`negpos_00_04` etc. themselves.

**⚠️ FCR/aFRR block columns (`negpos_00_04`, `neg_00_04`, `pos_00_04`, ...)
are German local clock time (CET/CEST), not UTC or a fixed 4-hour UTC
span.** Confirmed via regelleistung.net's own FCR Cooperation
documentation: "the duration of product delivery is usually 4 hours,
**subject to daylight saving time shift**" -- i.e. on the two DST-transition
days per year, one block is genuinely only 3 (spring) or 5 (autumn) real
UTC hours long, not 4, because the block boundaries are pinned to German
wall-clock hours, not to UTC. **Practical consequence: do not naively
treat `negpos_08_12` as "08:00-12:00 UTC" or as a fixed-duration 4-hour
UTC window when joining this against an hourly UTC series (e.g.
`smard_price_de_lu`) or computing an energy-weighted average from the
capacity price** -- convert the block's nominal CET/CEST hours to UTC for
the specific `delivery_date` in question first (accounting for that day's
actual CET/CEST offset), the same way `edh/rebap.py` and
`edh/redispatch_measures.py` already do for their own per-row local-time
fields. Verified against regelleistung.net's own market-design
documentation for the shared FCR/aFRR "EFA block" structure and gate
closure times (stated as `CET`/`CEST`, adjusting for DST) -- not
independently re-derived by inspecting individual DST-transition rows in
this hub's own downloaded files.

**Units:** reBAP/ID-AEP/AEP-Module in EUR/MWh (energy prices); NRV-Saldo in
MW (a power, not a price -- positive = under-supplied, negative =
over-supplied); FCR/aFRR in EUR/MW/h (a capacity price, per hour of the 4h
block) -- none of these are directly comparable to each other or to
`smard_price_*` without knowing which market mechanism each belongs to,
see each asset's description.

**⚠️ `aep_modules`' Module 3 column has a raw-data quirk, already corrected
in `edh/aep_modules.py` before this asset ever sees it**: netztransparenz.de
encodes "doesn't apply" as a literal `0.0` 99.93% of the time instead of
the expected `N.A.`-style placeholder -- `edh.aep_modules.download_aep_modules`
converts that to NaN already, so `aep_module3_eur_mwh` NaN correctly means
"excluded from any max/min across modules" throughout this hub. See that
module's docstring for how this was found (it silently broke an early
version of `energy-research`'s reBAP reconstruction).

**Deliberately no schedule** -- consistent with every other asset in this
hub (materialization is manual via the UI/CLI); see README.md's "Running
it" section.
"""

from datetime import date, timedelta

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from edh.aep_modules import AEP_MODULES_START_DATE, download_aep_modules
from edh.id_aep import ID_AEP_START_DATE, download_id_aep
from edh.nrv_saldo import NRV_SALDO_START_DATE, download_nrv_saldo
from edh.paths import (
    afrr_capacity_price_file,
    aep_modules_file,
    fcr_capacity_price_file,
    id_aep_file,
    nrv_saldo_file,
    rebap_price_file,
)
from edh.rebap import REBAP_START_DATE, download_rebap
from edh.regelleistung import AFRR_START_DATE, FCR_START_DATE, download_afrr_capacity_prices, download_fcr_prices

REBAP_REFRESH_WINDOW_DAYS = 14  # see module docstring -- publication lag isn't strictly monotonic
ID_AEP_REFRESH_WINDOW_DAYS = 3  # see module docstring -- ID-AEP has effectively no publication lag


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


def _make_netztransparenz_watermark_asset(
    asset_name: str,
    description: str,
    metadata: dict,
    output_file_fn,
    download_fn,
    start_date: date,
    refresh_window_days: int,
    trim_column: str,
):
    """Shared shape for `nrv_saldo`/`id_aep`/`aep_modules` -- same rolling-
    window-watermark + trim-trailing-NaN logic as `rebap_price` above
    (hand-rolled there since it was the first of this shape), factored out
    here since these three are otherwise near-identical. `trim_column` is
    the column whose `last_valid_index()` decides how much trailing
    not-yet-published data to drop."""

    @asset(
        name=asset_name,
        group_name="balancing_market",
        kinds={"api", "parquet"},
        tags={"load_pattern": "data_derived_watermark"},
        description=description,
        metadata=metadata,
    )
    def _asset(context: AssetExecutionContext) -> None:
        output_file = output_file_fn()
        existing = pd.read_parquet(output_file) if output_file.exists() else None

        if existing is not None and not existing.empty:
            start = existing.index.max().date() - timedelta(days=refresh_window_days)
            start = max(start, start_date)
        else:
            start = start_date
        end = date.today() + timedelta(days=1)  # download_fn's date_to is exclusive -- +1 to include today

        new_data = download_fn(start, end)
        combined = pd.concat([existing, new_data]) if existing is not None and not existing.empty else new_data
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()

        n_before_trim = len(combined)
        last_valid = combined[trim_column].last_valid_index()
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

    return _asset


nrv_saldo = _make_netztransparenz_watermark_asset(
    "nrv_saldo",
    description=(
        "German NRV-Saldo (Netzregelverbund-Saldo / GCC balance), quarter-hourly, from netztransparenz.de -- "
        "Germany-wide aggregate real-time imbalance (positive = under-supplied/short, negative = "
        "over-supplied/long). The direct, physically-measured counterpart to inferring imbalance direction "
        "indirectly from reBAP's spread against another price -- and the same quantity reBAP's own published "
        "calculation formula calls 'Balance_GCC' (see `aep_modules`). Idempotent, unpartitioned: re-fetches a "
        "trailing 14-day window every run (same settlement pipeline/lag as reBAP), drops any still-not-yet-"
        "published rows at the very end rather than persisting them as NaN. ⚠️ Known real multi-month gaps "
        "2014-2022 (not a download bug) -- see `edh/nrv_saldo.py` module docstring."
    ),
    metadata={
        "source": "netztransparenz.de (joint German TSO transparency platform)",
        "source_url": MetadataValue.url("https://www.netztransparenz.de/de-de/Regelenergie/NRV-und-RZ-Saldo/NRV-Saldo-viertelstuendlich"),
        "region": "DE",
        "resolution": "15 min (quarter-hourly)",
        "unit": "MW",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": (
            "data_derived_watermark: idempotent append, no partitions -- always re-fetches a trailing 14-day "
            "window every run (same lag profile as reBAP), and drops any still-NaN rows at the very end before "
            "writing rather than persisting a placeholder"
        ),
    },
    output_file_fn=nrv_saldo_file,
    download_fn=download_nrv_saldo,
    start_date=NRV_SALDO_START_DATE,
    refresh_window_days=REBAP_REFRESH_WINDOW_DAYS,
    trim_column="nrv_saldo_mw",
)

id_aep = _make_netztransparenz_watermark_asset(
    "id_aep",
    description=(
        "German ID-AEP (Index Ausgleichsenergiepreis / \"IP-Index\"), quarter-hourly, from netztransparenz.de -- "
        "volume-weighted average of the last continuous-intraday trades before delivery (closes 5 minutes "
        "before T), one of the three direct inputs to reBAP's own published calculation formula (see "
        "`aep_modules`'s Module 2). Idempotent, unpartitioned: re-fetches a trailing 3-day window every run -- "
        "much shorter than reBAP's 14 days, since ID-AEP has effectively no publication lag (confirmed "
        "empirically). NaN where the index is undefined (too little intraday trading volume that quarter-hour)."
    ),
    metadata={
        "source": "netztransparenz.de (joint German TSO transparency platform)",
        "source_url": MetadataValue.url("https://www.netztransparenz.de/de-de/Regelenergie/Ausgleichsenergiepreis/Index-Ausgleichsenergiepreis"),
        "region": "DE",
        "resolution": "15 min (quarter-hourly)",
        "unit": "EUR/MWh",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": (
            "data_derived_watermark: idempotent append, no partitions -- re-fetches a trailing 3-day window "
            "every run (effectively no publication lag, unlike reBAP/NRV-Saldo/AEP-Module's ~2-week delay)"
        ),
    },
    output_file_fn=id_aep_file,
    download_fn=download_id_aep,
    start_date=ID_AEP_START_DATE,
    refresh_window_days=ID_AEP_REFRESH_WINDOW_DAYS,
    trim_column="id_aep_eur_mwh",
)

aep_modules = _make_netztransparenz_watermark_asset(
    "aep_modules",
    description=(
        "German AEP Module 1/2/3, quarter-hourly, from netztransparenz.de -- the three published components "
        "reBAP is the max (short)/min (long) of: Module 1 (real PICASSO/MARI aFRR/mFRR activation prices), "
        "Module 2 (ID-AEP +/- a minimum distance, see `id_aep`), Module 3 (scarcity component, active only "
        "above 80% of dimensioned FRR capacity). Together with `nrv_saldo` and `id_aep`, this reconstructs real "
        "reBAP to 99.99% exact match (energy-research's `20_rebap_exact_reconstruction.py`). Idempotent, "
        "unpartitioned: re-fetches a trailing 14-day window every run (same settlement pipeline/lag as reBAP). "
        "⚠️ `aep_module3_eur_mwh`'s raw 0.0-means-inactive quirk is already corrected to NaN -- see "
        "`edh/aep_modules.py` module docstring."
    ),
    metadata={
        "source": "netztransparenz.de (joint German TSO transparency platform)",
        "source_url": MetadataValue.url("https://www.netztransparenz.de/de-de/Regelenergie/Ausgleichsenergiepreis/AEP-Module"),
        "region": "DE",
        "resolution": "15 min (quarter-hourly)",
        "unit": "EUR/MWh",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": (
            "data_derived_watermark: idempotent append, no partitions -- always re-fetches a trailing 14-day "
            "window every run (same lag profile as reBAP), and drops any still-NaN rows (by Module 1, the "
            "always-present column) at the very end before writing rather than persisting a placeholder"
        ),
    },
    output_file_fn=aep_modules_file,
    download_fn=download_aep_modules,
    start_date=AEP_MODULES_START_DATE,
    refresh_window_days=REBAP_REFRESH_WINDOW_DAYS,
    trim_column="aep_module1_eur_mwh",
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
            "any fully-past months missing since the last materialization -- see module docstring. "
            "⚠️ Block columns are CET/CEST (German local clock time), NOT UTC -- see 'block_timezone_warning' "
            "metadata below before joining this against any UTC-indexed series."
        ),
        metadata={
            "source": "regelleistung.net CRDS API",
            "source_url": MetadataValue.url("https://www.regelleistung.net/apps/crds"),
            "region": "DE",
            "resolution": "daily (one row per delivery day; each column is a 4-hour delivery block)",
            "unit": "EUR/MW/h",
            "timestamp_timezone": "index (delivery_date) is naive, represents UTC midnight -- the BLOCK COLUMNS do not, see below",
            "block_timezone_warning": MetadataValue.md(
                "**⚠️ `negpos_00_04`/`neg_00_04`/`pos_00_04` etc. are German local clock time (CET/CEST), not "
                "UTC.** regelleistung.net's own FCR Cooperation documentation: block delivery duration is "
                "\"usually 4 hours, subject to daylight saving time shift\" -- on the two DST-transition days "
                "per year, one block is really only 3 (spring) or 5 (autumn) UTC hours, not 4, because the "
                "block boundaries are pinned to German wall-clock hours. **Do not treat `..._08_12` as "
                "\"08:00-12:00 UTC\"** when joining against an hourly-UTC series (e.g. `smard_price_de_lu`) or "
                "computing an energy-weighted average -- convert each block's nominal CET/CEST hours to UTC "
                "for that specific `delivery_date` first, the same way `edh/rebap.py` does for its own "
                "per-row local-time field. See module docstring for sourcing."
            ),
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


balancing_market_assets = [rebap_price, nrv_saldo, id_aep, aep_modules, fcr_capacity_price, afrr_capacity_price]
