"""regelleistung.net CRDS API client for FCR and aFRR capacity prices.

Downloads monthly RESULT_OVERVIEW Excel files from the public CRDS API and
extracts Germany's settlement/marginal capacity prices per 4-hour delivery
block, normalized to EUR/MW/h throughout (both products' raw files switch
from an EUR/MW-per-4h-block convention to EUR/MW/h partway through their
history -- see `_parse_fcr_results`/`_parse_afrr_capacity_results`).

Migrated 2026-09-30 from ~/research/vpp-learning/vpp/regelleistung.py
(unchanged logic, just the download-range helpers folded into
`download_fcr_prices`/`download_afrr_capacity_prices` themselves rather
than a separate pipeline script) -- see that repo's git history for the
original investigation into the CRDS API and the old/new column-name
formats.

FCR (PRL) 4-hour-block tender results available from 2021-01-01 (before
that, FCR used a single daily NEGPOS_00_24 product -- not covered here).
aFRR (SRL) capacity data available from 2018-10-01.

**⚠️ The 4-hour block columns (`negpos_00_04`, `neg_00_04`, `pos_00_04`,
...) are German local clock time (CET/CEST), NOT UTC**, even though the
`delivery_date` index itself is a plain, timezone-unambiguous calendar
date. Confirmed 2026-09-30 via regelleistung.net's own FCR Cooperation
documentation: block delivery duration is "usually 4 hours, subject to
daylight saving time shift" -- on the two DST-transition days per year,
one block is really only 3 (spring) or 5 (autumn) UTC hours, not 4,
because the boundaries are pinned to German wall-clock hours, not UTC.
**Do not treat `..._08_12` as "08:00-12:00 UTC"** -- e.g. when joining
against an hourly-UTC series (`smard_price_de_lu`) or computing an
energy-weighted average, convert each block's nominal CET/CEST hours to
UTC for that specific `delivery_date` first (same per-day CET/CEST
offset logic `edh/rebap.py` already applies). See
`edh_dagster/assets/balancing_market.py`'s `block_timezone_warning`
asset metadata for the same warning surfaced in the Dagster UI, and
README.md's "Known data-quality caveats".
"""

import warnings
from datetime import date, timedelta
from io import BytesIO
from typing import Literal

import pandas as pd
import requests

_BASE_URL = "https://www.regelleistung.net/apps/crds/api/v2"
_TIMEOUT_SHORT = 30
_TIMEOUT_LONG = 120

FCR_START_DATE = date(2021, 1, 1)
AFRR_START_DATE = date(2018, 10, 1)  # aligned with SMARD price series' own start

ProductType = Literal["FCR", "aFRR"]


def _list_monthly_result_files(
    product_type: ProductType,
    date_from: date,
    date_to: date,
    market: Literal["CAPACITY", "ENERGY"] = "CAPACITY",
) -> list[dict]:
    """Metadata dicts for monthly RESULTS files in the given date range."""
    resp = requests.get(
        f"{_BASE_URL}/tenders/files",
        params={"productTypes": product_type, "from": date_from.isoformat(), "to": date_to.isoformat()},
        timeout=_TIMEOUT_SHORT,
    )
    resp.raise_for_status()
    return [
        f for f in resp.json()
        if f["fileType"] == "RESULTS" and f["dateRangeType"] == "MONTH" and f["market"] == market
    ]


def _download_file(filename: str) -> bytes:
    resp = requests.get(f"{_BASE_URL}/tenders/files/{filename}", timeout=_TIMEOUT_LONG)
    resp.raise_for_status()
    return resp.content


def _last_day_of_month(d: date) -> date:
    next_month = d.replace(day=28) + timedelta(days=4)
    return next_month.replace(day=1) - timedelta(days=1)


def _iter_months(date_from: date, date_to: date):
    """Yield `(month_start, month_end)` pairs covering `date_from..date_to`,
    each clipped to the requested range."""
    cur = date_from.replace(day=1)
    while cur <= date_to:
        month_end = _last_day_of_month(cur)
        yield (max(date_from, cur), min(date_to, month_end))
        cur = (month_end + timedelta(days=1)).replace(day=1)


def _collect_files(product_type: ProductType, date_from: date, date_to: date, market: Literal["CAPACITY", "ENERGY"] = "CAPACITY") -> list[dict]:
    """Monthly RESULTS files; queries one month at a time (the API caps how
    wide a single `from`/`to` listing request can be)."""
    files: list[dict] = []
    seen: set[str] = set()
    for m_from, m_to in _iter_months(date_from, date_to):
        for f in _list_monthly_result_files(product_type, m_from, m_to, market):
            if f["fileName"] not in seen:
                files.append(f)
                seen.add(f["fileName"])
    return sorted(files, key=lambda x: x["dateRange"])


_FCR_DE_PRICE_CANDIDATES = [
    "GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW]",  # 2022-09+ (per-block, /4 below)
    "DE_SETTLEMENTCAPACITY_PRICE_[EUR/MW]",  # 2021-2022-08 (per-block, /4 below)
]


def _parse_fcr_results(content: bytes) -> pd.DataFrame:
    """Parse an FCR monthly RESULT_OVERVIEW_CAPACITY_MARKET_FCR Excel file.

    Handles both the old (pre-2022-09) abbreviated column name and the
    newer full-country-name column. Files still on the old 24-hour product
    (pre-2021, `NEGPOS_00_24`) are returned as an empty DataFrame.

    Returns a wide DataFrame indexed by `delivery_date` (UTC-midnight
    Timestamps), one column per 4-hour block: `negpos_00_04` ...
    `negpos_20_24`. Prices in EUR/MW/h (raw EUR/MW-per-block divided by 4).
    **The block columns are CET/CEST, not UTC -- see module docstring.**
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        xl = pd.ExcelFile(BytesIO(content))
        raw = xl.parse(xl.sheet_names[0], header=0)
    assert isinstance(raw, pd.DataFrame)
    df: pd.DataFrame = raw

    df = df[df["TENDER_NUMBER"] == 1].copy()  # type: ignore[assignment]
    if "NEGPOS_00_04" not in df["PRODUCTNAME"].values:
        return pd.DataFrame()

    raw_col = next((c for c in _FCR_DE_PRICE_CANDIDATES if c in df.columns), None)
    if raw_col is None:
        raise ValueError(f"Germany price column not found. Columns: {list(df.columns)}")

    df["price"] = pd.to_numeric(df[raw_col], errors="coerce") / 4  # type: ignore[operator]
    df["col"] = df["PRODUCTNAME"].str.lower()  # negpos_00_04, ...
    df["delivery_date"] = pd.to_datetime(df["DATE_FROM"], utc=True).dt.normalize()

    return (
        df[["delivery_date", "col", "price"]]
        .pivot(index="delivery_date", columns="col", values="price")
        .rename_axis(None, axis="columns")
        .rename_axis("delivery_date")
    )


_AFRR_DE_MARGINAL_PER_HOUR = "GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]"  # 2022+ (already EUR/MW/h)
_AFRR_DE_MARGINAL_PER_BLOCK = "GERMANY_MARGINAL_CAPACITY_PRICE_[EUR/MW]"  # pre-2022 (per-block, /4 below)


def _parse_afrr_capacity_results(content: bytes) -> pd.DataFrame:
    """Parse an aFRR monthly RESULT_OVERVIEW_CAPACITY_MARKET_aFRR Excel file.

    Handles both the old format (price per 4h block, normalized by /4) and
    the new format (price already per hour).

    Returns a wide DataFrame indexed by `delivery_date` (UTC-midnight
    Timestamps), one column per direction x block: `neg_00_04` ...
    `pos_20_24`. Prices in EUR/MW/h.
    **The block columns are CET/CEST, not UTC -- see module docstring.**
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        xl = pd.ExcelFile(BytesIO(content))
        raw = xl.parse(xl.sheet_names[0], header=0)
    assert isinstance(raw, pd.DataFrame)
    df: pd.DataFrame = raw

    if _AFRR_DE_MARGINAL_PER_HOUR in df.columns:
        divisor, raw_col = 1, _AFRR_DE_MARGINAL_PER_HOUR
    elif _AFRR_DE_MARGINAL_PER_BLOCK in df.columns:
        divisor, raw_col = 4, _AFRR_DE_MARGINAL_PER_BLOCK
    else:
        raise ValueError(f"aFRR Germany price column not found. Columns: {list(df.columns)}")

    df["price"] = pd.to_numeric(df[raw_col], errors="coerce") / divisor  # type: ignore[operator]
    df["col"] = df["PRODUCT"].str.lower()  # pos_00_04, neg_00_04, ...
    df["delivery_date"] = pd.to_datetime(df["DATE_FROM"], utc=True).dt.normalize()

    return (
        df[["delivery_date", "col", "price"]]
        .pivot(index="delivery_date", columns="col", values="price")
        .rename_axis(None, axis="columns")
        .rename_axis("delivery_date")
    )


def download_fcr_prices(date_from: date, date_to: date) -> pd.DataFrame:
    """Download FCR capacity prices, `[date_from, date_to]` (both
    inclusive). Columns `negpos_00_04` ... `negpos_20_24`, EUR/MW/h, indexed
    by naive UTC-midnight `delivery_date`. Empty if `date_from > date_to` or
    no files are available in range. **The block columns themselves are
    CET/CEST, not UTC -- see module docstring before using them.**"""
    if date_from > date_to:
        return pd.DataFrame()

    files = _collect_files("FCR", date_from, date_to)
    if not files:
        return pd.DataFrame()

    frames = [_parse_fcr_results(_download_file(f["fileName"])) for f in files]
    df = pd.concat(frames)
    df = df[~df.index.duplicated(keep="last")].sort_index()

    ts_from, ts_to = pd.Timestamp(date_from, tz="UTC"), pd.Timestamp(date_to, tz="UTC")
    return df.loc[ts_from:ts_to].tz_localize(None)


def download_afrr_capacity_prices(date_from: date, date_to: date) -> pd.DataFrame:
    """Download aFRR capacity prices, `[date_from, date_to]` (both
    inclusive). Columns `neg_00_04` ... `pos_20_24`, EUR/MW/h, indexed by
    naive UTC-midnight `delivery_date`. Empty if `date_from > date_to` or no
    files are available in range. **The block columns themselves are
    CET/CEST, not UTC -- see module docstring before using them.**"""
    if date_from > date_to:
        return pd.DataFrame()

    files = _collect_files("aFRR", date_from, date_to)
    if not files:
        return pd.DataFrame()

    frames = [_parse_afrr_capacity_results(_download_file(f["fileName"])) for f in files]
    df = pd.concat(frames)
    df = df[~df.index.duplicated(keep="last")].sort_index()

    ts_from, ts_to = pd.Timestamp(date_from, tz="UTC"), pd.Timestamp(date_to, tz="UTC")
    return df.loc[ts_from:ts_to].tz_localize(None)
