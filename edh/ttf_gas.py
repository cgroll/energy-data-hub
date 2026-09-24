"""Download daily TTF (Title Transfer Facility) natural gas futures prices
from Yahoo Finance.

Migrated 2026-09-24 from
~/research/world-of-energy/pipeline/02_download_ttf_gas.py -- tested there
first (confirmed working against a ~2017-2026 history, incremental
re-fetch included) before being ported here; see that repo for the
original script and its DVC-tracked copy of the same data.

**Source:** the `yfinance` package against Yahoo Finance's `TTF=F`
continuous front-month futures contract -- an unofficial, free source
(no ICE Endex/EEX account needed), same caveat any Yahoo Finance data
carries: undocumented API, no SLA, could change or disappear without
notice.

**Not blocked from this hub's network the way EPEX SPOT is** (see the
2026-09-24 EPEX investigation) -- confirmed working via a live
incremental download on the same machine.
"""

from datetime import datetime

import pandas as pd
import yfinance as yf

TTF_TICKER = "TTF=F"


def download_prices(start_date: datetime | None = None) -> pd.DataFrame:
    """Download daily TTF gas futures OHLCV as a date-indexed DataFrame.

    Args:
        start_date: only fetch data from this date on. `None` downloads
            the maximum available history (yfinance's `period="max"`).

    Returns:
        DataFrame indexed by naive daily `date`, columns `open`, `high`,
        `low`, `close`, `volume`. Empty (but correctly shaped) if
        yfinance returns no rows for the requested range -- e.g. calling
        again the same day after already catching up to the latest
        available close.
    """
    ticker = yf.Ticker(TTF_TICKER)

    if start_date is not None:
        raw = ticker.history(start=start_date.strftime("%Y-%m-%d"))
    else:
        raw = ticker.history(period="max")

    columns = ["open", "high", "low", "close", "volume"]
    if raw.empty:
        return pd.DataFrame({c: [] for c in columns}, index=pd.DatetimeIndex([], name="date"))

    raw = raw[["Open", "High", "Low", "Close", "Volume"]]
    raw.columns = columns
    # yfinance returns a tz-aware index (exchange-local) -- stripped to a
    # naive calendar date, matching this hub's convention of naive
    # timestamps with the exact semantics stated in the asset's
    # `timestamp_timezone` metadata rather than assumed.
    raw.index = pd.to_datetime(raw.index).tz_localize(None).normalize()
    raw.index.name = "date"
    return raw.sort_index()
