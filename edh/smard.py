"""Download hourly SMARD (Bundesnetzagentur) series for the DE-LU market area.

Uses the smard.de web app's JSON endpoints (undocumented, but proven to work
across ~/research/world-of-energy, ~/research/delu-headline-forecast,
~/research/pecd-replication, and ~/research/pecd-power-validity-DE): a first
call lists available block-start timestamps, a second call fetches each
block's actual observations.

Canonical copy -- this is now the single place SMARD is downloaded from
(see ~/research/energy-data-hub/README.md). Consolidated from the
near-identical copies in pecd-replication, pecd-power-validity-DE,
delu-headline-forecast, t2m-averages-europe, vpp-learning, and
world-of-energy (2026-09-23); this version keeps the superset of variables
(generation, load, price, capacities) any of those needed.
"""

from datetime import datetime
from enum import IntEnum

import pandas as pd
import requests

BASE_URL = "https://www.smard.de/app"
DEFAULT_START_DATE = datetime(2015, 1, 1)


class Variable(IntEnum):
    """SMARD variable IDs for the series this project needs."""

    SOLAR = 4068
    WIND_ONSHORE = 4067
    WIND_OFFSHORE = 1225
    TOTAL_LOAD = 410
    PRICE_DE_LU = 4169
    CAPACITY_SOLAR = 188
    CAPACITY_WIND_ONSHORE = 186
    CAPACITY_WIND_OFFSHORE = 4076


def download_series(
    variable: Variable,
    region: str = "DE-LU",
    resolution: str = "hour",
    start_time: datetime | None = None,
) -> pd.DataFrame:
    """Download one SMARD series as a UTC-timestamp-indexed single-column DataFrame."""
    index_url = f"{BASE_URL}/chart_data/{variable.value}/{region}/index_{resolution}.json"
    response = requests.get(index_url, timeout=30)
    response.raise_for_status()
    block_timestamps = response.json()["timestamps"]

    if start_time is not None:
        start_ms = int(start_time.timestamp() * 1000)
        # Keep the block that *contains* start_time -- its own start
        # timestamp can be well before start_time, since SMARD serves data
        # in multi-day blocks -- plus every later block. Filtering on
        # "block start >= start_time" alone silently drops that
        # currently-in-progress block the moment any of its data has
        # already been fetched once, permanently losing whatever's between
        # start_time and the next block's start on every future run. Real
        # bug, found via hourly_gap_check (see BEST_PRACTICES.md) and fixed
        # 2026-09-23 -- redundantly re-fetching part of an already-seen
        # block is harmless (download_series's own de-dup below handles
        # it); silently skipping data is not.
        block_timestamps = sorted(block_timestamps)
        keep_from = 0
        for i, ts in enumerate(block_timestamps):
            if ts > start_ms:
                break
            keep_from = i
        block_timestamps = block_timestamps[keep_from:]

    col = variable.name.lower()
    if not block_timestamps:
        return pd.DataFrame({col: []}, index=pd.DatetimeIndex([], name="timestamp"))

    all_ts_ms: list[int] = []
    all_values: list[float] = []
    for block_ts in block_timestamps:
        data_url = (
            f"{BASE_URL}/chart_data/{variable.value}/{region}/"
            f"{variable.value}_{region}_{resolution}_{block_ts}.json"
        )
        block_response = requests.get(data_url, timeout=30)
        if block_response.status_code != 200:
            continue
        for point_ts_ms, value in block_response.json()["series"]:
            if value is not None:
                all_ts_ms.append(point_ts_ms)
                all_values.append(value)

    # Epoch milliseconds are UTC by definition -- parsed directly rather than
    # via datetime.fromtimestamp(), which depends on the local machine's
    # timezone and isn't reproducible across environments.
    index = pd.to_datetime(all_ts_ms, unit="ms", utc=True).tz_localize(None)
    df = pd.DataFrame({col: all_values}, index=index)
    df.index.name = "timestamp"
    df = df.sort_index()
    return df[~df.index.duplicated(keep="last")]
