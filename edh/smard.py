"""Download hourly SMARD (Bundesnetzagentur) series for the DE-LU market area.

Uses the smard.de web app's JSON endpoints (undocumented, but proven to work
across ~/research/world-of-energy, ~/research/delu-headline-forecast,
~/research/pecd-replication, and ~/research/pecd-power-validity-DE): a first
call lists available block-start timestamps, a second call fetches each
block's actual observations.

Canonical copy -- this is now the single place SMARD is downloaded from
(see ~/research/energy-platform/energy-data-hub/README.md). Consolidated from the
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
    """SMARD variable IDs.

    Full catalog, migrated from ~/research/smard-data/src/smard_data/config.py
    (2026-09-23, see that repo's README/commit history for provenance) --
    that repo predates this hub (first commit 2025-04) and is being
    retired in its favor. Values unchanged from that source.
    """

    # Generation, by fuel type
    BROWN_COAL = 1223
    NUCLEAR = 1224
    WIND_OFFSHORE = 1225
    HYDRO = 1226
    OTHER_CONVENTIONAL = 1227
    OTHER_RENEWABLE = 1228
    BIOMASS = 4066
    WIND_ONSHORE = 4067
    SOLAR = 4068
    HARD_COAL = 4069
    PUMPED_STORAGE = 4070
    NATURAL_GAS = 4071

    # Consumption
    TOTAL_LOAD = 410
    RESIDUAL_LOAD = 4359
    PUMPED_STORAGE_LOAD = 4387

    # Prices
    PRICE_DE_LU = 4169
    PRICE_DE_LU_NEIGHBORS = 5078
    PRICE_BE = 4996
    PRICE_NO2 = 4997
    PRICE_AT = 4170
    PRICE_DK1 = 252
    PRICE_DK2 = 253
    PRICE_FR = 254
    PRICE_IT_NORTH = 255
    PRICE_NL = 256
    PRICE_PL = 257
    PRICE_PL2 = 258
    PRICE_CH = 259
    PRICE_SI = 260
    PRICE_CZ = 261
    PRICE_HU = 262

    # Forecasts
    FORECAST_OFFSHORE = 3791
    FORECAST_ONSHORE = 123
    FORECAST_SOLAR = 125
    FORECAST_OTHER = 715
    FORECAST_WIND_SOLAR = 5097
    FORECAST_TOTAL = 122

    # Capacity
    CAPACITY_BIOMASS = 189
    CAPACITY_HYDRO = 3792
    CAPACITY_WIND_OFFSHORE = 4076
    CAPACITY_WIND_ONSHORE = 186
    CAPACITY_SOLAR = 188
    CAPACITY_OTHER_RENEWABLE = 194
    CAPACITY_BROWN_COAL = 4072
    CAPACITY_HARD_COAL = 4075
    CAPACITY_NATURAL_GAS = 198
    CAPACITY_PUMPED_STORAGE = 4074


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
