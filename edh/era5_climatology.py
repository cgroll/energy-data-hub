"""ERA5-Land climatology over Germany via Google Earth Engine.

Migrated 2026-10-09 from `energy-research`'s `erx/era5_climatology.py` --
promoted into the hub so the Germany-wide weather-climatology baseline
(temperature, wind speed, precipitation) is downloaded once and reusable
from any analysis, not re-fetched ad hoc per research repo. Dropped in the
move: the (day_of_year, hour)-pre-aggregated query path (`doy_hour_climatology`)
-- confirmed unused by every downstream consumer (they all build real
per-year quantiles from `hourly_series`'s raw output instead), so it was
dead weight, not a loss.

Access via Application Default Credentials, account `cgroll.ger@gmail.com`,
Cloud project `energy-platform-511005` (created 2026-10-08 specifically for
this). Earth Engine's own OAuth client (the "notebook"/`code.earthengine.
google.com` flow) got a hard "This app is blocked" from Google when
requesting Drive + Cloud Storage scopes -- a known Google policy of
blocking the deprecated manual copy/paste ("out-of-band") OAuth flow for
sensitive/restricted scopes, not an app-trust problem. The fix was to drop
those two scopes entirely (not needed here -- we only read values back into
Python, we don't export to Drive/GCS) and authenticate via `gcloud auth
application-default login` with only `earthengine` + `cloud-platform`
scopes, then hand that credential object to `ee.Initialize(credentials=...)`
directly rather than going through Earth Engine's own persistent-credentials
file (which is hard-coded to its own, blocked, OAuth client and can't be
swapped for a differently-issued refresh token).

**Why direct image-ID addressing instead of filtering.** `ee.Filter.
calendarRange('day_of_year'/'hour')` and an `ee.Filter.Or` of per-year
`ee.Filter.date(...)` ranges both reliably hit Earth Engine's interactive
"User memory limit exceeded" quota against `ECMWF/ERA5_LAND/HOURLY`
(~600k images, hourly since 1950) -- even for a single (day_of_year, hour)
combo reduced over a single point. Plain `ee.ImageCollection.filterDate()`
on a short explicit range is cheap (sub-second), and the collection's image
IDs are deterministic (`ECMWF/ERA5_LAND/HOURLY/YYYYMMDDTHH`), so addressing
each of the (few) wanted images directly via `ee.Image(<exact id>)` skips
any collection scan entirely.
"""

from datetime import datetime, timedelta

import ee
import google.auth
import pandas as pd

HOURLY_CHUNK = 3500  # largest single-image (unaveraged) batch that stayed under the memory quota in testing (4000 ok, 5000 failed)

PROJECT = "energy-platform-511005"
SCOPES = [
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/cloud-platform",
]
COLLECTION = "ECMWF/ERA5_LAND/HOURLY"
GERMANY_DATASET = "USDOS/LSIB_SIMPLE/2017"
REFERENCE_YEARS = range(1991, 2021)  # WMO 30-year climate-normal period
SCALE_M = 27830  # coarse on purpose -- a national mean doesn't need native 0.1 deg resolution

_initialized = False


def initialize() -> None:
    """Authenticate once per process via Application Default Credentials.

    Requires `gcloud auth application-default login --scopes=...` (see
    module docstring) to have been run already -- this does not trigger any
    browser flow itself.
    """
    global _initialized
    if _initialized:
        return
    credentials, _ = google.auth.default(scopes=SCOPES)
    ee.Initialize(credentials=credentials, project=PROJECT)
    _initialized = True


def germany_geometry() -> ee.Geometry:
    """Germany's national border (US Dept. of State LSIB, simplified) --
    ~356,000 km^2, close to the real ~357,000 km^2."""
    return (
        ee.FeatureCollection(GERMANY_DATASET)
        .filter(ee.Filter.eq("country_na", "Germany"))
        .geometry()
    )


def _hour_feature(geometry: ee.Geometry, band: str, dt: datetime) -> ee.Feature:
    """One real timestamp, single image (no multi-year averaging), Germany mean
    of a band that already exists on the image (e.g. `temperature_2m`)."""
    img_id = f"{COLLECTION}/{dt:%Y%m%dT%H}"
    val = ee.Image(img_id).select(band).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geometry,
        scale=SCALE_M,
        maxPixels=1e9,
        bestEffort=True,
    ).get(band)
    return ee.Feature(None, {"time_utc": dt.isoformat(), band: val})


def _wind_speed_hour_feature(geometry: ee.Geometry, dt: datetime) -> ee.Feature:
    """One real timestamp: 10 m wind speed from ERA5-Land's u/v components,
    combined into speed *per pixel first* (`u.hypot(v)`), then spatially
    averaged over Germany -- not the other way around. Averaging u and v
    across Germany first and only then taking the magnitude would let wind
    blowing in different directions in different parts of the country
    partially cancel out, understating the true mean speed.
    """
    img_id = f"{COLLECTION}/{dt:%Y%m%dT%H}"
    img = ee.Image(img_id)
    speed = img.select("u_component_of_wind_10m").hypot(img.select("v_component_of_wind_10m"))
    val = speed.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geometry,
        scale=SCALE_M,
        maxPixels=1e9,
        bestEffort=True,
    ).get("u_component_of_wind_10m")
    return ee.Feature(None, {"time_utc": dt.isoformat(), "wind_speed_10m": val})


def _fetch_hourly(feature_fn, start: datetime, end: datetime) -> pd.DataFrame:
    """Shared chunked-fetch loop behind `hourly_series` and
    `wind_speed_hourly_series`: one `ee.Feature` per real hour in
    `[start, end)`, `HOURLY_CHUNK` hours per Earth Engine request (see
    module docstring for why that chunk size)."""
    initialize()
    geometry = germany_geometry()
    total_hours = int((end - start).total_seconds() // 3600)
    rows = []
    for chunk_start in range(0, total_hours, HOURLY_CHUNK):
        n = min(HOURLY_CHUNK, total_hours - chunk_start)
        hours = [start + timedelta(hours=chunk_start + i) for i in range(n)]
        feats = ee.FeatureCollection([feature_fn(geometry, h) for h in hours])
        result = feats.getInfo()
        rows.extend(f["properties"] for f in result["features"])
    df = pd.DataFrame(rows)
    df["time_utc"] = pd.to_datetime(df["time_utc"])
    return df.sort_values("time_utc").reset_index(drop=True)


def hourly_series(band: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Germany-mean of `band`, one row per real hour in `[start, end)` (UTC) --
    the actual multi-year time series (not a pre-aggregated climatology),
    so a caller can build real per-year quantiles (e.g. a per-calendar-day
    boxplot, or a (month, day, hour)-matched p10/p50/p90 band) downstream.

    Queried in `HOURLY_CHUNK`-hour batches, each a single `ee.Image(<exact
    id>)` lookup. For the full 1991-2020 period (262,992 hours) this takes
    roughly 35-40 minutes.
    """
    return _fetch_hourly(lambda geometry, dt: _hour_feature(geometry, band, dt), start, end)


def wind_speed_hourly_series(start: datetime, end: datetime) -> pd.DataFrame:
    """Same as `hourly_series`, but for 10 m wind speed derived from
    `u_component_of_wind_10m` / `v_component_of_wind_10m` (see
    `_wind_speed_hour_feature` for why the per-pixel-first order matters).
    Column is `wind_speed_10m`, in m/s.
    """
    return _fetch_hourly(_wind_speed_hour_feature, start, end)
