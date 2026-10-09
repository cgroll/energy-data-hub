"""ERA5-Land Germany-wide weather climatology (temperature, wind speed,
precipitation) via Google Earth Engine -- migrated 2026-10-09 from
`energy-research`'s `erx/era5_climatology.py`, so it's downloaded once and
reusable from any analysis instead of being re-fetched per research repo.

Each asset is the real hourly base series (Germany spatial mean, one row
per real UTC hour, 1991-01-01 to 2021-01-01 -- the WMO 30-year climate-
normal reference period), not a pre-aggregated climatology -- a consumer
builds whatever temporal aggregation it needs (e.g. a (month, day, hour)-
matched p10/p50/p90 band to compare a forecast against) from these raw
series downstream. See `edh/era5_climatology.py` for the Earth Engine
access/quota notes.

**No schedule, `full_refresh`:** 1991-2020 is a fixed historical period --
once downloaded, there's no reason to refresh unless the methodology
changes. Each asset takes ~35-40 minutes to materialize (single-image
Earth Engine lookups, batched ~3500 hours per request).
"""

from datetime import datetime, timezone

import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset

from edh.era5_climatology import hourly_series, wind_speed_hourly_series
from edh.paths import (
    era5_de_precipitation_hourly_file,
    era5_de_temperature_hourly_file,
    era5_de_wind_speed_hourly_file,
)

_ERA5_TAGS = {"load_pattern": "full_refresh"}
REFERENCE_START = datetime(1991, 1, 1, tzinfo=timezone.utc)
REFERENCE_END = datetime(2021, 1, 1, tzinfo=timezone.utc)
EARTH_ENGINE_SOURCE_URL = "https://earthengine.google.com/"


def _base_metadata(unit: str) -> dict:
    return {
        "source": "ECMWF/ERA5_LAND/HOURLY via Google Earth Engine",
        "source_url": MetadataValue.url(EARTH_ENGINE_SOURCE_URL),
        "region": "Germany (spatial mean, USDOS/LSIB_SIMPLE/2017 national border)",
        "resolution": "hourly, 1991-01-01 to 2021-01-01 (WMO 30-year climate-normal period)",
        "unit": unit,
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": "full_refresh -- fixed historical period, no reason to re-materialize",
    }


@asset(
    group_name="era5_climatology",
    kinds={"parquet"},
    tags=_ERA5_TAGS,
    description=(
        "ERA5-Land hourly Germany spatial-average 2 m temperature, one row per real "
        "UTC hour, 1991-2020 (262,992 hours). Base series for any downstream "
        "climatology (e.g. a (month, day, hour)-matched quantile band)."
    ),
    metadata=_base_metadata("deg C"),
)
def era5_de_temperature_hourly(context: AssetExecutionContext) -> None:
    df = hourly_series("temperature_2m", REFERENCE_START, REFERENCE_END)
    df["avg_de_2m_temp"] = df["temperature_2m"] - 273.15
    df = df.drop(columns=["temperature_2m"]).rename(columns={"time_utc": "timestamp"})

    output_file = era5_de_temperature_hourly_file()
    df.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "path": MetadataValue.path(str(output_file)),
            "min_timestamp": MetadataValue.text(str(df.timestamp.min())),
            "max_timestamp": MetadataValue.text(str(df.timestamp.max())),
            "value_range_c": MetadataValue.text(f"[{df.avg_de_2m_temp.min():.1f}, {df.avg_de_2m_temp.max():.1f}]"),
            "preview": MetadataValue.md(df.tail(3).to_markdown(index=False)),
        }
    )


@asset(
    group_name="era5_climatology",
    kinds={"parquet"},
    tags=_ERA5_TAGS,
    description=(
        "ERA5-Land hourly Germany spatial-average 10 m wind speed, one row per real "
        "UTC hour, 1991-2020. Derived from u/v wind components combined into speed "
        "*per pixel* before the Germany-wide spatial mean (not averaged u/v first) "
        "-- see edh/era5_climatology.py::_wind_speed_hour_feature."
    ),
    metadata=_base_metadata("m/s"),
)
def era5_de_wind_speed_hourly(context: AssetExecutionContext) -> None:
    df = wind_speed_hourly_series(REFERENCE_START, REFERENCE_END)
    df = df.rename(columns={"time_utc": "timestamp", "wind_speed_10m": "avg_de_wind_speed_10m"})

    output_file = era5_de_wind_speed_hourly_file()
    df.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "path": MetadataValue.path(str(output_file)),
            "min_timestamp": MetadataValue.text(str(df.timestamp.min())),
            "max_timestamp": MetadataValue.text(str(df.timestamp.max())),
            "value_range_ms": MetadataValue.text(
                f"[{df.avg_de_wind_speed_10m.min():.1f}, {df.avg_de_wind_speed_10m.max():.1f}]"
            ),
            "preview": MetadataValue.md(df.tail(3).to_markdown(index=False)),
        }
    )


@asset(
    group_name="era5_climatology",
    kinds={"parquet"},
    tags=_ERA5_TAGS,
    description=(
        "ERA5-Land hourly Germany spatial-average precipitation, one row per real "
        "UTC hour, 1991-2020. Straight from the total_precipitation_hourly band "
        "(already a per-hour accumulation, scaled m -> mm here)."
    ),
    metadata=_base_metadata("mm"),
)
def era5_de_precipitation_hourly(context: AssetExecutionContext) -> None:
    df = hourly_series("total_precipitation_hourly", REFERENCE_START, REFERENCE_END)
    df["avg_de_precipitation_mm"] = df["total_precipitation_hourly"] * 1000
    df = df.drop(columns=["total_precipitation_hourly"]).rename(columns={"time_utc": "timestamp"})

    output_file = era5_de_precipitation_hourly_file()
    df.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "path": MetadataValue.path(str(output_file)),
            "min_timestamp": MetadataValue.text(str(df.timestamp.min())),
            "max_timestamp": MetadataValue.text(str(df.timestamp.max())),
            "value_range_mm": MetadataValue.text(
                f"[{df.avg_de_precipitation_mm.min():.3f}, {df.avg_de_precipitation_mm.max():.3f}]"
            ),
            "preview": MetadataValue.md(df.tail(3).to_markdown(index=False)),
        }
    )


era5_climatology_assets = [
    era5_de_temperature_hourly,
    era5_de_wind_speed_hourly,
    era5_de_precipitation_hourly,
]
