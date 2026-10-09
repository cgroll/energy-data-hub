"""Full-grid ERA5 reanalysis for Germany+offshore, via the Copernicus
Climate Data Store (CDS) -- the weather-driver half of a capacity-factor-
methodology validation (energy-research's `pipeline/54`-`57`, 2026-10-09
conversation): does a from-scratch pvlib/windpowerlib + MaStR-grid-
weighted conversion, fed this real reanalysis, track PECD's own capacity
factor and real SMARD generation? Deliberately reanalysis, not a forecast
-- isolates the weather-to-power conversion methodology from any
forecast-model skill question.

Unlike `edh/era5_climatology.py` (ERA5-**Land**, Germany-wide *spatial
mean*, 3 variables, via Earth Engine), this is the full **ERA5** product
(not land-only -- needed for genuine offshore coverage), kept as the full
spatial **grid** (not averaged), with the extra variables a capacity-
factor calculation needs: 100m wind (turbine power curves use hub-height
wind, not 10m) and solar radiation. Via CDS/cdsapi, the same access this
hub already uses for PECD, not Earth Engine.

Variables: 100m/10m wind u+v components (pvlib's Faiman cell-temperature
term needs 10m wind too), 2m temperature, surface solar radiation
downwards. 0.25 deg native ERA5 grid, Germany+offshore bbox: lat 47-56,
lon 3-15 (a deliberately generous box -- comfortably covers Germany, North
Sea, and Baltic Sea offshore zones, plus a neighbor-country margin).

ssrd accumulation convention, confirmed empirically 2026-10-09 (a direct
test request against CDS, not just assumed from generic ERA5
documentation, which describes a different raw-archive since-00Z
convention): this product ships ssrd as a clean *1-hour accumulation
ending at the timestamp* -- rises and falls smoothly within each day,
resets to ~0 overnight, not a since-midnight running total. No extra
differencing needed -- divide by 3600 for mean W/m^2 (a consumer's job,
not this module's).

Chunked by month -- a one-shot multi-month request hits CDS's "cost limits
exceeded" cap (confirmed 2026-10-09 for a full year in one request). Each
month's parquet is the incremental-download checkpoint, same pattern as
PECD's Europe decade pull -- a re-run only fetches months still missing.
CDS delivers each month as a zip (not a plain .nc, despite data_format=
netcdf) containing two files split by GRIB stepType: instantaneous
variables (wind, temperature) and accumulated (ssrd) -- unzipped and
merged here, then the raw zip/nc deleted (parquet is the only thing kept).
"""

import zipfile
from pathlib import Path

import pandas as pd
import xarray as xr

from edh.paths import era5_grid_month_file

DE_BBOX_AREA = [56, 3, 47, 15]  # N, W, S, E
VARIABLES = [
    "100m_u_component_of_wind", "100m_v_component_of_wind",
    "10m_u_component_of_wind", "10m_v_component_of_wind",
    "2m_temperature", "surface_solar_radiation_downwards",
]
RENAME = {
    "100m_u_component_of_wind": "u100",
    "100m_v_component_of_wind": "v100",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "2m_temperature": "t2m",
    "surface_solar_radiation_downwards": "ssrd",
}
KEEP_COLS = ["cell_lon", "cell_lat", "valid_time", "u100", "v100", "u10", "v10", "t2m", "ssrd"]

# Available history, confirmed on disk 2026-10-09 (also the earliest/latest
# this hub has ever fetched/imported) -- extend as new months become needed.
EARLIEST_YEAR_MONTH = (2018, 10)
LATEST_YEAR_MONTH = (2026, 3)


def all_year_months(start: tuple[int, int] = EARLIEST_YEAR_MONTH, end: tuple[int, int] = LATEST_YEAR_MONTH) -> list[tuple[int, int]]:
    """All (year, month) pairs from `start` to `end`, inclusive."""
    periods = pd.period_range(
        start=pd.Period(year=start[0], month=start[1], freq="M"),
        end=pd.Period(year=end[0], month=end[1], freq="M"),
        freq="M",
    )
    return [(p.year, p.month) for p in periods]


def download_month(year: int, month: int) -> Path:
    """Fetch one month of the full ERA5 grid from CDS, convert to parquet.
    No-op (returns the existing file) if that month's parquet is already on
    disk -- this is the incremental-download checkpoint."""
    import cdsapi

    output_file = era5_grid_month_file(year, month)
    if output_file.exists():
        return output_file

    zip_path = output_file.with_suffix(".zip")
    extract_dir = output_file.parent / f"_raw_{year}_{month:02d}"

    client = cdsapi.Client()
    client.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": VARIABLES,
            "year": str(year),
            "month": f"{month:02d}",
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": DE_BBOX_AREA,
            "data_format": "netcdf",
        },
        str(zip_path),
    )

    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(extract_dir)
    zip_path.unlink()

    instant = xr.open_dataset(extract_dir / "data_stream-oper_stepType-instant.nc")
    accum = xr.open_dataset(extract_dir / "data_stream-oper_stepType-accum.nc")
    ds = xr.merge([instant, accum])

    df = ds.to_dataframe().reset_index()
    df = df.rename(columns={"latitude": "cell_lat", "longitude": "cell_lon"})
    df = df[KEEP_COLS].sort_values(["cell_lat", "cell_lon", "valid_time"]).reset_index(drop=True)
    df.to_parquet(output_file, index=False)

    instant.close()
    accum.close()
    for f in extract_dir.iterdir():
        f.unlink()
    extract_dir.rmdir()

    return output_file


def import_month_from_netcdf(year: int, month: int, source_nc: Path) -> Path:
    """Convert an already-downloaded raw ERA5 netcdf (full ERA5 variable
    names, e.g. from a sibling repo's own CDS pull) straight to this hub's
    parquet schema, skipping CDS entirely. Used once (2026-10-09) to
    import 91 already-fetched months from `~/research/pecd-replication`
    instead of re-downloading identical data."""
    output_file = era5_grid_month_file(year, month)
    if output_file.exists():
        return output_file

    ds = xr.open_dataset(source_nc).rename(RENAME)
    df = ds.to_dataframe().reset_index()
    df = df.rename(columns={"latitude": "cell_lat", "longitude": "cell_lon", "time": "valid_time"})
    df = df[KEEP_COLS].sort_values(["cell_lat", "cell_lon", "valid_time"]).reset_index(drop=True)
    df.to_parquet(output_file, index=False)
    ds.close()
    return output_file


def load_era5_grid(start: tuple[int, int] = EARLIEST_YEAR_MONTH, end: tuple[int, int] = LATEST_YEAR_MONTH) -> pd.DataFrame:
    """Concatenate every month's parquet currently on disk within
    `[start, end]` into one continuous frame. Months not yet downloaded are
    silently skipped rather than erroring, same convention as
    `edh.pecd.load_europe_capacity_factors`."""
    parts = [
        pd.read_parquet(era5_grid_month_file(y, m))
        for y, m in all_year_months(start, end)
        if era5_grid_month_file(y, m).exists()
    ]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
