"""Full-grid ERA5 reanalysis for Germany+offshore (`edh/era5_grid.py`) --
the weather-driver half of a from-scratch capacity-factor-methodology
validation against PECD/SMARD (see that module's docstring for the full
rationale). Not related to `edh/era5_climatology.py` (ERA5-Land, Germany-
wide spatial mean, 3 variables, via Earth Engine) -- this is the full
ERA5 product kept as a spatial grid, with the extra variables (100m wind,
solar radiation) a capacity-factor calculation needs.

**No schedule, `full_refresh`:** this is historical reanalysis -- each
month, once fetched, never changes. Materialize by hand to extend the
range forward as new months become available upstream.
"""

from dagster import AssetExecutionContext, MetadataValue, asset

from edh.era5_grid import DE_BBOX_AREA, EARLIEST_YEAR_MONTH, LATEST_YEAR_MONTH, all_year_months, download_month, load_era5_grid
from edh.paths import era5_grid_month_file

_ERA5_GRID_TAGS = {"load_pattern": "full_refresh"}
CDS_SOURCE_URL = "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels"


@asset(
    group_name="era5_grid",
    kinds={"parquet"},
    tags=_ERA5_GRID_TAGS,
    description=(
        "Full-grid ERA5 reanalysis, Germany+offshore bounding box, 0.25 deg, hourly -- 100m/10m wind "
        "u+v components, 2m temperature, surface solar radiation downwards. Downloaded/cached per month "
        "(each month's raw CDS zip is parsed to parquet and deleted); this asset ensures every month in "
        f"[{EARLIEST_YEAR_MONTH}, {LATEST_YEAR_MONTH}] is on disk, the combined series is assembled on "
        "demand by edh.era5_grid.load_era5_grid rather than materialized as one file."
    ),
    metadata={
        "source": "ERA5 (C3S/ECMWF), reanalysis-era5-single-levels",
        "source_url": MetadataValue.url(CDS_SOURCE_URL),
        "region": f"bbox {DE_BBOX_AREA} (N, W, S, E) -- Germany + North/Baltic Sea offshore + neighbor margin",
        "resolution": "hourly, 0.25 deg grid",
        "unit": "u100/v100/u10/v10 in m/s, t2m in K, ssrd in J/m^2 (1h accumulation ending at timestamp)",
        "timestamp_timezone": "naive, represents UTC",
        "update_pattern": "full_refresh per missing month, static historical reference data",
    },
)
def era5_grid_de(context: AssetExecutionContext) -> None:
    year_months = all_year_months()
    missing = [(y, m) for y, m in year_months if not era5_grid_month_file(y, m).exists()]
    for y, m in missing:
        download_month(y, m)

    df = load_era5_grid()
    months_on_disk = [f"{y}-{m:02d}" for y, m in year_months if era5_grid_month_file(y, m).exists()]
    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "months_on_disk": MetadataValue.text(f"{len(months_on_disk)} of {len(year_months)} "
                                                  f"({months_on_disk[0]} to {months_on_disk[-1]})" if months_on_disk else "none"),
            "cell_count": int(df[["cell_lon", "cell_lat"]].drop_duplicates().shape[0]) if len(df) else 0,
            "preview": MetadataValue.md(df.tail(3).to_markdown()) if len(df) else MetadataValue.md("*empty*"),
        }
    )


era5_grid_assets = [era5_grid_de]
