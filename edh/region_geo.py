"""Region/zone reference data + crosswalk helpers: Eurostat LAU->NUTS3, and
PECD's PEON/PEOF wind-zone rasterized masks.

Ported from `mastr-power-capacities-germany`'s `pipeline/02_download_nuts.py`
(LAU-NUTS), `pipeline/06_download_pecd_masks.py` (masks), `mpg/grid.py`, and
`pipeline/07_build_wind_zone_panel.py`'s `fractional_zone_weights` -- moved
into the hub (2026-09-23 design conversation) so PECD-linking work has no
silent, Dagster-invisible dependency on that sibling repo's own pipeline
runs. Requires a `~/.cdsapirc` with a valid CDS API key for the masks.

**Solar's region assignment (municipality_key -> NUTS3 -> NUTS2) uses the
LAU-NUTS correspondence below.** Wind's PEON/PEOF zone assignment does not
need it at all -- `mastr_units_wind` already carries real (longitude,
latitude) and a direct `wind_onshore_or_offshore` column, so the
fractional-raster-mask match below runs straight off those, the same
function for both onshore and offshore (just against different masks) --
confirmed against the actual pipeline code, correcting an earlier
(narrative, not code-verified) assumption that offshore used `sea_location`
bucketing instead.
"""

import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

from edh.paths import country_borders_file, lau_nuts_correspondence_file, nuts_regions_file, pecd_mask_file

LAU_NUTS_URL = "https://ec.europa.eu/eurostat/documents/345175/501971/EU-27-LAU-2024-NUTS-2024.xlsx"
NUTS_URL = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_01M_2024_4326.geojson"
NEIGHBOR_COUNTRIES = ["DE", "NL", "BE", "DK", "PL", "SE", "LU"]


def download_lau_nuts_correspondence() -> tuple[int, Path]:
    """Fetch Eurostat's LAU (municipality) -> NUTS3 crosswalk, Germany
    sheet only. Public, unauthenticated."""
    output_file = lau_nuts_correspondence_file()
    raw = pd.read_excel(LAU_NUTS_URL, sheet_name="DE")
    correspondence = raw[["LAU CODE", "NUTS3"]].rename(columns={"LAU CODE": "municipality_key", "NUTS3": "nuts3_code"})
    correspondence["municipality_key"] = correspondence["municipality_key"].astype(str).str.zfill(8)
    correspondence.to_parquet(output_file, index=False)
    return len(correspondence), output_file


def download_nuts_regions() -> tuple[Path, Path]:
    """Fetch Eurostat/GISCO's NUTS region geometries (levels 0-3), Germany
    only, plus country-level outlines for Germany's North/Baltic Sea
    neighbors (map context around offshore wind zones). One fetch, two
    outputs -- public, unauthenticated."""
    nuts = gpd.read_file(NUTS_URL)

    nuts_de = nuts[nuts["CNTR_CODE"] == "DE"][["NUTS_ID", "LEVL_CODE", "NUTS_NAME", "NAME_LATN", "geometry"]].reset_index(drop=True)
    regions_file = nuts_regions_file()
    nuts_de.to_file(regions_file, driver="GeoJSON")

    country_borders = nuts[(nuts["LEVL_CODE"] == 0) & (nuts["CNTR_CODE"].isin(NEIGHBOR_COUNTRIES))][
        ["CNTR_CODE", "NAME_LATN", "geometry"]
    ].reset_index(drop=True)
    borders_file = country_borders_file()
    country_borders.to_file(borders_file, driver="GeoJSON")

    return regions_file, borders_file


def download_pecd_mask(scheme: str) -> Path:
    """Fetch PECD v4.2's rasterized wind-zone region mask ("peon" or
    "peof") from the CDS "weights and masks" widget -- a NetCDF giving each
    0.25-degree grid cell's fractional area coverage (0-1) for every
    European zone. Requires `~/.cdsapirc`."""
    import cdsapi

    output_file = pecd_mask_file(scheme)
    zip_path = output_file.with_suffix(".zip")
    request = {"pecd_version": "pecd4_2", "file_version": "fv1", "variable": f"{scheme}_region_mask"}

    client = cdsapi.Client()
    client.retrieve("sis-energy-pecd", request, str(zip_path))

    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
        assert len(names) == 1, f"expected a single file in {zip_path.name}, got {names}"
        with z.open(names[0]) as src, open(output_file, "wb") as dst:
            dst.write(src.read())
    zip_path.unlink()
    return output_file


def nearest_grid_index(values: np.ndarray, grid: np.ndarray) -> np.ndarray:
    """Index of the nearest point in a regularly-spaced 1-D `grid` for each of `values`."""
    step = grid[1] - grid[0]
    idx = np.rint((values - grid[0]) / step).astype(int)
    return np.clip(idx, 0, len(grid) - 1)


def fractional_zone_weights(units: pd.DataFrame, mask_file: Path, zone_prefix: str = "DE") -> pd.DataFrame:
    """One row per (unit_id, zone_id) with nonzero weight, weights summing
    to 1 per unit -- each unit's capacity fractionally split across every
    zone with nonzero coverage at its nearest 0.25-degree cell, renormalized
    across `zone_prefix`'s own zones (so a neighboring country's residual
    coverage at a border cell doesn't leak German capacity out of the
    German total). The rare zero-coverage cell (only at the border) falls
    back to the single nearest zone by mask-weighted centroid distance.

    `units` must have `unit_id`, `longitude`, `latitude` (rows with missing
    coordinates are the caller's responsibility to drop beforehand).
    """
    ds = xr.open_dataset(mask_file)
    zones = sorted(z for z in ds["region"].values.tolist() if str(z).startswith(zone_prefix))
    mask_values = ds["mask"].sel(region=zones).values  # (zone, lat, lon)
    lats, lons = ds["latitude"].values, ds["longitude"].values
    ds.close()

    lat_idx = nearest_grid_index(units["latitude"].to_numpy(), lats)
    lon_idx = nearest_grid_index(units["longitude"].to_numpy(), lons)
    weights = mask_values[:, lat_idx, lon_idx]  # (zone, n_units)
    totals = weights.sum(axis=0)

    zero_coverage = totals == 0
    if zero_coverage.any():
        lat_grid, lon_grid = np.meshgrid(lats, lons, indexing="ij")
        zone_totals = mask_values.sum(axis=(1, 2))
        centroid_lat = (mask_values * lat_grid).sum(axis=(1, 2)) / zone_totals
        centroid_lon = (mask_values * lon_grid).sum(axis=(1, 2)) / zone_totals

        unit_lat = units["latitude"].to_numpy()[zero_coverage]
        unit_lon = units["longitude"].to_numpy()[zero_coverage]
        dist2 = (unit_lat[:, None] - centroid_lat[None, :]) ** 2 + (unit_lon[:, None] - centroid_lon[None, :]) ** 2
        fallback_zone_idx = dist2.argmin(axis=1)
        weights = weights.copy()
        weights[:, zero_coverage] = 0.0
        weights[fallback_zone_idx, np.flatnonzero(zero_coverage)] = 1.0
        totals = weights.sum(axis=0)

    normalized = weights / totals
    zone_idx_arr, unit_idx_arr = np.nonzero(normalized > 0)
    return pd.DataFrame(
        {
            "unit_id": units["unit_id"].to_numpy()[unit_idx_arr],
            "zone_id": np.array(zones)[zone_idx_arr],
            "weight": normalized[zone_idx_arr, unit_idx_arr],
        }
    )
