"""Region-assigned wind+solar capacity events, annual capacity panels,
offshore footprint polygons, and NUTS/country-border reference geometry.

Migrated 2026-09-23 from `mastr-power-capacities-germany`'s
`pipeline/02_download_nuts.py` and `pipeline/03_build_capacity_panel.py`,
scoped to wind+solar -- see `edh/capacity_panel.py` and `edh/region_geo.py`
module docstrings for the methodology. Built to support the `energy-insights`
book's first page (a stripped-down reproduction of that repo's `04_eda`
notebook), and reused by anything else that wants a region-assigned view of
the hub's MaStR data.
"""

import pandas as pd
from dagster import AssetExecutionContext, AssetOut, MetadataValue, MaterializeResult, asset, multi_asset

from edh.capacity_panel import annual_capacity_panel, build_capacity_events, offshore_region_hulls
from edh.paths import (
    capacity_by_region_year_file,
    capacity_by_region_year_pv_category_file,
    capacity_events_file,
    country_borders_file,
    lau_nuts_correspondence_file,
    mastr_storage_location_ids_file,
    mastr_units_file,
    nuts_regions_file,
    offshore_regions_file,
)
from edh.region_geo import download_nuts_regions

_CAPACITY_TAGS = {"load_pattern": "full_refresh"}


@multi_asset(
    outs={
        "nuts_regions": AssetOut(group_name="capacity", kinds={"geojson"}, tags=_CAPACITY_TAGS),
        "country_borders": AssetOut(group_name="capacity", kinds={"geojson"}, tags=_CAPACITY_TAGS),
    },
    description=(
        "German NUTS region geometries (levels 0-3) and country-level outlines for Germany's North/Baltic Sea "
        "neighbors -- static Eurostat/GISCO reference geometry, not MaStR-specific."
    ),
)
def region_geometries(context: AssetExecutionContext):
    regions_file, borders_file = (nuts_regions_file(), country_borders_file())
    if not (regions_file.exists() and borders_file.exists()):
        regions_file, borders_file = download_nuts_regions()

    static_metadata = {
        "source": "Eurostat/GISCO",
        "source_url": MetadataValue.url("https://ec.europa.eu/eurostat/web/nuts"),
        "region": "DE (+ North/Baltic Sea neighbors for country_borders)",
        "resolution": "n/a (static reference geometry)",
        "unit": "n/a",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, static reference data",
    }
    yield MaterializeResult(asset_key="nuts_regions", metadata={"path": MetadataValue.path(str(regions_file)), **static_metadata})
    yield MaterializeResult(asset_key="country_borders", metadata={"path": MetadataValue.path(str(borders_file)), **static_metadata})


@asset(
    deps=["mastr_units_wind", "mastr_units_solar", "mastr_storage_location_ids", "lau_nuts_correspondence"],
    group_name="capacity",
    kinds={"parquet"},
    tags=_CAPACITY_TAGS,
    description=(
        "Region-assigned wind+solar unit-level table (one row per plant): region_code (NUTS3, or a synthetic "
        "offshore pseudo-region), capacity, commissioning/shutdown dates, coordinates, and (solar only) "
        "behind-the-meter pv_category."
    ),
    metadata={
        "source": "Derived: mastr_units_wind/solar, region-assigned via edh/region_geo.py's LAU-NUTS crosswalk",
        "region": "DE",
        "resolution": "per-unit master data, not a time series",
        "unit": "capacity_mw: MW",
        "timestamp_timezone": "n/a (dates only)",
        "update_pattern": "full_refresh, rebuilt from whatever the upstream mastr_units_* assets currently hold",
    },
)
def mastr_capacity_events(context: AssetExecutionContext) -> None:
    wind = pd.read_parquet(mastr_units_file("wind"))
    solar = pd.read_parquet(mastr_units_file("solar"))
    storage_location_ids = pd.read_parquet(mastr_storage_location_ids_file())["location_id"]
    lau_nuts = pd.read_parquet(lau_nuts_correspondence_file())

    events = build_capacity_events(wind, solar, lau_nuts, storage_location_ids)

    output_file = capacity_events_file()
    events.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(events),
            "path": MetadataValue.path(str(output_file)),
            "capacity_mw_by_technology": MetadataValue.md(events.groupby("technology")["capacity_mw"].sum().round(0).to_markdown()),
            "preview": MetadataValue.md(events.tail(5).to_markdown()),
        }
    )


@asset(
    deps=["mastr_capacity_events"],
    group_name="capacity",
    kinds={"parquet"},
    tags=_CAPACITY_TAGS,
    description="Annual (region_code, technology, year) installed-capacity snapshot panel, year-end.",
    metadata={
        "source": "Derived: mastr_capacity_events",
        "region": "DE",
        "resolution": "annual, year-end snapshots",
        "unit": "capacity_mw: MW",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, rebuilt from whatever mastr_capacity_events currently holds",
    },
)
def mastr_capacity_by_region_year(context: AssetExecutionContext) -> None:
    events = pd.read_parquet(capacity_events_file())
    events["commissioning_date"] = pd.to_datetime(events["commissioning_date"])
    events["final_shutdown_date"] = pd.to_datetime(events["final_shutdown_date"])

    panel = annual_capacity_panel(events, ["region_code", "technology"])
    output_file = capacity_by_region_year_file()
    panel.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(panel),
            "path": MetadataValue.path(str(output_file)),
            "years": MetadataValue.text(f"{panel['year'].min()}-{panel['year'].max()}"),
            "preview": MetadataValue.md(panel.tail(5).to_markdown()),
        }
    )


@asset(
    deps=["mastr_capacity_events"],
    group_name="capacity",
    kinds={"parquet"},
    tags=_CAPACITY_TAGS,
    description="Annual (region_code, pv_category, year) solar-only installed-capacity snapshot panel, year-end.",
    metadata={
        "source": "Derived: mastr_capacity_events",
        "region": "DE",
        "resolution": "annual, year-end snapshots",
        "unit": "capacity_mw: MW",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, rebuilt from whatever mastr_capacity_events currently holds",
    },
)
def mastr_capacity_by_region_year_pv_category(context: AssetExecutionContext) -> None:
    events = pd.read_parquet(capacity_events_file())
    events["commissioning_date"] = pd.to_datetime(events["commissioning_date"])
    events["final_shutdown_date"] = pd.to_datetime(events["final_shutdown_date"])
    solar = events[events["technology"] == "solar"]

    panel = annual_capacity_panel(solar, ["region_code", "pv_category"])
    output_file = capacity_by_region_year_pv_category_file()
    panel.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(panel),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(panel.tail(5).to_markdown()),
        }
    )


@asset(
    deps=["mastr_capacity_events"],
    group_name="capacity",
    kinds={"geojson"},
    tags=_CAPACITY_TAGS,
    description=(
        "Offshore wind footprint polygons (convex hull of currently-installed turbine coordinates), one row per "
        "pseudo-region (North Sea / Baltic Sea)."
    ),
    metadata={
        "source": "Derived: mastr_capacity_events",
        "region": "DE (offshore)",
        "resolution": "n/a (current snapshot only)",
        "unit": "capacity_mw: MW",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, rebuilt from whatever mastr_capacity_events currently holds",
    },
)
def offshore_regions(context: AssetExecutionContext) -> None:
    events = pd.read_parquet(capacity_events_file())
    events["commissioning_date"] = pd.to_datetime(events["commissioning_date"])
    events["final_shutdown_date"] = pd.to_datetime(events["final_shutdown_date"])

    hulls = offshore_region_hulls(events)
    output_file = offshore_regions_file()
    hulls.to_file(output_file, driver="GeoJSON")

    context.add_output_metadata(
        {
            "dagster/row_count": len(hulls),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(hulls.drop(columns="geometry").to_markdown()),
        }
    )


capacity_assets = [
    region_geometries,
    mastr_capacity_events,
    mastr_capacity_by_region_year,
    mastr_capacity_by_region_year_pv_category,
    offshore_regions,
]
