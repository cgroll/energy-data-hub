"""Region-assigned wind+solar capacity events, annual capacity panel,
offshore footprint polygons, and behind-the-meter PV category
classification.

Ported and scoped to wind+solar (2026-09-23 design conversation) from
`mastr-power-capacities-germany`'s `pipeline/03_build_capacity_panel.py` --
same methodology, but only the two technologies the hub currently ingests
(no biomass/hydro/gsgk, and storage only as far as its `location_id`, see
`edh/mastr.py`).

**Region assignment:** onshore units get `region_code` = NUTS3, via
`municipality_key` joined against the LAU-NUTS correspondence
(`edh/region_geo.py`); offshore wind has no municipality, so its
`sea_location` ("Nordsee"/"Ostsee") maps directly to a synthetic
pseudo-region instead. Units the crosswalk can't resolve are dropped.

**Behind-the-meter PV category:** each solar unit is classified using
`feed_in_type` plus a location-based join against storage units (shared
`location_id`) -- chosen over MaStR's own `SpeicherAmGleichenOrt` field
(holds nonsense values) and storage's own reverse-link field (lower match
rate), per the original repo's investigation.
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPoint

OFFSHORE_NORTH_SEA_CODE = "DEZZ-NORDSEE"
OFFSHORE_BALTIC_SEA_CODE = "DEZZ-OSTSEE"

FULL_FEED_IN = "Volleinspeisung"
PARTIAL_FEED_IN_PREFIX = "Teileinspeisung"


def classify_pv_category(feed_in_type: pd.Series, location_id: pd.Series, storage_location_ids: pd.Series) -> pd.Series:
    """One of "full_feed_in" / "self_consumption_with_storage" /
    "self_consumption_no_storage" / "unknown" per solar unit."""
    has_storage = location_id.isin(set(storage_location_ids.dropna()))
    is_full_feed_in = feed_in_type == FULL_FEED_IN
    is_partial_feed_in = feed_in_type.astype(str).str.startswith(PARTIAL_FEED_IN_PREFIX)

    category = pd.Series("unknown", index=feed_in_type.index, dtype="object")
    category[is_full_feed_in] = "full_feed_in"
    category[is_partial_feed_in & has_storage] = "self_consumption_with_storage"
    category[is_partial_feed_in & ~has_storage] = "self_consumption_no_storage"
    return category.astype("category")


def assign_region_code(units: pd.DataFrame, lau_nuts: pd.DataFrame) -> pd.DataFrame:
    """Assign each unit a `region_code`: NUTS3 via `municipality_key` for
    onshore units, a synthetic offshore pseudo-region (no municipality at
    sea) for offshore wind. Drops rows the crosswalk can't resolve."""
    df = units.copy()
    df["municipality_key"] = df["municipality_key"].astype(str).str.extract(r"(\d+)")[0].str.zfill(8)
    df = df.merge(lau_nuts, on="municipality_key", how="left")
    df["region_code"] = df["nuts3_code"]
    if "sea_location" in df.columns:
        df.loc[df["sea_location"] == "Nordsee", "region_code"] = OFFSHORE_NORTH_SEA_CODE
        df.loc[df["sea_location"] == "Ostsee", "region_code"] = OFFSHORE_BALTIC_SEA_CODE
    return df[df["region_code"].notna()].copy()


def build_capacity_events(wind: pd.DataFrame, solar: pd.DataFrame, lau_nuts: pd.DataFrame, storage_location_ids: pd.Series) -> pd.DataFrame:
    """Region-assigned wind+solar unit-level table, one row per plant:
    `unit_id`, `technology`, `region_code`, `state`, `capacity_mw`,
    commissioning/shutdown dates, coordinates, `pv_category` (solar only,
    `None` for wind), `usage_sector`, `installation_type`."""
    parts = []
    for technology, units in [("wind", wind), ("solar", solar)]:
        df = assign_region_code(units, lau_nuts)
        df["capacity_mw"] = df["net_capacity_kw"].astype(float) / 1000.0
        df["commissioning_date"] = pd.to_datetime(df["commissioning_date"], errors="coerce")
        df["final_shutdown_date"] = pd.to_datetime(df["final_shutdown_date"], errors="coerce")
        df = df[df["commissioning_date"].notna() & (df["capacity_mw"] > 0)].copy()

        if technology == "solar":
            df["pv_category"] = classify_pv_category(df["feed_in_type"], df["location_id"], storage_location_ids)
        else:
            df["pv_category"] = pd.Series(pd.NA, index=df.index, dtype="object")

        for col in ("usage_sector", "installation_type"):
            if col not in df.columns:
                df[col] = pd.NA

        df["technology"] = technology
        parts.append(df[[
            "unit_id", "technology", "region_code", "state", "capacity_mw",
            "commissioning_date", "final_shutdown_date", "longitude", "latitude",
            "pv_category", "usage_sector", "installation_type",
        ]])

    events = pd.concat(parts, ignore_index=True)
    for col in ("region_code", "technology", "pv_category", "usage_sector", "installation_type"):
        events[col] = events[col].astype("string")
    return events


def annual_capacity_panel(events: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Installed capacity + unit count per group, year-end snapshots from
    the first commissioning year through the current year."""
    start_year = events["commissioning_date"].dt.year.min()
    end_year = pd.Timestamp.today().year

    snapshots = []
    for year in range(start_year, end_year + 1):
        snapshot_date = pd.Timestamp(year=year, month=12, day=31)
        installed = events["commissioning_date"] <= snapshot_date
        still_online = events["final_shutdown_date"].isna() | (events["final_shutdown_date"] > snapshot_date)
        online = events[installed & still_online]
        if online.empty:
            continue
        panel = online.groupby(group_cols, observed=True).agg(capacity_mw=("capacity_mw", "sum"), unit_count=("unit_id", "count")).reset_index()
        panel["year"] = year
        snapshots.append(panel)

    return pd.concat(snapshots, ignore_index=True)


def offshore_region_hulls(events: pd.DataFrame) -> gpd.GeoDataFrame:
    """Offshore wind footprint polygons (convex hull of every currently
    -installed turbine's real coordinates), one row per pseudo-region."""
    today = pd.Timestamp.today()
    offshore_now = events[
        events["region_code"].str.startswith("DEZZ")
        & (events["commissioning_date"] <= today)
        & (events["final_shutdown_date"].isna() | (events["final_shutdown_date"] > today))
    ].dropna(subset=["longitude", "latitude"])

    rows = []
    for region_code, grp in offshore_now.groupby("region_code"):
        hull = MultiPoint(list(zip(grp["longitude"], grp["latitude"]))).convex_hull
        rows.append({"region_code": region_code, "capacity_mw": grp["capacity_mw"].sum(), "turbine_count": len(grp), "geometry": hull})

    return gpd.GeoDataFrame(rows, crs="EPSG:4326")
