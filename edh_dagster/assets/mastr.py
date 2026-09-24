"""MaStR (Marktstammdatenregister) wind + solar installed-capacity units
(+ storage, location-only).

Migrated 2026-09-23 from the standalone `mastr-power-capacities-germany`
repo's `pipeline/01_download_mastr.py` (see
energy-data-hub/ARCHITECTURE.md's migration criterion) -- scope wind +
solar, plus storage's `location_id` only (see `edh/mastr.py` module
docstring for why storage stops there):

1. `mastr_sqlite_tables` -- one `@multi_asset` that downloads the MaStR bulk
   export (if not already cached) and parses it into a local SQLite db,
   producing 6 separately visible assets: `wind_extended`/`solar_extended`/
   `storage_extended` (unit master data) and `wind_eeg`/`solar_eeg`/
   `storage_eeg` (EEG subsidy registration data, parsed for free by the same
   call but not consumed downstream yet).
2. `mastr_units_wind` / `mastr_units_solar` -- harmonized, snake_case
   parquet per technology (1 row per plant), built from the `_extended`
   tables above.
3. `mastr_solar_technical_detail` -- solar orientation/tracking detail
   (same `solar_extended` SQLite table, different columns), needed to tell
   PECD's utility-fixed vs. utility-tracking technology apart (see
   `edh_dagster/assets/pecd.py`).
4. `mastr_storage_location_ids` -- just `unit_id`/`location_id` from
   `storage_extended`, needed for `edh/capacity_panel.py`'s behind-the-meter
   PV classification (see `edh_dagster/assets/capacity.py`).

**Snapshot pinning:** this is `full_refresh`, but pinned to whatever
Gesamtdatenexport snapshot is already cached locally rather than
auto-rolling to "today" on every run -- see `edh/mastr.py` module docstring
for why, and `MastrDownloadConfig.force_redownload` below for the deliberate
escape hatch. The snapshot date actually used is surfaced as
`mastr_snapshot_date` metadata on `mastr_sqlite_tables`' outputs so staleness
is visible rather than silent.

**No schedule:** unlike SMARD, this isn't wired into a daily refresh
schedule -- routine re-runs would just re-parse the same pinned snapshot for
no benefit. Materialize by hand (optionally with `force_redownload=True`)
when you actually want newer MaStR data.
"""

from dagster import AssetExecutionContext, AssetOut, Config, MetadataValue, MaterializeResult, asset, multi_asset

from edh.mastr import (
    cached_zip_date,
    delete_cached_zips,
    extract_solar_technical_detail,
    extract_storage_location_ids,
    extract_units_parquet,
    refresh_sqlite_tables,
)
from edh.paths import mastr_solar_technical_detail_file, mastr_storage_location_ids_file, mastr_units_file

SOURCE_URL = "https://www.marktstammdatenregister.de/MaStR/Datendownload"


class MastrDownloadConfig(Config):
    force_redownload: bool = False


_MASTR_TAGS = {"load_pattern": "full_refresh"}


_MASTR_SQLITE_TABLES = ["wind_extended", "wind_eeg", "solar_extended", "solar_eeg", "storage_extended", "storage_eeg"]


@multi_asset(
    outs={table_name: AssetOut(group_name="mastr", kinds={"sqlite"}, tags=_MASTR_TAGS) for table_name in _MASTR_SQLITE_TABLES},
    description=(
        "Wind + solar + storage unit master data (`_extended`) and EEG subsidy "
        "registration data (`_eeg`), parsed from the MaStR bulk XML export "
        "into a local SQLite db. Pinned to the locally cached export "
        "snapshot -- set `force_redownload: true` in run config to fetch "
        "the newest one instead. See `edh/mastr.py` module docstring."
    ),
)
def mastr_sqlite_tables(context: AssetExecutionContext, config: MastrDownloadConfig):
    if config.force_redownload:
        delete_cached_zips()

    date = cached_zip_date()
    row_counts = refresh_sqlite_tables(["wind", "solar", "storage"], date)
    snapshot_label = date or "today (freshly fetched)"

    static_metadata = {
        "source": "Marktstammdatenregister (Bundesnetzagentur)",
        "source_url": MetadataValue.url(SOURCE_URL),
        "region": "DE",
        "resolution": "per-unit master data, not a time series",
        "unit": "n/a (raw registry extract, not a physical quantity)",
        "timestamp_timezone": "n/a",
        "update_pattern": (
            "full_refresh, pinned to the cached export snapshot -- see "
            "mastr_snapshot_date metadata and edh/mastr.py module docstring"
        ),
        "mastr_snapshot_date": MetadataValue.text(snapshot_label),
    }

    for table_name in _MASTR_SQLITE_TABLES:
        yield MaterializeResult(
            asset_key=table_name,
            metadata={"dagster/row_count": row_counts[table_name], **static_metadata},
        )


def _make_units_asset(technology: str):
    @asset(
        name=f"mastr_units_{technology}",
        deps=[f"{technology}_extended"],
        group_name="mastr",
        kinds={"parquet"},
        tags=_MASTR_TAGS,
        description=(
            f"Harmonized {technology} unit table (1 row per plant), rebuilt "
            f"from the `{technology}_extended` SQLite table above."
        ),
        metadata={
            "source": "Marktstammdatenregister (Bundesnetzagentur)",
            "source_url": MetadataValue.url(SOURCE_URL),
            "region": "DE",
            "resolution": "per-unit master data, not a time series",
            "unit": "kW (net/gross capacity columns); else categorical/date/id",
            "timestamp_timezone": "n/a (dates only, no intraday timestamps)",
            "update_pattern": "full_refresh, rebuilt from whatever mastr_sqlite_tables currently holds",
        },
    )
    def _units_asset(context: AssetExecutionContext) -> None:
        output_file = mastr_units_file(technology)
        row_count, preview = extract_units_parquet(technology, output_file)

        context.add_output_metadata(
            {
                "dagster/row_count": row_count,
                "path": MetadataValue.path(str(output_file)),
                "preview": MetadataValue.md(preview.to_markdown()) if preview is not None else MetadataValue.md("*empty*"),
            }
        )

    return _units_asset


mastr_units_wind = _make_units_asset("wind")
mastr_units_solar = _make_units_asset("solar")


@asset(
    deps=["solar_extended"],
    group_name="mastr",
    kinds={"parquet"},
    tags=_MASTR_TAGS,
    description=(
        "Per-solar-unit orientation (`main_orientation`) and tilt bucket "
        "(`main_orientation_tilt_bucket`), keyed by unit_id -- joins against "
        "`mastr_units_solar`. Needed to classify ground-mounted units into "
        "PECD's utility-fixed vs. utility-tracking technology code."
    ),
    metadata={
        "source": "Marktstammdatenregister (Bundesnetzagentur)",
        "source_url": MetadataValue.url(SOURCE_URL),
        "region": "DE",
        "resolution": "per-unit master data, not a time series",
        "unit": "n/a (orientation/tilt are self-reported categorical buckets, not angles)",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, rebuilt from whatever mastr_sqlite_tables currently holds",
    },
)
def mastr_solar_technical_detail(context: AssetExecutionContext) -> None:
    output_file = mastr_solar_technical_detail_file()
    row_count, preview = extract_solar_technical_detail(output_file)

    context.add_output_metadata(
        {
            "dagster/row_count": row_count,
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(preview.to_markdown()) if preview is not None else MetadataValue.md("*empty*"),
        }
    )


@asset(
    deps=["storage_extended"],
    group_name="mastr",
    kinds={"parquet"},
    tags=_MASTR_TAGS,
    description=(
        "Per-storage-unit `location_id` only (not the full harmonized unit "
        "table) -- used to join against `mastr_units_solar`'s own "
        "`location_id` for the solar<->storage co-location check that "
        "drives behind-the-meter PV classification (see "
        "`edh/capacity_panel.py`)."
    ),
    metadata={
        "source": "Marktstammdatenregister (Bundesnetzagentur)",
        "source_url": MetadataValue.url(SOURCE_URL),
        "region": "DE",
        "resolution": "per-unit master data, not a time series",
        "unit": "n/a (id column only)",
        "timestamp_timezone": "n/a",
        "update_pattern": "full_refresh, rebuilt from whatever mastr_sqlite_tables currently holds",
    },
)
def mastr_storage_location_ids(context: AssetExecutionContext) -> None:
    output_file = mastr_storage_location_ids_file()
    row_count, preview = extract_storage_location_ids(output_file)

    context.add_output_metadata(
        {
            "dagster/row_count": row_count,
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(preview.to_markdown()) if preview is not None else MetadataValue.md("*empty*"),
        }
    )


mastr_assets = [
    mastr_sqlite_tables,
    mastr_units_wind,
    mastr_units_solar,
    mastr_solar_technical_detail,
    mastr_storage_location_ids,
]
