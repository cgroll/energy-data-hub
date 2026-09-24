"""MaStR (Marktstammdatenregister) bulk-export client for wind + solar
(+ storage, location-only).

Fetches the Bundesnetzagentur's daily "Gesamtdatenexport" bulk XML dump via
`open-mastr` and parses it into a local SQLite database, then extracts a
harmonized, chunked parquet per technology -- the same chunked/streamed
approach (and OOM lessons) as `mastr-power-capacities-germany`'s
`pipeline/01_download_mastr.py`, which this hub asset supersedes for
wind/solar (see ARCHITECTURE.md's migration criterion).

**Storage is deliberately still not a full technology here** -- only its
`location_id` is extracted (`extract_storage_location_ids`), because that's
all `edh/capacity_panel.py::classify_pv_category` needs (a location-based
join against solar, to tell "self-consumption + battery" apart from
"self-consumption, no battery"). Full storage capacity/EEG data stays out
of scope until something actually needs it -- `TECHNOLOGY_TABLES` is the
place to extend it later.

**Snapshot pinning, not daily auto-refresh:** MaStR publishes a new full
export daily under a date-stamped filename. Left at `date=None`, open-mastr
resolves to "today" on every call, which would silently re-pull a fresh
multi-GB export on every routine re-run. Instead, `cached_zip_date()` pins
each refresh to whatever Gesamtdatenexport zip is already cached locally, if
any -- getting a genuinely newer snapshot is a deliberate action
(`force_redownload` on the paired Dagster asset's run config, which deletes
the cached zip first), never an implicit side effect. When a requested
technology isn't yet present in the cached zip (e.g. adding solar after an
earlier wind-only run), open-mastr's own range-request logic
(`partial_download_with_unzip_http`) fetches just the missing XML members,
not the whole export.

**Why wind + solar only:** the full MaStR bulk export has 34 tables; most
(combustion, nuclear, gas, market_actors, ...) are never requested here and
stay empty schema stubs in the SQLite db -- open-mastr only parses whatever
technology names are passed to `Mastr.download(data=[...])`.

**Why chunked streaming:** solar's `_extended` table is ~6.3M rows / ~96
columns; loading it into pandas in one shot has previously triggered the
OOM killer on a 14GB machine (see the mastr-power-capacities-germany
pipeline's module docstring for the exact failure modes). Every read here
goes through `pd.read_sql_table(..., chunksize=CHUNK_SIZE)`.
"""

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from edh.paths import MASTR_HOME_DIR

# open-mastr reads OUTPUT_PATH at import time to decide where its own
# data/xml_download + data/sqlite live -- must be set before the import.
os.environ["OUTPUT_PATH"] = str(MASTR_HOME_DIR)

from open_mastr import Mastr  # noqa: E402
from sqlalchemy import func, inspect as sa_inspect, select, table as sa_table  # noqa: E402

CHUNK_SIZE = 200_000
XML_DOWNLOAD_DIR = MASTR_HOME_DIR / "data" / "xml_download"

TECHNOLOGY_TABLES = {"wind": "wind_extended", "solar": "solar_extended", "storage": "storage_extended"}
EEG_TABLES = {"wind": "wind_eeg", "solar": "solar_eeg", "storage": "storage_eeg"}

STORAGE_LOCATION_COLUMNS = ["EinheitMastrNummer", "LokationMastrNummer"]
STORAGE_LOCATION_RENAME = {"EinheitMastrNummer": "unit_id", "LokationMastrNummer": "location_id"}

# Columns present on both technologies' `_extended` tables (original German
# names -- open-mastr 0.17.1 doesn't actually translate these despite the
# package docs, confirmed by inspecting the live schema).
COMMON_COLUMNS = [
    "EinheitMastrNummer",
    "Energietraeger",
    "Bundesland",
    "Landkreis",
    "Gemeindeschluessel",
    "Postleitzahl",
    "Laengengrad",
    "Breitengrad",
    "Nettonennleistung",
    "Bruttoleistung",
    "Inbetriebnahmedatum",
    "GeplantesInbetriebnahmedatum",
    "DatumEndgueltigeStilllegung",
    "DatumBeginnVoruebergehendeStilllegung",
    "DatumWiederaufnahmeBetrieb",
    "EinheitBetriebsstatus",
    "EinheitSystemstatus",
]
TECH_EXTRA_COLUMNS = {
    "wind": ["WindAnLandOderAufSee", "Seelage"],
    "solar": ["Einspeisungsart", "LokationMastrNummer", "Nutzungsbereich", "ArtDerSolaranlage"],
}
COLUMN_RENAME = {
    "EinheitMastrNummer": "unit_id",
    "Energietraeger": "energy_source",
    "Bundesland": "state",
    "Landkreis": "district",
    "Gemeindeschluessel": "municipality_key",
    "Postleitzahl": "postal_code",
    "Laengengrad": "longitude",
    "Breitengrad": "latitude",
    "Nettonennleistung": "net_capacity_kw",
    "Bruttoleistung": "gross_capacity_kw",
    "Inbetriebnahmedatum": "commissioning_date",
    "GeplantesInbetriebnahmedatum": "planned_commissioning_date",
    "DatumEndgueltigeStilllegung": "final_shutdown_date",
    "DatumBeginnVoruebergehendeStilllegung": "temporary_shutdown_start_date",
    "DatumWiederaufnahmeBetrieb": "resumption_of_operation_date",
    "EinheitBetriebsstatus": "unit_operational_status",
    "EinheitSystemstatus": "unit_system_status",
    "WindAnLandOderAufSee": "wind_onshore_or_offshore",
    "Seelage": "sea_location",
    "Einspeisungsart": "feed_in_type",
    "LokationMastrNummer": "location_id",
    "Nutzungsbereich": "usage_sector",
    "ArtDerSolaranlage": "installation_type",
}
CATEGORY_COLUMNS = [
    "energy_source",
    "state",
    "district",
    "municipality_key",
    "postal_code",
    "unit_operational_status",
    "unit_system_status",
    "wind_onshore_or_offshore",
    "sea_location",
    "feed_in_type",
    "usage_sector",
    "installation_type",
    "technology",
]
FLOAT32_COLUMNS = ["longitude", "latitude", "net_capacity_kw", "gross_capacity_kw"]

# Solar orientation/tracking detail -- a separate column set of the same
# `solar_extended` table (not a separate SQL table), needed to tell PECD's
# utility-fixed (62) apart from utility-tracking (63) technology code (see
# edh/pecd.py::classify_pecd_technology). Mirrors
# mastr-power-capacities-germany's original technical-detail split.
SOLAR_TECH_DETAIL_COLUMNS = ["EinheitMastrNummer", "Hauptausrichtung", "HauptausrichtungNeigungswinkel"]
SOLAR_TECH_DETAIL_RENAME = {
    "EinheitMastrNummer": "unit_id",
    "Hauptausrichtung": "main_orientation",
    "HauptausrichtungNeigungswinkel": "main_orientation_tilt_bucket",
}
# MaStR reports tilt as a self-selected bucket (e.g. "21 - 40 Grad"), not a
# precise angle, hence a category column rather than a float.
SOLAR_TECH_DETAIL_CATEGORY_COLUMNS = ["main_orientation", "main_orientation_tilt_bucket"]


def cached_zip_date() -> str | None:
    """Date string (YYYYMMDD) of an already-downloaded Gesamtdatenexport
    zip, if any -- see module docstring on snapshot pinning."""
    if not XML_DOWNLOAD_DIR.exists():
        return None
    existing = sorted(XML_DOWNLOAD_DIR.glob("Gesamtdatenexport_*.zip"))
    return existing[-1].stem.removeprefix("Gesamtdatenexport_") if existing else None


def delete_cached_zips() -> None:
    """Drop the locally cached export(s) so the next refresh is forced to
    fetch the newest snapshot instead of reusing/extending the old one."""
    if XML_DOWNLOAD_DIR.exists():
        for zip_path in XML_DOWNLOAD_DIR.glob("Gesamtdatenexport_*.zip"):
            zip_path.unlink()


def _table_row_count(db: Mastr, table_name: str) -> int:
    with db.engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(sa_table(table_name))).scalar()


def refresh_sqlite_tables(technologies: list[str], date: str | None) -> dict[str, int]:
    """(Re-)download + parse each technology's `_extended` and `_eeg` tables,
    pinned to `date` (or the newest snapshot if `date` is None). Returns
    row counts keyed by SQL table name."""
    db = Mastr()
    for technology in technologies:
        db.download(data=[technology], date=date, keep_old_downloads=True)

    row_counts = {}
    for technology in technologies:
        row_counts[TECHNOLOGY_TABLES[technology]] = _table_row_count(db, TECHNOLOGY_TABLES[technology])
        row_counts[EEG_TABLES[technology]] = _table_row_count(db, EEG_TABLES[technology])
    return row_counts


def _stream_columns_to_parquet(
    table_name: str,
    wanted: list[str],
    rename: dict[str, str],
    category_cols: list[str],
    float32_cols: list[str],
    extra_cols: dict,
    output_file: Path,
) -> tuple[int, pd.DataFrame | None]:
    """Stream `wanted` columns from `table_name` into `output_file`, chunked
    (see module docstring on why: solar's `_extended` table is ~6.3M rows).
    Returns (row count, tail of the last chunk written) -- the tail doubles
    as a cheap preview without a second full read of a potentially
    multi-hundred-MB file."""
    db = Mastr()
    available = {c["name"] for c in sa_inspect(db.engine).get_columns(table_name)}
    present = [c for c in wanted if c in available]

    writer = None
    row_total = 0
    last_chunk = None
    for chunk in pd.read_sql_table(table_name, con=db.engine, columns=present, chunksize=CHUNK_SIZE):
        chunk = chunk.reindex(columns=wanted).rename(columns=rename)
        for col, value in extra_cols.items():
            chunk[col] = value
        for col in float32_cols:
            if col in chunk:
                chunk[col] = chunk[col].astype("float32")
        for col in category_cols:
            if col in chunk:
                chunk[col] = chunk[col].astype("category")

        arrow_table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(output_file, arrow_table.schema)
        writer.write_table(arrow_table)
        row_total += len(chunk)
        last_chunk = chunk

    if writer is not None:
        writer.close()
    return row_total, (last_chunk.tail(5) if last_chunk is not None else None)


def extract_units_parquet(technology: str, output_file: Path) -> tuple[int, pd.DataFrame | None]:
    """Stream `technology`'s harmonized unit columns from SQLite into
    `output_file`, chunked."""
    wanted = COMMON_COLUMNS + TECH_EXTRA_COLUMNS.get(technology, [])
    return _stream_columns_to_parquet(
        TECHNOLOGY_TABLES[technology], wanted, COLUMN_RENAME, CATEGORY_COLUMNS, FLOAT32_COLUMNS,
        {"technology": technology}, output_file,
    )


def extract_solar_technical_detail(output_file: Path) -> tuple[int, pd.DataFrame | None]:
    """Stream solar's orientation/tracking detail from SQLite into
    `output_file`, chunked."""
    return _stream_columns_to_parquet(
        TECHNOLOGY_TABLES["solar"], SOLAR_TECH_DETAIL_COLUMNS, SOLAR_TECH_DETAIL_RENAME,
        SOLAR_TECH_DETAIL_CATEGORY_COLUMNS, [], {}, output_file,
    )


def extract_storage_location_ids(output_file: Path) -> tuple[int, pd.DataFrame | None]:
    """Stream storage's `unit_id`/`location_id` from SQLite into
    `output_file`, chunked -- see module docstring on why storage stops
    there rather than getting the full harmonized treatment."""
    return _stream_columns_to_parquet(
        TECHNOLOGY_TABLES["storage"], STORAGE_LOCATION_COLUMNS, STORAGE_LOCATION_RENAME,
        [], [], {}, output_file,
    )
