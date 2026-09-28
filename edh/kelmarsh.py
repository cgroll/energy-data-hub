"""Kelmarsh wind farm data (Zenodo record 5841834) -- UK, 6x Senvion MM92,
12.3 MW installed. Provides the real, metered ground truth used to
validate PECD's onshore wind capacity factor (zone `UK03`, confirmed via
`peon_region_mask.nc`) against actual generation -- see energy-insights'
`page_kelmarsh_vs_pecd`.

Migrated 2026-09-28 from `energy-research`'s exploratory prototype
(`pipeline/01_download_kelmarsh.py` + `02_compare_kelmarsh_pecd.py`), which
established there: zone UK03 as the right PECD zone for Kelmarsh's
coordinates, that a naive PECD-vs-actual comparison lines up well
(r=0.81 on monthly means over 2016-2021), and that most of the remaining
mismatch is explained by real downtime rather than a PECD modeling error
(correcting for hourly-averaged `Energy meter based availability` lifts
the monthly correlation to r=0.96) -- see that repo's PROJECT.md for the
full investigation. Promoted here now that the comparison looks sound;
the analysis notebook itself is rebuilt against this hub's own outputs in
energy-insights, not re-derived from `energy-research`'s copy.

**Source:** https://zenodo.org/records/5841834, Cubico Sustainable
Investments Ltd, CC-BY-4.0. Only the two files needed for farm-level
generation are fetched here -- turbine static specs and the site's
fiscal/grid meter export -- not the much larger (~1.5 GB combined)
per-turbine SCADA zips, which this comparison doesn't need.

**Load pattern: `full_refresh`.** The underlying Zenodo record is a fixed,
versioned archival dataset (2016-01-01 to 2021-07-01, not an ongoing
feed) -- small and with no revision risk, so re-downloading and
overwriting the whole thing every run is simpler than any watermark
logic. See `docs/load_patterns.md`.
"""

import io
import urllib.request
import zipfile

import pandas as pd

RECORD_FILES_BASE = "https://zenodo.org/api/records/5841834/files"
STATIC_URL = f"{RECORD_FILES_BASE}/Kelmarsh_WT_static.csv/content"
GRID_ZIP_URL = f"{RECORD_FILES_BASE}/Kelmarsh_Grid_3088.zip/content"


def _download(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()


def download_wt_static() -> pd.DataFrame:
    """Per-turbine static specs: coordinates, rated power (kW), hub
    height/rotor diameter (m), elevation (m), commercial operations date
    -- one row per turbine, for the 6 Senvion MM92 units."""
    raw = _download(STATIC_URL)
    return pd.read_csv(io.BytesIO(raw))


def download_grid_meter() -> pd.DataFrame:
    """10-minute site grid meter export: `Grid Meter Energy Export (kWh)`
    -- the real, metered generation at the farm's grid connection point --
    plus Greenbyte's own availability flags (`Grid Meter Data
    Availability`, `Energy meter based availability`), 2016-01-01 to
    2021-07-01.

    The source CSV wraps its data in a `#`-prefixed metadata block, and
    the header row itself starts with `# Date and time` -- both handled
    here rather than left for a downstream consumer to work around.
    Timestamps are parsed as UTC (the source states UTC explicitly) and
    then stripped to naive, matching this hub's "naive but true UTC
    instant" convention (see BEST_PRACTICES.md).
    """
    zip_bytes = _download(GRID_ZIP_URL)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        device_data_name = next(n for n in zf.namelist() if n.startswith("Device_Data_"))
        raw_bytes = zf.read(device_data_name)

    lines = raw_bytes.decode("utf-8").splitlines()
    header_idx = next(i for i, line in enumerate(lines) if line.startswith("# Date and time"))

    df = pd.read_csv(io.BytesIO(raw_bytes), skiprows=header_idx)
    df.columns = [c.lstrip("# ").strip() for c in df.columns]
    df["Date and time"] = pd.to_datetime(df["Date and time"], utc=True).dt.tz_localize(None)
    return df.set_index("Date and time").rename_axis("timestamp")
