"""Kelmarsh wind farm data (Zenodo record 5841834) -- UK, 6x Senvion MM92,
12.3 MW installed. Provides the real, metered ground truth used to
validate PECD's onshore wind capacity factor (zone `UK03`, confirmed via
`peon_region_mask.nc`) against actual generation -- see energy-insights'
`page_kelmarsh_vs_pecd`.

Migrated 2026-09-28 from `energy-research`'s exploratory prototype, which
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
Investments Ltd, CC-BY-4.0.

**Load pattern: `full_refresh`.** The underlying Zenodo record is a fixed,
versioned archival dataset (2016-01-01 to 2021-07-01, not an ongoing
feed) -- small and with no revision risk, so re-downloading and
overwriting the whole thing every run is simpler than any watermark
logic. See `docs/load_patterns.md`.

**2026-09-29 addition -- per-turbine SCADA (wind speed / power):**
migrated from `energy-research`'s `03_download_kelmarsh_scada.py` +
`04_compare_windspeed_reconstruction.py`, which found that real per-
turbine nacelle wind speed run through `windpowerlib`'s real MM92/2050
power curve (an exact nameplate match for Kelmarsh's turbines) tracks
actual generation far better than PECD (hourly NMAE 7.3% vs. PECD's
35.8%, both availability-adjusted) -- strong evidence PECD's hourly-scale
error is mostly its coarse weather-grid input, not its conversion
formula. See that repo's PROJECT.md for the full investigation, including
two negative/calibration results worth knowing before reusing this data:
a "density-adjusted" wind speed variant looked better raw but turned out
to be a data-coverage artifact (its missing rows concentrate in low-
availability hours), and even summing each turbine's own real metered
power (no model at all) still misses the grid meter by NMAE 4.0% --
ordinary transformer/house-load loss, a real ceiling no wind-speed model
can beat.

The raw per-year SCADA zips (~1.5 GB combined across 2016-2021, one file
per turbine per year with ~250 columns each) are disposable intermediates
here too, same as the grid meter zip -- downloaded to memory, parsed, and
discarded; only `Wind speed (m/s)`, `Density adjusted wind speed (m/s)`,
`Power (kW)`, and `Data Availability` are kept, not the full per-turbine
telemetry (temperatures, vibration, curtailment-by-cause breakdowns,
...). A future consumer needing more of that telemetry should extend
`download_turbine_scada` rather than re-parsing the zips separately.
"""

import io
import urllib.request
import zipfile

import pandas as pd

RECORD_FILES_BASE = "https://zenodo.org/api/records/5841834/files"
STATIC_URL = f"{RECORD_FILES_BASE}/Kelmarsh_WT_static.csv/content"
GRID_ZIP_URL = f"{RECORD_FILES_BASE}/Kelmarsh_Grid_3088.zip/content"
SCADA_ZIPS = {
    2016: "Kelmarsh_SCADA_2016_3082.zip",
    2017: "Kelmarsh_SCADA_2017_3083.zip",
    2018: "Kelmarsh_SCADA_2018_3084.zip",
    2019: "Kelmarsh_SCADA_2019_3085.zip",
    2020: "Kelmarsh_SCADA_2020_3086.zip",
    2021: "Kelmarsh_SCADA_2021_3087.zip",
}
SCADA_COLUMNS_TO_KEEP = [
    "Wind speed (m/s)",
    "Density adjusted wind speed (m/s)",
    "Power (kW)",
    "Data Availability",
]


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


def _parse_turbine_csv(raw_bytes: bytes) -> tuple[str, pd.DataFrame]:
    lines = raw_bytes.decode("utf-8").splitlines()
    turbine_name = next(l for l in lines if l.startswith("# Turbine:")).split(":", 1)[1].strip()
    header_idx = next(i for i, line in enumerate(lines) if line.startswith("# Date and time"))

    df = pd.read_csv(io.BytesIO(raw_bytes), skiprows=header_idx)
    df.columns = [c.lstrip("# ").strip() for c in df.columns]
    df["Date and time"] = pd.to_datetime(df["Date and time"], utc=True).dt.tz_localize(None)
    df = df.set_index("Date and time")

    keep = [c for c in SCADA_COLUMNS_TO_KEEP if c in df.columns]
    return turbine_name, df[keep]


def download_turbine_scada() -> pd.DataFrame:
    """10-minute per-turbine SCADA: real nacelle `Wind speed (m/s)` (plus
    `Density adjusted wind speed (m/s)` where Greenbyte computed it --
    ~92% coverage, concentrated-missing during low-availability periods,
    see module docstring), each turbine's own metered `Power (kW)`, and
    `Data Availability`, 2016-01-03 to 2021-06-30, all 6 turbines, long
    format (one row per turbine per timestamp, `turbine` column
    identifies which).

    Downloads and parses all 6 years' SCADA zips (~1.5 GB combined) in
    memory -- nothing raw is written to disk, only this narrow extraction.
    Slow (network-bound on 1.5 GB): expect several minutes.
    """
    frames = []
    for zip_name in SCADA_ZIPS.values():
        zip_bytes = _download(f"{RECORD_FILES_BASE}/{zip_name}/content")
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            turbine_files = [n for n in zf.namelist() if n.startswith("Turbine_Data_")]
            for name in turbine_files:
                turbine_name, df = _parse_turbine_csv(zf.read(name))
                df = df.reset_index().rename(columns={"Date and time": "timestamp"})
                df["turbine"] = turbine_name
                frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(["turbine", "timestamp"]).reset_index(drop=True)
