"""Download the German AEP Module 1/2/3 series from netztransparenz.de --
the three published components reBAP is the max/min of.

**What it is**: netztransparenz.de's own calculation methodology ("reBAP
Model description", in force since 2023-11-01) builds reBAP as
`max(Module1, Module2, Module3)` for a quarter-hour where the GCC balance
(`edh/nrv_saldo.py`'s `nrv_saldo_mw`) was positive (short), or
`min(Module1, Module2, Module3)` if negative (long):

- **Module 1 (base component)**: the real, settled price of aFRR/mFRR
  balancing energy actually activated that quarter-hour, from the European
  PICASSO (aFRR) and MARI (mFRR) platforms -- the only one of the three
  modules built from genuinely observed activation prices rather than a
  calibrated formula.
- **Module 2 (incentivising component)**: `ID-AEP +/- ΔP`, a minimum
  distance from `edh/id_aep.py`'s ID-AEP that scales with the GCC balance's
  size (saturating at 500 MW average power) and whose sign follows the
  balance's own sign.
- **Module 3 (scarcity component)**: a parabolic penalty function of the
  GCC balance, active only once `|balance|` reaches 80% of dimensioned FRR
  capacity in that direction -- a deliberately steep "scarcity alarm", not
  a cost reflection, capped at `2 x Pr_ID.Limit` (currently 19,998
  EUR/MWh).

**Source mechanism**, same `CsvDownloadHandler.ashx` LotesCharts endpoint
as `edh/rebap.py`, found by reading the "AEP-Module" overview page's own
inline chart config: `ProduktId=27` /
`WebApiRoute="NrvSaldo/AEPModule/Qualitaetsgesichert"`. Found and
validated 2026-10-05 in `energy-research`'s exploratory pipeline before
being promoted here: the full `max`/`min(Module1,2,3)` formula
reconstructs real reBAP to a 99.99% exact match once the Module-3 quirk
below is corrected -- see `energy-insights`' `page_rebap_formula_reconstruction`
for that reconstruction, rebuilt against this hub's own assets.

**Availability confirmed empirically:** earliest data 2022-06-22 00:00
local (CEST) -- consistent with the module-based reBAP methodology taking
effect per BNetzA decision BK6-21-192 (2022-04-28). **Publication lag: ~2
weeks**, same "qualitätsgesichert" tier as reBAP itself (unlike ID-AEP's
near-zero lag) -- the underlying settlement process is the same one
reBAP's own quality-assurance pipeline uses.

**⚠️ Module 3's "doesn't apply" case is encoded two different, inconsistent
ways in the raw source, not documented anywhere by netztransparenz.de --
found empirically.** Sometimes the raw cell is `N.E.` ("nicht einschlägig",
parsed as NaN like any other `N.A.`-style placeholder); 99.93% of the time
it's instead a literal `0,00`. The calculation formula's own "the scarcity
component has no effect" below the 80% threshold means the inactive case
must be *excluded* from the `max`/`min` across modules, not treated as a
real candidate value of zero -- a genuinely *active* Module 3 starts at
Module 2's own value right at the 80% threshold and grows from there
(the formula's own continuity condition), so it essentially never
legitimately lands on exactly 0.00 while active. Confirmed on the
2022-06-22-onward download: of all non-N.E. raw values, 99.93% are exactly
0.0 and only ~0.02% of all quarter-hours have a genuinely nonzero Module 3
(range roughly -6,700 to +10,100 EUR/MWh, consistent with the scarcity
parabola). **This module converts exact 0.0 to NaN for `aep_module3_eur_mwh`
before returning** -- without that fix, a consumer naively computing
`max`/`min(module1, module2, module3)` gets the wrong answer whenever the
spurious zero wins (it very often does, since module1/2 are typically far
from zero): confirmed empirically while prototyping this reconstruction --
this exact bug dropped the match rate from 99.99% to 89.6% before being
found and fixed.
"""

import base64
import io
import json
import urllib.parse
from datetime import date

import numpy as np
import pandas as pd
import requests

BASE_URL = "https://www.netztransparenz.de/DesktopModules/LotesCharts/CsvDownloadHandler.ashx"
AEP_MODULES_START_DATE = date(2022, 6, 22)  # earliest date the server accepts (see module docstring)

_SETTINGS = {
    "DataType": 20,
    "ProduktId": 27,
    "CultureName": "de-DE",
    "Title": "AEP Module",
    "DiagramType": "line",
    "TimeInterval": 15,
    "DataUnit": "EUR/MWh",
    "CsvColumns": ["50Hertz", "Amprion", "TenneT TSO", "TransnetBW"],
    "TsoIds": [0],
    "NrvDirection": 0,
    "WebApiRoute": "NrvSaldo/AEPModule/Qualitaetsgesichert",
    "WebApiBaseUri": "https://lotes-UNB-svc-netzt.corp.transmission-it.de/StatistikApi/",
}

_TZ_OFFSET = {"CET": "+01:00", "CEST": "+02:00"}
_VALUE_COLS = {"AEP Modul 1": "aep_module1_eur_mwh", "AEP Modul 2": "aep_module2_eur_mwh", "AEP Modul 3": "aep_module3_eur_mwh"}


def _build_url(local_from: str, local_to: str) -> str:
    request = {
        "LocalFrom": local_from,
        "LocalTo": local_to,
        "ResultTimeZone": "cet",
        "Settings": _SETTINGS,
    }
    json_str = json.dumps(request, separators=(",", ":"))
    b64 = base64.b64encode(json_str.encode("utf-8")).decode("ascii")
    return f"{BASE_URL}?request={urllib.parse.quote(b64)}"


def _download_window(local_from: date, local_to: date) -> pd.DataFrame:
    url = _build_url(local_from.isoformat(), local_to.isoformat())
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=120)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig")
    if not text.lstrip().startswith("Datum;"):
        raise RuntimeError(f"Unexpected response for {local_from}..{local_to}: {text[:200]!r}")

    return pd.read_csv(
        io.StringIO(text),
        sep=";",
        decimal=",",
        na_values=["N.E.", "N.A."],
        dtype={"Datum": str, "Zeitzone": str, "von": str, "bis": str},
    )


def _year_windows(date_from: date, date_to: date) -> list[tuple[date, date]]:
    """`[date_from, date_to)` split into calendar-year chunks -- same
    pattern as `edh/rebap.py`."""
    windows = []
    year = date_from.year
    while date(year, 1, 1) < date_to:
        window_from = max(date_from, date(year, 1, 1))
        window_to = min(date_to, date(year + 1, 1, 1))
        windows.append((window_from, window_to))
        year += 1
    return windows


def download_aep_modules(date_from: date, date_to: date) -> pd.DataFrame:
    """Download AEP Module 1/2/3, `[date_from, date_to)`, chunked by
    calendar year.

    Returns a DataFrame indexed by naive timestamp representing true UTC,
    columns `aep_module1_eur_mwh` / `aep_module2_eur_mwh` /
    `aep_module3_eur_mwh` (EUR/MWh; NaN where a module doesn't apply that
    quarter-hour -- Module 3's raw `0.0`-means-inactive quirk already
    corrected to NaN here, see module docstring). Empty DataFrame if
    `date_from >= date_to`.
    """
    if date_from >= date_to:
        return pd.DataFrame(columns=list(_VALUE_COLS.values()))

    frames = [_download_window(w_from, w_to) for w_from, w_to in _year_windows(date_from, date_to)]
    raw = pd.concat(frames, ignore_index=True)

    offset = raw["Zeitzone"].map(_TZ_OFFSET)
    if offset.isna().any():
        unknown = sorted(raw.loc[offset.isna(), "Zeitzone"].unique())
        raise RuntimeError(f"Unknown Zeitzone label(s): {unknown}")

    naive_local = pd.to_datetime(raw["Datum"] + " " + raw["von"], format="%d.%m.%Y %H:%M")
    ts_str = naive_local.dt.strftime("%Y-%m-%d %H:%M:%S") + offset
    timestamp = pd.to_datetime(ts_str, utc=True).dt.tz_localize(None)  # true UTC, naive -- hub convention

    out = pd.DataFrame(
        {out_col: raw[raw_col].to_numpy() for raw_col, out_col in _VALUE_COLS.items()},
        index=timestamp,
    ).sort_index()
    out.index.name = "timestamp"

    # See module docstring: raw 0.0 means "inactive" 99.93% of the time, not a real candidate
    # value -- must be excluded from any downstream max/min across modules, same as NaN.
    out.loc[out["aep_module3_eur_mwh"] == 0.0, "aep_module3_eur_mwh"] = np.nan

    out = out[~out.index.duplicated(keep="first")]
    return out[(out.index >= pd.Timestamp(date_from)) & (out.index < pd.Timestamp(date_to))]
