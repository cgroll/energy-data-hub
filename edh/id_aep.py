"""Download the German ID-AEP (Index Ausgleichsenergiepreis / "IP-Index")
from netztransparenz.de.

**What it is** (netztransparenz.de's own English name: "IP-Index"): a
volume-weighted average price of the *most recent* continuous intraday
trades for each quarter-hour delivery product, once cumulative traded
volume for that product reaches 500 MW. If 500 MW isn't reached on the
quarter-hour product alone within the interval, the index additionally
pulls in the most recent trades of the matching hourly product until the
combined 500 MW is reached; if even that isn't reached, the index is
undefined for that quarter-hour (a genuine gap, not an `N.A.`-style
placeholder).

**Why this matters, not just "another price series":** continuous intraday
trading closes 5 minutes before delivery, so ID-AEP is the last
market-clearing price known *before* a balancing group's final schedule is
locked in -- unlike the day-ahead price (fixed ~12-36h ahead) or reBAP
(settled from real metered values *after* delivery, ~2-week publication
lag). It is also one of the three direct inputs to reBAP's own published
calculation formula -- see `edh/aep_modules.py`'s "AEP Module 2", which is
built as `ID-AEP +/- a minimum distance`.

**Source mechanism**, same `CsvDownloadHandler.ashx` LotesCharts endpoint
as `edh/rebap.py` (see that module's docstring for the general mechanism),
`ProduktId=0` / `WebApiRoute="IdAep"` / `DataType=30`. Migrated 2026-10-05
from `~/research/energy-research/pipeline/15_download_id_aep.py`, which
discovered this endpoint.

**Availability confirmed empirically:** earliest data 2020-07-01 00:00
local (CEST) -- the server's own error message for anything before that is
"keine Daten vor dem 30.06.2020 22:00". Much shorter history than
reBAP/NRV-Saldo (2014-01-01) -- this index itself is a newer market
product. **Publication lag: effectively none**, unlike reBAP/NRV-Saldo's
~2-week "qualitätsgesichert" delay -- a same-day request returns real
(non-NaN) values through the most recent completed quarter-hour, confirmed
empirically.

**CSV column layout differs from reBAP/NRV-Saldo/AEP-Module**: `"Datum
von"`, `"(Uhrzeit) von"`, `"Zeitzone"`, `"(Uhrzeit) bis"`, `"Zeitzone.1"`
(pandas auto-dedupes the duplicate `Zeitzone` header), `"ID AEP in
EUR/MWh"` -- not the `"Datum"`/`"Zeitzone"`/`"von"`/`"bis"` layout the
other netztransparenz.de LotesCharts endpoints in this hub use.
"""

import base64
import io
import json
import urllib.parse
from datetime import date

import pandas as pd
import requests

BASE_URL = "https://www.netztransparenz.de/DesktopModules/LotesCharts/CsvDownloadHandler.ashx"
ID_AEP_START_DATE = date(2020, 7, 1)  # earliest date the server accepts (see module docstring)

_SETTINGS = {
    "DataType": 30,
    "ProduktId": 0,
    "CultureName": "de-DE",
    "Title": "Index Ausgleichsenergiepreis",
    "DiagramType": "line",
    "TimeInterval": 15,
    "DataUnit": " EUR/MWh",
    "CsvColumns": ["ID AEP"],
    "TsoIds": [],
    "NrvDirection": None,
    "WebApiRoute": "IdAep",
    "WebApiBaseUri": "https://lotes-UNB-svc-netzt.corp.transmission-it.de/StatistikApi/",
}

_TZ_OFFSET = {"CET": "+01:00", "CEST": "+02:00"}
_VALUE_COL = "ID AEP in EUR/MWh"


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
    if not text.lstrip().startswith("Datum von;"):
        raise RuntimeError(f"Unexpected response for {local_from}..{local_to}: {text[:200]!r}")

    return pd.read_csv(
        io.StringIO(text),
        sep=";",
        decimal=",",
        na_values=["N.A."],
        dtype={"Datum von": str, "(Uhrzeit) von": str, "Zeitzone": str},
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


def download_id_aep(date_from: date, date_to: date) -> pd.DataFrame:
    """Download ID-AEP, `[date_from, date_to)`, chunked by calendar year.

    Returns a DataFrame indexed by naive timestamp representing true UTC,
    single column `id_aep_eur_mwh` (EUR/MWh; NaN where the index is
    undefined -- too little intraday trading volume that quarter-hour).
    Empty DataFrame if `date_from >= date_to`.
    """
    if date_from >= date_to:
        return pd.DataFrame(columns=["id_aep_eur_mwh"])

    frames = [_download_window(w_from, w_to) for w_from, w_to in _year_windows(date_from, date_to)]
    raw = pd.concat(frames, ignore_index=True)

    offset = raw["Zeitzone"].map(_TZ_OFFSET)
    if offset.isna().any():
        unknown = sorted(raw.loc[offset.isna(), "Zeitzone"].unique())
        raise RuntimeError(f"Unknown Zeitzone label(s): {unknown}")

    naive_local = pd.to_datetime(raw["Datum von"] + " " + raw["(Uhrzeit) von"], format="%d.%m.%Y %H:%M")
    ts_str = naive_local.dt.strftime("%Y-%m-%d %H:%M:%S") + offset
    timestamp = pd.to_datetime(ts_str, utc=True).dt.tz_localize(None)  # true UTC, naive -- hub convention

    out = pd.DataFrame(
        {"id_aep_eur_mwh": raw[_VALUE_COL].to_numpy()},
        index=timestamp,
    ).sort_index()
    out.index.name = "timestamp"

    out = out[~out.index.duplicated(keep="first")]
    return out[(out.index >= pd.Timestamp(date_from)) & (out.index < pd.Timestamp(date_to))]
