"""Download the German reBAP (balancing energy price) from netztransparenz.de.

Source: netztransparenz.de (the four German TSOs' joint transparency
platform). The public reBAP page (https://www.netztransparenz.de/de-de/
Regelenergie/Ausgleichsenergiepreis/reBAP) renders a chart via a legacy DNN
"LotesCharts" module, whose "download CSV" button calls a plain,
unauthenticated GET endpoint:

    GET /DesktopModules/LotesCharts/CsvDownloadHandler.ashx?request=<base64 JSON>

where `<base64 JSON>` encodes `{LocalFrom, LocalTo, ResultTimeZone,
Settings}` (discovered by reading the page's own `DownloadHandler.js` -- no
account, API key, or registration needed). Migrated 2026-09-30 from
~/research/vpp-learning/pipeline/01_download_rebap.py, which discovered
this endpoint; see that repo's git history for the original investigation.
A different netztransparenz.de endpoint (ASP.NET WebForms postback) is
already used by `edh/redispatch_measures.py` -- unrelated mechanism, same
site.

reBAP is published quarter-hourly and is symmetric by design: the same
single price applies whether a balancing group was short or long in that
interval (confirmed both by netztransparenz.de's own documentation and
empirically -- the "unterdeckt"/"ueberdeckt" columns match in every row,
with the documented exception of capacity-reserve (KapRes) activation
quarter-hours).

**Publication lag:** the most recent few days come back as `N.A.`
("quality-assured price not yet published") rather than a number --
handled by the Dagster asset (`edh_dagster/assets/balancing_market.py`),
not here; this module just passes `NaN` through.

The server rejects date ranges before 2014-01-01 ("keine Daten vor dem
31.12.2013 23:00") and returns an internal-server-error for very large
ranges in one call -- `download_rebap` chunks internally by calendar year
to stay well under that limit, regardless of how wide a range is requested.
"""

import base64
import io
import json
import urllib.parse
from datetime import date

import pandas as pd
import requests

BASE_URL = "https://www.netztransparenz.de/DesktopModules/LotesCharts/CsvDownloadHandler.ashx"
REBAP_START_DATE = date(2014, 1, 1)  # earliest date the server accepts ("qualitätsgesichert" reBAP)

_SETTINGS = {
    "DataType": 20,
    "ProduktId": 10,  # "reBAP unterdeckt" chart; response also includes "ueberdeckt"
    "CultureName": "de-DE",
    "Title": "reBAP unterdeckt",
    "DiagramType": "line",
    "TimeInterval": 15,
    "DataUnit": "EUR/MWh",
    "CsvColumns": ["50Hertz", "Amprion", "TenneT TSO", "TransnetBW"],
    "TsoIds": [0],
    "NrvDirection": 0,
    "WebApiRoute": "NrvSaldo/rebap/qualitaetsgesichert",
    "WebApiBaseUri": "https://lotes-UNB-svc-netzt.corp.transmission-it.de/StatistikApi/",
}

_TZ_OFFSET = {"CET": "+01:00", "CEST": "+02:00"}


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
    """One raw CSV request, `[local_from, local_to)` (`local_to` exclusive --
    matches the endpoint's own convention)."""
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
        na_values=["N.A."],  # most recent days: "quality-assured" price not yet published
        dtype={"Datum": str, "Zeitzone": str, "von": str, "bis": str},
    )


def _year_windows(date_from: date, date_to: date) -> list[tuple[date, date]]:
    """`[date_from, date_to)` split into calendar-year chunks -- one request
    per year, matching the pattern that's already proven not to hit the
    server's large-range internal-server-error."""
    windows = []
    year = date_from.year
    while date(year, 1, 1) < date_to:
        window_from = max(date_from, date(year, 1, 1))
        window_to = min(date_to, date(year + 1, 1, 1))
        windows.append((window_from, window_to))
        year += 1
    return windows


def download_rebap(date_from: date, date_to: date) -> pd.DataFrame:
    """Download reBAP, `[date_from, date_to)`, chunked by calendar year.

    Returns a DataFrame indexed by naive timestamp representing true UTC
    (converted from the source's Europe/Berlin wall-clock + explicit
    CET/CEST label per row -- exact, no DST-transition ambiguity, same
    approach as `edh/redispatch_measures.py`), columns `rebap_eur_mwh` and
    `rebap_ueberdeckt_eur_mwh` (EUR/MWh; identical except in rare
    KapRes-activation quarter-hours -- see module docstring). Empty
    DataFrame if `date_from >= date_to`.
    """
    if date_from >= date_to:
        return pd.DataFrame(columns=["rebap_eur_mwh", "rebap_ueberdeckt_eur_mwh"])

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
        {
            # .to_numpy(): avoid label-alignment against raw's own RangeIndex
            # when combined with the explicit `index=timestamp` below
            "rebap_eur_mwh": raw["reBAP unterdeckt"].to_numpy(),
            "rebap_ueberdeckt_eur_mwh": raw["reBAP ueberdeckt"].to_numpy(),
        },
        index=timestamp,
    ).sort_index()
    out.index.name = "timestamp"

    out = out[~out.index.duplicated(keep="first")]  # year-boundary requests can overlap by one row
    return out[(out.index >= pd.Timestamp(date_from)) & (out.index < pd.Timestamp(date_to))]
