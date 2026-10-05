"""Download the German NRV-Saldo (Netzregelverbund-Saldo) from
netztransparenz.de.

**What it is** (netztransparenz.de's own definition): for each of the four
German TSOs, the sum of all balancing measures employed gives that TSO's
own Regelzonen-Saldo (RZ-Saldo) -- the aggregate deviation between
consumption and generation across every balancing group in that TSO's
zone. The sum of the four RZ-Salden is the NRV-Saldo for Germany as a
whole -- the same quantity reBAP's own published calculation formula calls
"Balance_GCC" (GCC = German Control Cooperation), see `edh/aep_modules.py`.
It is **not** simply "real load minus day-ahead auction volume" -- the
"scheduled" side it's measured against is each balancing group's own
nomination (fed by day-ahead trades, intraday trades, and bilateral
contracts together), not day-ahead volume alone.

**Sign convention** (confirmed on the source page): positive = the NRV
was, on average across that quarter-hour, **under-supplied** (net short,
positive/upward balancing energy needed); negative = **over-supplied**
(net long, negative/downward balancing energy needed).

**Source mechanism**, same `CsvDownloadHandler.ashx` LotesCharts endpoint
as `edh/rebap.py` (see that module's docstring for the general mechanism),
`ProduktId=6` / `WebApiRoute="NrvSaldo/nrvsaldo/qualitaetsgesichert"` --
the "quality-assured" variant, matching reBAP's own choice (a near-real-
time "betrieblich" variant also exists on the source page but isn't used
here). `TsoIds=[0]` returns a single `Deutschland` column (the national
aggregate), not a per-TSO breakdown. Found and validated against reBAP
2026-10-05 in `energy-research`'s exploratory pipeline before being
promoted here -- see that repo's `09_nrv_saldo_vs_rebap.py` for the
original spread-vs-imbalance investigation, and `edh/aep_modules.py` for
how NRV-Saldo feeds directly into reBAP's own published formula.

**Availability confirmed empirically:** earliest data 2014-01-01 (server
silently clips anything requested before that, same cutoff as reBAP, both
computed from the same underlying settlement process). Quality-assured
data has a publication lag of roughly 2 weeks, same order of magnitude as
reBAP's own lag -- handled by the Dagster asset
(`edh_dagster/assets/balancing_market.py`), not here; this module just
passes `NaN` through for not-yet-published rows.

**⚠️ Known real, multi-month gaps in this series' history -- not a
download bug.** Confirmed in `energy-research`'s own investigation (see
`09_nrv_saldo_vs_rebap.py`): 2014/2015 (~8.5% missing each), 2016 (a single
unbroken gap 2016-02-11 to 2016-10-31, ~72% of the year), 2018 (three
separate gaps totaling ~33% of the year), 2022 (a single unbroken gap
2022-02-28 to 2022-05-31, ~25% of the year). 2017, 2019-2021, and
2023-onward are each ≥99.99% complete. Unlike `rebap_gap_check`, this
asset's gap check (`edh_dagster/checks/balancing_market.py`) does not fail
loudly on these -- see that module for why.
"""

import base64
import io
import json
import urllib.parse
from datetime import date

import pandas as pd
import requests

BASE_URL = "https://www.netztransparenz.de/DesktopModules/LotesCharts/CsvDownloadHandler.ashx"
NRV_SALDO_START_DATE = date(2014, 1, 1)  # earliest date the server accepts, same cutoff as reBAP

_SETTINGS = {
    "DataType": 20,
    "ProduktId": 6,  # "NRV-Saldo qualitätsgesichert" -- revised/final, matching reBAP's own choice
    "CultureName": "de-DE",
    "Title": "NRV-Saldo qualitätsgesichert",
    "DiagramType": "line",
    "TimeInterval": 15,
    "DataUnit": "MW",
    "CsvColumns": ["50Hertz", "Amprion", "TenneT TSO", "TransnetBW"],
    "TsoIds": [0],  # 0 = national aggregate (returns a single "Deutschland" column)
    "NrvDirection": 6,
    "WebApiRoute": "NrvSaldo/nrvsaldo/qualitaetsgesichert",
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
        na_values=["N.A."],
        dtype={"Datum": str, "Zeitzone": str, "von": str, "bis": str},
    )


def _year_windows(date_from: date, date_to: date) -> list[tuple[date, date]]:
    """`[date_from, date_to)` split into calendar-year chunks -- same
    pattern as `edh/rebap.py`, keeps each request well under the server's
    large-range internal-server-error limit."""
    windows = []
    year = date_from.year
    while date(year, 1, 1) < date_to:
        window_from = max(date_from, date(year, 1, 1))
        window_to = min(date_to, date(year + 1, 1, 1))
        windows.append((window_from, window_to))
        year += 1
    return windows


def download_nrv_saldo(date_from: date, date_to: date) -> pd.DataFrame:
    """Download NRV-Saldo, `[date_from, date_to)`, chunked by calendar year.

    Returns a DataFrame indexed by naive timestamp representing true UTC
    (same conversion approach as `edh/rebap.py`), single column
    `nrv_saldo_mw` (MW; positive = under-supplied, negative =
    over-supplied). Empty DataFrame if `date_from >= date_to`.
    """
    if date_from >= date_to:
        return pd.DataFrame(columns=["nrv_saldo_mw"])

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
        {"nrv_saldo_mw": raw["Deutschland"].to_numpy()},
        index=timestamp,
    ).sort_index()
    out.index.name = "timestamp"

    out = out[~out.index.duplicated(keep="first")]  # year-boundary requests can overlap by one row
    return out[(out.index >= pd.Timestamp(date_from)) & (out.index < pd.Timestamp(date_to))]
