"""Download and parsing helpers for netztransparenz.de's per-measure
Redispatch export (Format 5) -- one row per redispatch measure, with real
start/end timestamps, direction, MW/MWh, affected plant/cluster name, and a
coarse primary-energy-type flag.

Canonical copy -- see ~/research/energy-platform/energy-data-hub/README.md. Consolidated
unchanged from ~/research/pecd-replication/pecdr/redispatch_measures.py
(2026-09-23). The complementary, coarser monthly series lives in
`edh/smard_redispatch.py`.
"""

import json
import re

import numpy as np
import pandas as pd
import requests

REDISPATCH_PAGE_URL = "https://www.netztransparenz.de/en/Ancillary-Services/System-operations/Redispatch"
EARLIEST_AVAILABLE_DATE = "2021-01-01"  # the page's own date picker refuses anything earlier


def _extract_form_fields(html: str) -> dict[str, str]:
    """Every `name`/`value` pair from `<input>` elements on the page --
    ASP.NET WebForms' view-state/event-validation hidden fields plus the
    visible date-picker inputs, needed verbatim to replicate a real
    postback (see `download_redispatch_measures`).
    """
    fields = {}
    for tag in re.findall(r"<input\b[^>]*>", html, re.IGNORECASE):
        name_match = re.search(r'name="([^"]*)"', tag)
        if not name_match:
            continue
        value_match = re.search(r'value="([^"]*)"', tag)
        fields[name_match.group(1)] = value_match.group(1) if value_match else ""
    return fields


def download_redispatch_measures(from_date: str = EARLIEST_AVAILABLE_DATE, to_date: str | None = None) -> bytes:
    """Download the raw per-measure Redispatch CSV (Format 5) for
    `[from_date, to_date]` (`to_date` defaults to today), exactly as the
    page's own "Download CSV" button would produce it.

    Unlike SMARD's chart CSVs, this isn't a plain URL -- the button is an
    ASP.NET WebForms postback (`__doPostBack('dnn$ctr2598$View$DownloadCSVButton',
    '')` against the same page), carrying the full view-state/
    event-validation hidden fields plus the two RadDatePicker date fields.
    Replicated here by GETting the page once for a fresh view-state, then
    POSTing it back with `__EVENTTARGET` set to the button.

    The date range needs each RadDatePicker's `..._dateInput_ClientState`
    hidden field set to a JSON blob, not just the plain text/date fields --
    confirmed empirically: overriding only the plain fields silently no-ops
    server-side (the response is byte-identical to the page's own
    year-to-date default, regardless of what the plain fields say), while
    adding `ClientState` reproduces exactly what the calendar widget itself
    sends on a real click.
    """
    if to_date is None:
        to_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    def us_date(iso_date: str) -> str:
        ts = pd.Timestamp(iso_date)
        return f"{ts.month}/{ts.day}/{ts.year}"

    def client_state(iso_date: str) -> str:
        timestamp = f"{iso_date}-00-00-00"
        return json.dumps({
            "enabled": True,
            "minDateStr": "1980-01-01-00-00-00",
            "maxDateStr": "2099-12-31-00-00-00",
            "validationText": timestamp,
            "valueAsString": timestamp,
            "lastSetTextBoxValue": us_date(iso_date),
        })

    session = requests.Session()
    page = session.get(REDISPATCH_PAGE_URL, timeout=30)
    page.raise_for_status()

    fields = _extract_form_fields(page.text)
    fields.update({
        "__EVENTTARGET": "dnn$ctr2598$View$DownloadCSVButton",
        "__EVENTARGUMENT": "",
        "dnn$ctr2598$View$FromDatePicker": from_date,
        "dnn$ctr2598$View$FromDatePicker$dateInput": us_date(from_date),
        "dnn_ctr2598_View_FromDatePicker_dateInput_ClientState": client_state(from_date),
        "dnn$ctr2598$View$ToDatePicker": to_date,
        "dnn$ctr2598$View$ToDatePicker$dateInput": us_date(to_date),
        "dnn_ctr2598_View_ToDatePicker_dateInput_ClientState": client_state(to_date),
    })

    response = session.post(REDISPATCH_PAGE_URL, data=fields, timeout=120)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "").split(";")[0]
    if content_type != "text/csv":
        raise RuntimeError(
            f"Expected a text/csv response, got content-type={content_type!r} -- "
            "netztransparenz's page markup likely changed; re-inspect the postback fields."
        )
    return response.content


# netztransparenz silently dropped the "OWP " prefix from these four
# offshore grid-connection substations starting August 2025 (confirmed by
# checking the raw data directly: "OWP UW Büttel" et al. appear only
# through 2025-07, "UW Büttel" et al. only from 2025-08 onward, for all
# four simultaneously) -- without this list, a plain "OWP" substring check
# would silently reclassify real offshore volume as "ambiguous" from that
# point on.
RENAMED_OFFSHORE_SUBSTATIONS = {"UW BÜTTEL", "UW DIELE", "UW DÖRPEN-WEST", "UW EMDEN-OST"}


def classify_entity_name(name: str) -> str:
    """Bucket a `BETROFFENE_ANLAGE` plant/cluster name by what its name
    alone reveals about its technology. Offshore wind farms are almost
    always individually named (`OWP ...`), plus the four renamed
    substations above; onshore wind and PV are only individually named a
    small fraction of the time -- most sit under an anonymous
    substation/cluster code, returned as "ambiguous".
    """
    n = str(name).upper()
    if "OWP" in n or "OFFSHORE" in n or n in RENAMED_OFFSHORE_SUBSTATIONS:
        return "offshore_wind"
    if n.startswith("PV ") or " PV " in f" {n} " or "PHOTOVOLTA" in n:
        return "pv"
    if n.startswith("WP ") or " WP " in f" {n} " or "WINDPARK" in n:
        return "onshore_wind_named"
    return "ambiguous"


def load_redispatch_measures(path) -> pd.DataFrame:
    """Parse the raw per-measure CSV into a tidy DataFrame with columns
    `start`, `end`, `month`, `direction` ("reduction"/"increase"), `mwh`,
    `avg_mw`, `max_mw`, `reason`, `instructing_tso`, `entity`,
    `primary_energy_type`, `tech` (the name-based classification above).

    Handles three real quirks in the raw file: `;`-delimited with a
    leading UTF-8 BOM, comma decimals, and a handful of mis-encoded
    `RICHTUNG` values (e.g. "erh¿hen" instead of "erhöhen") -- normalized
    by checking a "reduz"/"erh" prefix rather than an exact string match.

    **`start`/`end` are Europe/Berlin wall-clock, not UTC** -- confirmed
    directly from the raw CSV's own `ZEITZONE_VON`/`ZEITZONE_BIS` columns
    (CEST/CET), which correctly straddle real DST transitions. Those two
    columns are read here only to confirm this, then discarded: `start`/
    `end` are parsed from the bare date+time fields with no DST
    disambiguation, so the ~2 hours/year at the fall DST transition where
    local wall-clock time is genuinely ambiguous (both an 02:xx CEST and
    an 02:xx CET instant exist) are not distinguished -- a small, known
    imprecision (order of 1-4 rows out of ~89k), not a silent full-scale
    UTC/local mismatch like the one `edh/smard.py` deliberately avoids.
    """
    raw = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    raw.columns = [c.strip() for c in raw.columns]

    start = pd.to_datetime(raw["BEGINN_DATUM"] + " " + raw["BEGINN_UHRZEIT"], format="%d.%m.%Y %H:%M")
    end = pd.to_datetime(raw["ENDE_DATUM"] + " " + raw["ENDE_UHRZEIT"], format="%d.%m.%Y %H:%M")
    mwh = raw["GESAMTE_ARBEIT_MWH"].astype(str).str.replace(",", ".").astype(float)
    avg_mw = raw["MITTLERE_LEISTUNG_MW"].astype(str).str.replace(",", ".").astype(float)
    max_mw = raw["MAXIMALE_LEISTUNG_MW"].astype(str).str.replace(",", ".").astype(float)
    direction = np.where(raw["RICHTUNG"].str.startswith("Wirkleistungseinspeisung reduz"), "reduction", "increase")

    tidy = pd.DataFrame({
        "start": start,
        "end": end,
        "month": start.values.astype("datetime64[M]"),
        "direction": direction,
        "mwh": mwh,
        "avg_mw": avg_mw,
        "max_mw": max_mw,
        "reason": raw["GRUND_DER_MASSNAHME"],
        "instructing_tso": raw["ANWEISENDER_UENB"],
        "entity": raw["BETROFFENE_ANLAGE"],
        "primary_energy_type": raw["PRIMAERENERGIEART"],
    })
    tidy["tech"] = tidy["entity"].apply(classify_entity_name)
    return tidy


def distribute_to_hourly(starts: pd.Series, ends: pd.Series, values: pd.Series) -> pd.Series:
    """Spread each (start, end, value) measure across the hourly bins it
    overlaps, weighted by overlap duration -- total value is conserved
    exactly (each measure's `value` is split proportionally to how much
    of its own duration falls in each hour, so the pieces sum back to the
    original value). Real start/end timestamps are often sub-hourly
    (15-minute steps) and can span multiple hours, unlike SMARD's
    monthly redispatch-by-source series.
    """
    hourly: dict[pd.Timestamp, float] = {}
    for start, end, value in zip(starts, ends, values):
        duration_h = (end - start).total_seconds() / 3600
        if duration_h <= 0:
            continue
        first_hour = start.floor("h")
        last_hour = (end - pd.Timedelta(seconds=1)).floor("h")
        for hour_start in pd.date_range(first_hour, last_hour, freq="h"):
            hour_end = hour_start + pd.Timedelta(hours=1)
            overlap_h = (min(end, hour_end) - max(start, hour_start)).total_seconds() / 3600
            if overlap_h <= 0:
                continue
            hourly[hour_start] = hourly.get(hour_start, 0.0) + value * overlap_h / duration_h
    return pd.Series(hourly, dtype=float).sort_index()
