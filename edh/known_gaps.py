"""Registry of known, investigated, permanent gaps in SMARD source data.

Why this file exists at all: a check that stays red forever for a known,
understood, unfixable issue trains you to ignore red -- which then hides
a genuinely new problem the day one actually appears. `hourly_gap_check`
(checks/smard.py) treats every timestamp listed here as expected-missing;
an asset only fails that check for a gap NOT in this registry. A red
check should always mean "something new happened", never "yeah, that
old thing again".

Investigated 2026-09-23 by querying SMARD's raw block response directly
for each date (not just inferring from our own parquet output):

- `smard_load`'s 2018-10-28 01:00 gap is a `null` value **at the exact
  UTC instant of that year's DST fall-back transition** (Germany's
  clocks went CEST->CET at 03:00 local = 01:00 UTC) -- SMARD's own
  series has a real entry for that timestamp, but its value is `null`,
  not a number. Not an artifact of anything we do; our parser correctly
  drops null-valued points (see edh/smard.py), which is what turns a
  source-side null into an apparent gap downstream.
- The three `capacity_wind_*` gaps also land on DST fall-back Sundays,
  but manifest differently: the timestamp is entirely **absent** from
  SMARD's raw block (not present-with-null) -- not confirmed to be the
  same underlying mechanism as the load case, just the same date
  pattern.
- `smard_load`'s other two gaps (2018-10-08, 2018-10-30) are *not* on
  DST dates, but show the identical null-at-source pattern -- so DST is
  a real contributor but not the sole cause; SMARD's data appears to
  have sporadic nulls generally. Root cause not pinned down further.

Add an entry here only after directly confirming the value is null/absent
in SMARD's own raw response (fetch the specific weekly block directly and
inspect it -- see the investigation above for the method), not merely
because a gap-check run reported it once. A gap you haven't personally
verified against the source doesn't belong on an allowlist -- that's
exactly how a real, actionable problem quietly becomes "known and
ignored".
"""

KNOWN_GAPS: dict[str, dict[str, str]] = {
    "smard_load": {
        "2018-10-08 16:00": "null at source; not a DST date, cause not further investigated",
        "2018-10-28 01:00": "null at source; confirmed exact UTC instant of that year's DST fall-back transition",
        "2018-10-30 11:00": "null at source; not a DST date, cause not further investigated",
    },
    "smard_capacity_wind_onshore": {
        "2017-10-29 00:00": "absent from source block, on that year's DST fall-back Sunday",
    },
    "smard_capacity_wind_offshore": {
        "2015-10-25 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2016-10-30 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2017-10-29 00:00": "absent from source block, on that year's DST fall-back Sunday",
    },
}
