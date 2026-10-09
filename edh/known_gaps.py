"""Registry of known, investigated, permanent gaps in SMARD source data.

Why this file exists at all: a check that stays red forever for a known,
understood, unfixable issue trains you to ignore red -- which then hides
a genuinely new problem the day one actually appears. `hourly_gap_check`
(checks/smard.py) treats every timestamp listed here as expected-missing;
an asset only fails that check for a gap NOT in this registry. A red
check should always mean "something new happened", never "yeah, that
old thing again".

Investigated 2026-09-23 (two rounds) by querying SMARD's raw block response
directly for each date (not just inferring from our own parquet output):

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
- `smard_consumption_residual_load`'s 3 gaps land at the exact same
  timestamps as `smard_load`'s -- expected, since residual load is
  derived from load minus renewable generation and inherits load's own
  nulls. Not independently re-verified against SMARD's raw response
  (would just re-find the same three nulls already confirmed above).
- The same DST-fall-back-Sunday absent-from-source-block pattern already
  confirmed for `smard_capacity_wind_onshore`/`_offshore` also affects
  `smard_capacity_brown_coal`, `_hard_coal`, `_natural_gas`, and
  `_pumped_storage` -- each verified individually via a fresh
  `download_series()` call (not inferred from the wind pattern).
  `_natural_gas` is missing only the 2016/2017 instances, not 2015 (its
  own series starts later, so 2015-10-25 never fell inside its expected
  range to begin with).
- `smard_forecast_onshore`/`_solar`/`_wind_solar` each miss exactly
  2023-10-28 22:00, 2024-10-26 22:00, 2025-10-25 22:00 -- the evening
  before each year's DST fall-back Sunday, not the same clock time as the
  capacity-series pattern but the same underlying DST mechanism, verified
  absent on a fresh fetch for `_onshore` (the other two share the same
  gap timestamps, not independently re-verified).

**Ruled out, NOT added here (2026-09-23):** `smard_capacity_hydro`'s and
`smard_capacity_brown_coal`'s Dec-2020/Dec-2022 multi-day "gaps" were
checked the same way and came back **present** on a fresh fetch --
meaning the local parquet was simply stale (likely the exact
block-selection bug `edh/smard.py::download_series` docstring records as
fixed 2026-09-23), not a real source gap. Backfilled in place instead of
allowlisted; see git history for that commit rather than looking for an
entry here.

**Still open, deliberately not resolved yet (2026-09-23) -- flagged for a
follow-up decision, not silently allowlisted:**
- `smard_price_pl`: a genuine (confirmed absent on a fresh fetch), ~2.7-year
  gap from 2017-03-01 through 2019-11-19, plus messy scattered gaps in
  Jan-Feb 2017 -- ~29.5k hours total, far too many to enumerate as
  individual entries the way this file does for everything else. Possibly
  tied to `PRICE_PL2` existing as a parallel/successor series (unconfirmed).
  Needs a different documentation approach (e.g. asset-level metadata
  describing the range) rather than a `KNOWN_GAPS` entry.
- `smard_forecast_residual_load`: recurring ~1-month gaps landing in the same
  Sept/Oct-Dec window across multiple years (2018, 2023, 2024), plus dense
  scattered micro-gaps throughout Oct 2023 -- confirmed genuinely absent at
  source, but the recurring annual pattern isn't root-caused.
- `smard_generation_hydro` / `smard_generation_other_conventional`: smaller
  (103 / 69 hours) but share several *exact* timestamps between the two
  independently-fetched series (e.g. 2026-01-14, 2026-08-06, 2026-09-05..07,
  2026-09-19..21) -- confirmed genuinely absent at source, and the
  cross-series correlation suggests one shared SMARD-side event per
  window, not independent per-series noise.
- `smard_forecast_load`: several multi-day blocks landing right around
  the Sept/Oct month boundary and DST weekend in both 2023 and 2024 (e.g.
  2023-09-30..10-01, 2024-09-30..10-06), plus one Dec-30/31 2024 block --
  same recurring-annual-window character as `smard_forecast_residual_load`, not
  root-caused, not allowlisted.

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
    "smard_capacity_brown_coal": {
        "2015-10-25 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2016-10-30 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2017-10-29 00:00": "absent from source block, on that year's DST fall-back Sunday",
    },
    "smard_capacity_hard_coal": {
        "2015-10-25 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2016-10-30 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2017-10-29 00:00": "absent from source block, on that year's DST fall-back Sunday",
    },
    "smard_capacity_natural_gas": {
        "2016-10-30 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2017-10-29 00:00": "absent from source block, on that year's DST fall-back Sunday",
    },
    "smard_capacity_pumped_storage": {
        "2015-10-25 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2016-10-30 00:00": "absent from source block, on that year's DST fall-back Sunday",
        "2017-10-29 00:00": "absent from source block, on that year's DST fall-back Sunday",
    },
    "smard_forecast_onshore": {
        "2023-10-28 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
        "2024-10-26 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
        "2025-10-25 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
    },
    "smard_forecast_solar": {
        "2023-10-28 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
        "2024-10-26 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
        "2025-10-25 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
    },
    "smard_forecast_wind_solar": {
        "2023-10-28 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
        "2024-10-26 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
        "2025-10-25 22:00": "absent from source block, evening before that year's DST fall-back Sunday",
    },
    "smard_consumption_residual_load": {
        "2018-10-08 16:00": "null at source; inherited from smard_load (residual load = load minus renewable generation)",
        "2018-10-28 01:00": "null at source; inherited from smard_load, exact UTC instant of that year's DST fall-back transition",
        "2018-10-30 11:00": "null at source; inherited from smard_load",
    },
}
