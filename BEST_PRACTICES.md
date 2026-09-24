# Best practices

Standards every asset in this hub is expected to meet. See
`docs/load_patterns.md` for the vocabulary referenced below.

## Load pattern preference

1. **Default to `data_derived_watermark`** for anything incremental. It's
   self-healing and needs no separate bookkeeping to stay correct -- see
   `docs/load_patterns.md` for why. Don't reach for
   `run_log_derived_watermark` here; nothing in this hub is at a data
   volume where re-reading the current output every run is a real cost.
2. **Prefer `full_refresh` over incremental whenever the source data can
   change retroactively** (revisions, restatements) -- an incremental
   append would silently freeze a revised value. Known example:
   redispatch data (SMARD restates recent months). If you're not sure
   whether a new source revises historical values, default to
   `full_refresh` until you've confirmed otherwise; getting this wrong
   silently produces stale data, which is worse than a slower job.
3. Every asset **must** declare which pattern it uses via a tag:
   `tags={"load_pattern": "full_refresh" | "data_derived_watermark" | "run_log_derived_watermark"}`.
   This is enforced by convention, not code, today -- check it on review
   rather than assuming it's there.

## Timestamps: UTC everywhere, no exceptions

Every timestamp with time-of-day precision in this hub **must** be UTC --
naive (no tzinfo) is fine, but the values themselves must represent true
UTC instants, never a civil/wall-clock local time (Europe/Berlin or
otherwise). This isn't just a documentation convention (see
`timestamp_timezone` below) -- it's a requirement on the actual values.

Why this matters more than it might seem: a civil local time has real,
periodic ambiguity/gaps around DST transitions (a missing hour every
spring, an ambiguous repeated hour every fall) that UTC never has by
construction. Two sources in this hub sit on opposite sides of this
correctly today for the same underlying reason -- neither ever passes
through wall-clock local time:

- `edh/smard.py::download_series` parses SMARD's raw Unix epoch
  milliseconds directly (`pd.to_datetime(ms, unit="ms", utc=True)`) --
  unambiguous UTC by definition, no DST logic needed at all.
- `edh/pecd.py::load_region_timeseries_zip` reads PECD's CSV timestamps
  as-is -- ERA5 reanalysis output is natively UTC-gridded, again never
  local civil time. (Solar's separate -1h correction there is an
  unrelated, constant labeling-convention quirk, not a DST issue -- see
  that module's docstring.)

**Known violation, fixed 2026-09-24:** `edh/redispatch_measures.py`
previously parsed netztransparenz.de's `start`/`end` fields as naive
Europe/Berlin wall-clock with no conversion -- a real violation of this
rule, caught by questioning why PECD's solar shift wasn't also applied to
wind (it shouldn't be; see that investigation) and then asking the
broader question this section answers. Fixed by using the same raw CSV's
own `ZEITZONE_VON`/`ZEITZONE_BIS` (CET/CEST) columns to convert to exact
UTC -- see that module's docstring for the fix and
`docs/pecd_data_availability.md`-style reasoning trail.

**Bare calendar dates are a separate, allowed category.** A date with no
time-of-day component (MaStR commissioning/shutdown dates, TTF gas's
daily OHLCV index, `smard_redispatch_by_source`'s monthly `month` column)
doesn't have a timezone to get wrong in the same sense -- state
`timestamp_timezone: "n/a"` / `"bare calendar date, no time-of-day"` for
these rather than forcing a UTC framing that doesn't apply.

When adding a new source: check whether its raw timestamps are already
UTC/epoch-based (nothing to do), civil-local with an explicit offset/zone
flag per row (convert exactly, the way the redispatch fix does -- don't
guess a fixed offset), or civil-local with no such flag (flag the
DST-transition ambiguity honestly in the docstring and
`timestamp_timezone`, the way the original redispatch code did before the
fix, rather than silently treating it as UTC).

## Required metadata

Every asset's static `metadata=` (not just its docstring) must state:

- `source` -- who publishes this data (e.g. "SMARD (Bundesnetzagentur)")
- `source_url` -- the actual endpoint/page, as `MetadataValue.url(...)`
- `region` -- the geographic scope (e.g. "DE-LU", "DE")
- `resolution` -- the native time resolution (e.g. "hourly", "monthly")
- `unit` -- physical unit of the value column(s), e.g. `"MW"`,
  `"EUR/MWh"`, `"GWh"`. State the *verification method* in the asset's
  description if the unit wasn't read directly off an authoritative
  source label (e.g. "magnitude cross-checked against known DE grid
  load, not read off an explicit SMARD unit label" -- see `smard.py`'s
  module docstring for a worked example). Never leave this field out
  because you're not 100% sure; an honestly-hedged unit beats a missing
  one.
- `timestamp_timezone` -- the exact semantics of the timestamp index,
  e.g. `"naive, represents UTC"` vs. `"naive, represents Europe/Berlin
  wall-clock"`. Don't assume this is obvious -- it's a well-known source
  of silent off-by-one-or-two-hour bugs, and different sources in this
  hub could plausibly use different conventions.
- `update_pattern` -- one line, human-readable version of the
  `load_pattern` tag (the tag is for filtering/tooling, this is for a
  human skimming the asset's overview page).

Runtime (`context.add_output_metadata`, i.e. per materialization) should
report at minimum: row count (as `dagster/row_count` -- the well-known
key Dagster renders specially), the timestamp/date range actually
covered, and a small preview (`MetadataValue.md`) of the last few rows.

## Quality checks

Every `data_derived_watermark` asset **must** have a paired
`hourly_gap_check` asset check (`edh_dagster/checks/smard.py`) that
verifies its output's timestamp index has no missing hours between its
min and max. This is specifically about catching the
`data_derived_watermark` failure mode documented in
`docs/load_patterns.md`: a bad run once producing a wrongly-timestamped
row silently causes every future run to skip real data, and nothing
about the normal incremental-append logic would ever surface that on its
own -- a completed run always looks successful. The check doesn't
prevent the bad row; it makes an existing gap visible instead of silent.

`full_refresh` assets don't need this check the same way -- a bad run
there doesn't compound silently across future runs the way a poisoned
watermark does, since the next run re-fetches everything regardless.

New asset checks beyond gap detection (schema/range sanity checks, cross-
source consistency, ...) are expected to accumulate here as real
failure modes are found -- this isn't meant to be the final list.
