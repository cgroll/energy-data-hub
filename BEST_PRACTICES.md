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
