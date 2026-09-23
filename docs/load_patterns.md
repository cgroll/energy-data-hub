# Load patterns

Every asset in this hub follows exactly one of these patterns. Each asset
declares which one via a `load_pattern` tag (see `BEST_PRACTICES.md`), so
it's visible in the UI and filterable via asset selection
(`tags:load_pattern=full_refresh`), not just documented in prose.

## `full_refresh`

Re-downloads and recomputes the *entire* series every run; the output is
fully overwritten, nothing is appended to.

**Use when the source data can change retroactively** -- a run that only
appended new rows would freeze any retroactively-revised value at
whatever was first observed, silently going stale. Example in this hub:
`smard_redispatch_by_source` (SMARD restates recent months after
publication).

Also the right default whenever a series is cheap enough that
incremental logic isn't worth the complexity, independent of whether the
source revises data.

## `data_derived_watermark` (preferred incremental pattern -- see BEST_PRACTICES.md)

The watermark -- "how far have we already loaded" -- is derived by
reading the asset's *own current output* (e.g. `existing.index.max()`),
not from any separate record of when a job last ran. Each run fetches
only what's newer than that and appends it.

Also known in the literature as a **state-derived** or **self-deriving**
watermark; the closest widely-used analogue is dbt's standard incremental
pattern (`WHERE ts > (SELECT MAX(ts) FROM {{ this }})`).

**Properties:**
- Self-healing / idempotent: no separate bookkeeping to drift from
  reality. A failed run, a skipped run, or externally-copied-in data
  (see the hub README's book-repo section) all resolve correctly on the
  next run, because the watermark is re-derived from what's actually on
  disk every time -- never trusted from a log.
- Requires reading the current output back every run. Fine at this hub's
  data volumes (a few MB per series); would need reconsidering for much
  larger targets (see the `run_log_derived_watermark` note below).
- **Known failure mode:** a corrupted or wrongly-timestamped row in the
  output can poison the watermark going forward (e.g. one bad row dated
  in the future silently makes every later run think it's already
  caught up past that point, skipping real data in between). This is
  exactly what the `hourly_gap_check` asset checks guard against --
  see `BEST_PRACTICES.md`.

Use for anything that only grows going forward and isn't retroactively
revised. Example in this hub: all 8 `smard_*` series assets.

## `run_log_derived_watermark` (documented, not used in this hub)

The watermark instead comes from a separate record of *when a job last
ran successfully* (a control/audit table, a scheduler's own run history,
...), independent of what actually landed in the output.

**Why this hub doesn't use it:** it requires that separate record to
stay perfectly consistent with reality. A run that "succeeded" per the
log but wrote nothing (crash after commit, partial write), or output
data that was edited/replaced outside of Dagster (exactly what this hub
does deliberately when seeding an asset from a book repo's existing
downloads), both silently desynchronize a run-log-derived watermark from
the truth -- in a way a data-derived watermark simply can't, since it
has no separate state to desynchronize from.

**Why it exists at all in the wider ETL world:** at large data volumes,
re-reading the entire target every run to compute `MAX(ts)` gets
expensive. The standard industrial compromise (e.g. Azure Data Factory's
"incremental copy" tutorial pattern) is a *hybrid*: the watermark
*value* is still derived from the data, just cached in a small separate
table after each successful run instead of being recomputed by scanning
the full target every time. Worth revisiting if/when this hub manages
data too large to cheaply re-read (e.g. a future full-resolution ERA5
asset) -- not a concern at today's SMARD-scale data.

## `partitioned_backfill` (not a watermark strategy -- a different axis, not used yet)

Dagster's own first-class feature (`PartitionsDefinition`): each time
slice (e.g. one year) is tracked and materializable independently, with
its own status in the UI. Re-processing one slice is a **backfill**, not
a full refresh of everything.

Not the same concern as the watermark question above -- it's about
*granularity of tracking and targeted reprocessing*, orthogonal to
whether a given partition is itself computed via full-refresh or
incremental logic. Not used anywhere in this hub today (SMARD is cheap
enough that full refresh is a viable fallback and nothing here needs
per-year backfills); the natural future candidate is PECD/ERA5 once
those are generalized into the hub, where per-year reprocessing (e.g. a
methodology fix for one year of bias correction) is a real, recurring
need in `pecd-replication` already.
