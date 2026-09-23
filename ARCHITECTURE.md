# Target architecture

Where this hub fits into the bigger picture across `~/research`'s energy
projects, and where things are still just a plan vs. already built. This
is the accumulated result of a long design conversation (2026-09-23) --
written down so it doesn't only live in that transcript.

**Status legend:** ✅ built and running · 🔜 designed, not implemented yet

## The four layers

```
┌─────────────────────────────────────────────────────────────┐
│  energy-data-hub (this repo)                            ✅   │
│  Raw + shared-reusable data, Dagster-native.                 │
│  SMARD done (47 series). MaStR/PECD/ERA5 planned.            │
└───────────────────────────┬───────────────────────────────────┘
                             │ live Dagster `deps`
              ┌──────────────┴───────────────┐
              ▼                               ▼
┌──────────────────────────┐   ┌───────────────────────────────┐
│  models               🔜  │   │  the book                 🔜  │
│  own retrain cadence,     │──▶│  one combined MyST site,      │
│  lives in hub or book     │   │  pages = executable Dagster   │
│  depending on reuse       │   │  assets, deps on hub (+models)│
└──────────────────────────┘   └───────────────────────────────┘
                             ▲
                 promotion   │  (mature notebook → real page;
                             │   new dataset → real hub asset)
              ┌──────────────┴───────────────┐
              │  exploration repos         🔜  │
              │  temporary, per theme,        │
              │  DVC snapshot of the hub       │
              └────────────────────────────────┘
```

## 1. The hub -- raw + shared data ✅

This repo. Dagster-orchestrated, `data/` untracked (no DVC, no version
history -- see README.md's "Why Dagster here, why not DVC"). Owns
whatever is genuinely **shared across multiple consumers or otherwise
foundational** -- not just "code that happened to be duplicated" (that
was true for SMARD; it's a coincidence, not the actual criterion).

Migration criterion for an existing standalone repo's data-acquisition
code (worked out concretely for MaStR, not yet executed): the cut point
is the same one that already separated SMARD acquisition from book-level
analysis -- **raw/general-purpose processed data moves natively into the
hub** (its own dependencies added to the hub's `pyproject.toml`, or an
optional extra -- see below); **project-specific EDA/analysis stays put**
until it graduates into a page in the shared book (see layer 3).

**Open:** dependency growth as more domains join (MaStR needs
`open-mastr`/`geopandas`/`shapely`; PECD/ERA5 will need
`cdsapi`/`xarray`/`netcdf4`). Planned mitigation: `uv`'s optional
dependency groups (`energy-data-hub[mastr]`, `[pecd]`, ...) so a plain
`uv sync` doesn't pull in every domain's stack -- not implemented yet,
revisit when the second domain actually gets migrated.

## 2. The book -- one combined site 🔜

**Decision (2026-09-23):** one MyST book across all domains, multiple
sections (grid/SMARD, MaStR/capacity, battery/VPP, ...), not N
independently-published per-domain books like today's `pecd-replication`
etc. Replaces the current pattern of one book repo per project.

- **Pages are real, executable Dagster assets**, not the external/
  non-executing kind used in this pilot's `pecd-power-validity-DE`
  connection (`dagster_book_asset.py` was a stepping stone, not the
  final shape). A page's compute function runs its notebook
  (jupytext-paired `.md`/`.ipynb`, executed via papermill or equivalent)
  and declares `deps=[...]` on whichever hub (and model) assets it uses.
- **Materializing a page alone** re-runs it against whatever's currently
  in the hub's `data/` -- no upstream refresh.
- **Materializing `+page`** (Dagster's upstream-selection syntax) first
  refreshes every upstream hub/model dependency, then the page --
  exactly the "update deliberately, optionally cascading" behavior asked
  for.
- **Materializing ≠ publishing.** Running a page's notebook is separate
  from building the MyST site and deploying it to GitHub Pages --
  publishing stays a deliberate, manual step, so a routine hub data
  refresh never force-republishes a page whose surrounding prose hasn't
  been reviewed against the new numbers.
- **Known risk, not a tooling problem:** re-executing a notebook updates
  computed numbers/charts automatically, but hand-written prose that
  cites a specific figure doesn't self-correct. Needs a writing
  convention (favor describing patterns/methodology over hardcoded
  numbers where possible), not a Dagster feature.
- Lives in its own repo (own venv -- keeps `jupytext`/`myst`/plotting
  dependencies out of the hub), wired into the hub's `workspace.yaml` as
  another code location, same mechanism already proven with
  `pecd-power-validity-DE`.

**Open:** what happens to the *existing* per-domain book repos
(`pecd-replication`, `mastr-power-capacities-germany`, ...) -- migrate
their content into the combined book and retire them, or let them keep
publishing independently indefinitely alongside the new combined book?
Not decided; likely resolved case-by-case as each domain's shared data
migrates into the hub.

## 3. Models -- a layer between data and pages 🔜

A model is just another asset: `hub data → model asset → page asset`.

- **Own retrain cadence**, deliberately *not* auto-cascaded by a page's
  `+page` rebuild by default -- training is usually too expensive/
  consequential to trigger implicitly, and you often want to compare
  model versions rather than silently replace them on every page view.
- **Lives in the hub** if reused by multiple pages/domains; **lives in
  the book repo itself** (as its own Dagster asset -- the book is
  Dagster-native too, not just a consumer) if it's specific to one
  page's narrative.
- **Metadata over full versioning**, matching the hub's existing
  no-unnecessary-versioning stance: capture metrics/hyperparameters/
  training window as Dagster materialization metadata per run rather
  than keeping every past model binary -- unless there's a real need to
  roll back to or compare specific past versions, which would be a
  deliberate exception, not the default.

## 4. Exploration repos -- temporary, per theme 🔜

The piece that's genuinely new relative to the rest of this design: a
place for work that isn't ready to be a hub asset or a book page yet.

**Lifecycle:**
1. Spin up a lightweight repo per emerging idea/theme (a stripped-down
   variant of `project-book-template-dvc` -- `uv` + DVC + jupytext
   notebooks, deliberately *without* the MyST book-publishing machinery,
   since it's not meant to ever be published on its own).
2. First DVC stage **pulls a snapshot from the hub**, e.g.:
   ```yaml
   stages:
     pull_hub_snapshot:
       cmd: cp ~/research/energy-data-hub/data/smard/load.parquet data/hub_snapshot/
       outs:
         - data/hub_snapshot/load.parquet
   ```
   This freezes the input: the hub can keep refreshing in the background
   without silently changing what the exploration is working against.
   **No separate provenance manifest needed** -- `dvc.lock` already
   records exactly which hub state was pulled and how; re-pulling fresh
   is a deliberate `dvc repro --force`, never implicit.
3. Build notebooks on top of the frozen snapshot -- try things, develop
   new logic, no pressure to make it "production quality" yet.
4. **On maturity, promote in two possible directions, independently:**
   - A newly-created, broadly useful **derived dataset** → becomes a
     real hub asset (full `BEST_PRACTICES.md` treatment: description,
     units, timezone, load-pattern tag, checks).
   - A stable **notebook** → graduates into a real page in the combined
     book (swaps DVC-snapshot consumption for live Dagster `deps` on the
     now-relevant hub/model assets).
5. **The exploration repo is then disposable** -- delete or archive it
   once both promotions (if any) are done. It was scaffolding for
   de-risking an idea, not a permanent home for anything.

A notebook doesn't need to know in advance whether it'll become a hub
asset, a book page, both, or neither -- that falls out of the
exploration itself, which is the point of keeping this phase cheap and
separate from the other three layers.

**Open:** no template exists yet for this. Next concrete step when
picked up.
