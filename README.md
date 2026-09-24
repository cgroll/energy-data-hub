# Energy Data Hub

Shared ingestion layer for the energy-related projects in `~/research`
(`pecd-replication`, `pecd-power-validity-DE`, `delu-headline-forecast`,
`hackathon-power-system-planning`, `mastr-power-capacities-germany`,
`t2m-averages-europe`, `vpp-learning`, ...). Orchestrated with
[Dagster](https://dagster.io/), not DVC — see [Why Dagster here, why not
DVC](#why-dagster-here-why-not-dvc) below.

**Status: pilot.** Only SMARD (generation, load, price, SMARD's own
capacity series) and redispatch (SMARD by-source + netztransparenz
per-measure) are consolidated here so far — these were duplicated,
near-identically, across 6 book repos. PECD, MaStR, ERA5 and NUTS/region
data are still downloaded independently in each repo that needs them and
are the natural next candidates once this pilot proves out.

## Why this exists

Before this repo, `pipeline/*download_smard*.py` existed as 6 separate,
almost-identical copies (`pecd-replication`, `pecd-power-validity-DE`,
`delu-headline-forecast`, `t2m-averages-europe`, `vpp-learning`,
`world-of-energy`), each with its own local DVC cache holding the same
SMARD series. One book (`hackathon-power-system-planning`) already reads
a sibling repo's processed output directly by absolute path as a
deliberate workaround — this hub formalizes that pattern instead of
special-casing it per dependency.

## Why Dagster here, why not DVC

The book repos (`pecd-replication` etc.) keep using DVC — that's
unchanged, and intentional: `dvc repro` pinned to a git commit is what
makes a book's published numbers reproducible, and that property is worth
keeping. This hub has different requirements DVC doesn't fit well:

- **Scheduled, recurring pulls** ("give me whatever's new every morning")
  — DVC has no scheduler; Dagster schedules/sensors are built for exactly
  this.
- **No need to keep every past version of most series** — this hub's
  assets overwrite their one current output file in place. Dagster's
  materialization history (timestamps, row counts, as metadata) is the
  audit trail, not old file snapshots. A book repo that *does* want a
  pinned, reproducible input still gets that — via its own DVC pipeline,
  once it reads a snapshot of a hub output into its own tracked `data/`.
- **Visible lineage across many, mostly-independent sources** — the
  Dagster Asset Graph shows exactly which raw source feeds which
  processed table, across SMARD/PECD/MaStR/ERA5/redispatch, in one place,
  instead of scattered in docstring comments.
- **Books should see their input data without auto-updating** — each book
  repo declares its own [external asset](#books-external-assets) here,
  so the Asset Graph shows e.g. "`book_pecd_power_validity_de` depends on
  `smard_load`, last refreshed 2 days after the book was last built" —
  visible staleness, with the book's own text/analysis update staying a
  deliberate, manual decision.

## Layout

```
edh/                  # plain Python package: HTTP clients, parsing, paths
  smard.py            # SMARD chart-API client (generation/load/price/capacity)
  smard_redispatch.py # SMARD's monthly redispatch-by-source CSV
  redispatch_measures.py  # netztransparenz.de per-measure redispatch export
  paths.py            # output file locations under data/
edh_dagster/           # Dagster layer: asset defs
  assets/
    smard.py           # one asset per SMARD series (group "smard")
    redispatch.py       # SMARD-by-source + netztransparenz (group "redispatch")
  definitions.py         # Definitions() entry point (see pyproject.toml [tool.dagster])
data/                   # not versioned (.gitignore) -- current-state outputs only
```

## Running it

```bash
uv sync
export DAGSTER_HOME=~/research/energy-platform/energy-data-hub/.dagster_home  # persists run/event history across restarts
uv run dagster dev
```

Opens the Dagster UI at http://localhost:3000 — Assets tab shows the full
graph. Materialize an asset by hand from there, or:

```bash
uv run dagster asset materialize --select smard_load -f edh_dagster/definitions.py
```

Without `DAGSTER_HOME` set, `dagster dev` falls back to a temp directory
that's wiped on exit -- works fine for materializing hub assets, but a
book's `make report-dagster` (which talks to `DagsterInstance.get()` from
a separate process) would then write into its *own* throwaway instance
instead of the one the UI is showing, and never show up. Always set it to
the same path for every `dagster dev` / `report-dagster` invocation.

**No schedules right now (removed 2026-09-23):** every asset here is
on-demand only, triggered by hand from the UI or `dagster asset
materialize` — deliberately, since nothing currently needs a regular
refresh. Re-adding a schedule later (e.g. a daily SMARD pull) is just a
`ScheduleDefinition` + wiring it into `Definitions()`, same shape as
before.

## How a book repo consumes hub data

No `dvc import`, no shared client package yet (pilot scope) — a book repo
reads the hub's parquet files directly:

```python
HUB_SMARD = Path.home() / "research" / "energy-platform" / "energy-data-hub" / "data" / "smard"
load = pd.read_parquet(HUB_SMARD / "load.parquet")
```

If the hub isn't available (different machine), fail loudly with a clear
`FileNotFoundError` — same convention already used for the
`mastr-power-capacities-germany` → `hackathon-power-system-planning` reuse.

### Books: external assets

A book repo that wants its input data visible in the hub's Asset Graph
(without Dagster controlling *when* the book is rebuilt) declares itself
as an external asset in its own repo — see
`pecd-power-validity-DE/dagster_book_asset.py` for the pilot example. It
declares `deps` on the hub assets it actually reads, but has no compute
function; after actually rebuilding the book, running
`make report-dagster` there sends a bare materialization event so the hub
UI reflects it — Dagster never triggers the book build itself.

To see both in one Asset Graph, point a workspace at both code locations
-- `workspace.yaml` in this repo already does, with the hub itself as one
entry and every connected book repo as a sibling entry (paths relative to
this file, i.e. `../<book-repo>/...`):

```yaml
# workspace.yaml (this repo)
load_from:
  - python_file:
      relative_path: edh_dagster/definitions.py
      working_directory: .
      executable_path: .venv/bin/python
  - python_file:
      relative_path: ../pecd-power-validity-DE/dagster_book_asset.py
      working_directory: ../pecd-power-validity-DE
      executable_path: ../pecd-power-validity-DE/.venv/bin/python
```

```bash
cd ~/research/energy-platform/energy-data-hub
export DAGSTER_HOME=$(pwd)/.dagster_home
uv run dagster dev -w workspace.yaml
```

Connecting another book repo later is just one more `python_file` entry
here, same shape as the `pecd-power-validity-DE` one.

## Open questions / not decided yet

- Whether to add a tiny shared client package (`energy-data-hub-client`)
  instead of book repos hardcoding the hub's absolute data path.
- Generalizing beyond SMARD/redispatch to PECD, MaStR, ERA5, NUTS/region
  geometries once this pilot is validated.
