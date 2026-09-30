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
  rebap.py            # netztransparenz.de reBAP (balancing energy price)
  regelleistung.py    # regelleistung.net FCR/aFRR capacity prices
  paths.py            # output file locations under data/
edh_dagster/           # Dagster layer: asset defs
  assets/
    smard.py           # one asset per SMARD series (group "smard")
    redispatch.py       # SMARD-by-source + netztransparenz (group "redispatch")
    balancing_market.py # reBAP + FCR/aFRR capacity prices (group "balancing_market")
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

## Known data-quality caveats

- **`pecd_country_capacity_factors_simple`'s solar blend uses Germany's
  technology mix for every country.** PECD gives solar as 4 separate
  technologies (industrial/commercial rooftop, residential rooftop,
  utility fixed-tilt, utility tracking) with no guidance on how to blend
  them into one series; the only real weights this hub has came from
  Germany-specific market stats (BSW-Solar/BNetzA/pv-magazine, see
  `energy-insights/pages/06_pecd_simple_vs_mastr_weighted.py`). Every one
  of the ~52 other PECD countries currently reuses those same DE weights
  (`edh/pecd.py::DEFAULT_SOLAR_COUNTRY_WEIGHTS`) purely so there's *some*
  apples-to-apples number, not because it reflects that country's real
  market. This is a real, potentially large distortion: a country whose
  solar is mostly utility-scale (e.g. Spain, or most of North Africa) or
  mostly rooftop will get a blended capacity factor biased toward
  whichever technology's weather-response happens to look most like
  Germany's mix, not its own. Checked 2026-09-24 whether better data
  exists -- SolarPower Europe's EU Market Outlook does publish
  per-country rooftop/utility segment tables, but they're member-only;
  no free source at all splits utility further into fixed-tilt vs.
  tracking per country. `edh/pecd.py::SOLAR_COUNTRY_WEIGHT_OVERRIDES` is
  the place to add a real country's weights once sourced -- until a
  country has an entry there, treat its solar capacity factor in this
  asset as illustrative, not authoritative. Wind (onshore area-weighted,
  offshore unweighted, both from PECD's own zone geometry) has no
  equivalent problem -- it's a real per-country calculation, not a
  borrowed default.
- **`rebap_price`'s publication lag isn't strictly monotonic.** Found
  2026-09-30 while migrating this asset from `~/research/vpp-learning`: a
  batch of days had come back `N.A.` ("quality-assured price not yet
  published") on an earlier download, but had real values on a later
  request -- *while newer days in between already had real values*. A
  naive "only append what's after the last stored row" watermark would
  have missed that gap forever. The asset works around this by always
  re-fetching a trailing 14-day window every run (see
  `edh_dagster/assets/balancing_market.py`), not by chasing the specific
  gap -- cheap enough to just always do, and self-heals this whole class
  of issue rather than one investigated instance of it.
- **`fcr_capacity_price` is missing 2021-10-03** (German Unity Day, a
  Sunday that year) -- confirmed directly against regelleistung.net's own
  monthly source file, not a parsing bug here. Allowlisted in
  `edh_dagster/checks/balancing_market.py::KNOWN_REGELLEISTUNG_GAPS`.
- **⚠️ `fcr_capacity_price`/`afrr_capacity_price`'s 4-hour block columns
  (`negpos_00_04`, `neg_00_04`, `pos_00_04`, ...) are German local clock
  time (CET/CEST), NOT UTC** -- easy to get wrong since the `delivery_date`
  *index* genuinely is a plain, timezone-unambiguous calendar date, and it's
  tempting to assume the block columns inherit that same UTC-safety.
  Confirmed 2026-09-30 via regelleistung.net's own FCR Cooperation
  documentation: block delivery duration is "usually 4 hours, subject to
  daylight saving time shift" -- on the two DST-transition days per year,
  one block is genuinely only 3 (spring) or 5 (autumn) real UTC hours, not
  4, because the boundaries are pinned to German wall-clock hours. **Do
  not treat `..._08_12` as "08:00-12:00 UTC"** when joining against an
  hourly-UTC series (e.g. `smard_price_de_lu`) or computing an
  energy-weighted average from the capacity price -- convert each block's
  nominal CET/CEST hours to UTC for that specific `delivery_date` first,
  the same way `edh/rebap.py` already does for its own per-row local-time
  field. See `edh/regelleistung.py`'s module docstring and the
  `block_timezone_warning` metadata on both assets in the Dagster UI.

## Open questions / not decided yet

- Whether to add a tiny shared client package (`energy-data-hub-client`)
  instead of book repos hardcoding the hub's absolute data path.
- Generalizing beyond SMARD/redispatch to PECD, MaStR, ERA5, NUTS/region
  geometries once this pilot is validated.
