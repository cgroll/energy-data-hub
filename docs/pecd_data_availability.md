# PECD data availability (research notes, 2026-09-24)

Findings from directly probing CDS's `sis-energy-pecd` API (constraint
solver + real test downloads), gathered while scoping a full-Europe,
longer-history wind pull. Captured here so the actual download (not yet
done) can be picked up later without re-deriving all of this.

## Spatial resolution: solar has country-level, wind doesn't

`spatial_resolution=nuts_0` (true country-level, one column per country)
is only offered for `solar_photovoltaic_generation_capacity_factor`.
Confirmed two ways: a direct `nuts_0` request for
`wind_power_onshore_capacity_factor` is rejected outright, and CDS's own
`apply_constraints` drops both wind capacity-factor variables from the
allowed set entirely once `nuts_0` is selected (bidirectional check, both
onshore and offshore). Wind is zone-level only:
`peon`/`p2on` (onshore), `peof`/`p2of` (offshore).

This hub has `pecd_solar_country_capacity_factors` -- real PECD `nuts_0`
product, DE column only, all 4 solar technologies. There used to also be a
hub-side workaround for wind (`pecd_wind_onshore_country_capacity_factor`,
a naive unweighted mean across DE's PEON zones replicating PECD's own
documented `nuts_0` aggregation method) -- removed 2026-09-24 once
`energy-insights`' `06_pecd_simple_vs_mastr_weighted` notebook showed the
same conclusion (onshore wind: simple zone aggregation tracks
MaStR-weighting to ~0.975 hourly correlation, without meaningfully
improving on plain unweighted zone averaging) more thoroughly and for all
three technologies, making the single-purpose onshore-only asset and its
validation notebook (`05_pecd_wind_country_level_check`) redundant.

## Wind zone schemes: `peon`≈`p2on`, but `p2of` ≪ `peof`

Tested with real 1-year (2015) downloads, all of Europe, one technology:

| Scheme | Zones (Europe+) | DE zones | Compressed size (1yr, 1 tech) |
|---|---|---|---|
| `peon` (onshore) | 153 | 7 (DE01-DE07) | 2.55 MB |
| `p2on` (onshore) | 152 | 7 (DE01-DE07) | 2.85 MB |
| `peof` (offshore) | 124 | 6 (DE011-DE015_OFF, DE02_OFF) | 1.05 MB |
| `p2of` (offshore) | **26** | **3** (DE011_OFF, DE012_OFF, DE02_OFF) | 0.66 MB |

**Onshore: no difference** -- `peon`/`p2on` are essentially the same
granularity everywhere, not just for DE. **Offshore: `p2of` is much
coarser** (~5x fewer zones europe-wide, half as many for DE) -- the
scheme to use if the goal is "as aggregated as possible" for offshore.

This hub currently uses `peon`/`peof` (the pre-existing choice, before
this research). Switching offshore to `p2of` would be a real
simplification if a future consumer wants fewer offshore zones to reason
about; no reason to switch onshore.

## Technology codes: one "real fleet" option per wind category

From the CDS web-form field definition (`technology`), not just the bare
codes:

- **Offshore** (3 options): `20` = *Existing technologies* (today's real
  fleet), `21`/`22` = hypothetical future turbine specs (SP316/HH155,
  SP370/HH155 -- specific power / hub height).
- **Onshore** (10 options): `30` = *Existing technologies*, `31`-`39` =
  hypothetical future specs (SP199/277/335 x HH100/150/200).

`30`/`20` are the only ones representing the actual historical fleet --
the rest are forward-looking scenario variants, not meant for historical
validation. This hub's existing choice (tech `30` onshore, `20` offshore)
is correct and not an arbitrary subset -- it's the only one that means
"what actually happened."

(Also worth knowing: codes `40`-`43` are *not* onshore wind, they're
Concentrated Solar Power pre/post-dispatch x storage variants -- easy to
misread from the bare constraint-solver code list without the form
labels.)

## Historical range: confirmed real back to 1980, catalogue claims to 1950

`EXPORT_START_PERIOD = 2015-01` in `edh/pecd.py` is **this hub's own
chosen window** (matching MaStR/SMARD's practical data availability), not
a PECD limitation -- the inline comment there previously claimed
otherwise and has been corrected.

CDS's constraint solver lists `year` as valid 1950-2026 for the
historical wind CF product. Verified for real (not just catalogue
metadata) with an actual test download: `peon`, technology `30`, year
1980 -- succeeded, returned a full year (PECD/CDS ignores the `month`
filter for this product and always returns whichever full year(s) were
requested), and the data itself is genuine: DE zone capacity factors mean
~0.23-0.29, full 0-0.85 range, no all-zero or all-NaN columns. Not
spot-checked earlier than 1980.

## Geographic domain: Europe + North Africa + Middle East, not global

From the country columns actually present in a `nuts_0` solar download
and the CSV's own geographic bounding box metadata
(`westBoundLongitude -31, eastBoundLongitude 45, southBoundLatitude 18,
northBoundLatitude 75`):

- All of Europe (incl. Balkans, Baltics)
- North Africa: Morocco, Algeria, Tunisia, Libya, Egypt
- Middle East: Israel, Jordan, Lebanon, Syria, Turkey
- Ukraine, Moldova

**Not covered:** the Americas, sub-Saharan Africa, Asia beyond
Turkey/the Levant.

## Next step: built 2026-09-24, not yet run

Pull: `peon` onshore (technology `30`) + `p2of` offshore (technology
`20`), full bounding box (not DE-filtered -- every country column
above), 1980-2025. Split into decade-sized CDS requests
(`edh.pecd.EUROPE_DECADES`: 1980-89, 90-99, 2000-09, 10-19, 20-25)
rather than one combined 46-year request -- the only combined-request
size actually proven to work is the existing 11-year (2015-2025) DE
pull, and a failed request means redoing the whole thing
(`retrieve_with_retries` has no partial-request recovery), so decades
bound that risk to a tenth of the work instead of all of it.

**Storage shape, decided:** one parquet per (technology, decade), e.g.
`pecd_wind_onshore_tech30_1990-1999.parquet` under
`data/pecd/capacity_factors_europe/` -- not one giant all-Europe file.
A decade's parquet existing on disk is the incremental-download
checkpoint (a re-run only fetches missing decades); combining them into
one continuous series happens on demand
(`edh.pecd.load_europe_capacity_factors`), not as a materialized asset
output, so adding a new decade later never requires rebuilding a big
file. The raw CDS zip is deleted immediately after each decade is
parsed to parquet -- deliberately not kept as a cache, since the parsing
logic (CSV header-skip, column selection) is stable and unlikely to
ever change; re-downloading from CDS is cheaper than permanently
storing ~150MB of raw zips against reprocessing that won't happen.

Implemented in `edh/paths.py` (`pecd_europe_decade_zip`,
`pecd_europe_decade_capacity_factors_file`), `edh/pecd.py`
(`EUROPE_DECADES`, `EUROPE_WIND_SPATIAL_RESOLUTION`,
`download_and_convert_europe_decade`, `load_europe_capacity_factors`),
and the Dagster assets `pecd_wind_onshore_europe_capacity_factors` /
`pecd_wind_offshore_europe_capacity_factors` in
`edh_dagster/assets/pecd.py`.

**Estimated size:** ~120 MB (onshore) + ~30 MB (offshore) ≈ 150 MB
compressed raw for 1980-2025 (proportionally more back to 1950, not
pulled here); parquet-per-decade will be smaller still.
**Estimated time:** single-year single-technology test requests already
took 5-10 min each in the CDS queue; decade-sized requests are untested
but expected to take longer per request (no hard number yet -- CDS
queue time doesn't obviously scale linearly with request size). Not yet
actually run against CDS -- materializing the two Dagster assets (or
calling `download_and_convert_europe_decade` directly) is the remaining
step.

Solar was left out of this pass: `pecd_solar_country_capacity_factors`
already gets PECD's true `nuts_0` product for every country in one
2015-2025 zip (just DE-filtered at load time today), so extending it to
1980-2025 full-Europe is a smaller, separate follow-up -- lower
priority since it wasn't part of the wind-focused scoping conversation
this file otherwise documents.
