# Wayfinder map — Manifest parser hardening (2026-09-07)

**Status:** Ticket #1 (survey) DONE — verdict: safe, single-PR path. Ticket #2 (strict parsers) in progress.
**Surveyed by:** session-level analysis during the pre-aggregation map (Ticket 3 built `parseRollups` strict; Tickets 3-6 reviews repeatedly flagged the same silent paths in the OLDER parsers as pre-existing).
**Driver:** ADR-0022 map carry-item — "shared parser-hardening for joins/filters/calculated_measures YAML blocks (same silent asMap/asSeq paths the rollups parser fixed)".

---

## Destination

Every YAML block in `ModelLoader` parses under the SAME discipline `parseRollups` established: **never silent**. A malformed entry fails loud as a typed `ManifestError.ParseFailure`; a scalar-where-list-is-expected fails loud; nothing is dropped or coerced to empty without a typed error naming the block and the entry.

## Why

Two silent paths in the OLDER parsers (`parseJoins`, `parseCalculatedMeasures`, `parseFilters`) produce wrong-model loads instead of errors:

1. `flatMap(asMap)` silently DROPS non-map entries in a list (`joins: ["oops"]` → the join vanishes; queries later fail in confusing ways).
2. `asSeq` silently coerces non-list values to empty (`filters: myfilter` indentation slip → zero filters loaded).

Both are data-quality holes: the model loads, downstream queries misbehave, and nothing points at the YAML.

## Tickets

### Ticket #1 — compat survey (this ticket)

**Question:** does any existing test fixture or example model DEPEND on the silent paths (i.e. would strict parsing break a currently-loading model)?

**Scope:** read-only sweep of `sm8-core/src/test`, `sm8-platform/src/test`, `examples/`, `docs/` model YAML for: non-map list entries, scalar-where-list values, missing-name entries in joins/calculated_measures.

**Acceptance criteria:** a written verdict — either "nothing relies on silent paths, strict parsers are safe" or a list of fixtures that must be fixed first. No code changes.

**VERDICT (2026-09-07): nothing relies on silent paths — strict parsers are safe as a single PR.** Inventory: `examples/hospital-cleaning/models/encounters.yml` (the only repo YAML with `calculated_measures:`, fully valid map entries); 15 `ModelLoaderM1Spec` tests (all valid map entries with names); 3 `filters:` blocks across `ManifestValidatorSpec` / `ModelLoaderSpec` / `EndToEndPipelineSpec` (all valid). Zero joins fixtures exist; zero scalar-where-list usages; zero non-map entries. `patients.yml` is documentation for a future loader subset and is not ModelLoader-loaded today — its silently-ignored `is_time_dimension`/`smallest_time_grain` fields are a SEPARATE future-spec issue, not parser scope. Ticket #2 unblocked.

### Ticket #2 — strict parsers (pending #1's verdict)

**Question:** can the three older parsers match `parseRollups`'s discipline without breaking any currently-loading model?

**Scope:** `sm8-core/manifest/ModelLoader.scala` — `parseJoins`, `parseCalculatedMeasures`, `parseFilters` (+ shared `asMap`/`asSeq` handling), mirroring the entry-wise fold + typed-refusal pattern; tests per parser per silent path.

**Acceptance criteria:** every previously-silent malformed input now fails loud as typed `ManifestError`; all existing valid fixtures still load; dual review + PR + STOP.
