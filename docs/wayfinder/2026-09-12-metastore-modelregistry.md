# Wayfinder decision ticket — Metastore-backed ModelRegistry (semantic metastore, v0)

**Ticket status**: REVISION 3 (post second grilling round; READY-TO-OPEN pending final inline additions)
**Date**: 2026-09-12
**Wayfinder map**: #378 (Tier 2+ refinements — operator-ergonomics + governance axis)
**Related ADRs**: ADR-0032 (loader-seam deferral rationale), ADR-008-L GAP 3 (ModelRegistry existence)

## Destination

A scope for a **metastore-backed `ModelRegistry` implementation** in the sm8 connector clear enough to build without further planning — storage engine choice, seam placement, what gets persisted (versions, lineage, ownership), what stays in git. Implementation only when the trigger fires; this ticket resolves the design now so the build is mechanical later.

## Context

ADR-0032 recorded the seam for any future persistence behind `ModelLoader.fromStream` and deferred the build pending a real trigger. Today's conversation surfaced a related but distinct question: the connector's `ModelRegistry` (ADR-008-L GAP 3, `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/ModelRegistry.scala`) is already the seam for runtime model-by-name resolution, with `NoopModelRegistry` and `SessionCatalogModelRegistry` as the only implementations. A **semantic metastore** is concretely a third implementation of that trait, plus a write path that records model versions / lineage / ownership as definitions are loaded.

**Authoritative source of truth remains git.** The metastore is a derived index, NOT a parallel definition store. (This is the "cache-plus-yaml-fallback" read-path, refined after bonehound's grilling; see Q4 below.)

Current state:

- Definitions live in git via YAML (review, rollback, audit-trail for free).
- Three of the four ADR-0032 triggers (lineage history, multi-team authoring, access-control binding, usage telemetry) are NOT firing today.
- The connector's `ModelRegistry` is the only seam that needs to change; **but** a `LineageRecorder` hook touching `ModelLoader.fromStream` requires a hook interface — that hook interface is a core change (post-grilling correction to the original "no core change" claim). See F1 below.

## Why this is a *semantic metastore*, not a metric store

(The two terms were conflated in the original conversation; the distinction is recorded here so the ticket scope is unambiguous.)

| axis | metastore | metric store |
|---|---|---|
| holds | definitions + metadata (versions, lineage, ownership) | definitions + served values |
| serves | the ENGINE via ModelRegistry | BI tools / humans via a query router |
| sm8 status | seam stubbed (`NoopModelRegistry`); no impl | seam stubbed (ModelLoader); no impl |
| scope if built | ~600 LOC (revised from ~300 after grilling) | ~3000 LOC, server-like |

A metric store needs a metastore first. Building the metastore is the correct increment.

## Trigger-discipline (re ADR-0032 §4 conflict — bonehound F1)

ADR-0032 §4 says verbatim: *"Adopt a store only when a real requirement appears, not for fashion."* The original ticket draft asked Q5 (the tripwire) as a follow-up to "decide what to build" when Q5 is actually the input that selects among the options. **Revised framing**:

- This ticket IS ahead-of-trigger; we accept the cost because the cost is one decision ticket, not one build ticket. The ticket resolves the design now so the build, when a trigger fires, is mechanical (no second decision cycle).
- The build is still gated on a concrete trigger; this ticket does NOT authorize the build.
- If a future reader finds the tripwire never materialized, the right move is to close this ticket, not to build anyway.

## Decision to resolve (in this order — Q5 leads per bonehound F3)

1. **Q5 (keystone)**: What is the first trigger that would justify v1 build, and how would the ticket owner know?
   - The current candidate tripwires (from ADR-0032 + this conversation): (a) lineage-history query (operator asks "which model fed this rollup number?"), (b) second deployment using sm8 in production, (c) governance / access-control requirement lands. The ticket commits to (a) as the v1 trigger; (b) and (c) are v2.
2. **Q1** (depends on Q5): Storage engine — file (SQLite/DuckDB) or server (Postgres)? File is right for v1 single-host deployments; Postgres only if Q5's trigger is concurrent-authoring or multi-host.
3. **Q2**: Transparent hook or operator-explicit write? v1 = transparent hook via a new `MetastoreWriter` connector-side abstraction (does NOT add a hook to core; the hook installs in the connector path that wraps `ModelLoader.fromStream`). Explicit-write command is a v2 convenience.
4. **Q3**: Minimal field set — `(model_name TEXT, version_hash TEXT, git_commit_sha TEXT, source_path TEXT, load_timestamp INTEGER, lineage_dependencies TEXT JSON ARRAY, requested_by TEXT)`. All seven columns answer the v1 trigger (lineage query); lineage_dependencies stores raw strings (no new ADT in core; resolves F2).
5. **Q4 (authoritative-source, post-grilling)**: The metastore is a **derived index**. Git commit SHA is the version key. **Diverge policy**: on read, if `git_commit_sha` does not match the resolved HEAD of the YAML file, the metastore row is flagged `stale=true` and the resolver falls through to `SessionCatalogModelRegistry`. Drift is **detected and reported**, never auto-resolved (bonehound F2).
6. **Q6 (added)**: Retention — out of scope for v1 (growth is unbounded; owner monitors). Schema evolution — `version_hash` is content-addressed (SHA-256 of canonicalized YAML), so any field change produces a new row; no ALTER TABLE needed for v1. (Bonehound F7.)

## Architecture (revisor's-eyeball after grilling)

The original draft claimed "no core change." **Corrected**:

- `HookManager` lives in `sm8-core` (`io.sm8.core.HookManagerImpl`). The v1 build adds a hook surface there (`io.sm8.core.manifest.ModelLoadHook` — a tiny interface, no IO coupling). That is a CORE change, but a minimal one: the interface is pure data (`(yaml: String, parsed: Model) => Unit`).
- The connector's `LineageRecorderPlugin` is a connector-side adapter that implements `ModelLoadHook` and writes to the metastore.
- Core stays IO-free: the hook is invoked by the loader, but the loader doesn't know what the hook does (composition via plugin). RFC §3 preserved.

Honest layer-discipline map:

```
core
  io.sm8.core.manifest.ModelLoadHook         [NEW interface, no IO, ~20 LOC]
  io.sm8.core.manifest.ModelLoader           [adds hook-fanout call, ~10 LOC]
  sm8-core tests: ModelLoadHookSpec           [NEW, ~60 LOC]

connector
  connectors/spark-connector/.../LineageRecorderPlugin   [NEW, implements ModelLoadHook]
  connectors/spark-connector/.../MetastoreStorage        [NEW, SQLite/DuckDB impl]
  connectors/spark-connector/.../ModelRegistry.SqlMetastoreImpl  [NEW, ~120 LOC]
  connector tests: LineageRecorderSpec + SqlMetastoreSpec  [NEW, ~200 LOC]
```

Revised LOC estimate: **~600 LOC + ~260 LOC tests**. (Bonehound F4: 2-3× the original 300 LOC. Accepted.)

## Non-functional requirements (added after F5)

- **Concurrent access**: SQLite WAL mode for v1; `BEGIN IMMEDIATE` for the write path so concurrent runner processes serialize on the metastore (acceptable: writes are O(1) per load).
- **Corruption recovery — read path vs write path separated (bonehound F9)**: on `PRAGMA integrity_check` failure: WRITE path = log + disable writes until operator intervention; READ path = per-row `stale=true` fall-through as usual (per-row staleness is orthogonal to whole-store corruption). The metastore being unreadable must not block resolution.
- **Test isolation (bonehound F10)**: unconditional hook invocation in `ModelLoader` means every fixture-loading test fires the hook. The `ModelLoadHook` registry defaults to EMPTY (no-op); tests that exercise the recorder register an in-memory hook explicitly; connector integration tests use a temp-file metastore. No global state.
- **Backup (sabertooth NEW-F4)**: v1 backup story = the SQLite file is a single artifact; document `sqlite3 metastore.db '.backup ...'` in the runbook; no automated backup in v1.
- **Schema versioning of the metastore itself**: one `schema_version` row; a `_metastore_migrations` table for future ALTER TABLE scripts (no v1 migrations needed; reserved for v2).

## Validator-first asymmetry treatment (F4)

The metastore is a NEW persistence entry point. The PR must document the same asymmetry treatment as PR #400/#402: the **server path** (`PlatformModelLoader.validateAndLoad`) and the **connector path** (`GateBTraceRunner.fromStream`) record lineage via the same `ModelLoadHook`, but only the server path is in the canonical request funnel; the connector path is a one-shot CLI that records opportunistically. Scaladoc must call this out.

## Options being considered

**A. SQLite single-file + transparent hook (recommended baseline).**
File-on-disk SQLite; schema fixed in the spec output; `LineageRecorder` plugin fires on every `ModelLoader.fromStream` through the connector path. `SqlMetastoreModelRegistry.resolveModel` checks the metastore first; if absent or `stale=true`, falls through to `SessionCatalogModelRegistry` (the existing path). Smallest correct change. ~600 LOC core+connector+tests. Pure file, no server.

**B. DuckDB file (columnar).**
Same shape as A but DuckDB; future-proof for ad-hoc lineage analytics via SQL. Adds one dependency. ~700 LOC. Worth doing only if Q5 is "operator wants to write SQL against lineage", which is currently NOT the trigger.

**C. Postgres + explicit operator command.**
Server-bearing; operator runs `sm8 lineage record` per model. Most explicit; worst dev-box ergonomics; only justified if Q5 is multi-host or concurrent-authoring. ~900 LOC.

**D. Iceberg side-table (added per bonehound F6).**
sm8 already ships Iceberg; one more table is zero new deps. BUT: requires a Spark session to read/write; the connector's CLI runs Spark anyway, so the cost is hidden. Trade-off: lineage becomes queryable as Iceberg (consistent with the rollup tables) but adds the cost of full-table scans for lineage queries. NOT recommended for v1; revisit if option A's query ergonomics fail.

## Skills every session should consult

- **grilling** for Q1–Q6 (decision discipline)
- **domain-modeling** if Q3 surfaces new ADT candidates (post-grilling: resolved as raw strings)
- **scala-spark-batch-bugs** + **scala-jvm-safety** for anything touching connector code

## Standing preferences (per the sm8 Execution Rules Checklist)

- Dual review + PR + RULE 9 stop on any implementation that follows.
- Layer discipline per RFC §3: `ModelLoadHook` interface in core (zero IO coupling); `LineageRecorder` plugin + `SqlMetastoreModelRegistry` in connector.
- Validator-first asymmetry documented for the new persistence entry point (mirror PR #400 / #402).
- Manifest changes go through `ModelLoader`, not around it (seam discipline from ADR-0032).

## Notes

- This ticket is a **decision** ticket, not a build ticket. Resolving Q1–Q6 produces the spec; the build is a separate ticket (or series) once the v1 trigger fires.
- Cross-reference: ADR-0032, ADR-008-L GAP 3, wayfinder map #378.
- Grilled twice (round 1: bonehound + sabertooth — 4+5 BLOCKINGs, all resolved; round 2: same pair — READY-TO-OPEN with inline additions, applied in r3).
- **RFC §3 footnote (sabertooth NEW-F1)**: connector-side SQL (SQLite) is justified by data-flow purity — the hook interface in core is pure data; all SQL lives in the connector. RFC §3's "knows about a specific data source → ADAPTER" rule is satisfied by keeping the connector as the only SQL-speaking module; no new storage-adapter layer is introduced for a single consumer.
- **Sunset review (sabertooth NEW-F2)**: if the trigger (Q0/T2 or equivalent) has not fired within **6 months of ticket open**, this ticket is auto-reviewed for spin-down; the design may be archived into ADR-0032 as an appendix and the ticket closed without prejudice.
- **Lineage-edge v2 promotion path (sabertooth NEW-F3)**: v1 uses raw string pairs for lineage edges; if a consumer needs typed edges, v2 promotes to a core ADT (`LineageEdge`) — a named, deferred core change.
- **Drift report destination (bonehound F11)**: v1 drift reports go to the runner's stderr log + a `lineage_drift` row in the metastore itself (queryable); no separate UI.
- **F8 resolution (HookManager integration)**: v1 uses a **new parallel `ModelLoadHookRegistry` in `io.sm8.core.manifest`** with best-effort semantics — NOT the existing fail-fast `HookManager` (whose RFC §9 contract, "hook throws abort the pipeline", is wrong for best-effort lineage writes). Registry is ~80–120 LOC (not the ~20 LOC originally estimated); total LOC estimate stands at ~600 + ~260 tests.

## Decisions so far

- **Option A (SQLite single-file) is the recommended baseline**, contingent on Q5's trigger being lineage-history (the current candidate).
- **Layer placement**: `ModelLoadHook` interface in core (pure-data); all persistence in connector. RFC §3 preserved.
- **Diverge policy**: metastore is a derived index; git commit SHA is the version key; `stale=true` falls through to `SessionCatalogModelRegistry`. Drift is detected and reported, never auto-resolved.
- **No CORE trait change to `ModelRegistry`**; the new core change is a minimal `ModelLoadHook` interface in `io.sm8.core.manifest`. (This is a CORRECTION to the original draft's "no core change" claim.)
