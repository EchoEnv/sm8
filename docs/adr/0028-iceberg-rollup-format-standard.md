# ADR-0028: Iceberg as the rollup table format standard

**Status:** Proposed.
**Date:** 2026-09-08.

## Context

The rollup lane is complete and live: `RollupMaterializer` writes
pre-aggregated tables (PR #311-era), `RollupRewriter` routes queries to
them (bug #354 fix, PR #356), the observation harness verifies the
routing end-to-end, and `sm8 rollup-report` measures the traffic.

Today `RollupMaterializer.persistCatalog` writes rollups through Spark's
plain catalog layer:

```scala
df.write.mode("overwrite").saveAsTable(tableName)
```

This is **delete-then-write, non-atomic**. Between the drop and the
commit, any query routed to the rollup reads an empty or partial table.
If the aggregation job fails mid-write, the old rollup is already
destroyed — every rollup-shaped query then fails (or silently serves a
gap) until the next successful refresh. The routing lane amplifies this
exposure: the more queries route, the bigger the blast radius of a bad
refresh.

The runbook `docs/runbooks/rollup-refresh.md:40` already describes this
gap in prose ("stage to a temp table and swap (future work)") — it is a
known operational concern, not yet a formal carry-ticket.

Separately, the sm8 architecture (RFC §3, "engine-portable semantic
layer") positions connectors as interchangeable adapters behind one
contract. Today Spark is the only rollup *writer*; `trino-connector`
and `duckdb-connector` exist as modules (scaffolding in place) but do
not yet implement rollup writing — they currently serve read paths
only. A deployment with no Spark cluster at all cannot materialize
rollups today.

Both problems have the same root: **the rollup table has no standard
transactional format.** Plain Parquet-under-a-catalog has no snapshot
semantics, no atomic swap, and no cross-engine writer contract.

## Decision

**Apache Iceberg is the rollup table format standard.**

Three commitments follow:

1. **Format standard.** Rollup tables are Iceberg tables. Any
   Iceberg-capable engine can read them — Spark, Trino, DuckDB (read),
   Presto, Flink — with snapshot isolation and time-travel for free.
   This is the reader contract, and it is the reason Iceberg (not
   Delta) is chosen: Iceberg's catalog-neutral, engine-neutral design
   matches sm8's own engine-portable promise. Delta is documented as a
   rejected alternative (§ Alternatives).

2. **Writers are per-connector, behind the existing
   `RollupMaterializer` seam.** Each connector implements rollup
   writing in its own dialect; the *format* is the invariant, not the
   writer.

   | Connector | Iceberg read | Iceberg write | Rollup-writer status |
   | --- | --- | --- | --- |
   | spark-connector | ✅ native | ✅ full | **Ships in Slice 1** |
   | trino-connector | ✅ native (planned) | ✅ full | **Planned Slice 2** |
   | duckdb-connector | ⚠️ via extension (planned) | ❌ upstream write support emerging | **Deferred** pending upstream maturity |

   trino-connector / duckdb-connector modules exist in the repo today
   but do not yet implement rollup writing. The "planned" label above
   reflects the staged implementation order; readers and writers both
   are connector-level concerns.

3. **Atomicity comes from Iceberg snapshots, not from application
   code.** The stage-then-swap choreography a plain-catalog
   implementation would need (stage table → validate → atomic rename →
   drop old) **collapses into a single Iceberg commit**. A failed
   aggregation simply never commits; readers on the previous snapshot
   are untouched. This dissolves the atomic-refresh-swap runbook
   concern — the format provides the transaction.

### Pinned versions and minimum session configuration

| Item | Value |
| --- | --- |
| Iceberg runtime (Spark 3.5 path) | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.x` |
| Iceberg runtime (Spark 4 path) | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.7.x+` (when `-Pspark4` profile is active; Iceberg 1.5.x does NOT support Spark 4) |
| Profile policy | The Spark connector supports `-Pspark4` (Spark 4.1.1) via `pom.xml`. The Iceberg runtime dep is profile-gated: Slice 1 ships with both dep variants and the materializer selects the matching one at classpath construction time based on the active profile. A deployment running `-Pspark4` MUST use Iceberg ≥ 1.7; a deployment running Spark 3.5 MUST use Iceberg 1.5.x. The wrong pairing fails loud at class construction (the Iceberg session factory does not bind to the runtime jar's Spark API surface). |
| Minimum Spark session config (Iceberg branch) | `spark.sql.catalog.<name>=org.apache.iceberg.spark.SparkCatalog` and `spark.sql.catalog.<name>.type=hadoop` (the zero-extra-infra option; HMS/Nessie/Polaris/Glue all use the same writer path with the catalog-type swapped) |
| Configuration knob | `rollup.table.format = parquet | iceberg` (connector-scoped config; default `parquet` for zero-behavior-change deployments) |

Iceberg's snapshot-commit atomicity only holds when the writer is
configured against a proper Iceberg catalog. The Slice 1 README
section MUST include the 2-line session-config recipe above — without
it, the branch will silently write a non-atomic table.

### Migration commitment (closes H1)

- v0.1.x (this release cycle): `rollup.table.format = parquet` is the
  default. No forced migration.
- v0.2.x: deployments get a startup WARN when `rollup.table.format`
  is unset on a fresh deployment (encourages an opt-in decision).
- v0.3.x: `rollup.table.format` defaults to `iceberg` for new
  deployments; existing parquet rollups keep serving until manually
  re-materialized.
- The opt-in gate stays available for the entire v0.x line; we
  commit to flipping the default at v0.3.0 (so deployments that stay
  on v0.1.x have time to migrate).

This turns the two-branch ossification concern into a dated decision
rather than an open question.

## Layer placement (RFC §3)

Per RFC §3's core-boundary table:

- **core**: no change. Iceberg is a storage technology — "knows about a
  specific data source" is an adapter concern. `RollupSpec`,
  `RollupRewriter`, and `RollupRewriteRefusal` remain format-blind
  (they operate on `RelOp` and column contracts, not files).
- **connectors** (adapters): the Iceberg writer lives in the Spark
  connector (Slice 1) and the Trino connector (Slice 2, planned),
  each behind the connector's own `RollupMaterializer`. Connector-specific
  format configuration (Iceberg catalog URI, warehouse path) belongs
  here.
- **plugins / hooks**: no change. The `rollup-refusal-observer` plugin
  and the hook surfaces are format-blind.

The one-line test: "which Iceberg catalog is configured?" is a
deployment concern → adapter layer. The routing decision and the
refusal taxonomy never touch storage → core stays frozen.

## Migration path (no-downtime claim, narrowed)

The "no forced cutover and no reader downtime" claim was too broad in
the first draft. The honest sequence for a deployment switching a
rollup from parquet to iceberg:

1. Configure Iceberg catalog session-side (`spark.sql.catalog.<name>=...
   type=hadoop`, plus warehouse location).
2. Set `rollup.table.format = iceberg`.
3. Run `sm8 rollup-refresh <model>` ONCE — `saveAsTable` under an
   Iceberg catalog creates a NEW Iceberg table at the same name. The
   old Parquet table is untouched (the Spark-side behavior under
   different catalog types is name-distinct by default; we explicitly
   verify this in Slice 1's contract test).
4. Run the slice 1 contract test (see § Tests) to confirm:
   (a) the new table is readable by `df.read.format("iceberg")`,
   (b) the routing fold's `rollupSourceRef` (sm8-core/.../RollupRewriter.scala:
   903-916) and `SparkSourceResolver.resolveByName` (which scans the
   model's source catalog) resolve to the NEW table — this depends on
   the Iceberg catalog and the model's `SourceRef.ByName` catalog
   being the same catalog, or the resolver being extended to consult
   the Iceberg catalog directly.
5. Drop the old Parquet table at leisure (manual, not in the refresh
   path).

The "no reader downtime" claim is true ONLY if step 4 passes (the
routing fold finds the new table before the old one is dropped). We
pin this in a Slice 1 contract test; if it fails, the migration path
becomes a coordinated cutover (operator pauses rollup serving during
the refresh).

## Implementation plan (staged, one slice per PR)

### Slice 1 — Spark writer + config gate + contract tests (this PR's umbrella)

- New dependency (compile + runtime, spark-connector only):
  `org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.x`.
- `RollupMaterializer.persistCatalog` gains a format branch selected
  by connector config: `rollup.table.format = parquet | iceberg`
  (**default `parquet`** — zero behavior change for existing
  deployments until they opt in).
- **Spark-3.5 vs Spark-4 runtime selection.** The Iceberg runtime
  dependency is profile-gated in the spark-connector pom, mirroring
  the existing `-Pspark4` profile: Spark 3.5 builds (default) depend
  on `iceberg-spark-runtime-3.5_2.13:1.5.x`; `-Pspark4` builds
  depend on `iceberg-spark-runtime-4.1_2.13:1.7.x+` (Iceberg 1.5.x
  does not support Spark 4). No application code selects between
  them — Maven resolves the correct runtime jar per profile, and the
  Iceberg session factory binds to whichever is on the classpath. A
  wrong pairing fails loud at session construction (documented in the
  pinned-versions table above).
- Iceberg branch: `df.write.format("iceberg").mode("overwrite")
  .saveAsTable(tableName)` — Iceberg's overwrite IS an atomic
  snapshot commit. No staging table, no swap, no drop-first.
- README section "minimum viable Iceberg config" with the 2-line
  session recipe above.
- Tests:
  - **Atomic refresh.** Materialize → corrupt the aggregation input →
    refresh fails → assert the previous snapshot still serves correct
    results.
  - **Concurrent read.** Start a long-running read against the
    rollup; refresh mid-read; assert the read completes against one
    consistent snapshot (no partial blend).
  - **Schema drift loud failure.** Change a measure's input type in
    the model; refresh fails loud at commit (Iceberg's strict
    `schema-evolution.strategy=strict` rejects type narrowing);
    old snapshot intact.
  - **Routing cutover.** After refresh, assert
    `RollupRewriter.rollupSourceRef` + `SparkSourceResolver.resolveByName`
    resolve to the NEW Iceberg table (not the old Parquet table).
  - **Migration parity.** Old Parquet table remains readable until
    manually dropped; new Iceberg table co-exists.
  - **Harness parity REPLACED.** `scripts/rollup-observe.sh` was
    using `eager=false` (temp-view branch) and did not exercise
    `persistCatalog` at all — its PASS verdict was vacuous. Replace
    with a dedicated `RollupMaterializerIcebergSpec` that exercises
    the eager path against an embedded `HadoopCatalog` (no external
    services). Pin: harness calls the SAME branch the production
    refresh path uses.
  - **Parquet default unchanged.** With no config, behavior is
    byte-identical to today.

### Slice 2 — Trino writer (planned, separate PR)

- `trino-connector` implements `RollupMaterializer` in its own dialect
  (`CREATE TABLE ... WITH (format = 'ICEBERG')` + `INSERT OVERWRITE` /
  `INSERT`). Trino version floor: 432+ (Trino 432 added Iceberg
  catalog improvements).
- Contract tests: a table written by the Spark writer is read+rewritten
  by the Trino writer against the same embedded catalog.

### Slice 3 — DuckDB (deferred, no PR)

- Blocked on upstream write support. Re-evaluate when DuckDB's Iceberg
  writer reaches production grade. Documented here so the deferral is
  a decision, not an oversight.

## Alternatives considered

1. **Delta Lake.** Functionally equivalent for the atomicity goal
   (`MERGE INTO` + snapshot isolation). Rejected as the *standard*
   because: (a) engine coupling — Delta's read/write story outside
   Spark is thinner (Delta-Standalone is Spark-adjacent; Trino supports
   it but DuckDB's support is newer and weaker); (b) Iceberg's
   catalog-neutral design matches sm8's `SourceResolver` /
   engine-portable seam better; (c) the sm8 community-facing position
   is "engine-neutral semantic layer" — standardizing on the
   engine-neutral table format is the coherent choice.
2. **Staging-table swap on plain Parquet.** Superseded by this ADR —
   Iceberg's overwrite IS an atomic snapshot commit, which makes
   staging code unnecessary.
3. **Hudi.** Same family as Iceberg; heavier write-path machinery
   (indexing, compaction) that a periodic-rollup workload does not
   need. Rejected.

## Consequences

- **Positive:** atomic refresh (no reader window, failed refresh keeps
  the old snapshot), cross-engine rollup access (Trino/DuckDB read
  Iceberg natively when their connectors materialize), time-travel for
  refresh audits, schema-drift fails loud at commit with the old
  snapshot intact, and the atomic-refresh-swap runbook concern
  dissolves.
- **Cost:** the Iceberg runtime jar (~30-40 MB) on the
  spark-connector classpath; a catalog choice (the lowest-friction
  option is `type=hadoop` against a local warehouse path; HMS/Nessie/
  Polaris/Glue add operational complexity).
- **Risk:** deployments that cannot add the Iceberg runtime keep using
  the default `parquet` branch (no forced migration during v0.x). The
  v0.3.0 default-flip commit turns the two-branch risk into a dated
  decision rather than an open question.

## Non-consequences

- Source ingest is unchanged. `SourceRef.ByPath` keeps reading whatever
  `format:` the operator declares. Iceberg-as-standard applies to
  *rollup tables written by sm8*, not to source ingestion. (Delta as a
  source format is conditional on the delta-spark jar being on the
  connector classpath, which is not the case today — out of scope for
  this ADR.)
- `RollupSpec`, `RollupRewriter`, `RollupRewriteRefusal` are unchanged
  (core stays format-blind; RFC §3).
- The observation harness needs an Iceberg-aware harness to exercise
  the persistCatalog branch — the existing `scripts/rollup-observe.sh`
  exercises the temp-view branch only (vacuous for this ADR); Slice 1
  delivers `RollupMaterializerIcebergSpec` which the harness will
  eventually call. The harness script itself is updated to point at the
  new test path.
- No plugin or hook change.

## Tests (slice 1, updated to address R1 findings)

- **Atomic refresh.** Materialize → corrupt the aggregation input →
  refresh fails → assert the previous snapshot still serves correct
  results.
- **Concurrent read.** Start a long-running read against the rollup;
  refresh mid-read; assert the read completes against one consistent
  snapshot (no partial blend).
- **Schema drift loud failure.** Change a measure's input type in the
  model; refresh fails loud at commit (Iceberg's strict schema
  strategy); old snapshot intact.
- **Routing cutover.** Verify `RollupRewriter.rollupSourceRef` and
  `SparkSourceResolver.resolveByName` resolve to the NEW Iceberg
  table after refresh, when both old Parquet and new Iceberg tables
  exist simultaneously.
- **Migration parity.** Old Parquet table remains readable until
  manually dropped.
- **Harness replacement (replaces the vacuous `scripts/rollup-observe.sh`
  Iceberg parity claim).** New `RollupMaterializerIcebergSpec` exercises
  the eager path with an embedded HadoopCatalog. The harness script is
  updated to invoke this spec.
- **Parquet default unchanged.** With no config, behavior is
  byte-identical to today.
