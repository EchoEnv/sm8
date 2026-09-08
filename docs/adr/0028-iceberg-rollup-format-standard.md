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

Separately, the sm8 architecture (RFC §3, "engine-portable semantic
layer") positions connectors as interchangeable adapters behind one
contract. Today Spark is the only rollup *writer*; Trino and DuckDB
connectors exist for reads. A deployment with no Spark cluster at all
cannot materialize rollups — and the natural question is "why not let
Trino write them?"

Both problems have the same root: **the rollup table has no standard
transactional format.** Plain Parquet-under-a-catalog has no snapshot
semantics, no atomic swap, and no cross-engine writer contract.

## Decision

**Apache Iceberg is the standard table format for sm8 rollup tables.**

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

   | Connector | Iceberg write capability | Rollup-writer status |
   | --- | --- | --- |
   | Spark | Full (native) | **Ships with this ADR** (first slice) |
   | Trino | Full (`CREATE TABLE`, `INSERT INTO`, `DELETE`) | Next slice, same ADR umbrella |
   | DuckDB | Read-oriented; write support emerging upstream | **Deferred** until upstream write support stabilizes |

3. **Atomicity comes from Iceberg snapshots, not from application
   code.** The stage-then-swap choreography a plain-catalog
   implementation would need (stage table → validate → atomic rename →
   drop old) **collapses into a single Iceberg commit**. A failed
   aggregation simply never commits; readers on the previous snapshot
   are untouched. This dissolves the atomic-refresh-swap ticket
   (docs/runbooks/rollup-refresh.md carry-item) — the format provides
   the transaction.

## Layer placement (RFC §3)

Per RFC §3's core-boundary table:

- **core**: no change. Iceberg is a storage technology — "knows about a
  specific data source" is an adapter concern. `RollupSpec`,
  `RollupRewriter`, and `RollupRewriteRefusal` remain format-blind
  (they operate on `RelOp` and column contracts, not files).
- **connectors** (adapters): the Iceberg writer lives in the Spark
  connector (first slice) and the Trino connector (next slice), each
  behind the connector's own `RollupMaterializer`. Connector-specific
  format configuration (Iceberg catalog URI, warehouse path) belongs
  here.
- **plugins / hooks**: no change. The `rollup-refusal-observer` plugin
  and the hook surfaces are format-blind.

The one-line test: "which Iceberg catalog is configured?" is a
deployment concern → adapter layer. The routing decision and the
refusal taxonomy never touch storage → core stays frozen.

## Implementation plan (staged, one slice per PR)

### Slice 1 — Spark writer + config gate (this PR)

- New dependency: `org.apache.iceberg:iceberg-spark-runtime-3.5_2.13`
  (spark-connector only; test scope for the integration tests,
  compile scope for the writer).
- `RollupMaterializer.persistCatalog` gains a format branch selected by
  connector config: `rollup.table.format = parquet | iceberg`
  (**default `parquet`** — zero behavior change for existing
  deployments until they opt in).
- Iceberg branch: `df.write.format("iceberg").mode("overwrite")
  .saveAsTable(tableName)` — Iceberg's overwrite IS an atomic snapshot
  commit. No staging table, no swap, no drop-first.
- Tests: refresh-failure-mid-job (kill the aggregation; the previous
  snapshot keeps serving), concurrent read during refresh (readers see
  one snapshot or the other, never a blend), schema-drift loud failure
  (type narrowing fails at commit, old snapshot survives), and the
  observation harness re-run against an Iceberg rollup
  (`scripts/rollup-observe.sh` PASS).

### Slice 2 — Trino writer (follow-up PR, same ADR umbrella)

- `trino-connector` implements `RollupMaterializer` in its own dialect
  (`CREATE TABLE ... WITH (format = 'ICEBERG')` + `INSERT OVERWRITE` /
  `DELETE` + `INSERT`).
- Contract tests against a real Trino instance (or an embedded catalog
  fixture) pin the parity: a table written by the Spark writer must be
  readable and refreshable by the Trino writer and vice versa.

### Slice 3 — DuckDB (deferred, no PR)

- Blocked on upstream write support. Re-evaluate when DuckDB's Iceberg
  writer reaches production grade. Documented here so the deferral is
  a decision, not an oversight.

## Migration

Existing Parquet rollups keep serving unchanged — the format branch
defaults to `parquet`. A deployment opts into Iceberg per-connector
(config), then:

1. Set `rollup.table.format = iceberg`.
2. Run `sm8 rollup-refresh <model>` once per model — new refreshes
   write Iceberg tables (fresh `saveAsTable` creates an Iceberg table
   when the Iceberg catalog/session is configured; the old Parquet
   table remains until manually dropped, so there is no forced
   cutover and no reader downtime).
3. Drop the legacy Parquet tables at leisure.

No data migration tooling is required in v1: the refresh path is the
migration path.

## Alternatives considered

1. **Delta Lake.** Functionally equivalent for the atomicity goal
   (`MERGE INTO` + snapshot isolation). Rejected as the *standard*
   because: (a) engine coupling — Delta's read/write story outside
   Spark is thinner (Delta-Standalone is Spark-adjacent; Trino supports
   it but DuckDB's support is newer and weaker); (b) Iceberg's
   catalog-neutral design matches sm8's `SourceResolver` /
   engine-portable seam better; (c) the sm8 community-facing position
   is "engine-neutral semantic layer" — standardizing on the
   engine-neutral table format is the coherent choice. Delta remains
   readable through `SourceRef.ByPath` (`format: delta`) for source
   ingest — that is an orthogonal source-format option, not a rollup
   standard.
2. **Staging-table swap on plain Parquet** (the smaller ticket). Keeps
   zero deps but leaves a seconds-long reader window, no snapshot
   isolation on failure, and no cross-engine writer contract. Worth
   doing *if* Iceberg is rejected; superseded by this ADR otherwise.
3. **Hudi.** Same family as Iceberg; heavier write-path machinery
   (indexing, compaction) that a periodic-rollup workload does not
   need. Rejected.

## Consequences

- **Positive:** atomic refresh (no reader window, failed refresh keeps
  the old snapshot), cross-engine rollup access (Trino/DuckDB read
  Iceberg natively when their connectors materialize), time-travel for
  refresh audits, schema-drift fails loud at commit with the old
  snapshot intact, and the atomic-refresh-swap runbook item dissolves.
- **Cost:** the Iceberg runtime jar (~30-40 MB) on the
  spark-connector classpath; a catalog choice (Hive Metastore, JDBC,
  Hadoop, AWS Glue, Nessie, Polaris — Iceberg supports all; the
  deployment picks one and configures it); Iceberg session
  configuration for the writer (catalog URI + warehouse).
- **Risk:** deployments that cannot add the Iceberg runtime keep using
  the default `parquet` branch — no forced migration. The opt-in gate
  is the mitigation.

## Non-consequences

- Source ingest is unchanged. `SourceRef.ByPath` keeps reading whatever
  `format:` the operator declares (parquet, csv, delta, iceberg —
  Spark's reader already handles all of them). Iceberg-as-standard
  applies to *rollup tables written by sm8*, not to source ingestion.
- `RollupSpec`, `RollupRewriter`, `RollupRewriteRefusal` are unchanged
  (core stays format-blind; RFC §3).
- The observation harness works unchanged against an Iceberg rollup
  (the routing fold and the telemetry are format-blind).
- No plugin or hook change.

## Tests (slice 1)

- **Atomic refresh.** Materialize → corrupt the aggregation input →
  refresh fails → assert the previous snapshot still serves correct
  results.
- **Concurrent read.** Start a long-running read against the rollup;
  refresh mid-read; assert the read completes against one consistent
  snapshot (no partial blend).
- **Schema drift.** Change a measure's input type in the model;
  refresh fails loud at commit; old snapshot intact.
- **Harness parity.** `scripts/rollup-observe.sh` with the Iceberg
  format configured: same PASS verdict (rewrites=4, refusals=2,
  noGroupSetMatch on the ineligible mix).
- **Parquet default.** With no config, behavior is byte-identical to
  today (the parquet branch is untouched).
