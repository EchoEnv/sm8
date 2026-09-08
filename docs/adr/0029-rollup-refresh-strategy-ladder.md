# ADR-0029: Rollup refresh strategy ladder

**Status:** Proposed.
**Date:** 2026-09-08.

## Context

ADR-0028 made Iceberg the rollup table format standard (Slice 1 shipped
in PR #358). The refresh path is now an atomic whole-table overwrite:

```scala
// connectors/spark-connector, RollupMaterializer.persistCatalog
df.write.format("iceberg").mode("overwrite").saveAsTable(s"iceberg_cat.$tableName")
```

Correctness is unconditional: after every refresh the table equals the
aggregate of the base, regardless of what churned in the base (inserts,
updates, deletes, backfills). The cost is also unconditional: **every
refresh rewrites the full rollup**, O(N) in rolled rows, even when the
base changed by one partition-hour.

This ADR decides **when that cost is acceptable, and what the upgrade
path looks like when it stops being acceptable** — written down now so
Slice 2 (Trino writer) does not freeze assumptions that a later refresh
strategy would invalidate.

Two adjacent decisions this ADR also settles, because reviewers and
operators raised both:

1. **Why overwrite and not MERGE INTO.** MERGE needs a delta worth
   merging. The refresh source is a full recompute (same plan as the
   overwrite path), so MERGE over it yields the identical table through
   a strictly more expensive plan (join + row matching +, under
   merge-on-read, equality-delete files needing later compaction).
   Worse: incremental maintenance without base-table CDC is silently
   wrong for the aggregate algebra — an update in the base requires
   *removing the old contribution*, which "insert new partial sums"
   never does. AVG needs sum+count decomposition; MIN/MAX become
   unmergeable when their contributing rows are deleted. No CDC, no
   safe increment — MERGE would be theater.
2. **DSv1 vs DSv2 writes.** Slice 1 uses DSv1 (`df.write...saveAsTable`)
   for cross-version safety (Spark 3.5 + Iceberg 1.5.x ↔ Spark 4 +
   Iceberg 1.7.x from one source line). Tier 1 below is the migration
   point to DSv2 (`df.writeTo(...).overwritePartitions()`), which is
   also the Iceberg-documented modern path. The Trino writer (Slice 2)
   is born on SQL `INSERT OVERWRITE`, so it is unaffected.

## Decision

Rollup refresh is a **strategy ladder**. Tiers 0–1 are pure
optimization (identical correctness contract, less I/O). Tiers 2–4 add
a correctness surface (aggregate algebra under deltas) and each requires
its own ADR before implementation.

**v2 policy: ship Tier 1, stop there.** Tier 2 opens only against
measured evidence (criteria below), not anticipation.

| Tier | Strategy | Correctness surface | Est. size | Status |
| --- | --- | --- | --- | --- |
| 0 | Whole-table overwrite (`mode("overwrite")`) | none — full recompute | shipped (#358) | **current** |
| 1 | Dynamic partition overwrite (DSv2 `overwritePartitions`) | none — only partitions present in the recomputed source are replaced | ~140 LOC + tests | **v2 target** |
| 2 | Partition-scoped MERGE INTO, delta = Iceberg snapshot diff | aggregate algebra under deltas; MOR/COW choice; snapshot-diff fidelity | ~400 LOC | gated on criteria |
| 3 | Cross-partition MERGE + delete-file compaction policy | + compaction concurrency, split-row updates | ~700 LOC | gated on criteria |
| 4 | Continuous incremental (external CDC) | + freshness SLO, late-arrival recovery, ordering | 1k+ LOC | deliberately out of scope |

### Tier 1 — dynamic partition overwrite (v2 target)

Only partitions present in the refreshed DataFrame are swapped;
untouched partitions stay byte-identical. Still a single atomic commit
per partition set; still no CDC dependency.

Validity assumption, **enforced not hoped**: the base increment touched
only partitions the refresh recomputes. This holds for the dominant
rollup lane — time-grained tables where the day's batch writes today's
partition. Enforcement: refresh callers pass the partition scope; the
materializer refuses (typed `EngineError`) a scope that doesn't cover
every partition present in the recomputed source.

Partition column derivation: a rollup with `timeGrain` +
`grainDimension` (both-or-neither, validated at declaration per
`RollupSpec`) partitions on the `date_trunc(grain, dim)` column the
materializer already emits. Grain-less rollups (`kind: cross`,
`keys: []`, or dimension-only grains) are **single-partition** and
fall back to Tier 0 whole-table overwrite — degenerate-grain guard
already typed in #352 carries over.

Spark-version note: `spark.sql.sources.partitionOverwriteMode` semantics
differ subtly between 3.5 and 4.x; the DSv2 explicit
`overwritePartitions` call bypasses the session-conf ambiguity, which is
part of why Tier 1 is the DSv2 migration point.

### Tier 2 gate (numbers, not vibes)

Tier 2 opens only when ALL of:

1. **Measured**: partition-level rewrite cost of Tier 1 exceeds
   threshold on a representative model — refresh wall-clock > 5 min AND
   rewritten-but-unchanged bytes > 30% of table, from the observation
   harness (`scripts/rollup-observe.sh` + rollup-report telemetry), over
   ≥ 2 weeks of production traces.
2. **Base is Iceberg** (snapshot-diff delta source — no external CDC
   pipeline).
3. **Measure algebra audit is funded**: every measure class on every
   live rollup has a delta-combination test (sum trivial, avg via
   sum+count, min/max fallback to partition recompute on deletion,
   count-distinct = not mergeable = explicitly out).
4. A signed-off ADR-0030 covering MOR vs COW (`write.merge.mode`),
   delete-file compaction cadence, and snapshot-diff fidelity limits.

If any gate fails, stay at Tier 1 and re-measure next quarter.

### What Tiers 2–4 are, briefly (scope fences)

- **Tier 2**: partition-scoped `MERGE INTO` with the delta derived by
  diffing base-table Iceberg snapshots (added/deleted files → changed
  partitions only). COW (`write.merge.mode=copy-on-write`) is the
  expected posture: rollup tables are read-mostly BI surfaces; scan
  speed is the product; MOR's equality-delete files buy nothing at our
  refresh cadence.
- **Tier 3**: adds cross-partition row movement and a compaction
  policy coordinated with refresh concurrency. New failure class:
  one base row's update spans two grain levels.
- **Tier 4**: external CDC feeding continuous incremental maintenance.
  Requires an ADR for freshness-vs-correctness SLOs and a layer-discipline
  review (RFC §3: CDC ingestion lives in connector/adapters, core stays
  IO-free). Deliberately **out of scope** for v2 and not planned.

## Alternatives considered

1. **Go straight to MERGE INTO now.** Rejected: same result, more
   machinery, and silently wrong algebra without the audit funded. The
   ladder orders work by risk, not novelty.
2. **Freeze on Tier 0 forever.** Rejected as policy — O(N) rewrites at
   daily-grain scale are cheap, but the ladder exists so that when they
   stop being cheap, the upgrade is a written path, not an improvised
   one. Tier 1 is small and safe enough that declining it is the choice
   that needs justification.
3. **Iceberg `replaceWhere` (DSv2 `overwritePartitions` with a filter)
   as an early Tier 1.5.** Deferred: identical commit mechanics to
   Tier 1 for our partition-shaped scopes, adds a predicate language to
   keep in sync with the router. Revisit only if a real workload needs
   non-partition scopes.

## Consequences

- Slice 2 (Trino writer) is unaffected: SQL `INSERT OVERWRITE` is the
  Trino-idiomatic Tier-0/1 shape; the ladder is per-writer strategy,
  the *contract* (atomic swap, typed failures, prior-snapshot safety)
  is shared.
- The refresh-strategy seam stays behind `RollupMaterializer.persistCatalog`
  (connector layer). `RollupSpec`, `RollupRewriter`, refusals, and
  telemetry are unchanged — core stays format- and strategy-blind
  (RFC §3).
- Tier 1 adds one caller-visible knob (partition scope on refresh) with
  a typed refusal when the scope is incomplete. Wire format and routing
  are untouched.
- Observation harness gains a Tier-1 check: unchanged-partition
  byte-identity + refresh-scope refusal test. This becomes the evidence
  source for the Tier 2 gate.
- If the Tier 2 gates never open, the ladder still paid for itself:
  Tier 1 removes most of the O(N) rewrite cost for the dominant
  time-grain lane at ~half-day cost.

## Tests (Tier 1, when implemented)

- **Partition isolation.** Refresh with scope = today; assert
  yesterday's partition files unchanged (byte/content identity via
  snapshot metadata, not row spot-checks).
- **Scope coverage refusal.** Recomputed source contains partitions
  outside the declared scope → typed `EngineError`, no commit.
- **Degenerate grain fallback.** `kind: cross` / grain-less rollup
  takes the Tier 0 whole-table path; single-partition tables are
  byte-identical in behavior to #358.
- **Atomicity preserved.** Failed refresh inside one partition set
  leaves the previous snapshot serving (contract test from #358
  re-exercised under DSv2).
- **Cross-version parity.** Same test suite green on Spark 3.5 +
  Iceberg 1.5.x and (profile `-Pspark4`) Spark 4.1 + Iceberg 1.7.x.
