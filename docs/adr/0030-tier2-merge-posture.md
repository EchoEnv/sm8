# ADR-0030: Tier 2 merge posture — MOR/COW decision, idempotency contract, and refresh-watermark metadata

**Status:** Proposed.
**Date:** 2026-09-09.

## Context

ADR-0029 defined the rollup refresh strategy ladder and shipped Tier 1
(dynamic partition overwrite via DSv2, PR #361). Tier 2 — **row-level
delta MERGE with the delta derived from base-table Iceberg snapshot
diffs** — stays gated behind the numeric criteria in ADR-0029 §Gate B
(none of which have fired yet; this ADR does NOT open Tier 2). What this
ADR does decide, before any Tier 2 code exists, is the **posture** Tier 2
would adopt if the gates open:

1. **MOR vs COW** for rollup tables under row-level operations — and the
   hybrid variant (MOR for open windows + compaction after close) that
   an external design reference proposed as a default.
2. **The merge-key and idempotency contract** any Tier 2 implementation
   must satisfy (never incrementally `+=`; re-merge must be a no-op).
3. **Refresh watermark metadata** — a freshness/finality signal the
   routing lane currently lacks ("a row exists in the rollup" is not
   "the bucket is final").
4. **Cascading rollup sources** (hourly → daily built FROM hourly, not
   from base) — previewed here, deferred to its own ADR-0031.

Everything here is written against the established seams: core stays
format- and strategy-blind (RFC §3); Iceberg is the format standard
(ADR-0028); the strategy seam is `RollupMaterializer.persistCatalog`
(connector); refusal observability and the observation harness (ADR-0025/
0027, PRs #351/#355) are the measurement plane; `AggregateFn.decomposability`
(core, regression-guarded by `AggregateFnDecomposabilitySpec`) is the
aggregate-algebra source of truth (ADR-0029 §Context).

## Decision

### D1 — Write-distribution posture: COW remains the default; MOR-hybrid is a gated experiment

The ADR-0029 posture stands: rollup tables are **read-mostly BI surfaces;
scan speed is the product**. Defaults when Tier 2 opens:

```text
write.delete.mode = copy-on-write
write.update.mode = copy-on-write
write.merge.mode  = copy-on-write
```

**The MOR-for-rollups default proposed by the external reference is
rejected as a default and adopted as an experiment.** Rationale:

- MOR's advantage is cheap small-delta writes. That advantage exists only
  under row-level MERGE (Tier 2+). At Tier 0/1 there are no row-level
  ops — `overwritePartitions` is a metadata swap — so the posture choice
  is moot today.
- Under Tier 2, MOR buys cheap writes but taxes every read (merge data +
  delete files at scan time) and adds a compaction obligation. Our read
  path is the routing lane's product surface (`RollupRewriter` routes
  production queries at these tables); trading scan speed for write
  cheapness inverts the cost model.
- **However**, the hybrid (MOR for still-open windows, compaction
  immediately after window close) is a legitimate candidate for the
  highest-churn lane (the current time bucket, re-merged every refresh).
  Whether it wins is an empirical question.

**The experiment (runs only if Gate B opens Tier 2):** one representative
model runs dual rollup tables — identical grain/measures, one COW, one
MOR-hybrid with post-close `rewrite_data_files` — behind a routing
allowlist. The observation harness (PRs #351/#355) measures: refresh
wall-clock, scan latency at the routed surface, delete-file accumulation
between compactions, compaction cost, and refusal/telemetry deltas.
Decision rule: MOR-hybrid adopts per-table only where measured scan cost
delta < 10% AND refresh cost saving > 30%. Otherwise COW stays
universal. The experiment's dual-table period is bounded (2 weeks) and
its tables are excluded from production routing until the rule picks a
winner.

Table properties are set per-table by the **connector** at create time
(the same CTAS/create surface Tier 1 uses); the platform never sets
them out-of-band. Compaction (`rewrite_data_files`) is a **platform-side
scheduled maintenance** op — it is not part of the refresh path and must
not run concurrently with a refresh on the same table (serialized by the
orchestrator; same discipline as ADR-0029's refresh concurrency note).

### D2 — Merge-key and idempotency contract (Tier 2 invariants)

Any Tier 2 MERGE implementation MUST satisfy all of the following. These
are contract tests, not conventions:

1. **Merge key = grain bucket + all dimension columns** (composite). The
   key is derived from the rollup declaration (grain dimension +
   `spec.dimensions`), never hand-specified per refresh. Including the
   partition column in the ON clause is required (it is the grain bucket
   by construction) so Iceberg can prune.
2. **Source-key uniqueness**: the delta source must be unique on the
   merge key. A duplicate-key delta refuses typed
   (`EngineError.UnsupportedCapability`, capability
   `RollupMaterializer.merge.duplicateKeys`) — matching the
   `ScopeUncovered` refusal pattern from Tier 1. Non-unique keys are a
   model/base divergence, which must fail loud, not last-write-wins.
3. **Idempotency (the spark-batch retried-job rule, made testable):**
   re-running the same merge against an unchanged source must leave the
   table byte-identical (snapshot content hash equal). Contract test:
   merge → snapshot-hash → merge again → hash unchanged. Any `+=`-style
   accumulation into rollup measures is forbidden; matched rows are
   **overwritten with the recomputed value** (`UPDATE SET m = s.m`), not
   incremented.
4. **Exact-delta primary, full-bucket-recompute fallback.** The primary
   Tier 2 strategy merges the snapshot-diff delta with pre/post-images
   (per ADR-0029: Additive measures combine contributions; Algebraic
   need pre+post to maintain sum/count/sumSq; Positional/Holistic/
   Approximable never participate and fall back to Tier 1 partition
   recompute for their columns). When delta extraction is unreliable for
   a bucket (overlapping windows, ambiguous lineage), the documented
   fallback is **full-bucket re-aggregation from base + overwrite of
   exactly those buckets** — the same invariant, the safer mechanism.
   Strategy choice is per-bucket, decided by data properties
   (delta fidelity), not hardcoded per table (scala-data-driven-refactor:
   classify data, dispatch behavior).
5. **Layer-1 raw-event dedup is explicitly out of scope.** The rollup
   lane does not silently deduplicate base-table rows (`ROW_NUMBER()`
   tricks). Base-table hygiene belongs to the producing pipeline; a
   rollup layer that dedupes silently masks upstream data-quality
   failures — exactly the "silently wrong" class the observation
   harness exists to surface. If dedup-at-read ever becomes a real
   requirement it must be an explicitly declared model feature with its
   own telemetry, decided in its own ADR.

### D3 — Refresh watermark metadata (freshness/finality signal)

**Problem.** Routing (core) currently decides rollup-vs-base from
existence + schema (`RollupSchemaStale` refusal, PR #345) — but "a row
exists in the rollup" is not "that bucket is final". Under Tier 2
incremental maintenance, a bucket can be present and stale (late rows
still expected). Routing needs a freshness signal; core must stay
IO-free, so the signal must be produced connector-side and consumed
without core learning about Iceberg.

**Decision:**

- The connector maintains a **rollup watermark table** — itself an
  Iceberg table, one row per (rollup_table, partition bucket):
  `is_final` (lateness threshold passed; no further re-processing
  expected), `last_refreshed_at`, `last_commit_snapshot_id`.
- Written by the refresh path (connector) after each successful commit;
  read by the connector's resolution layer (the same seam that resolves
  rollup table names for the router).
- **Core consumes it only through the existing refusal vocabulary.** The
  router's staleness gate gains a sibling refusal —
  `RollupBucketStale(bucket)` — emitted when the resolution layer
  reports the queried bucket non-final AND the model declares a freshness
  policy requiring finality. Core never queries the watermark table; the
  connector passes a boolean/bucket-set verdict across the existing
  resolution seam. If the model has no freshness policy, routing behaves
  exactly as today (existence + schema only).
- The watermark table is **observability surface, not correctness
  authority**: the rollup row data remains the source of truth; the
  watermark only scopes re-processing (D2-4's "non-final buckets only")
  and powers the harness's freshness report (PR #350's
  `sm8 rollup-report` gains a per-bucket finality column).
- Watermark writes ride the refresh's atomicity: a watermark row is
  committed only after the data commit succeeded (never before), so a
  failed refresh can never advance the watermark (Tier 1/2 atomicity
  inherited, not re-implemented).

### D4 — Cascading rollup sources: previewed, deferred to ADR-0031

Building daily FROM hourly (instead of from base) is attractive — it
turns Tier 2's "MERGE over full recompute is a pessimization" objection
into a true small-delta story for cascaded grains, and it composes with
D2's idempotency invariant. But it adds a new correctness surface this
ADR does not want to open: cascade-validity is per-measure-class
(`AggregateFn.decomposability` — Additive mergeable, Algebraic iff
partial state stored, Positional/Holistic/Approximable never), and a
stale/failed hourly now propagates into daily (new blast-radius class;
scala-impact-analysis: ripple extends across the rollup-source chain).
ADR-0031 will define: the core-side validity predicate, the
connector-side cascade-source resolution, the platform-side sequencing
(hourly completes before daily), and the staleness propagation rules.
Until then, all rollups build from base (status quo).

## Alternatives considered

1. **MOR everywhere from day one of Tier 2** (the external reference's
   default). Rejected as default, retained as the D1 experiment: our read
   path is the product; the write-cheapness win is workload-dependent
   and measurable, not assumable.
2. **COW everywhere, never experiment.** Rejected: the open-window
   re-merge pattern is exactly where MOR-hybrid plausibly wins, and the
   dual-table experiment is cheap and bounded. Deciding without data
   would be guessing in both directions.
3. **Watermark as core concept (core owns the table schema).** Rejected:
   violates RFC §3 (IO in core). The connector owns the table; core owns
   only the refusal verdict shape.
4. **Watermark as routing authority (router refuses any non-final
   bucket unconditionally).** Rejected: freshness-policy-dependent
   routing is a model-level decision; unconditional refusal would break
   models that accept approximate freshness. The policy-gated
   `RollupBucketStale` refusal keeps today's behavior the default.
5. **Incremental `+=` merge for hot buckets** (the external reference's
   explicitly-wrong pattern, which it correctly flags). Rejected and
   forbidden by D2-3: not idempotent, double-counts under overlapping
   windows, unverifiable by re-run.

## Consequences

- Tier 2 remains gated by ADR-0029 §Gate B; this ADR opens nothing. Its
  decisions activate only when Tier 2 work is funded.
- The D1 experiment consumes observation-harness budget for ~2 weeks
  when it runs; its dual tables are router-excluded, so no production
  query sees them.
- D2's contract tests (idempotency, key-uniqueness refusal, partition
  pruning verified in the physical plan) extend `RollupMaterializerTier1Spec`'s
  pattern into a future `RollupMaterializerTier2Spec` when Tier 2 lands.
- D3 gives `sm8 rollup-report` a freshness/finality dimension (per-bucket
  `is_final`) and gives the harness a stale-bucket detector — extending
  the measurement plane without touching the routing contract's default
  behavior.
- Compaction becomes a platform-side scheduled maintenance obligation
  under Tier 2 (post-close windows under MOR-hybrid; periodic under
  COW for file-size hygiene — small-file fragmentation from repeated
  Tier 1 scoped writes is worth a runbook entry TODAY, independent of
  this ADR).
- Core changes are limited to the `RollupBucketStale` refusal variant
  (same closed vocabulary as `RollupSchemaStale`) — no IO, no format
  concepts, no strategy concepts cross the seam (RFC §3).
- The small-file/compaction runbook item applies at Tier 1 now: hourly
  scoped writes fragment touched partitions; schedule periodic
  `rewrite_data_files` + set `write.target-file-size-bytes` on rollup
  tables. Platform-side, no code change.

## Tests (when Tier 2 is funded — listed now to pin the contract early)

- **Idempotency.** Same merge twice against unchanged source → identical
  snapshot content hash (D2-3).
- **Duplicate-key refusal.** Delta source with duplicate merge keys →
  typed refusal, no commit (D2-2).
- **Partition pruning in the physical plan.** The executed MERGE plan
  shows the bucket predicate pushed to the Iceberg scan (spark-batch
  mantra 1: what you wrote isn't what runs — verify the plan, not the
  SQL).
- **Watermark monotonicity.** A failed refresh never advances the
  watermark; a successful one advances it atomically after the data
  commit (D3).
- **Policy-gated staleness refusal.** Model without freshness policy →
  non-final buckets route normally; model with policy →
  `RollupBucketStale` refusal observed and counted by the harness (D3).
- **MOR-hybrid experiment gates.** Harness traces produce the D1
  decision-rule numbers (scan delta < 10%, refresh saving > 30%) for
  each candidate table.
- **Fallback fidelity.** A bucket whose delta extraction is ambiguous
  falls back to full-bucket re-aggregation and yields byte-identical
  results to a from-base recompute of that bucket (D2-4).
