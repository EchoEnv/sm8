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
5. **Snapshot-diff fidelity boundaries** — when the Iceberg
   snapshot-diff extraction is reliable vs when it must defer to
   full-bucket recompute (closes ADR-0029 §Gate B's open item 4).

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
  delete files at scan time) AND carries a **read-side latency tax that
  persists between compaction runs** (R1 review, teal H1): until the
  next `rewrite_data_files` snapshot is published AND query planners
  route to it, the routed surface pays the MoR scan tax. Our read path
  is the routing lane's product surface (`RollupRewriter` routes
  production queries at these tables); trading scan speed for write
  cheapness inverts the cost model.
- **However**, the hybrid (MOR for still-open windows, compaction
  immediately after window close) is a legitimate candidate for the
  highest-churn lane (the current time bucket, re-merged every refresh).
  Whether it wins is an empirical question.

**The experiment (runs only if Gate B opens Tier 2):** one representative
model runs dual rollup tables — identical grain/measures, one COW, one
MOR-hybrid with post-close `rewrite_data_files` — behind a routing
allowlist. The observation harness (PRs #351/#355) measures per
candidate table with rigorously specified metrics (R1 review, teal H4):

```text
Scan-latency p95 at the routed surface, measured over a 7-day window,
STRATIFIED BY snapshots-since-last-compaction (the MoR read tax is
conditional on that stratification — a single mean hides it).
Refresh cost = wall-clock from job-start to data-commit-snapshot-
published, plus total compute time (driver + executor seconds).
Ambiguity rate: fraction of buckets that fell back to full-bucket
re-aggregation (D2-6) per refresh cycle.
Decision rule: MOR-hybrid adopts per-table only where scan-p95 delta
< 10% AND refresh-wall-clock saving > 30%. Otherwise COW stays
universal for that table.
```

The experiment needs a **synthetic-probe path** in the observation
harness (R1 review, wren H3): the current harness has no way to measure
scan latency for a table that production routing excludes. The probe is
a scheduled query loop against the candidate tables; it is part of the
Tier 2 implementation work, not a platform obligation.

The experiment's dual-table period is bounded (2 weeks of refresh
traffic, not wall-clock — explicitly anchored to refresh count, R1
review, teal M2) and its tables are excluded from production routing
until the rule picks a winner.

Table properties are set per-table by the **connector** at create time
(the same CTAS/create surface Tier 1 uses); the platform never sets
them out-of-band. Compaction (`rewrite_data_files`) is a **platform-side
scheduled maintenance** op — it is not part of the refresh path.

**Compaction-vs-refresh serialization** (R1 review, wren M2): the
orchestrator holds a per-rollup advisory lock. Refresh and compaction
each acquire the lock before reading the rollup's current snapshot id;
whichever begins first runs to completion before the other acquires.
The lock is advisory (a row in a small `compaction_coordination`
Iceberg table, not a JVM primitive) so platform restarts do not strand
it. The lock implementation is platform-side and out-of-scope here;
this ADR pins the rule.

### D2 — Merge-key and idempotency contract (Tier 2 invariants)

Any Tier 2 MERGE implementation MUST satisfy all of the following. These
are contract tests, not conventions:

1. **Merge key = grain bucket + all dimension columns** (composite).
   The key is derived from the rollup declaration (grain dimension +
   `spec.dimensions`), never hand-specified per refresh. The merge-key
   shape matches `RollupRewriter.rollupSourceRef(model, spec)` — the
   same identity the router uses — so the on-the-wire and on-the-table
   keys are identical. Including the partition column in the ON clause
   is required (it is the grain bucket by construction).
2. **Partition-pruning verification** (R1 review, teal C3). The claim
   that the bucket predicate is pushed into the scan side of MERGE is
   **verified by a plan check, not asserted in prose.** Contract test:
   capture the Iceberg physical plan for the MERGE; assert the plan
   contains a partition filter on the grain column on the scan side
   before the join/merge node. If a runtime version stops pushing the
   predicate (Iceberg Spark integration does this for some ON forms),
   the test fails loud. **Tier 2 must NOT ship with the pruning test
   failing** — either the merge key form is adjusted to push the
   predicate or this ADR is amended to remove the pruning claim.
3. **Source-key uniqueness**: the delta source must be unique on the
   merge key. A duplicate-key delta refuses typed
   (`EngineError.UnsupportedCapability`, capability
   `RollupMaterializer.merge.duplicateKeys`) — matching the
   `ScopeUncovered` refusal pattern from Tier 1. Non-unique keys are a
   model/base divergence, which must fail loud, not last-write-wins.
4. **Duplicate-key detection timing** (R1 review, teal H2): the refusal
   fires at the EARLIEST stage that can detect it, not after the
   aggregation job has wasted compute. Pre-shuffle distinct-count probe
   on the delta source; if the probe fails, refuse before aggregation.
   The cost of the probe is part of the contract test (probe must be
   cheaper than the aggregation it replaces).

**D2-5 — Ambiguity rate as the Tier 2 real metric (R1 review, teal C4).**
Tier 2's refresh-cost saving is bounded by `(1 − ambiguity_rate)`. The
D1 experiment reports ambiguity rate alongside its scan/refresh numbers;
a high ambiguity rate collapses Tier 2's expected value over Tier 1
regardless of the MOR-hybrid decision. Ambiguity rate is per-refresh,
per-bucket: any bucket hitting D2-4's classification rule (a/b/c/d)
counts as ambiguous for that refresh.

5. **Idempotency (spark-batch retried-job rule, formalized):** re-running
   the same merge against an unchanged source must leave the table
   **content-identical**. "Content-identical" is defined the same way
   ADR-0029 defines it for Tier 1 (the Tier 1 spec pins the contract):
   data-file references are reused AND per-file content hashes match
   (R1 reviews, both reviewers flag C1 — every Iceberg MERGE produces
   a new snapshot id; that is normal and not a violation). The
   contract test is:
   ```
   merge(source)
     post1 = manifest_entries(snapshot).map(m =>
            (m.data_file.file_path, m.data_file.content_hash)).sorted
     merge(source)            // identical source
     post2 = manifest_entries(snapshot).map(m =>
            (m.data_file.file_path, m.data_file.content_hash)).sorted
     assert post1 == post2     // content-identity, not snapshot-identity
   ```
   Any `+=`-style accumulation into rollup measures is forbidden;
   matched rows are **overwritten with the recomputed value**
   (`UPDATE SET m = s.m`), not incremented.
6. **Exact-delta primary, full-bucket-recompute fallback.** The primary
   Tier 2 strategy merges the snapshot-diff delta with pre/post-images
   (per ADR-0029: Additive measures combine contributions; Algebraic
   measures maintain the **Welford partial state** `(sum, count, m2)`
   for variance/Stddev — NOT `(sum, count, sumSq)`, which is the wrong
   shape; the Welford schema is the established one in `AggregateFn`
   algebra and must be reused, R1 review teal H3). Positional / Holistic
   / Approximable never participate; for those measure classes the
   bucket falls back to a Tier 1-style partition recompute for that
   measure's columns specifically (not for the bucket wholesale —
   additive measures in the same bucket still get the row-level delta).
   When delta extraction is unreliable for a bucket, the fallback
   (R1 review, teal C4 — the dominant fallback case in practice) is
   **full-bucket re-aggregation from base + overwrite of exactly that
   bucket's partitions via Tier 1's `overwritePartitions`**. This is
   **structurally identical to Tier 1 on those partitions** — not a
   duplicate mechanism, but the same one. The D1 experiment therefore
   measures the **ambiguity rate** as its primary signal: if the rate
   is high, MOR-hybrid wins nothing because we are mostly doing Tier 1
   work; if the rate is low, MOR-hybrid's per-bucket delta wins as
   predicted. The classification rule for "ambiguous" is pinned in D5
   (snapshot-diff fidelity boundaries).
7. **Input-layer raw-event dedup is explicitly out of scope.** The rollup
   lane does not silently deduplicate base-table rows (`ROW_NUMBER()`
   tricks). Base-table hygiene belongs to the producing pipeline; a
   rollup layer that dedupes silently masks upstream data-quality
   failures — exactly the "silently wrong" class the observation
   harness exists to surface. If dedup-at-read ever becomes a real
   requirement it must be an explicitly declared model feature with
   its own telemetry, decided in its own ADR.

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
  Iceberg table in the same catalog as the rollup. Schema (R1 review,
  teal M4): one row per `(model_name, rollup_name, bucket_value)`;
  columns `is_final` (lateness threshold passed; no further
  re-processing expected), `last_refreshed_at` (driver wall-clock),
  `last_commit_snapshot_id` (Iceberg snapshot id of the data commit
  that produced this state, for diagnostic joins).
- Written by the refresh path (connector) after each successful data
  commit; read by the connector's resolution layer (the same seam that
  resolves rollup table names for the router).
- **Cross-table atomicity is sequential-but-not-atomic, by Iceberg's
  design** (R1 review, teal C2): two commits to two separate Iceberg
  tables are NEVER cross-table atomic. The watermark commit follows the
  data commit; a crash between them leaves the watermark stale-but-valid
  (data is current; the watermark simply does not yet reflect it — it
  advances on the next refresh and never claims "more final" than the
  data). The orchestrator pins the rule: a watermark row is committed
  **only after** the data commit returned success; never before. A
  **monotonicity contract** (test) asserts `is_final` never regresses
  from true to false for a given (model, rollup, bucket). A watermark
  row whose snapshot id does not match the rollup's current snapshot id
  is treated as stale and re-derived on the next refresh.
- **Core consumes it only through the existing refusal vocabulary**
  (RFC §3). The refusal vocabulary gains a sibling —
  `RollupBucketStale(buckets: Set[BucketKey])` — emitted when the
  resolution layer reports queried buckets non-final AND the model
  declares a freshness policy requiring finality. **The rewriter
  signature does NOT change** (R1 review, wren H2): the freshness
  verdict rides in the `RelOp.Scan.resolution` slot that is today
  `None` for rollup scans. **Emission rule mirrors `RollupSchemaStale`**
  (R1 review, wren H1): the rewriter never instantiates the case —
  only the connector's resolution layer does, after it looks up the
  watermark table.
- **Freshness policy home** (R1 review, wren M3): the policy is a
  declarative optional field on `RollupSpec`
  (`freshness: Option[FreshnessPolicy]`) — no rewriter signature
  change, no connector-injected config. A model without `freshness`
  routes unchanged (today's behavior). A model with
  `freshness = FinalRequired` gets `RollupBucketStale` on any
  non-final bucket in its scope. This keeps the routing contract's
  default behavior identical.
- **Bucket-key cardinality** (R1 review, wren M1): the refusal carries
  a `Set[BucketKey]`, not a singular bucket — real queries touch N
  partitions; per-bucket emission would either flood the refusal
  vocabulary or hide the verdict. One refusal, full set; the harness
  and routing metric see the cardinality.
- The watermark table is **observability surface, not correctness
  authority**: the rollup row data remains the source of truth; the
  watermark only scopes re-processing (D2-6's "non-final buckets only")
  and powers the harness's freshness report (PR #350's
  `sm8 rollup-report` gains a per-bucket finality column).

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

### D5 — Snapshot-diff fidelity boundaries (R1 review, teal M3)

The Tier 2 delta is the diff of base-table Iceberg snapshots. The diff
is reliable for: appended data files, deleted data files, removed
position deletes (in MOR). The diff is NOT reliable for: schema
evolution (column additions/drops/type changes), partition evolution
(adding a partition transform), or wholesale partition rewrites (COW
refresh of the base). When any boundary is crossed, the affected
bucket falls back to full-bucket re-aggregation (D2-6). This is a
sibling decision (not buried in D2-6 prose) so it is discoverable
from a single read of the Decisions section.

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
- 
## Tests (when Tier 2 is funded — listed now to pin the contract early)

- **Idempotency (content-identity, R1 C1).** Same merge twice against
  unchanged source → the sorted set of
  `(data_file.file_path, data_file.content_hash)` pairs from the
  manifest entries is identical across the two post-merge snapshots
  (snapshot ids differ — that is expected and not a violation).
- **Duplicate-key refusal timing (R1 teal H2).** Delta source with
  duplicate merge keys → typed refusal BEFORE the aggregation job
  runs (asserted via SparkListener job count or
  `df.queryExecution.numJobs`); probe cost < aggregation cost.
- **Partition pruning in the physical plan (R1 teal C3).** The executed
  MERGE plan shows the bucket predicate pushed to the Iceberg scan
  side (spark-batch mantra 1: verify the plan, not the SQL).
- **Watermark monotonicity (R1 teal C2).** A failed refresh never
  advances the watermark; `is_final` never regresses true → false; a
  successful refresh advances it after the data commit (sequential
  ordering asserted, cross-table atomicity explicitly NOT claimed).
- **Policy-gated staleness refusal.** Model without freshness policy →
  non-final buckets route normally; model with
  `freshness = FinalRequired` → `RollupBucketStale(buckets)` refusal
  observed and counted by the harness (D3).
- **MOR-hybrid experiment gates (R1 teal H4).** Harness traces produce
  the D1 decision-rule numbers — scan-latency p95 stratified by
  snapshots-since-last-compaction, refresh wall-clock +
  compute-time, ambiguity rate — for each candidate table.
- **Fallback fidelity (R1 teal C4).** A bucket whose delta extraction
  is ambiguous falls back to full-bucket re-aggregation and yields
  content-identical results to a from-base recompute of that bucket.
- **File-size hygiene (R1 teal M5).** After N scoped Tier 1 refreshes
  on a partition, assert file count and total bytes stay within a
  bound (or explicitly surface the drift), so small-file growth is a
  failing test rather than an ops-only concern.
- **Synthetic probe (R1 wren H3).** The harness can measure scan
  latency on routing-excluded candidate tables (needed by the D1
  experiment).
