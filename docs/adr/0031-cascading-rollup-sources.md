# ADR-0031: Cascading rollup sources — daily-from-hourly, per-Decomposability cascade validity, staleness propagation

**Status:** Proposed.
**Date:** 2026-09-09.

## Context

ADR-0028 made Iceberg the format standard. ADR-0029 shipped Tier 1
(dynamic partition overwrite via DSv2, PR #361). ADR-0030 set the
posture for Tier 2 (row-level delta MERGE — gated, not opened). This
ADR does NOT open Tier 2 either. It opens a **separate question** the
sibling ADRs explicitly deferred:

> What happens when a rollup is **built FROM another rollup** instead
> of FROM the base table?

Today every rollup builds from base. The cascade
`base → hourly → daily → weekly` is a known optimization (and a
common one in production semantic-layer deployments — see the
external design reference that informed ADR-0030 §D2-7's additive-only
constraint). Three things changed since the original ADR-0029
reasoning, making this ADR worth writing now rather than as part of
Tier 2 implementation:

1. **Algebra is already formalized.** `AggregateFn.decomposability`
   (core, regression-guarded by `AggregateFnDecomposabilitySpec`) names
   the cascade-eligibility classes directly: Additive, Algebraic,
   Positional, Holistic, Approximable. The Algebraic partial-state
   shape is already Welford `(count, sum, m2)` (ADR-0022/0023
   cancellation tripwire), which is additive across groups — a
   prerequisite for cascading that we already pay for at the
   single-grain level.
2. **ADR-0030 §D4 previewed cascading** as Tier-2's cost-model
   enabler: building daily FROM hourly is the case where row-level
   delta MERGE actually pays off (small delta, not full base scan).
   Until ADR-0031, Tier 2's primary signal is "ambiguity rate on base
   scans" — the base-derived-only frame.
3. **Cascading introduces a new correctness surface** (staleness
   propagation, new blast-radius class) that needs its own ADR
   boundary before any code touches it. ADR-0030 explicitly defers to
   ADR-0031.

This ADR pins that boundary. It changes zero code today; the
implementation lands when (and only when) Gate B opens Tier 2.

## Decision

### D1 — Cascade-validity predicate (core; declarative, not engine-aware)

A rollup **B** is **cascade-eligible from rollup A** iff every measure
on B has `Decomposability` that participates in cascading, AND B's
grouping is a coarser-or-equal partition refinement of A's. The
predicate is a **pure function of the two rollup declarations** — no
IO, no engine concept, no format concept (RFC §3: core stays
format- and strategy-blind).

```text
cascade_eligible(B, A) =
  forall measure m in B.measures:
    AggregateFn.decomposability(m.expr.fn) in { Additive, Algebraic }
  AND
    B.grain coarsens A.grain          // see D2
  AND
    B.dimensions ⊇ A.dimensions        // every A dim names a B dim
  AND
    A is fully final                   // see D3 staleness rule
```

Per-Decomposability-class behavior (re-states ADR-0030 §D2-6 for the
cascading case):

| Class | Cascade behavior |
|---|---|
| `Additive` (Sum, Count, Min, Max) | mergeable across groups; pre+post image sufficient |
| `Algebraic` (Avg, Stddev×2, Variance×2) | mergeable across groups via Welford partial state `(n, sum, m2)`; pre+post image required; the Algebraic shape already in `RollupMaterializer.stateColumns` (lines 466-500) is the cascade-ready form |
| `Positional` (First, Last) | **never cascades** — argmin/argmax state no connector exposes; the cascaded rollup falls back to its base-derived build for these measure columns |
| `Holistic` (Median, Percentile×2) | **never cascades** — no bounded partial state suffices; the cascaded rollup falls back to its base-derived build for these measure columns |
| `Approximable` (CountDistinct, ApproxPercentile) | **never cascades** — exact semantics differ from approximate semantics; forbidden by ADR-0022 in v1 routing |

A rollup with **any** Positional/Holistic/Approximable measure is
**partially cascade-eligible**: the cascade-eligible measures cascade;
the rest fall back to base-derived build per the existing Tier 1/2
path. This is **per-measure, not per-rollup**, matching the
scala-data-driven-refactor discipline (classify data → dispatch
behavior, never name-based if/else).

The predicate is a method on `RollupSpec` (or a companion object
method, depending on which is cleaner for the two-arg form):
`RollupSpec.cascadeEligibleFrom(other: RollupSpec): Boolean`. A
`RollupMaterializerSpec`-class test pins the matrix exhaustively
against `Decomposability` (5 classes × 2 grain-coarsening directions ×
dimension-inclusion directions — small but the regression guarantee).

### D2 — Grain coarsening (the geometric predicate)

"B's grain coarsens A's grain" means every partition of B contains
one or more complete partitions of A. Concretely:

- A is `timeGrain = hour, grainDimension = order_date_hour`
- B is `timeGrain = day, grainDimension = order_date_day`
- B's `order_date_day = date_trunc('day', order_date_hour)` — every
  B partition is a 24:1 aggregation of A partitions (full coverage,
  no partial hours)

A partial-coarsening case is **forbidden**: B with
`timeGrain = day, grainDimension = order_date_day` but B's source A
was `timeGrain = hour` with a missing last hour (incomplete day) is
**NOT cascade-eligible** — the resulting daily bucket would silently
undercount. The same refusal vocabulary pattern as ADR-0029's
`ScopeUncovered` applies: `CascadeCoverageUncovered(reason: String)`,
typed `EngineError.UnsupportedCapability`.

This is the **first place** the project explicitly reasons about the
"complete partition coverage" invariant — it is the cascading analog
of the per-bucket source-⊆-scope check in Tier 1's `decideStrategy`.
The connector's resolution layer must verify it before any cascade
build runs.

### D3 — Staleness propagation across the cascade chain

A **stale** rollup (one whose buckets are not yet final, per
ADR-0030 §D3's watermark) cannot be the source of a cascade build.
The cascade inherits the staleness of its source — a stale hourly
rollup makes a stale daily rollup by definition (the daily rollup
**cannot be more final than its source**).

The propagation rule:

1. **The cascade source must be `is_final=true` for every bucket in
   the build scope** — verified via the watermark table at build time.
   Any non-final source bucket makes the cascade build refuse typed
   (`CascadeSourceNotFinal(buckets: Set[BucketKey])`, sibling to
   `RollupBucketStale`).
2. **Cascade source failures cascade as well** (new failure class for
   scala-impact-analysis: ripple across the rollup-source chain). A
   failed hourly refresh fails the dependent daily refresh; the
   daily refresh's failure mode is `CascadeSourceFailed`, not its own
   refresh error — the harness report attributes the failure to the
   root cause (the hourly job) with the daily job noted as the
   dependent. The platform's sequencing (D4) prevents the
   "double-build-then-fail" pattern: daily only runs after hourly
   completes successfully for its scope.
3. **The cascade refresh inherits the source's snapshot id**, not its
   own — the cascaded rollup's "last commit" points at the source's
   snapshot id. This is how a downstream observer can verify the
   staleness chain end-to-end without re-querying every level.

The new refusal `CascadeSourceNotFinal` follows the same pattern as
`RollupBucketStale` (ADR-0030 §D3): the rewriter never instantiates it,
the connector's resolution layer does, the refusal carries
`Set[BucketKey]`.

### D4 — Platform sequencing: hourly must complete before daily

The orchestrator (sm8-platform refresh surface or the operator's
scheduler) is responsible for sequencing rollup refreshes in cascade
order. The contract:

```
build(rollup_hourly)  succeeds for scope S
   THEN
build(rollup_daily)  may run, with scope = cascade_coarsen(S)
   ELSE
build(rollup_daily)  refuses typed (CascadeSourceNotFinal on the
                    hourly buckets in its coarsened scope)
```

**No concurrent cascade builds.** Two cascades racing on overlapping
hourly scopes produce inconsistent daily buckets; the orchestrator
serializes by cascade-source snapshot id (whichever started first
runs to completion; the second sees the new snapshot and proceeds
against it).

**Cycle detection.** A cascade graph must be a DAG (no rollup is its
own ancestor, transitively). The validation runs at model-load time:
the platform walks the cascade-source graph from each rollup; a cycle
refuses typed at deployment, not at refresh time. Cycles are a
configuration error, not a runtime surprise.

**Hourly-completion signal.** The platform reads the hourly rollup's
watermark table (ADR-0030 §D3) to determine completion of a scope,
not its own refresh log — the watermark is the single source of truth
for "is this bucket final enough to cascade?".

### D5 — Connector resolution: where the cascade source lives

The connector resolves "build this rollup FROM that rollup" at
materialization time. The seam is `RollupMaterializer.materialize` —
when called with a `cascadeSource: Option[RollupSpec]` parameter, the
materializer reads from the cascade source's table instead of the
base model. The semantics:

```text
materialize(model, spec, cascadeSource = Some(hourlySpec)):
  - Validate cascade-eligibility (D1 predicate, pure; no IO).
  - Validate grain coarsening (D2 predicate, pure; no IO).
  - Resolve cascade source's table from the routing layer
    (connector resolution layer, NOT core — same pattern as the
    watermark verdict in ADR-0030 §D3).
  - Verify cascade source's watermark is_final=true for every bucket
    in the scope (D3, connector-side; refusal is typed).
  - Run the aggregation against the cascade source's table (not
    the base table). Everything else — partition spec, snapshot
    commit, scope-coverage refusal — is identical to the
    non-cascaded path.
```

The parameter is **optional with default `None`**, preserving all
existing call sites (`RollupRefresher`, harness, Tier 1/2 specs).
When `None`, behavior is byte-identical to today (build from base).

### D6 — Algebra audit (Gate B item 2 reuse)

ADR-0029 §Gate B item 2 requires "every measure class on every live
rollup has a delta-combination test, partitioned by `Decomposability`."
This ADR **formalizes the audit shape** via the cascade-eligibility
predicate: the per-Decomposability-class behavior table in D1 IS the
audit shape, parameterized for cascade rather than for delta. The
audit cost is borne once and reused twice (Tier 2 delta-combination +
cascading).

A future PR can implement the audit by enumerating all live rollups
and applying D1's predicate; the output is a typed
`DecomposabilityAuditRow(rollup, measure, class, cascadeEligible)` per
row. The harness can then produce a "rollup X measure Y is
non-cascade-eligible because class Z" report — same shape as the
existing `sm8 rollup-report` (PR #350).

## Alternatives considered

1. **No cascade ever — build everything from base forever.** Rejected:
   ADR-0030 §D2-4's fallback is structurally Tier 1 (whole-partition
   recompute), and the cascade case is exactly where Tier 2's
   small-delta payoff lives. Forbidding cascades means Tier 2's value
   is bounded to the base-derived-only frame, which ADR-0030 §D1
   already measures as "ambiguity rate" — a pessimistic ceiling.
2. **Implicit cascade — auto-detect hourly-from-base + daily-from-base
   opportunities at refresh time.** Rejected (scala-impact-analysis):
   adds a new implicit-dependency graph at runtime, harder to reason
   about than an explicit cascade source declared on `RollupSpec`.
   Explicit cascades cycle-detect at load time; implicit cascades
   cycle-detect at runtime, where the cost of getting it wrong is a
   silent cascade loop.
3. **Cascade only for Additive measures; Algebraic rollups always
   recompute from base.** Rejected: Welford partial state is
   already additive across groups; refusing to cascade on Algebraic
   wastes the work already done at the single-grain level. The
   Welford `(n, sum, m2)` shape is the established one (ADR-0022/0023
   tripwire), so cascading Algebraic costs no extra complexity.
4. **Cascade via view (e.g., a Spark view that reads hourly and
   produces daily on-the-fly, with no separate daily table).**
   Rejected: views are query-time; the routing lane reads the rollup
   table, not the base. Cascade-eligible measures belong in their
   own rollup table so the routing lane can serve them without a
   per-query aggregation cost. This is exactly the Tier 1 → Tier 2
   boundary; ADR-0029's whole architecture is "materialize, don't
   compute at query time."
5. **Cycle-breaking by time-of-last-refresh (oldest cascade source
   wins).** Rejected: cycles are configuration errors; refusing typed
   at load time is the only safe behavior. Time-of-last-refresh is
   undefined when cycles involve concurrent refresh attempts.

## Consequences

- Cascading adds a new failure class (`CascadeSourceNotFinal`,
  `CascadeCoverageUncovered`, `CascadeSourceFailed`) into the existing
  refusal vocabulary — same shape, same emission rules (connector
  instantiates; rewriter never does).
- The cascade-source graph is a DAG enforced at model-load time.
- New connector parameter `cascadeSource: Option[RollupSpec]` on
  `RollupMaterializer.materialize` — default `None`, backward-compatible.
- No new format concept crosses the seam: Iceberg tables, partitions,
  snapshots, watermarks all reuse the ADR-0028/0030 machinery. The
  cascade is purely a **source-selection** decision, not a new
  table flavor.
- **Algebra audit work is consolidated.** Tier 2 delta-combination
  tests + cascade-eligibility audits share the per-Decomposability
  shape; doing ADR-0031 now saves the same per-measure audit work
  Tier 2 would otherwise redo.
- Staleness propagation introduces a new blast-radius class
  (scala-impact-analysis): a stale hourly can make a daily build
  refuse typed. The harness surfaces this as
  `CascadeSourceNotFinal` refusal, attributed to the root cause.
- Tier 2 implementation (when funded) needs to handle cascading in
  its source selection — but this ADR pins the cascade interface
  (D5) so Tier 2 implements against a stable shape.
- **Out-of-scope follow-up**: cross-engine cascade sources
  (Spark-built hourly consumed by Trino-built daily) inherit the
  Slice 2 (Trino writer) cross-engine read story. ADR-0031 does not
  resolve cross-engine cascade source resolution; that lives in
  Slice 2's read-side work.

## Tests (when cascading implementation is funded — listed now to pin the contract)

- **Cascade-eligibility predicate matrix.** For each
  `Decomposability` × grain-coarsening × dimension-inclusion
  combination, the predicate returns the expected boolean. Pin via a
  `RollupSpec.cascadeEligibleFromSpec` (regression-guarded).
- **Grain coarsening refusal.** B with partial-coarsening of A
  (e.g., daily grain over an incomplete-hour hourly source) →
  `CascadeCoverageUncovered` typed refusal.
- **Staleness propagation.** Cascade build with a non-final source
  bucket → `CascadeSourceNotFinal` refusal with the bucket set;
  harness attributes to the source.
- **Source-failure propagation.** Hourly refresh fails → daily
  refresh refuses with `CascadeSourceFailed`; the harness report
  attributes to the root cause.
- **DAG enforcement.** Model with a cascade cycle (A from B, B from A)
  → deployment-time refusal; refresh is never attempted.
- **Cycle detection edge cases.** Self-cycle (A from A) → refusal.
  Three-node cycle → refusal. DAG with three levels (base → hourly →
  daily → weekly) → accepted.
- **Cascade source freshness cross-rollup.** Verify the cascaded
  rollup's last-commit-snapshot-id points at the source's snapshot
  id, not its own.
- **Idempotency under cascading.** Re-running a cascade build against
  an unchanged source produces a content-identical cascaded rollup
  (per ADR-0030 §D2-5's content-identity contract test, applied to
  the cascaded rollup).
