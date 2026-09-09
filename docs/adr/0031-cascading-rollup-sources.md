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

A rollup **B** (the coarser, cascade TARGET) is **cascade-eligible from
rollup A** (the finer, cascade SOURCE) iff every measure on B has
`Decomposability` that participates in cascading, AND B's grouping is a
coarser-or-equal partition refinement of A's. The predicate is a **pure
function of the two rollup declarations** — no IO, no engine concept, no
format concept (RFC §3: core stays format- and strategy-blind).

**Convention (R1 reviews, both reviewers caught the original inversion):
A = the cascade SOURCE (finer, e.g. hourly); B = the cascade TARGET
(coarser, e.g. daily). A coarser rollup groups by FEWER dimension keys,
not more.**

```text
cascade_eligible(B, A) =
  forall measure m in B.measures:
    AggregateFn.decomposability(m.expr.fn) in { Additive, Algebraic }
  AND
    partial-state-present(m, A)      // A must physically carry the
                                     // partial columns m's class needs
                                     // (R1 finch F6): e.g. B declares
                                     // Avg(F) ⇒ A must have m2__F +
                                     // count__F + sum__F columns, not
                                     // just sum__F
  AND
    B.grain coarsens A.grain          // see D2 (geometric, structural)
  AND
    A.dimensions ⊇ B.dimensions      // every B dim names an A dim —
                                     // coarser = fewer dims (R1 both
                                     // reviewers: was inverted)
  AND
    A is fully final for the scope   // see D3 staleness rule
```

Per-Decomposability-class behavior (re-states ADR-0030 §D2-6 for the
cascading case; the Additive row is SPLIT — Sum/Count are truly
additive while Min/Max are binary-reducible, a distinct property R1
review finch F10 called out — same direction-error class that
`AggregateFn.scala` previously made with Min/Max):

| Class | Members | Cascade behavior |
|---|---|---|
| `Additive` (true sum-type) | Sum, Count | mergeable across groups by plain summation of partials; pre+post image sufficient |
| `Additive` (binary-reducible) | Min, Max | mergeable by re-applying the binary op across partials (`min(min_a, min_b)`); NOT plain summation — distinct merge expression |
| `Algebraic` (Avg, Stddev×2, Variance×2) | mergeable across groups via **Welford merge** — NOT plain `SUM(m2)`: the correct merge is `m2_ab = m2_a + m2_b + δ²·n_a·n_b/n_ab` where `δ = mean_b − mean_a` (R1 finch F3 — the cross-group term is easy to miss and a naive `SUM(m2)` silently under-counts dispersion). ADR-0023's 1e8±1.0 cancellation tripwire must be re-validated against this merge expression in the contract tests |
| `Positional` (First, Last) | **never cascades** — argmin/argmax state is not exposed by sm8's current connectors (note: Spark 3.3+ has `min_by`/`max_by` — the restriction is about sm8's connector surface, not Spark's capability; R1 finch F13); falls back to base-derived build for these measure columns |
| `Holistic` (Median, Percentile×2) | **never cascades** — no bounded partial state suffices; falls back to base-derived build for these columns |
| `Approximable` (CountDistinct, ApproxPercentile) | **never cascades** — exact vs approximate semantics differ; forbidden by ADR-0022 in v1 routing |

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

**Non-time-grain coarsens** (R1 reviews, turtle M1 + finch F9): the
predicate generalizes structurally, not just over time. For
B = `region`-only cascaded from A = `region + hour` (same timeGrain,
fewer dims): B's grouping is coarser because A's rows partition
 cleanly into B's groups (A.groups ⊇ B.groups as sets of dim tuples).
For B with the SAME timeGrain but fewer non-grain dims: same rule —
every A group maps to exactly one B group. The check is **structural**
(a function of the two declarations; core computes it at validation
time) and is SEPARATE from the dynamic completeness check (does A
actually have all its partitions for the scope? — connector-side,
D3).

A partial-coarsening case is **forbidden**: B with
`timeGrain = day, grainDimension = order_date_day` but B's source A
was `timeGrain = hour` with a missing last hour (incomplete day) is
**NOT cascade-eligible** — the resulting daily bucket would silently
undercount. The same refusal vocabulary pattern as ADR-0029's
`ScopeUncovered` applies: `CascadeCoverageUncovered(reason: String)`,
typed `EngineError.UnsupportedCapability`.

**Layer split** (R1 review, turtle M2): the STRUCTURAL coarsening
relation (declared shapes coarsen each other) is a **core** predicate
— pure, no IO, computed at model-validation time. The DYNAMIC
completeness check (does A actually have all its partitions populated
for the scope being cascaded?) is a **connector** concern — it reads
the source's watermark/manifest. D1's predicate uses only the
structural half; D3's staleness rule uses the dynamic half. The two
are distinct and both are required.

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

The propagation rule (R1 reviews, rabbit F2 + turtle M3):

1. **The cascade source must be `is_final=true` for every bucket in
   the build scope** — verified via the watermark table at build time.
   Any non-final source bucket makes the cascade build refuse typed
   (`CascadeSourceNotFinal(buckets: Set[BucketKey])`, sibling to
   `RollupBucketStale`).
2. **Cascade is atomic relative to the source's snapshot.** The
   materializer reads the cascade source's snapshot id at build start,
   pins it for the duration of the aggregation, and the cascaded
   rollup's last_commit_snapshot_id points at that snapshot id (not at
   the cascade rollup's own). A concurrent source refresh between
   build start and cascade commit does NOT contaminate the cascaded
   rollup — the cascade read is point-in-time against the pinned
   snapshot. **Composition with ADR-0030 §D3 monotonicity**: the
   monotonicity contract is preserved because the cascade never
   "observes" a newer source snapshot; it observes the snapshot it
   pinned. ADR-0030's monotonicity is about the watermark's evolution
   over time — the cascade doesn't disturb that, it reads a specific
   historical snapshot.
3. **Cascade source failures cascade as well** (new failure class for
   scala-impact-analysis: ripple across the rollup-source chain). A
   failed hourly refresh fails the dependent daily refresh; the daily
   refresh's failure mode is `CascadeSourceFailed`, not its own
   refresh error — the harness report attributes the failure to the
   root cause (the hourly job) with the daily job noted as the
   dependent. The platform's sequencing (D4) prevents the
   "double-build-then-fail" pattern: daily only runs after hourly
   completes successfully for its scope.
4. **Four distinct failure classes** (R1 reviews, rabbit F7 + turtle
   M5) — the harness report must distinguish them unambiguously:
   - `CascadeSourceMissing(model, sourceRollupName)` — the source
     rollup's table does not exist (never materialized, or purged).
     Distinct from the others: a missing source is not a build
     problem to retry, it's a configuration or ordering problem.
   - `CascadeSourceNotFinal(buckets: Set[BucketKey])` — the source
     exists but its buckets are not final yet (D3 propagation rule 1).
   - `CascadeSourceFailed(previousError)` — the source's last refresh
     failed; the cascade depends on a successful source state.
   - `CascadeCoverageUncovered(reason)` — the source is final and
     successful but a bucket in the cascade scope has incomplete A-side
     partition coverage (the missing-last-hour case).

The new refusals follow the same pattern as `RollupBucketStale`
(ADR-0030 §D3): the rewriter never instantiates them, the
connector's resolution layer does, every refusal carries
`Set[BucketKey]`.

**MOR-hybrid cross-reference** (R1 review, rabbit F12): if the source
rollup is in MOR-hybrid mode (per ADR-0030 §D1's experiment), the
cascade reads may encounter equality-delete files from un-compacted
buckets. The cascade reads pinned snapshot semantics already address
this (the pinned snapshot is a coherent Iceberg state at the snapshot
id, regardless of MOR vs COW history of that snapshot). The ADR-0030
monotonicity contract on the watermark guarantees the pinned snapshot
id has `is_final=true` per D3 rule 1 — no additional cross-reference
needed beyond the existing snapshot-id pin.

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

`cascade_coarsen(S)` is a **core helper on the `Scope` type**
(R1 review, turtle L3): `Scope.coarsenTo(timeGrain): Scope` — maps
each bucket key from the source's finer grain to the target's coarser
grain via `date_trunc` for time grains, and via the target's dimension
set for non-grain coarsens. The helper is pure (no IO) and lives in
core alongside `RollupSpec.cascadeEligibleFrom`.

**No concurrent cascade builds.** Two cascades racing on overlapping
hourly scopes produce inconsistent daily buckets; the orchestrator
serializes by cascade-source snapshot id (whichever started first
runs to completion; the second sees the new snapshot and proceeds
against it).

**Cycle detection — concrete seam** (R1 review, turtle H1): the
cascade graph is **declared on `RollupSpec`**, not on the
materializer call site. `RollupSpec` gains a new optional field
`cascadeSource: Option[RollupSpec]` — the *declaration* (which rollup
is this one built from). The materializer signature retains its own
`cascadeSource` parameter for explicit-call use, populated from the
`RollupSpec` field by default. With the cascade graph visible on
`RollupSpec`, the platform walks it at model-load time via a new
extension to `ModelValidator.validateCascadeDag(rollups: List[RollupSpec])`
(sibling to the existing `validateGrainDimensionDeclared` /
`validateGrainDimensionResolved` hooks in `ModelValidator.scala`).
A cycle refuses typed at deployment, not at refresh time. Cycles are
a configuration error, not a runtime surprise.

**Hourly-completion signal.** The platform reads the hourly rollup's
watermark table (ADR-0030 §D3) to determine completion of a scope,
not its own refresh log — the watermark is the single source of truth
for "is this bucket final enough to cascade?".

### D5 — Connector resolution: where the cascade source lives

The seam is split (R1 review, turtle H2) — mirroring how ADR-0030
§D3 splits freshness-policy declaration (`RollupSpec.freshness`) from
freshness-policy resolution (connector's watermark lookup):

- **Declaration** (which rollup-of-which-model is the cascade source)
  lives on `RollupSpec` (core). `RollupSpec.cascadeEligibleFrom(other)`
  operates on the declared cascade source. The platform's cascade
  validation walks the declaration graph (D4).
- **Resolution** (which Iceberg table to read from at materialization
  time) lives on the connector. `RollupMaterializer.materialize(...
  cascadeSource: Option[RollupSpec])` accepts the resolved source
  declaration; the connector's resolution layer turns the declaration
  into the source's Iceberg table identifier at build start.

The semantics:

```text
materialize(model, spec, cascadeSource: Option[RollupSpec]):
  - Validate cascade-eligibility (D1 predicate, pure; no IO).
  - Validate grain coarsening — structural half (D2; pure).
  - Resolve cascade source's table from the routing layer
    (connector resolution layer, NOT core — same pattern as the
    watermark verdict in ADR-0030 §D3).
  - Verify cascade source's watermark is_final=true for every bucket
    in the scope (D3 rule 1; connector-side; refusal is typed).
  - Pin the cascade source's snapshot id at build start (D3 rule 2).
  - Build the rollup using **cascade-source-aware stateColumns**
    (R1 review, rabbit F4). When cascadeSource is Some, the cascade
    reads from a table whose shape is the source rollup's
    (count__F, sum__F, m2__F columns) — NOT the base table's raw F
    column. The materializer's aggregation path branches on
    cascadeSource presence: cascade path applies the Welford merge
    (D1 Algebraic row cross-term); non-cascade path applies
    var_pop(F) to raw rows as today.
  - Commit the cascaded rollup with last_commit_snapshot_id pointing
    at the pinned source snapshot id.
```

The parameter is **optional with default `None`** when called directly,
preserving all existing call sites (`RollupRefresher`, harness, Tier 1/2
specs). When `None`, behavior is byte-identical to today (build from
base).

### D6 — Algebra audit (Gate B item 2 reuse, qualified)

ADR-0029 §Gate B item 2 requires "every measure class on every live
rollup has a delta-combination test, partitioned by `Decomposability`."
**This ADR formalizes the audit shape for the cascade case** (the
per-Decomposability table in D1), but the audit is **NOT shared with
Tier 2 delta-combination** (R1 reviews, rabbit F14/F15 + turtle M4):

- Cascade-eligibility predicate inputs: Decomposability class,
  partial-state-presence, coarsening relation, source finality.
- Tier 2 delta-combination predicate inputs: Decomposability class,
  pre+post image availability, snapshot-diff fidelity, ambiguous-bucket
  classification (ADR-0030 §D5).

The two predicates share the per-Decomposability-class taxonomy (the
`Decomposability` enum, the 5 classes) but their OTHER inputs differ.
The ADR-0029 Gate B item 2 audit must enumerate both predicates for
every measure; the shared taxonomy makes the enumeration cheaper, but
the work is **two separate per-measure audits**, not one shared one.
ADR-0031 does the cascade audit shape; ADR-0030 §D2-7 does the
delta-combination shape. A future PR (Gate B item 2 implementation)
enumerates both per measure.

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

- Cascading adds **four** new failure classes into the existing refusal
  vocabulary — `CascadeSourceMissing`, `CascadeSourceNotFinal`,
  `CascadeSourceFailed`, `CascadeCoverageUncovered` — same shape, same
  emission rules (connector instantiates; rewriter never does).
- The cascade-source graph is declared on `RollupSpec` (core,
  `cascadeSource: Option[RollupSpec]`) and is a DAG enforced at
  model-load time via `ModelValidator.validateCascadeDag`.
- `RollupMaterializer.materialize` gains a cascade-aware aggregation
  path: when the cascade source is present, `stateColumns` reads the
  source's partial-state shape (`count__F`/`sum__F`/`m2__F`) and
  applies the Welford merge with the explicit cross-group
  `δ²·n_a·n_b/n_ab` term — not `var_pop(F)` on raw rows.
- No new format concept crosses the seam: Iceberg tables, partitions,
  snapshots, watermarks all reuse the ADR-0028/0030 machinery. The
  cascade is purely a **source-selection** decision, not a new
  table flavor.
- **Algebra audit work is scoped, not shared.** Tier 2
  delta-combination (ADR-0030 §D2-7) and cascade-eligibility (this
  ADR §D1) share the per-Decomposability taxonomy but have distinct
  predicate inputs; each is a separate per-measure audit. Both
  consume the same enumeration harness shape.
- Staleness propagation introduces a new blast-radius class
  (scala-impact-analysis): a stale hourly can make a daily build
  refuse typed. The harness surfaces this as
  `CascadeSourceNotFinal`, attributed to the root cause.
- Tier 2 implementation (when funded) must handle cascading in its
  source selection — this ADR pins the cascade interface (D5) so
  Tier 2 implements against a stable shape.
- **Out-of-scope follow-up**: cross-engine cascade sources
  (Spark-built hourly consumed by Trino-built daily) inherit the
  Slice 2 (Trino writer) cross-engine read story.

## Tests (when cascading implementation is funded — listed now to pin the contract)

- **Cascade-eligibility predicate matrix.** For each
  `Decomposability` class × coarsening relation ×
  partial-state-presence combination, the predicate returns the
  expected boolean. The matrix is **5 classes × coarsen/not-coarsen ×
  partial-state-present/absent** — the partial-state dimension was
  missing from the original count (R1 finch F15); the matrix grows,
  not shrinks.
- **Grain coarsening refusal.** B with partial-coarsening of A
  (e.g., daily grain over an incomplete-hour hourly source) →
  `CascadeCoverageUncovered` typed refusal.
- **Staleness propagation.** Cascade build with a non-final source
  bucket → `CascadeSourceNotFinal` refusal with the bucket set;
  harness attributes to the source.
- **CascadeSourceMissing refusal.** Source table does not exist →
  typed refusal distinct from NotFinal and Failed.
- **Source-failure propagation.** Hourly refresh fails → daily
  refresh refuses with `CascadeSourceFailed`; the harness report
  attributes to the root cause.
- **DAG enforcement.** Model with a cascade cycle (A from B, B from A)
  → deployment-time refusal; refresh is never attempted.
- **Cycle detection edge cases.** Self-cycle (A from A) → refusal.
  Three-node cycle → refusal. DAG with three levels (base → hourly →
  daily → weekly) → accepted.
- **Welford merge cancellation tripwire** (R1 finch F8). The
  cascade cross-group merge expression
  (`m2_ab = m2_a + m2_b + δ²·n_a·n_b/n_ab`) must pass the same
  1e8±1.0 dispersion fixture that motivated ADR-0023's Welford
  migration — a naive `SUM(m2)` fails this fixture; the explicit
  merge expression must also pass.
- **Cascade source freshness cross-rollup.** Verify the cascaded
  rollup's last-commit-snapshot-id points at the source's pinned
  snapshot id, not its own.
- **Idempotency under cascading.** Re-running a cascade build against
  an unchanged source produces a content-identical cascaded rollup
  (per ADR-0030 §D2-5's content-identity contract test, applied to
  the cascaded rollup).
