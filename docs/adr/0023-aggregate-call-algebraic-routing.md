# ADR-0023: Algebraic measure routing over rollups — two-phase Aggregate+Project re-aggregation (no IR extension)

## Status

Proposed. **Date:** 2026-09-07. **Author:** SM8 agent. Follow-up to ADR-0022 (pre-aggregation) Ticket 4 follow-up lane. Dual review round 1 (architect + data-engineer, `mux/reasoning`): both APPROVE conditional on the amendments already folded into this revision — status flips to Accepted at merge (user is the sole merger).

**Supersedes the approach reverted in PR-338** (commit `cff414c`, revert `7ba90ed`): that PR flipped the `AlgebraicStateNotWired` gate while `rebaseAggregate` still emitted single-input `AggregateCall`s — Avg rebased to `Sum(sum__F)` (numerator only, silently wrong) and Stddev/Variance rebased to `Sum(sumsq__F)` (not a dispersion, no NULL guard). Both round-2 reviewers (architect + data-engineer swarm roles) independently recommended revert; PR-338 was closed, not merged. This ADR is the design the flip was missing.

## Context and Problem Statement

ADR-0022 landed the six-ticket pre-aggregation map. Algebraic measures (`Avg`, `StddevSample`, `StddevPopulation`, `VarianceSample`, `VariancePopulation`) are **declared and validated but refused by both the materializer and the router** (fail-open, base path) — a perf loss, never a wrong number. The refusal is `RollupRewriterRefusal.AlgebraicStateNotWired` (`sm8-core/src/main/scala/io/sm8/core/rel/RollupRewriter.scala:133`), deliberately distinct from the permanent `UnsplittableAggregate` (Holistic/Positional/Approximable) so the Ticket 6 observer can tell recoverable from permanent.

Since then, the prerequisite infrastructure landed:

- **PR-337 (merged)** — spark-connector writes per-group partial state columns for every rollup measure `F`: `count__F` (per-group count), `sum__F`, `sumsq__F`. Nothing reads them yet.
- **PR-336 (merged)** — `RollupRewriter.reconciledRollupSchema` derives state-column types from the base scan (no schema drift).
- **Guard builders (exist, Expr-level tested)** — `RollupRewriter.stddevSampGuardExpr` / `stddevPopGuardExpr` (`RollupRewriter.scala:678,696`) emit the full engine-parity NULL-guard contracts (`n < 2` → NULL for sample; `n = 0` → NULL for population) as `Expr` trees over named re-aggregated total columns.

The blocker, confirmed by dual review of PR-338: **`AggregateCall` is a single-input ADT** (`fn`, `input: Option[Expr]`, `alias`, `distinct`, `arguments` — `sm8-core/src/main/scala/io/sm8/core/rel/AggregateCall.scala:32`). It cannot express:

- `Avg(x)` from a rollup as `Sum(sum__x) / Sum(count__x)` — a two-input derived expression over re-aggregated totals;
- `StddevSample(x)` as `sqrt(CaseWhen(n < 2 → NULL, (sumsq − sum²/n) / (n − 1)))` — same, plus a scalar wrapper.

The current `rebaseAggregate` (`RollupRewriter.scala:637`) handles only `Decomposability.Additive` (Sum/Count/Min/Max re-pointed at state columns); every other case falls through `case _ => a` (`:661`) — the pass-through that made the reverted PR silently wrong.

**The question**: how should the router express algebraic re-aggregation without breaking the closed-ADT discipline (ADR-0022 §"RelOp is a closed ADT by design"), without UDFs (PR-332 closure-safety contract: built-in Column aggregations only), and without engine-specific routing paths (layer discipline, RULE#1)?

## Decision

**Two-phase re-aggregation at the RelOp level: `Aggregate → Project`.** The rewriter emits a plan that (1) re-aggregates the named partial state columns with ordinary Additive calls in an inner `RelOp.Aggregate` over the rollup scan, then (2) derives each algebraic measure in an outer `RelOp.Project` using the existing `Expr` vocabulary (`Divide`, `Subtract`, `Multiply`, `CaseWhen`, `FunctionCall("sqrt", …)`). Concretely, for a request `Aggregate(rollupScan, g, [Avg(x) AS a])`:

```
┌──────────────────────────────────────────────────────────────┐
│ Project( (sum__x_total / count__x_total) AS a )              │  ← derived (Expr)
└──────────────────────────┬───────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────┐
│ Aggregate(rollupScan, g=[dims…],                             │  ← re-aggregate
│   a=[Sum(sum__x) AS sum__x_total,                            │
│      Sum(count__x) AS count__x_total])                       │
└──────────────────────────────────────────────────────────────┘
```

Stddev/Variance additionally carry `Sum(sumsq__x) AS sumsq__x_total` in the inner Aggregate, and the outer Project expression is the **sqrt of the existing guard builders' output** (`stddevSampGuardExpr`/`stddevPopGuardExpr` produce the guarded variance/ratio; `FunctionCall("sqrt", …)` wraps it). All constructs are existing IR vocabulary: `RelOp.Aggregate`, `RelOp.Project`, `Expr.CaseWhen`, `Expr.Divide`, `Expr.FunctionCall`.

**Why this shape and not an IR extension**: the three reviewers' phrase "requires an IR extension" was true for any approach that keeps one `AggregateCall` per output measure. But the *plan shape* `Aggregate→Project` already exists in the closed ADT, is already compiled by the spark connector (`MinimalRelOpLowerer.lower` owns all 7 RelOp cases, `PortableQueryCompiler.compileRelOp` delegates), and is already rendered by `RelOpPlanPrinter`. No new ADT case, no new engine contract, no migration: **every engine that compiles Aggregate+Project+Expr inherits algebraic routing** (Spark today; DuckDB/Trino when their lowerers land).

### Routing contract (v1.1)

1. `AlgebraicStateNotWired` narrows: it fires **only** when a matched rollup lacks the state columns an algebraic measure needs (`count__F`/`sum__F` missing → recoverable refusal, base path). Wired-and-matched plans now route.
2. `UnsplittableAggregate` stays the permanent refusal for Positional/Holistic/Approximable — unchanged.
3. Output schema parity is a hard contract: the outer Project's final alias for each measure equals the original request's measure alias, and the result type is Double for all five algebraic fns. **The Double contract is enforced at materialization time, not implicitly**: the connector already casts `sum__F` and `sumsq__F` to double on write (`RollupMaterializer.scala:248-249`, PR-337), so the re-aggregation computes in double space on every engine; `count__F` stays integral and the outer `Divide` widens per engine semantics — the parity suite pins the widened result is Double.
4. Distinct flag: `Avg(DISTINCT x)` / `Stddev(DISTINCT x)` **cannot** be served from (sum, count) partials — the partials collapse distinctness. The matcher must refuse these with `UnsplittableAggregate` (permanent), mirroring the Approximable discipline: serving them would silently change semantics. (The existing `a.distinct` refusal at `RollupRewriter.scala:323` already routes here; the test contract below pins it for algebraic fns specifically.)
5. The gate flip happens **only after** the parity suite (below) is green — the exact sequencing mistake PR-338 made in reverse.
6. **Mixed Additive+Algebraic requests compose, never pick one arm**: a single `Aggregate` asking `[Count(*) AS rows, Avg(x) AS a]` must produce an inner Aggregate carrying ALL needed re-aggregates (`Sum(count__rows) AS count__rows_total`, `Sum(sum__x) AS sum__x_total`, `Sum(count__x) AS count__x_total` — distinct aliases via the `<state>__<F>_total` convention, which de-conflicts same-prefix columns like `count__rows` vs `count__x`) and an outer Project passing additive results through while deriving algebraic ones. PR-338's bug was rooted exactly in an arm dropping the other's inputs; a plan-shape test pins this composition.

### State representation: raw (n, sum, sumSq) now, Welford as a measured follow-up

ADR-0022 recorded a preference for Welford-merge columns (n, mean, M2) over raw (n, sum, sumSq). PR-337 shipped raw. This ADR routes on **raw columns as-is** (zero connector/materializer change) with a **pinned catastrophic-cancellation tripwire**, not a vibes-based one: the parity fixture is 30 rows with values `1e8 ± 1.0` (mean ≈ 1e8, unit-level dispersion); the assertion is rollup-path stddev within **relative ≤ 1e-6 OR absolute ≤ 1e-9 of the base path (whichever is tighter is NOT used — the pass criterion is the looser of the two, the log records measured drift either way)**; on failure the Welford migration below becomes mandatory before the gate flip ships. The Welford merge algebra is fully expressible in `Expr` (mean′ = (n₁m₁+n₂m₂)/(n₁+n₂); M2′ = M2₁+M2₂+n₁n₂(m₁−m₂)²/(n₁+n₂)), so migrating state columns to (n, mean, M2) later is a connector+materializer change plus a rewriter-local change — no plan-shape change.

## Consequences

**Core (sm8-core, IO-free — no new dependencies):**
- `RollupRewriter` gains `algebraicReaggregation`: builds the inner re-aggregate `AggregateCall`s (Sum over state columns — all Additive, so they reuse the existing rebase path) and the outer derived `Expr` per algebraic fn, wiring the guard builders + sqrt. Private until routing lands; tested at the plan level (`RollupRewriterSpec`): emitted plan structure, alias parity, refusal taxonomy, **plus these round-1-review-pinned cases**:
  - mixed `Count(*) + Avg(x)` composition (routing contract #6) — inner carries all three re-aggregates, outer composes both arms;
  - `Avg(DISTINCT x)` / `StddevSample(DISTINCT x)` → `UnsplittableAggregate` (taxonomy binding for routing contract #4 — prevents a future PR from "fixing" the distinct check);
  - multi-algebraic alias parity: `[Avg(fare) AS a, StddevSample(fare) AS s]` → outer Project aliases exactly `a`, `s` in request order;
  - `RelOpPlanPrinter` renders the algebraic plan (substring pins for `Project(`, the re-aggregate aliases, and the guard `CASE WHEN … THEN NULL`), so explain output cannot silently lose a clause.
- `matchesGroupSet`/grain gates unchanged — algebraic routing only changes Criterion 2's per-fn handling.

**Connector (spark-connector):**
- **`Expr.FunctionCall("sqrt", …)` is NOT compilable today**: `PortableExprCompiler.scala:190` returns typed `Left(EngineError.UnsupportedCapability)` for every `FunctionCall` (UDF resolution deferred). This is a **gate-flip-PR-owned fix**, not a follow-up: without it, the first algebraic query fails at the engine boundary after the gate opens — the same flip-without-capability class as PR-338. The flip PR must add a narrow builtin arm (or allowlist: `sqrt`, then `abs`/`coalesce` as needed) with a `PortableExprCompilerSpec` pin asserting `FunctionCall("sqrt", …) toColumn` yields a real sqrt `Column`, not `Left`.
- Parity regression suite extends the PR-332 harness with Algebraic cases: integral + nullable data, single-observation groups (`n=1` → NULL sample / `0` population), `n=0` groups, and the pinned cancellation tripwire fixture (above). Rollup path ≡ base path, or the gate does not flip.
- Connectors with their own Expr compilers inherit algebraic routing only after mirroring the same builtin arm — the flip PR notes the parallel-arm checklist even if Spark is the only engine exercised end-to-end.

**Explainer/observability:** `RelOpPlanPrinter` already renders Aggregate+Project — rewritten plans explain for free (test-pinned per the Core list above). The Ticket 6 observer's recoverable/permanent refusal *taxonomy* survives unchanged; note the platform-side observer that consumes `RollupRewriteRefusal` is still a Ticket-6-epic follow-up, not wired behavior today.

**Non-consequences (what this ADR does NOT do):**
- No change to `AggregateCall`/`AggregateFn`/`RelOp` ADTs — closed-ADT discipline preserved.
- No engine-specific routing in any adapter (the reverted PR's "engine-adapter concern" framing is dead: everything above is core-level RelOp shaping).
- No rollup serving of DISTINCT algebraic aggregates (permanent refusal, per routing contract #4).
- No new UDFs, no Catalyst extensions, no new plugin surface.

## Alternatives Considered

1. **Extend `AggregateCall` with a derived-expression variant** (`DerivedCall(expr, alias)`). Works, but adds a new ADT case every engine adapter must pattern-match forever, and duplicates what `Project` already expresses. The plan-shape solution gets identical plans with zero ADT surface change. Rejected as strictly more IR.
2. **Welford-merge `AggregateFn` cases compiled natively per engine** (e.g. `WelfordMerge` → Spark `udf`/Catalyst). Violates the no-UDF closure-safety contract (PR-332) or requires a Catalyst expression per engine — heavyweight, engine-specific, layer-leaking. Rejected. Welford remains available later via state-column migration (see Decision), which keeps merge algebra in portable `Expr`.
3. **Engine-native routing** (spark-connector special-cases algebraic measures over rollup tables behind the rewriter's back). Directly violates RULE#1 layer discipline (routing decisions in an adapter) — the exact class of bug PR-191's H1 regression was. Rejected outright.
4. **Status quo (keep refusing Algebraic forever)**. Safe but leaves the most common analytic aggregate (Avg) permanently unable to use rollups. Rejected once the two-phase shape made correct routing cheap.

## References

- ADR-0022 (`docs/adr/0022-pre-aggregation-sm8-native.md`) — parent design; Ticket 4 refusal taxonomy; "Known v1 limits" lane this ADR closes.
- PR-338 (closed, unmerged) — the reverted premature gate flip; both round-2 reviews' CRITICAL findings are the negative evidence motivating the two-phase shape.
- PR-337 (merged) — connector-side partial state columns `count__F` / `sum__F` / `sumsq__F`.
- PR-336 (merged) — `reconciledRollupSchema` state-column type derivation.
- `RollupRewriterSpec` — guard-builder Expr contracts pinned at `sm8-core/src/test/scala/io/sm8/core/rel/`.
- Textbook decomposable-aggregate theory (per `AggregateFn.Decomposability` scaladoc, Ticket 1 / PR-327).
