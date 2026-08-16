# ADR-008-K: Spark connector compile — joins + aggregations (PR-K)

**Status:** Accepted. **Date:** 2026-08-16. **Author:** SM8 agent (per user directive 2026-08-16: "join one to one, one to many, many to many op cross-join" + "aggregate").

## Context and Problem Statement

PR-H shipped the `rel/` IR (JoinKind, AggregateFn, AggregateCall, RelOp). PR-J shipped the typed `Model` extensions (`Measure.expr: AggregateCall`, `JoinSpec`, `CalculatedMeasure`). But `PortableQueryCompiler.compile()` still only did source → filters → dimension projection — the joins + aggregations landed in the Model with **no Spark-side compile**.

Without PR-K:
- `model.joins` is dead data (never applied).
- `model.measures` (typed AggregateCall) is dead data (never aggregated).
- `model.calculatedMeasures` is dead data (never rendered).

## Decision

Port the legacy's three compile stages into `PortableQueryCompiler.scala`:

| Stage | What | Scope |
|---|---|---|
| `applyJoins` | folds `model.joins` onto the DataFrame | 5 kinds (Inner/Left/Right/Full/Cross); **single-key** equi-join; multi-key + missing right-side → typed `UnsupportedCapability` |
| `applyAggregations` | applies `model.measures` + `model.calculatedMeasures` | groupBy+agg path (default) or **window path** when any calc references `Expr.All`; pre-validates fns |
| `renderAggregate` | `AggregateCall` → Spark `Column` | **6 wired fns** (Sum/Count/CountDistinct/Avg/Min/Max); the other 10 → typed `FeatureDeferred` at the compile boundary |

New compile pipeline: `resolveSource → applyFilters → applyJoins → applyAggregations` (dimensions become groupBy keys when measures exist; measure-less models keep the plain projection).

### Two contract decisions (beyond the legacy)

1. **Join-key ambiguity dedup (left-authoritative).** When both sides carry the join key, Spark keeps both columns and every later unqualified reference is `AMBIGUOUS_REFERENCE`. Fix: `joined.drop(rDf(rightKey))` — the left's copy is authoritative (the legacy DESIGN §6.3(4) "base-column-wins-on-collision" invariant). Discovered by the data-plane tests, not by reading the legacy (the legacy deduped in `SemanticJoinOp`, which is not ported).

2. **Cross is unconditional.** Per the `RelOp.Join` contract (PR-H): "for Cross, the condition is unused — the join is unconditional". Compiled as `crossJoin` (plain Cartesian), NOT `join(..., "cross")` (which applies the condition).

3. **Pre-validated FeatureDeferred boundary.** The 10 unwired aggregates surface as `EngineError.FeatureDeferred` (typed, at the `compile()` boundary — per ADR-008-H's "never a silent no-op") rather than the legacy's mid-pipeline `UnsupportedOperationException`. `renderAggregate` stays total for the 6 supported; its fallback throw is an internal-invariant violation, unreachable after pre-validation.

### Layer ownership (RFC §3)

| Concern | Layer |
|---|---|
| JoinKind / AggregateFn / AggregateCall / JoinSpec / Measure / CalculatedMeasure (data) | **core** (PR-H + PR-J) |
| `applyJoins` / `applyAggregations` / `renderAggregate` (Spark compile) | **connector** (`spark-connector/PortableQueryCompiler.scala`) — this PR |

## Conformance (RFC §12)

13 new data-plane tests (`PortableQueryCompilerJoinsAggsSpec`):

| Coverage | Tests |
|---|---|
| renderAggregate 6 wired fns | 3 (Sum+Count; Avg+Min+Max; CountDistinct) |
| applyJoins 5 kinds | 5 (row counts + null-on-miss for each kind) |
| Typed error boundaries | 3 (unwired aggregate → FeatureDeferred; multi-key → UnsupportedCapability; missing right-side → UnsupportedCapability) |
| Window path (Expr.All pct-of-total) | 1 |
| Measure-less projection (pre-PR-K path) | 1 |

## Spark concerns (per user directive)

- Zero spark imports in sm8-core (enforcer passes; the only `org.apache.spark` strings in core are docstring mentions of the grep command itself)
- Mantra #1 (closure-safety): constructor-injected SparkSession; the legacy's `@volatile var _spark` companion state is **not** ported
- Mantra #3 (schema-drift): fixtures declare explicit StructTypes
- Mantra #5 (driver-vs-executor): compile builds the lazy plan in the driver; no UDFs/accumulators/time sources → determinism by construction (PR-F replay-safety holds)

## Pre-commit gates

| Gate | Result |
|---|---|
| LSP diagnostics (compiler + spec) | ✅ 2/2 clean |
| Codegraph blast-radius | ✅ single caller (`SparkEngineProvider:141`), signature unchanged |
| Maven Enforcer | ✅ passes |
| Reactor | ✅ **976 green, 0 failures** (was 963, +13) |

## Sequence status

| PR | Status |
|---|---|
| PR-A … PR-F | ✅ merged |
| PR-H (#75) rel/ IR package | ✅ merged |
| PR-I (#76) CaseWhen + Alias | ✅ merged |
| PR-J (#77) Model extensions | ✅ merged |
| **PR-K (this)** | **Spark compile: joins + aggregations** |
| PR-L QueryBuilder (Model → RelOp) | next |
| PR-M v0.1.0 tag cut | deferred |
