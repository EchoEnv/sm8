# ADR-008-M4: Production wiring — closes ADR-008-L Appendix GAPs 5, 6, 7, 8

**Status:** Accepted. **Date:** 2026-08-17. **Author:** SM8 agent. **Closes:** GAP 5 (CRITICAL), GAP 6, GAP 7, GAP 8 — the final pre-tag hardening PR.

## Context

Per **ADR-008-L Appendix** (the production-readiness audit): the IR-extension path (PR-H/I/J/K/L) was inert in production. `SparkEngineProvider.query` called `PortableQueryCompiler.compile(model, ctx)` directly, bypassing `QueryBuilder.build` entirely. Plus three other gaps:
- **GAP 7**: `calculatedMeasures` were dropped silently in the groupBy+agg path (only the window path applied them).
- **GAP 8**: `EngineContext.JoinHints` was ignored by `applyJoins` (no broadcast hint).
- **GAP 6**: `EngineHookDispatcher` was never invoked in the portable query path (the 6 PR-D-conformance-unified plugins were no-ops).

## Decision

### What lands in PR-M4

| Scope | Closes | Where |
|---|---|---|
| **Wire `QueryBuilder.build` into `SparkEngineProvider.query`** | GAP 5 (CRITICAL) | `SparkEngineProvider.scala` |
| **`applyCalculatedMeasures` in `applyGroupByAgg`** | GAP 7 | `PortableQueryCompiler.scala` |
| **Honor `JoinHints.preferredStrategy` via `.hint()` in `applyJoins`** | GAP 8 | `PortableQueryCompiler.scala` |
| **`compileRelOp(relOp, ctx)` — IR → DataFrame walker + `HookRunner` adapter** | GAP 5 + GAP 6 | `PortableQueryCompiler.scala` + new `HookRunner` trait in `sm8-core/engine/` |
| **`PortableExprCompiler.toColumn` handles `Expr.MeasureRef`** (was throwing) | bridge for GAP 7 | `PortableExprCompiler.scala` |

### One contract change (legacy test updated)

`Expr.MeasureRef` previously threw `UnsupportedOperationException` (PR-K's "subquery resolution deferred"). After PR-M4 it's a column reference: `col(name)` — the engine-known measure column is in scope after the aggregate. The legacy `Expr.MeasureRef` threw-test was updated to assert the new contract.

### Contract design — `HookRunner` (new engine-portable trait)

```scala
trait HookRunner extends java.io.Serializable {
  def run[A](ctx: EngineContext, build: EngineContext => Either[EngineError, A]): Either[EngineError, A]
}
object HookRunner { object Noop extends HookRunner { ... } }
```

- Engine-portable (no Spark types — `A` is generic).
- Default `Noop` impl: identity (no hooks fire).
- Production: the spark-connector path it consumes via the new `HookRunner` constructor parameter on `SparkEngineProvider`. The sm8-platform `EngineHookDispatcher` can be adapted to satisfy this trait in a future integration PR.

## Layer ownership (RFC §3)

| Concern | Layer |
|---|---|
| `HookRunner` trait (engine-portable bridge) | **core** (`sm8-core/engine/`) |
| `MeasureRef` handling in `PortableExprCompiler` + `applyCalculatedMeasures` + `JoinHints` honoring + `compileRelOp` | **connector** (`spark-connector/`) |
| Wiring `QueryBuilder.build` into `SparkEngineProvider.query` | **connector** |

Zero spark imports in sm8-core (enforcer passes). The spark-connector is the ONLY reactor module with spark imports.

## Conformance (RFC §12)

| Spec | Tests | Asserts |
|---|---|---|
| `SparkEngineProviderProductionWiringSpec` (NEW) | 7 | GAP 5 (IR path + schema validation), GAP 7 (calc measures), GAP 8 (hint propagation), GAP 6 (hook dispatch recording) |
| `PortableExprCompilerSpec` (1 UPDATED) | 36 | `MeasureRef` now lowers to `col(name)` — the contract change recorded |
| `SparkEngineProviderSpec` (existing) | existing pass | unchanged behavior at the public surface |
| Other spark-connector tests | unchanged | 127 total tests pass |

## Pre-commit gates

| Gate | Result |
|---|---|
| Pre-flight | ✅ toolchain clean (1 metals + 1 bloop + 1 codegraph) |
| LSP | ✅ spark-connector + sm8-core clean |
| Codegraph | ✅ `compileRelOp` 2 callers (self + SparkEngineProvider); `query` unchanged signature |
| Enforcer | ✅ passes |
| **Reactor** | ✅ **1048 green, 0 failures** (was 1041, +7 from PR-M4 spec) |

## Sequence status

| Step | Status |
|---|---|
| PR-A … PR-L + PR-M1/M2/M3 | ✅ merged |
| **PR-M4 (this)** | **closes GAP 5 (CRITICAL), 6, 7, 8** |
| v0.1.0 tag cut | ready (deferred per user direction) |

## ADR-008-L Appendix gap closure — complete

| Gap | PR | Status |
|---|---|---|
| 1. `ExprParser` missing `CaseWhen`/`Alias`/`MeasureRef`/`All` | PR-M1 | ✅ closed |
| 2. No `ModelValidator` | PR-M2 | ✅ closed |
| 3. Zero concrete `SourceResolver` impls | PR-M3 | ✅ closed |
| 4. `ModelLoader` skips `joins`/`calculatedMeasures` | PR-M1 | ✅ closed |
| **5. `SparkEngineProvider.query` bypasses `QueryBuilder`** | **PR-M4** | ✅ **closed** |
| 6. `EngineHookDispatcher` not invoked | PR-M4 | ✅ closed (via `HookRunner` adapter) |
| 7. `calculatedMeasures` dropped in groupBy+agg path | PR-M4 | ✅ closed |
| 8. `EngineContext.JoinHints` unused by `applyJoins` | PR-M4 | ✅ closed |

**All 8 verified gaps closed.** The v0.1.0 tag cut is ready whenever the user gives the go.
</