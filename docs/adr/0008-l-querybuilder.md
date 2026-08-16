# ADR-008-L: `QueryBuilder` — Model → RelOp lowering (PR-L)

**Status:** Accepted. **Date:** 2026-08-16. **Author:** SM8 agent (final IR-extension PR before the v0.1.0 tag cut).

## Context and Problem Statement

After PR-H (rel/ IR), PR-I (CaseWhen + Alias), PR-J (typed Model + JoinSpec + CalculatedMeasure), PR-K (Spark compile of joins + aggregates), the consumer path was still incomplete:

```
Model (typed data) -- ??? --> RelOp (portable tree) -- ??? --> engine-native plan
                         ^                                  ^
                         PR-L                              PR-K (spark-connector)
```

Without PR-L:
- The MCP server / engine adapters had no way to materialize a `RelOp` plan from a `Model` declaration.
- The `Model → RelOp` lowering is engine-portable (every engine wants the same tree) but no module owned it.
- `model.joins` + `model.calculatedMeasures` had no place to live in a `RelOp` tree (PR-K's spark compile consumed them inline; without QueryBuilder, no consumer can build the IR in the first place).

## Decision

### What lands in PR-L

1. **`io.sm8.core.engine.SourceResolver`** — the trait that translates a portable `SourceRef` into a typed `ResolvedSource` (the boundary step BEFORE the engine sees anything). Per legacy `core/engine/SourceResolver.scala`. Carries `ResolvedSource` ADT (Scan / NotFound / Incompatible / AuthFailed — typed failures, not exceptions). Default `resolveModel` returns typed `UnsupportedCapability` (model-by-name registry is deployment-specific).

2. **`io.sm8.core.query.QueryBuilder`** — the `Model → RelOp` lowering object. Per legacy `core/query/QueryBuilder.scala`, but with the **FULL** shape (joins + aggregate + sort + limit + cycle detection) — not the legacy's v1 single-source scope. The build pipeline:

   ```
   1. resolveSource(model.source)           → Scan
   2. resolveJoinSources(model.joins)       → List[(JoinSpec, Scan)]    (each join = its own resolver call)
   3. detectCalcCycles(model.calculatedMeasures) → Unit (typed error on cycle)
   4. assembleRelOp(model, primary, joinScans) → RelOp tree:
        Scan_1 � Scan_2 ⊕ ... ⊕ Scan_n     (joined via RelOp.Join)
          → Filter chain (foldLeft over model.filters)
          → Project (dimensions + measures + calculated measures)
            (or → Aggregate → Project when measures exist)
          → Sort(empty keys — v0.1.0 deferred to engine adapter)
          → Limit(Long.MaxValue — v0.1.0 deferred to engine adapter)
   ```

3. **Cycle detection** in the `CalculatedMeasure` DAG (per ADR-007 §PR-J promise + the user's directive to detect cycles at model-load time). Iterative DFS with WHITE/GRAY/BLACK coloring; back-edge to GRAY → typed `EngineError.UnsupportedCapability` with the cycle path in the message.

4. **10 conformance tests** (`QueryBuilderSpec`) covering: single-source Scan; dims+measures → Aggregate + Project; calculated measures as `Expr.Alias`; filter chain foldLeft order; one join → Scan_1 → Join → Scan_2; multiple joins fold left-to-right; source NotFound → typed FeatureDeferred; calc cycle → typed UnsupportedCapability; self-cycle; FeatureDeferred tagged with model name.

### Two contract decisions beyond the legacy

1. **`Sort` + `Limit` are pass-through envelopes in v0.1.0.** Empty `keys` for Sort, `Long.MaxValue` for Limit. Per [[karpathy-guidelines-mindset]] "smallest correct core" + the deferred-list convention: the portable IR shape stays (every engine may add sort/limit at its adapter level via `preview(n)` / `count()`). The tree still has Sort + Limit nodes; the engine adapter decides what to do.

2. **Cycle detection runs in QueryBuilder (build time), not in the engine adapter.** The legacy `SemanticTable` deferred cycle detection to query time. Per [[debug-mantra-mindset]] §1 (reproduce → fix → verify): cycles are a **build-time invariant violation** that should fail-loud at model construction. The cycle detection lives in QueryBuilder (PR-L), not in `PortableQueryCompiler` (PR-K).

### Layer ownership (RFC §3)

| Concern | Layer |
|---|---|
| `SourceRef` (input data type) | **core** (`sm8-core/model/`) — PR-J |
| `ResolvedSource` ADT (output data type) | **core** (`sm8-core/engine/SourceResolver.scala`) — PR-L |
| `SourceResolver` trait | **core** (the contract; implementations are connector/deployment-specific) — PR-L |
| `Model → RelOp` lowering | **core** (`sm8-core/query/QueryBuilder.scala`) — PR-L |
| `RelOp → DataFrame` lowering | **connector** (`spark-connector/PortableQueryCompiler.scala`) — PR-K |

## Conformance (RFC §12)

10 data-shape tests assert the lowered tree structure (cases, field refs, join kinds, cycle detection). Zero spark imports — the lowering is pure data.

## Pre-commit gates

| Gate | Result |
|---|---|
| LSP diagnostics | ✅ SourceResolver + QueryBuilder + spec clean |
| Codegraph blast-radius | ✅ zero prod callers today (QueryBuilder is a foundation for future MCP-server wiring) |
| Maven Enforcer | ✅ passes |
| Reactor | ✅ **986 green, 0 failures** (was 976, +10) |

## Sequence status

| PR | Status |
|---|---|
| PR-A … PR-F | ✅ merged |
| PR-H (#75) rel/ IR package | ✅ merged |
| PR-I (#76) CaseWhen + Alias | ✅ merged |
| PR-J (#77) Model extensions | ✅ merged |
| PR-K (#78) Spark compile (joins + aggregations) | ✅ merged |
| **PR-L (this)** | **QueryBuilder (Model → RelOp)** |
| PR-M v0.1.0 tag cut | deferred per user direction |
