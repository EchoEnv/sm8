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

---

## Appendix: Open Gaps (post-PR-L production-readiness audit, 2026-08-16)

**Status:** Verified against code (not inferred). Recorded before the v0.1.0 tag cut per user directive ("review deeply again if any features we miss yet to run and launch for production grade"). Each gap cites the evidence and the RFC §3 layer that owns the fix.

### GAP 1 — `ExprParser` does not parse the PR-H/PR-I Expr cases (core)

**Evidence:** `sm8-core/src/main/scala/io/sm8/core/expr/ExprParser.scala` contains no grammar rule for `CaseWhen`, `Alias`, `MeasureRef`, or `All` (the only mention is a docstring comment at line 22). A YAML filter or calc expression using `CASE WHEN` fails at parse time with a typed `ExprParseError`.

**RFC §3 layer:** core — the Expr ADT and its parser are engine-portable data + data-shaping.

**Impact:** Users cannot write `CASE WHEN` / alias forms in YAML `filters:` / `calculated_measures:` strings; only programmatic `ModelBuilder` construction can produce them.

### GAP 2 — No `ModelValidator` (core)

**Evidence:** `sm8-core/src/main/scala/io/sm8/core/model/ModelValidator.scala` does not exist. `Model.of` enforces field-level validity (non-empty name, positive version) but performs no cross-reference validation: dimensions/measures/filters may reference fields absent from the source schema; `JoinSpec.keys` may reference columns absent on either side.

**RFC §3 layer:** core — cross-reference validation is engine-portable (the validator reads only the portable `ResolvedSource.Scan.schema`).

**Impact:** Malformed models pass construction and fail at engine-compile time (or produce wrong results) — the worst possible failure point per [[debug-mantra-mindset]] §1.

### GAP 3 — Zero concrete `SourceResolver` implementations in connectors (connector)

**Evidence:** `grep -rln "extends SourceResolver" connectors/` returns nothing. The only implementation is the test fixture `FakeResolver` in `QueryBuilderSpec.scala`. The spark-connector ships no bridge from `SourceRef.ByName("default", "people")` to `ResolvedSource.Scan` with the live `df.schema`.

**RFC §3 layer:** connector — per the Core Boundary table, the adapter "knows about a specific data source"; resolving a source against a live catalog/session is adapter behavior, not core.

**Impact:** `QueryBuilder.build` is unusable in production; the IR path has no on-ramp.

### GAP 4 — `ModelLoader` does not parse `joins` / `calculatedMeasures` from YAML (core)

**Evidence:** `ModelLoader.scala` has `parseDimensions`, `parseMeasures`, `parseFilters`, `parseSource` — no `parseJoins`, no `parseCalculatedMeasures` (grep returns nothing). PR-J added the Model fields; the manifest loader never reads them.

**RFC §3 layer:** core — `ModelLoader` lives in `sm8-core/manifest/`; the manifest format is engine-portable.

**Impact:** YAML authors cannot declare joins or calculated measures; the new Model fields are reachable only via `ModelBuilder` (programmatic). Two faces of the same model.

### GAP 5 — `SparkEngineProvider.query` bypasses `QueryBuilder.build` (connector) — **CRITICAL**

**Evidence:** `SparkEngineProvider.scala` (query body) calls `new PortableQueryCompiler(spark).compile(model, ctx)` directly. `QueryBuilder.build` has zero production callers (codegraph blast-radius in this PR's gates confirmed it and it was filed as a feature, not a blocker).

**RFC §3 layer:** connector — the adapter consumes the portable IR; per `adapters.md` the adapter implements core's contract, which now includes the `Model → RelOp` lowering.

**Impact:** The entire IR-extension path (PR-H/I/J/K/L) is inert in production. The `RelOp` tree, `JoinSpec` lowering, cycle detection, and the portable plan are exercised only by tests. This is the single largest gap: the user's directive ("all of these MUST structure align with RFC based") is honored in types but not in the runtime path.

### GAP 6 — `EngineHookDispatcher` not invoked in the portable compile path (plugin/hook wiring)

**Evidence:** `grep -nE "EngineHookDispatcher|hooks\." connectors/spark-connector/.../PortableQueryCompiler.scala` returns nothing. The reference plugins (audit/cache/materialize/row-cap/skew/broadcast) register hooks against `EngineImpl` (the SDK pipeline), but `SparkEngineProvider.query` never enters that pipeline — it compiles + collects directly.

**RFC §3 layer:** plugin + hook — per `hooks.md` "Where Hooks Live": hooks are defined and registered inside a `Plugin.setup(engine)` call; the engine dispatches them. The gap is the wiring between the portable query path and the hook manager (core-owned dispatch, plugin-owned registrations).

**Impact:** The 6 reference plugins are no-ops for queries served by `SparkEngineProvider`; they fire only on the legacy `EngineService` path in `sm8-platform`.

### GAP 7 — `calculatedMeasures` dropped in the groupBy+agg path (connector)

**Evidence:** `PortableQueryCompiler.applyGroupByAgg` maps `model.measures` into `agg(...)` columns and stops — no `withColumn` for `model.calculatedMeasures`. Only `applyWithWindows` (the `Expr.All` path) applies them.

**RFC §3 layer:** connector — the calc-measure rendering (Spark `withColumn`) is adapter behavior.

**Impact:** A model with calculated measures that do NOT reference `Expr.All` silently omits every calculated measure from the result (silent-incompleteness — violates ADR-008-H's "never a silent no-op" spirit even though no error is swallowed).

### GAP 8 — `EngineContext.JoinHints` unused by `applyJoins` (connector)

**Evidence:** `applyJoins(df, joins)` takes no `ctx` parameter; `JoinHints` (Broadcast / ShuffleHash / SortMerge) is read by nothing in the spark-connector (grep finds only the definition + its spec).

**RFC §3 layer:** connector — per `adapters.md`, the adapter decides engine-specific strategy; join-strategy hints are exactly that.

**Impact:** Perf miss, not correctness — every join uses Spark's default strategy (no explicit `broadcast(...)` hint).

### Remediation sequence (pre-tag hardening, proposed)

| PR | Scope | Layer | Closes |
|---|---|---|---|
| PR-M1 | `ExprParser` grammar for `CaseWhen` / `Alias` / `MeasureRef` / `All` + `ModelLoader` parsing for `joins` / `calculatedMeasures` | core | GAP 1, 4 |
| PR-M2 | `ModelValidator` (cross-reference validation against `ResolvedSource.Scan.schema`) | core | GAP 2 |
| PR-M3 | `SparkSourceResolver` (concrete `SourceResolver` bridging `spark.table` → `ResolvedSource.Scan`) | connector | GAP 3 |
| PR-M4 | Wire `QueryBuilder.build` into `SparkEngineProvider.query`; apply `calculatedMeasures` in `applyGroupByAgg`; honor `JoinHints` in `applyJoins`; hook dispatch around the portable compile | connector + hook wiring | GAP 5, 6, 7, 8 |

Each PR is independently mergeable; sequence is gated (green before next). None bumps the version (per user directive: no tag yet).
