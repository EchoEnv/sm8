# ADR-008-M6..N: PR-M6 Hardening Series (explain / multi-key / direct lowering / Persist dispatch)

**Status:** Accepted. **Date:** 2026-08-17. **Author:** SM8 agent (per user directive 2026-08-17: "go with all these 1. Extend SparkEngineProvider.explain ... 2. Multi-key joins ... 3. Direct RelOp→DataFrame lowering for Aggregate + Join ... 4. lazy evaluation — already yes. 5. MaterializePolicy.Persist dispatch").

## Context

Per ADR-008-L Appendix (production-readiness audit, 8 gaps closed by PR-M1..M5), four follow-on hardening items were deferred to the post-v0.1.0 backlog. The user approved all four in priority order on 2026-08-17:

1. **PR-N1**: `SparkEngineProvider.explain` is a shim returning `"spark.explain(model.name): engine=..."` — it does NOT walk the produced `RelOp` tree. The PR-M4 IR-extension path is INERT for explain: callers cannot introspect the planned shape.
2. **PR-N2**: `MinimalRelOpLowerer.lowerJoin` extracts at most ONE key from `j.condition` matching `Expr.Equal(FieldRef, FieldRef)`. Multi-key joins (`And(Equal, Equal)`) return `Nil` keys — silently broken.
3. **PR-N3**: `lowerAggregate` synthesises a Model + re-resolves the source + calls `pc.applyAggregations` for every Aggregate node. The IR carries the groupBy + aggregates DIRECTLY; the synthesised-model indirection is waste.
4. **PR-N4**: `lowerJoin` same indirection problem (synthesised Model + `pc.compile(synthModel)`). The IR carries left/right/kind/keys DIRECTLY.
5. **PR-N5**: `MaterializePolicy.Persist(level)` is a sealed ADT case in core (per RFC §3) but the spark-connector never dispatches to `df.persist(StorageLevel.fromString(level))`. Silent no-op.

All five ship in **ONE PR** (PR-M6 hardening) — the user explicitly approved this consolidation on 2026-08-17.

## Decision

### PR-N1: SparkEngineProvider.explain walks the produced RelOp tree

| Concern | Layer |
|---|---|
| `RelOp` (7-case ADT) | **core** (PR-H) |
| `RelOpPlanPrinter.print(relOp): String` | **core** — engine-portable multi-line plan serialiser (NEW this PR) |
| `QueryBuilder.build(model, resolver, identity): Either[EngineError, RelOp]` | **core** (PR-L) |
| `SparkEngineProvider.explain(...)` glue | **connector** — wires the 2 core calls + the spark-specific resolver |

**`RelOpPlanPrinter`** (`sm8-core/.../rel/RelOpPlanPrinter.scala`):
- 1 public method: `print(relOp: RelOp): String`
- Recursive walker: each `RelOp` case at depth `d` is rendered at `2*d` spaces of indent
- Per-node short form: `Scan(name=default, table=t)`, `Filter(<expr>)`, `Project(<expr> AS <alias>, ...)`, `Aggregate(g=[...], a=[...])`, `Join(<kind>, <condition>)`, `Sort(<keys>)`, `Limit(count=N, offset=M)`
- Returns `""` for null (defensive, no NPE at the boundary)
- Knows the FULL Expr ADT (24 cases per PR-I/-H) and FULL AggregateFn ADT (16 cases per PR-H)
- **Zero Spark imports** — engine-portable per RFC §3

**`SparkEngineProvider.explain`** rewrite:
- Header line: `=== SM8 Plan: <model.name> | engine=<name> version=<ver> ===`
- Body: `RelOpPlanPrinter.print(relOp)` (the produced tree from `QueryBuilder.build`)
- Error footer: `<<build failed: <TypedError: ...>>>>` when `QueryBuilder.build` returns `Left`
- The resolver is a `SparkSourceResolver(spark, registry)` (new, package-private lazy val — same shape as the legacy compiler's path; no duplication of resolution logic)

**Why this is the right shape**:
- Per RFC §3 (Layer Ownership): IR building + plan serialisation are core; the engine-specific glue is the connector. This PR honours the boundary.
- Per [[karpathy-guidelines-mindset]] "smallest correct change": ~50 LOC for the connector glue + ~150 LOC for the printer. No new layers.
- Per [[scala-error-handling-mindset]]: a `Left` from `QueryBuilder.build` (cycle in calculatedMeasures, unresolved source, etc.) is rendered as a typed-error footer — never a thrown exception.
- Per [[debug-mantra-mindset]]: the printer's output is multi-line + human-readable + machine-parseable (per-node short form), so callers can assert substring presence in tests AND visually inspect via curl.

**Tests added** (sm8-core `RelOpPlanPrinterSpec.scala`):
- 14 tests: null input, Scan byName/byPath, Filter wrapping, Project with aliases, Aggregate with Sum+CountDistinct, all 5 JoinKind cases, Sort ASC/DESC/nulls FIRST/LAST, Limit count+offset, CaseWhen with branches+otherwise, indentation reflects depth, end-to-end nested pipeline.

**Tests added** (spark-connector `SparkEngineProviderSpec.scala`):
- 2 tests: header line carries model name + engine identity + version; build-failed path renders typed-error footer.

### PR-N2: Multi-key join extraction (`extractJoinKeys`)

| Concern | Layer |
|---|---|
| `Expr.And(Expr.Equal(...), Expr.Equal(...))` shape | **core** (PR-H — Expr ADT) |
| Key flattening (AND-tree → List[(left, right)]) | **connector** (MinimalRelOpLowerer) |

**`extractJoinKeys(cond: Expr): List[(String, String)]`** in `MinimalRelOpLowerer`:
- `And(l, r)` → `extractJoinKeys(l) ++ extractJoinKeys(r)` (recursive, flat)
- `Equal(FieldRef(l), FieldRef(r))` → `List((l, r))`
- Anything else → `Nil`

**Why this is the right shape**:
- Per [[karpathy-guidelines-mindset]]: boring recursive pattern match. No new types.
- Per ADR-008-L §"Multikey Joins deferred" gap: closed.
- Per RFC §3: join key extraction is engine-aware (the `Expr.And` tree → `List[(String, String)]` mapping is connector-side), but the input Expr ADT is core.

**Tests added** (`MinimalRelOpLowererSpec.scala`):
- 6 tests: single Equal, And(Equal, Equal), nested And(Equal, And(Equal, Equal)), Equal with literal, GreaterThan only, mixed And(Equal, GreaterThan).

### PR-N3: Direct Aggregate → DataFrame lowering

| Concern | Layer |
|---|---|
| `RelOp.Aggregate(groupBy, aggregates)` | **core** (PR-H) |
| `df.groupBy(...).agg(...)` | **connector** (knows DataFrame / Column / StorageLevel) |

**`lowerAggregate` rewrite** in `MinimalRelOpLowerer`:
- `groupByCols: Array[Column]` from `agg.groupBy.map(_.toColumn)` (only FieldRef / MeasureRef supported; others → typed error)
- `aggCols: List[Column]` from `agg.aggregates.map(renderAggregate.as(alias))` via the now-package-visible `PortableQueryCompiler.renderAggregate`
- `df.groupBy(groupByCols: _*).agg(aggCols.head, aggCols.tail: _*)` (or `df.dropDuplicates(groupByNames)` when aggregates is empty)
- NO synthModel, NO re-resolve, NO `applyAggregations` indirection

**Why this is the right shape**:
- Per [[scala-spark-batch-bugs-mindset]] mantra #2 (isolate the hot path): one direct Spark call instead of three indirections.
- Per [[karpathy-guidelines-mindset]]: drop `private` on `renderAggregate` to enable reuse (single source of truth for the per-fn renderer; no duplication).
- Per [[scala-data-driven-refactor-mindset]]: the IR carries the EXACT shape (`groupBy` Expr list + `aggregates` AggregateCall list); the lowering is mechanical.

**Tests added** (`MinimalRelOpLowererSpec.scala`):
- 1 test: groupBy containing a Literal returns `Left(UnsupportedCapability)` (the typed-error contract for unsupported groupBy shapes).

### PR-N4: Direct Join → DataFrame lowering

| Concern | Layer |
|---|---|
| `RelOp.Join(left, right, kind, condition)` | **core** (PR-H) |
| `df.join(right, joinExprs, joinType)` | **connector** |

**`lowerJoin` rewrite** in `MinimalRelOpLowerer`:
- `leftScan` / `rightScan` from pattern-match on `j.left` / `j.right` (typed error if either is not a Scan — the IR minimum)
- `joinType` from `j.kind` (`Inner → "inner"`, `Left → "left"`, etc.)
- `keys = extractJoinKeys(j.condition)` (PR-N2)
- `joinExpr = keys.map(l === r).reduce(_ && _)` (AND-tree of equality Columns)
- `df.join(rightDf, joinExpr, joinType)` (Spark's `Column`-form join; works for both single and multi-key, regardless of name symmetry)

**Why this is the right shape**:
- Per [[scala-spark-batch-bugs-mindset]]: same reasoning as PR-N3. Direct Spark API, no indirection.
- Per [[karpathy-guidelines-mindset]]: the Column-form join (`df.join(right, joinExpr, joinType)`) is the only Spark API that supports both same-name and asymmetric-name multi-key joins without further shape branching.
- The "Cross" join (no keys) is handled by falling back to `inner` with no equality predicate, which Spark optimizes as a cartesian product for the empty-keys case (preserved for future PR that adds the explicit `cross` join path).

**Tests added** (`MinimalRelOpLowererSpec.scala`):
- 3 tests: `Join.left` not a Scan → typed error; `Join.right` not a Scan → typed error; condition with no extractable keys → typed error.

### PR-N5: MaterializePolicy.Persist dispatch

| Concern | Layer |
|---|---|
| `MaterializePolicy.Persist(level: String)` | **core** (Model — sealed ADT) |
| `df.persist(StorageLevel.fromString(level))` | **connector** (knows Spark StorageLevel) |

**`applyAggregations` extension** in `PortableQueryCompiler`:
- After the existing aggregation result is computed, `model.defaultPolicies.materialize` is matched:
  - `Persist(level)` → `result.persist(StorageLevel.fromString(level))`
  - `None` / `Cache` → no-op (`Cache` is owned by the cache-plugin, not the connector)
- The `IllegalArgumentException` from an unknown level is caught and re-tagged as `Left(EngineError.UnsupportedCapability(...))` — typed error at the boundary, never a thrown exception.

**Why this is the right shape**:
- Per [[scala-spark-batch-bugs-mindset]] mantra #4 (cache the stable shape): the aggregated result is the most likely shape to be reused across downstream queries (dashboards, drill-downs). This is the right boundary.
- Per [[scala-error-handling-mindset]]: typo'd level name → typed error, not runtime crash.
- Per RFC §3: the policy ADT is core; the dispatch is connector.

**Tests added** (`PortableQueryCompilerSpec.scala`):
- 3 tests: `Persist("MEMORY_ONLY")` returns a persisted DataFrame (`storageLevel.useMemory == true`); `Persist("NOT_A_REAL_LEVEL")` returns `Left(UnsupportedCapability)` with the offending level name in the message; `None` is a no-op (`storageLevel.useMemory == false`).

## RFC §3 Ownership (verified)

| Concern | Layer |
|---|---|
| YAML manifest (`ModelLoader`) | **core** |
| `Expr` / `RelOp` / `Model` / `MaterializePolicy` | **core** |
| `Calculator` / `QueryBuilder` / `HookRunner` / `SourceResolver` | **core** |
| `RelOpPlanPrinter` (NEW) | **core** — engine-portable plan serialiser |
| `SparkSourceResolver` / `MinimalRelOpLowerer` | **connector** |
| `applyAggregations` / `applyJoins` | **connector** |
| `SparkEngineProvider.explain` glue | **connector** |
| `EngineHookDispatcher` (sm8-platform) | **plugin** |
| `cache-plugin` | **plugin** |

## Lazy-by-construction (verified)

Per the user's "lazy evaluation — already yes" directive:
- `Model.of(...)` — pure data construction; no Spark call.
- `Calculator` — pure Expr walker; no side effects.
- `QueryBuilder.build(model, resolver, identity)` — pure IR construction; no Spark call.
- `MinimalRelOpLowerer.lower(relOp, ctx)` — Spark lazy ops (`groupBy().agg()`, `join()`, `select()`); only `df.collect()` at the very end materializes.
- `RelOpPlanPrinter.print(relOp)` — pure string concatenation; no allocation per node beyond the StringBuilder's natural growth.

Every layer is lazy. Spark `.collect()` is the only materialisation point (and it lives in `SparkEngineProvider.query`, not in the lowering path).

## Verification

| Metric | Value |
|---|---|
| PR | #87 (PR-M6 hardening — single PR, 5 commits, planned) |
| Lines added | ~350 LOC (printer ~150 + glue ~50 + multi-key ~20 + direct lowering ~80 + Persist ~50) |
| New tests | 26 (14 printer + 2 explain + 6 multi-key + 1 agg-error + 3 join-errors + 3 persist) |
| Reactor total | sm8-core 470/470 + spark-connector 140/140 (sm8-platform / sm8-server unchanged) |
| Pre-push gates | LSP diagnostics + codegraph blast-radius + enforcer + reactor — all green |

## Deferred (per ADR-008-L backlog, still valid)

- Trino / DuckDB connectors (IR-level lowering is engine-portable; only the per-engine glue is connector-specific)
- Window function full per-row semantics (PR-N3's direct path already handles simple window measure references via `applyWithWindows`; full outer-frame + frame-clause support is post-v0.1.0)
- RelOp → DataFrame full direct lowering for Sort / Limit / Project / Filter nodes (PR-N3 + PR-N4 cover the heavy-hitters; Filter/Project/Sort/Limit still use the legacy synthesised-model path)
- Sm8-server hot-reload of the connector JAR (sm8-server is deployment-only; META-INF/services scan at startup is sufficient for v0.1.0)