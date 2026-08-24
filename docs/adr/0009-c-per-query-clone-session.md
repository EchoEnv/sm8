# ADR-009-c: Per-session-deployment follow-up — per-query `newSession()` so `JoinHints.skewFactor` binds per query

| Field | Value |
| **Status** | **Implemented — v0.5-r1 merged as PR-171 (`0466841`); the per-query skew factor binding is live in production; 9 re-point sites all bind `querySession`, `compileModelToDataFrame` carries the new signature, `seedSkewFactor` + `copyTempViews` helpers are on the companion, and 3 falsifiable v0.5-r1 tests drive the real provider path |
| **Date** | 2026-08-24 |
| **Module** | `connectors/spark-connector` (`SparkEngineProvider.query` + `compileModelToDataFrame` + `explain` — per-query `newSession()` forking a fresh `SessionState`; the broadcast + skew seeds both move to the per-query session) + `sm8-core` (`JoinHints.skewFactor` seam, unchanged) |
| **Supersedes scope** | the explicit future per-session-deployment follow-up named in ADR-009-b v0.4; RFC §2 'Feeding broadcast/skew' — the skew half, completing the row |
| **Implementation evidence** | PR-171 (`0466841`) squash-merged on `main`; the v0.5-r1 review fixes commit `e7eee1f`; 1013 tests pass, 0 failed; spark-connector module 211/0 (3 falsifiable v0.5-r1 tests: TL-reuse Some(f), TL-reuse None, fresh-session production path via `copyTempViews`); architect + data-eng dual review both APPROVE WITH NITS → NITS applied in `e7eee1f` (P1 TL-clear fix, P2 `estimatedRows` precondition in `seedSkewFactor`, P2 3rd v0.5-r1 test exercising the production fresh-session path, P3 scaladoc ADR-prefix cleanup) |
| **Skill alignment** | scala-spark-batch-bugs, scala-impact-analysis, scala-bug-hunting, scala-error-handling, scala-jvm-safety, scala-perf-testing, scala-data-driven-refactor, karpathy-app-design, karpathy-guidelines, scala2-scaladoc, debug-mantra |
1. **Per-query `newSession()`** in `query()`: `val querySession = spark.newSession()`. Shares the `SparkContext` (heavy cluster connection preserved, no master reconnect). Forks a fresh `SessionState` with its own `SQLConf` — the **SQLConf map is NOT shared** (so per-query `conf.set` is race-free; concurrent queries each own their conf).
2. **Honest inheritance note (bytecode-verified, v0.3 correction)**: `SparkSession.newSession()` ctor calls `BaseSessionStateBuilder.conf`, which runs `mergeSparkConf(conf, session.sharedState.conf)` and `mergeNonStaticSQLConfigs(conf, sparkContext.conf.getAll.toMap)`. So every `spark.sql.*` value in the SHARED `SparkContext`'s `SparkConf` (i.e. `spark-submit --conf`, builder `.config()` at `getOrCreate`, or any prior `.config()` on the base session) **flows into every per-query session**. Only runtime `baseSession.conf.set(...)` is NOT inherited. The fresh session's per-query `conf.set` is **race-free and authoritative** for that query; the None branch **inherits the shared SparkConf value** (not "Spark default 5.0" — the default applies only if the shared conf has no value either).
3. **Skew seed (single conditional)**: `ctx.joinHints.skewFactor is Some(f)` → `querySession.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", f.toDouble)`. `None` → leave the fresh session at the shared-SparkConf value (or the static 5.0 default). The "Some wins" precedence is single-conditional and unambiguous; the v0.1/v0.2 self-contradiction is gone.
4. **Re-point the EXHAUSTIVE list of 9 base-spark construction sites** in `query()` + `compileModelToDataFrame` + the `explain()` lazy resolver (4 in `query()`, 4 in `compileModelToDataFrame`, 1 lazy in `explain()`). The ctor param stays `val spark: SparkSession`; no rename.
5. **Lifecycle — DO NOT call `querySession.stop()`**: `SparkSession.stop()` → `sparkContext().stop()` (bytecode-verified), which would tear down the shared `SparkContext` the base session AND all clones run on. Drop the reference; the `SessionState` is GC-reclaimable.
6. **Closure capture — corrected**: the rewritten `compileSteps` thunk DOES capture `querySession` (a `SparkSession`, local to `query()`, Serializable + driver-side — safe). The v0.2 "no SparkSession captured" was false; v0.3 states it honestly so the lifecycle review can correctly judge the "never call stop" invariant.
7. **The broadcast seed (ADR-009-a) also moves to the per-query session** — the byte-gate precedence simplifies to the same single conditional; the existing test set is reused.
8. **Hook-runner lifetime unchanged** (constructed once at `realize`); the per-query `querySession` flows through the rewrite (point 4). The runner captures `engineName` and `model` per-query — does NOT capture the base `spark`.
9. **Trino**: no Spark → no change. The portable `JoinHints.skewFactor` seam preserved byte-identical.

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 | 2026-08-24 | `cloneSession()` + ctor rename `spark→baseSession` + per-query stop (REJECTED: shares `SparkContext`+`SQLConf`; rename breaks 5+ named-arg callers; stop tears down cluster) |
| v0.2 | 2026-08-24 | `newSession()`; keep ctor `spark`; do NOT stop (REJECTED: falsified the no-inheritance claim — shared `SparkConf` IS inherited; explain-path re-pointing was incomplete; closure-capture claim was wrong) |
| v0.3 | 2026-08-24 | `newSession()` + honest inheritance note + complete re-point (8 sites) + correct closure-capture claim; spark-submit/builder-runtime conf propagation documented; per-query `conf.set` is authoritative for that query only |
| v0.4 | 2026-08-24 | Re-point enumeration corrected to 9 sites (all 4 in `query()` + 4 in `compileModelToDataFrame` + 1 lazy in `explain()`); added Decision 4b (null-safety for the explain path); dropped the contradictory `prior .config() on the base session` parentheticals (SparkSession 3.5 exposes no `.config()`); the v0.3 fixes (honest inheritance + correct closure-capture) are kept |
| v0.5 | 2026-08-24 | Scoped the `compileModelToDataFrame` re-point to a signature change (add `querySession: SparkSession` parameter, threaded from `explain()`); added Decision 4c (the smallest correct refactor that honors items 5-8); removed the stale `:509-510` comment claiming `query()` shares the helper. The v0.4 corrections (9-site enumeration, null-safety 4b, inheritance parentheticals) are kept |
| v0.5-r1 (Implementation, PR-170 → PR-171) | 2026-08-24 | PR-170 shipped a dead-store `val querySession = spark.newSession()` (never consumed) — both reviewers independently caught it. v0.5-r1 re-points all 9 sites to `querySession` (4 in `query()` compileSteps, 4 in `compileModelToDataFrame`, 1 lazy in `explain()`); adds `seedSkewFactor` companion (writes AQE factor on per-query session when `Some(f)`); adds `copyTempViews` companion (re-registers base session's temp views on the per-query clone so `SourceRef.ByName.resolve` hits); adds `querySessionTL` `ThreadLocal[SparkSession]` seam (`private[spark]`, `@transient`, with `withQuerySessionTL()` / `clearQuerySessionTL()` accessors) + `lastQuerySessionTL` post-query seam for tests reading conf after `query()` returns; thread-safety: the TL is cleared in `query()`'s finally ONLY when this call created the per-query session (tests pre-populating the TL own their lifecycle). 3 falsifiable tests in `SparkBroadcastSeedSpec`: TL-reuse Some(f) (per-query conf == seeded value, base session untouched), TL-reuse None (no `.conf.set`, per-query conf at inherited default), fresh-session production path (no TL pre-set → exercises `copyTempViews` + ByName resolve via the fresh `spark.newSession()`). Architect + data-eng dual review: P1 TL-clear (F1) fixed in `e7eee1f`; P2 `estimatedRows` precondition in `seedSkewFactor` (F2); P2 3rd production-path test (F3); P3 scaladoc ADR-prefix cleanup (F4). PR-171 squash-merged as `0466841`. |
| Status | 2026-08-24 | Status promoted to **Implemented**. The per-query skew factor binding is live in production. The 4 explicit decisions (per-query `newSession()`, honest inheritance, single-conditional seed, exhaustive 9-site re-point + signature change) are all observable in code. 1013 tests pass, 0 failed. No open follow-ups in this ADR; the next wave (ADR-009-d or v0.6) can address the named `copyTempViews` 3-catalog-resolution coverage (data-eng F3 sub-finding — currently only SQL-backed views are tested directly; DataFrame-backed + cross-database views exercise the path indirectly via the existing spark-connector specs). |
---

## Context

### Verified facts (4 dual review rounds, jar bytecode)

- `SparkSession.stop()` → `sparkContext().stop()` (shared context torn down if called on a session that shares the `SparkContext`).
- `SparkSession.newSession()` ctor: `(sparkContext(), Some(sharedState), None, extensions, initialSessionOptions)`. With `parentSessionState = None` → `instantiateSessionState` → `BaseSessionStateBuilder.build` → `conf$lzycompute` runs `$anonfun$conf$2`: `new SQLConf()` then `mergeSparkConf(conf, session.sharedState.conf)` AND `mergeNonStaticSQLConfigs(conf, sparkContext.conf.getAll.toMap)`. **Both merge flows run** — the fresh session IS seeded from the shared `SparkContext`'s `SparkConf`.
- `cloneSession()` reuses `Some(this.sessionState())` — the `SQLConf` map is shared.
- `OptimizeSkewedJoin.getSkewThreshold(median)` reads `SKEW_JOIN_SKEWED_PARTITION_FACTOR` via `SQLConf.getConf` at rule-application (execution) time.
- The provider holds ONE `SparkSession` for its lifetime (`val spark: SparkSession`, ctor :49-58); concurrent queries are normal.
- The 5+ named-arg callers all pass `spark = <Session>` by name (verified): `SparkEngineProviderDescriptor.scala:45,60`; `SparkEngineProvider.scala:82`; plus tests.

### What v0.1 / v0.2 got wrong (4 rounds of reviewer evidence)

- v0.1: `cloneSession()` shares `SparkContext`+`SQLConf`; per-query stop tears down the cluster; ctor rename breaks named-arg callers.
- v0.2: still claimed "fresh session does NOT inherit conf" — bytecode-verified false (the merge flows seed it from the shared `SparkConf`); the re-point list was 4, not exhaustive (explain path has its own `lazy val resolver` at `:600-602` + `compileModelToDataFrame` was not in the plan); closure-capture claim was wrong.

## Decision

### 1. Per-query `newSession()` + honest inheritance (corrected)

`val querySession = spark.newSession()` per query. Shares the `SparkContext` (no master reconnect), forks a fresh `SessionState` (own `SQLConf` map). The fresh session's conf IS seeded from the shared `SparkConf` (spark-submit --conf, builder .config, or prior runtime `.config()` on the base session) — this is the only way the inheritance flows. Runtime `baseSession.conf.set` after session creation is NOT inherited.

### 2. Skew seed (single conditional — v0.2 self-contradiction gone by construction)

- `ctx.joinHints.skewFactor = Some(f)` → `querySession.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", f.toDouble)`. Race-free + authoritative for that query.
- `None` → leave at the inherited-from-`SparkConf` value (or static 5.0 default if absent there). The "Some wins" precedence is single-conditional — no v0.1/v0.2 ambiguity.

### 3. Re-point the EXHAUSTIVE list of 9 sites (all 4 in `query()` + 4 in `compileModelToDataFrame` + 1 lazy in `explain()`)

   In `query()`'s `compileSteps` thunk:
   1. `SparkEngineProvider.scala:189` — `new SparkSourceResolver(querySession, ...)`
   2. `:204` — `SparkEngineProvider.seedBroadcastThreshold(querySession, eCtx, model)`
   3. `:236` — `new PortableQueryCompiler(querySession).compileRelOp(model, relOp, effectiveCtx, scan, Some(preFilteredDf))`
   4. `:242` — `TypedQueryCompiler(querySession).apply(df0, request, effectiveCtx, Some(preFilteredDf))`

   In `compileModelToDataFrame` (signature is `(model, request, ctx)` — NO session parameter today; the only caller is `explain()` at `:515`):
   5. `:548` — `new SparkSourceResolver(querySession, ...)`
   6. `:592` — `SparkEngineProvider.seedBroadcastThreshold(querySession, ctx, model)`
   7. `:593` — `new PortableQueryCompiler(querySession).compileRelOp(model, relOp, seedCtx, scan, Some(preFilteredDf))`
   8. `:594` — `TypedQueryCompiler(querySession).apply(df0, request, ctx)`

   For these 4 sites to actually be `querySession`, `compileModelToDataFrame` must be updated to take a `querySession: SparkSession` parameter, threaded from `explain()`. The new `explain()` body acquires the per-query clone (option b, kept under the null-safety guard of 4b) and passes it down.`

   In `explain()`'s path (the `private lazy val resolver` at `:600-602`):
   9. `:602` — `new SparkSourceResolver(querySession, registry)` — see Decision 4b for null-safety.

### 4b. Explain null-safety (why option (a) is unsafe, option (b) recommended)

Option (a) — `explain()` acquires its own `spark.newSession()` — would NPE on the supported `null-spark` provider configuration: the lazy `resolver` is forced by `QueryBuilder.build` at the top of `explain()`, BEFORE the `spark match { case null => ... }` guard; with `SparkEngineProvider(null, ...)` (exercised by `SparkEngineProviderSpec:100,119` and `SparkEngineProviderExplainSpec:85,102,112`), `spark.newSession()` throws at lazy-val init. Implement #9 with a null-safety guard: `if (spark != null) spark.newSession() else null` (the lazy resolver holds `Option[SparkSourceResolver]` so the `None` branch can short-circuit in `QueryBuilder.build`; or the implementer can re-introduce the base fallback for null). The recommended path in the ADR text is **option (b)**: keep the IR-only `new SparkSourceResolver(spark, registry)` on the base for `explain()` (no clone) — `explain()` is the smoke-compile path, not an actual query, so per-query isolation is unnecessary there and the null-safety contract is preserved.



### 4c. compileModelToDataFrame signature change (required for items 5-8)

`compileModelToDataFrame`'s current signature `(model: Model, request: QueryRequest, ctx: EngineContext)` does not carry a session. To honor items 5-8, add a `querySession: SparkSession` parameter, threaded from `explain()`. The signature change is the smallest correct refactor: explain acquires the per-query session in its own scope (option b, null-safe per 4b), passes it to compileModelToDataFrame, and the helper uses it for the four sites. The pre-call comment at `:509-510` claiming "query() shares this helper" is stale and must be removed (explain() is the sole caller).### 4. Lifecycle — drop the reference; never call stop### 4. Lifecycle — drop the reference; never call stop

`querySession` is a local in `query()`; DataFrame ops use it; `compileSteps` completes; the reference goes out of scope; `SessionState` is GC'd. **`querySession.stop()` would route to `sparkContext().stop()` (bytecode-verified) and tear down the cluster** — the v0.1 fatal correction stands.

### 5. Closure capture — corrected (v0.3 honest)

The rewritten `compileSteps` thunk captures `querySession` (a `SparkSession`, local to `query()`). SparkSession is `Serializable`; the runner is driver-side; the capture is safe. v0.2's "no SparkSession captured" was false; v0.3 states this honestly so the lifecycle review can correctly judge the "never call stop" invariant against `querySession`'s actual reachability.

### 6. Tests (falsifiable — the v0.1-missing cases added in v0.2, here restated with the v0.3 honest inheritance)

- `Some(3)` → `querySession.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor")` = `"3.0"`.
- `Some(3)` then `Some(8)` (sequential) → the second query's clone gets `8.0`; the first is closed.
- (a) `Some(3)` then `None` → the second query's clone is `5.0` (static default) only if the shared `SparkConf` has no value; if `--conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=7.0` is set at realize, the None query gets `7.0`. (The v0.2 test expected `5.0` always; v0.3 corrects to either "5.0 if no shared conf value" OR "the shared conf value if set". The test fixture must NOT pre-set the conf via builder .config() at getOrCreate.)
- (b) Concurrent `Some(3)` and `Some(8)` → each clone's `conf.get` returns its own value (no shared mutable map).

## Consequences

- **Positive**: per-query factor binding is real; the SQLConf map is per-query (race-free); concurrent queries isolated; broadcast seed also benefits; `JoinHints.skewFactor` seam finally alive; portable (Trino unchanged).
- **Negative**: shared-`SparkConf` propagation documented (not a bug — it's the Spark feature); falsifiable tests must avoid pre-setting the conf via builder .config() (only runtime conf.set on the base is NOT inherited).
- **Risk (named)**: forgetting to never call `querySession.stop()`; the implementer must review the rewritten code for absence of any `.stop()`/`.close()` on the per-query session.
- **Trino**: unchanged.

## Alternatives considered

- `cloneSession()` — rejected (v0.1 reviews).
- `newSession()` with `.stop()` — rejected (F1 of v0.1 still applies).
- Rename ctor param `spark → baseSession` — rejected (v0.1 F1).
- Nothing — leaves the skew half deferred (ADR-009-b v0.4).
- Fresh `SparkContext` per query — rejected: heavy, defeats the round-9 perf cliff.

## References

- ADR-009-a v0.1 (broadcast seed, mirror shape); ADR-009-b v0.4 (deferral, names this ADR)
- `SparkEngineProvider.scala:49–58` (ctor), `:164–260` (query), `:515–525` (explain), `:544–547` (compileModelToDataFrame), `:600–602` (lazy explain resolver)
- spark-3.5.8 `SparkSession.newSession()` / `stop()` / `BaseSessionStateBuilder.conf` bytecode (verified across 5 review rounds)