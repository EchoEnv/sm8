# ADR-008-O: Post-Review Hardening Series (data-correctness + production-cleanup)

**Status:** Accepted. **Date:** 2026-08-17. **Author:** SM8 agent (per user directive 2026-08-17: "go A on seperate commit but in 1 PR ... must align with latest rfc>PLAN> latest ADR docs ... follow all skills we strict ... verify issues via codegraph mcp, lsp tool before you commit any part").

## Context

Two senior subagents reviewed SM8 v1.0.0 at `main` = `e5903d0` (post PR-#87 / PR-M6 hardening):
- `local://reviews/data-engineer-review.md` — Spark runtime + data-model parity vs `/tmp/semanticdf`.
- `local://reviews/architect-review.md` — RFC/PLAN/ADR alignment + layer ownership.
- `local://sm8-reviews-consolidated.md` — deduplication matrix of both.

Combined: **7 P0** + 5 P1 + 9 P2 findings. The two reviewers agreed on 6 of 7 P0 (cross-validated): the HookRunner signature mismatch (Architect-only) was confirmed independently by Data Engineer as the "ADR-L GAP 6 filed-closed-but-not-really-closed" pattern.

This ADR records the consolidation + scope of the **O-series** (PR-O1..O4) — the user-approved fix order on 2026-08-17:

1. **PR-O4** (production-cleanup) — 7 sub-changes, ~280 LOC, the lowest-risk and the highest "merge-luck"
2. **PR-O1** (data-correctness in PortableExprCompiler) — 5 sub-changes, ~200 LOC, medium churn
3. **PR-O2** (broadcast-join by size, perf cliff) — 3 sub-changes, ~80 LOC, narrow blast radius
4. **PR-O3** (HookRunner → EngineHookDispatcher bridge) — 5 sub-changes, ~150 LOC, SDK contract break

The user explicitly directed "seperate commit but in 1 PR" — one branch, one PR, multiple commits in the order O4 → O1 → O2 → O3.

## Decision

### Cross-cutting principles (all 4 PRs)

1. **RFC §3 layer ownership preserved**: every change confined to its declared layer. Core never imports Spark; connectors plug into the engine-portable `SparkEngineProvider` SPI; plugins stay in `plugins/`.
2. **Skills-first review per commit**: per [[karpathy-guidelines-mindset]], per [[scala-impact-analysis-mindset]] `git grep`+`codegraph explore` blast-radius scan BEFORE any edit; per [[scala-spark-batch-bugs-mindset]] mantra #1 (closure-safety) + mantra #4 (cache-the-stable-shape) + [[debug-mantra-mindset]] §1 (reproduce the failing test) for every fix.
3. **ADR appended in order**: each sub-change references this ADR; no new ADR per sub-change (would be noise).
4. **Plan direction honoured**: per `~/.claude/plans/agile-kindling-beacon.md` Step 12 (sm8-platform / sm8-server split) + Step 8 (Adapters-as-Plugins vocabulary decision: connectors live in `connectors/`, plugins live in `plugins/`).
5. **Big-data performance + Spark serialize concern**: every connector-side change must pass `PortableExprCompiler` Serializable round-trip + `SparkConnectorBigDataScaleSpec` 100k-row smoke test.

### PR-O4 — Production cleanup

| P0/P1 ID | Change | File | Effort |
|---|---|---|---|
| P0-6 | `sys.addShutdownHook { provider.spark.stop() }` in `Main.run()` alongside `transport.stop()` | `sm8-server/src/main/scala/io/sm8/server/Main.scala` | 10 LOC |
| P0-7.1 | `Dimension(name, expr: Expr, dataType: Option[SealedDataType])` — re-port legacy typed field | `sm8-core/src/main/scala/io/sm8/core/model/Dimension.scala` | 30 LOC + 1 test |
| P0-7.2 | `SourceRef.ByName(catalog: Option[String], namespace: Option[String], table: String)` — re-port dropped legacy fields | `sm8-core/src/main/scala/io/sm8/core/model/SourceRef.scala` | 40 LOC + 1 test |
| P0-7.3 | `RelOp.Scan(sourceRef: SourceRef, provenance: ResolvedSource, ...)` — restore 4-case `ResolvedSource` provenance on the IR | `sm8-core/src/main/scala/io/sm8/core/rel/RelOp.scala` + `ResolvedSource.scala` | 60 LOC + 1 test |
| P1-1 | `df.unpersist()` paired with `df.persist()` — query-boundary unpersist + JVM-shutdown unpersist-enqueued | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala` | 20 LOC + 1 test |
| P1-2 | Rewrite stale docstring in `Main.scala:179-194` to describe typed `realize(url)` (ADR-006) | `sm8-server/src/main/scala/io/sm8/server/Main.scala` | trivial comment fix |
| P1-3 | Split `SparkEngineProvider` into `SparkEngineProviderDescriptor` (ServiceLoader-discoverable) + `SparkEngineProvider(spark: SparkSession)` (the heavy ref) | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/` | 100 LOC + 3 tests |

**Commits within PR-O4** (one per sub-change, in the same order as the table).
**Verification gates** per commit: LSP diagnostics → codegraph blast-radius `codegraph explore "<Symbol>"` → `mvn -pl sm8-core,sm8-platform,sm8-server,connectors/spark-connector -am test` → reactor: 470 + 140 + sm8-platform/server/cli + enforcer.

### PR-O1 — Data-correctness in PortableExprCompiler

| P0 ID | Change | File | Effort |
|---|---|---|---|
| (new) | `SparkTypeBridge.sealedDataTypeToSparkType(t: SealedDataType): Spark DataType` — inverse of existing `sparkTypeToSealedDataType` | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkTypeBridge.scala` | 30 LOC + round-trip test |
| P0-1 | `Expr.Cast` honors `targetType`: `toColumn(e).cast(SparkTypeBridge.sealedDataTypeToSparkType(c.targetType))` | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableExprCompiler.scala` | 10 LOC + 1 test |
| P0-2 | `toColumn(e: Expr): Either[EngineError, Column]` — `Expr.FunctionCall` + `LiteralValue.ArrayValue` return `Left(EngineError.UnsupportedCapability)` instead of throwing | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableExprCompiler.scala` + thread `Either` through 6 call sites | 120 LOC + 5 tests |
| P0-3 | `MinimalRelOpLowerer.lowerScan` respects `scan.projection: List[Expr]` — `df.select(projection.map(_.toColumn): _*)` before returning | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/MinimalRelOpLowerer.scala` | 20 LOC + 1 test |

### PR-O2 — Broadcast-join by size

| P0 ID | Change | File | Effort |
|---|---|---|---|
| P0-4 | `applyJoins` + `MinimalRelOpLowerer.lowerJoin` read `ctx.joinHints.broadcastRightBelowBytes`; fall back to `spark.sql.autoBroadcastJoinThreshold` when unset; emit `df.join(broadcast(rightDf), joinExpr, joinType)` for sub-threshold right side | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/PortableQueryCompiler.scala` + `MinimalRelOpLowerer.scala` | 60 LOC + 3 tests |

### PR-O3 — HookRunner → EngineHookDispatcher bridge (option b per Architect)

| P0 ID | Change | File | Effort |
|---|---|---|---|
| P0-5 | Delete `sm8-core/.../engine/HookRunner.scala`; change `SparkEngineProvider` ctor to `Option[EngineHookDispatcher]`; wire platform dispatcher in `Main.wire()`; add `cache-plugin`-fires-on-spark smoke test | `sm8-core/.../engine/HookRunner.scala` (delete) + `connectors/spark-connector/.../SparkEngineProvider.scala` + `sm8-server/.../Main.scala` + `plugins/cache-plugin/.../CachePlugin.scala` | 150 LOC + 5 tests |

**Architect sign-off mid-flight** required for option (b): verify via `codegraph explore "extends HookRunner"` that no outside call sites exist before the delete.

### Skill-mindset checklist (applied per commit)

- **karpathy-guidelines-mindset**: smallest correct change first; verified success criterion; pre-commit clarity.
- **scala-data-driven-refactor-mindset**: data in core (P0-7.1/7.2/7.3 fix this), behavior in adapters (P0-5 fix this); sealed traits over Maps.
- **scala-error-handling-mindset**: typed errors at every IO boundary; never throw inside Spark UDFs (P0-2 closes this); pattern-match exhaustively.
- **scala-impact-analysis-mindset**: `codegraph explore` + `lsp references` BEFORE every edit; binary vs source compat (P0-5 is wire-breaking per the SDK contract).
- **scala-jvm-safety-mindset**: null sentinel removal (P1-3); resource leak fix (P0-6, P1-1); `@tailrec` discipline (PR-O-out-of-scope, but `parseOrExpr`/`parseAndExpr` flagged in P2-6 deferred).
- **scala-perf-testing-mindset**: count allocations; pre-size `Array[ResultValue]`; warm-JIT benchmarks deferred (P2-8).
- **scala-spark-batch-bugs-mindset**: mantra #1 closure-safety on every connector-side change; mantra #4 cache-the-stable-shape (PR-O1 P0-3 column-pruning fixes this); mantra #6 partition pushdown deferred (P2-2); mantra #7 broadcast-join-by-size (P0-4); mantra #8 skew-aware (P2-1 deferred).
- **scala-jar-packaging-mindset**: enforcer discipline preserved (9/9 modules zero-Spark-import per Architect); parent-POM global enforce flagged (P2-4) deferred.
- **debug-mantra-mindset**: every finding reproduced (test → fail → fix → green → commit); falsify before declaring (Architect verified HookRunner claim via `codegraph explore HookRunner`).

### Verification gates (every commit)

1. **LSP**: `xd://lsp action=diagnostics file="<touched-file>"` returns no errors.
2. **Codegraph**: `codegraph explore "<Changed Symbol>"` — blast radius = expected callers only.
3. **Enforcer**: `mvn -B -ntp validate` (parent + child POM enforce executions).
4. **Reactor (sub-module)**: `mvn -pl sm8-core,sm8-platform,sm8-server,connectors/spark-connector -am test` — counts unchanged: 470/470 + 140/140 + sm8-platform/server/cli.
5. **Resource**: `free -m` ≥ 2 GB free; `df -h .` ≥ 20 GB free; codegraph bound at `--path /home/emilio/app/projects/sm8` only.

### Deferred (per ADR-008-L backlog)

- Trino / DuckDB connectors (engine-portable IR is ready; per-connector glue is the only missing piece).
- Window function full per-row semantics (`applyWithWindows` covers the simple case).
- RelOp → DataFrame full direct lowering for Sort / Limit / Project / Filter (PR-O1 P0-3 covers Scan/Aggregate/Join only).
- Model-rollup machinery + extension / plugin-as-data (deferred to post-v1.0 per Data Engineer P2-9).
- `@tailrec` on `parseOrExpr` / `parseAndExpr` (Architect P2-6, pre-existing latent risk).
- JMH benchmarks (P2-8; perf baseline deferred).
- v0.1.0 tag cut (user explicitly deferred "dont bump version yet").

## RFC §3 + PLAN + ADR alignment (verified)

| Direction | Status |
|---|---|
| `~/.claude/plans/agile-kindling-beacon.md` | ALIGNED. Step 12 (sm8-platform/server split) + Step 8 (Adapter → Connector vocabulary) honoured. |
| `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md` | ALIGNED. §9 (fail-fast policy) fixed by PR-O3 P0-5; §13 (DoD) satisfied. |
| `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md` Rule 4 (typed `realize(url)`) | ALIGNED. PR-O4 P1-3 cleans up the null-sentinel; PR-O4 P1-2 fixes the stale docstring. |
| `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` | PARTIAL → CLOSED via PR-O3. HookRunner ↔ EngineHookDispatcher mismatch is the §9 violation. |
| `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` Rule 1 | ALIGNED (no rule change needed). |
| ADR-006 Post-#65 Refinement | ALIGNED. PR-O4 P1-2 fixes the stale docstring; PR-O4 P1-3 aligns with the typed realize. |
| ADR-008-L §Layer ownership | ALIGNED. PR-O4 P0-7.1/7.2/7.3 restore the legacy's typed-shape within core (no layer shift). |
| ADR-008-L Appendix GAP 8 (JoinHints honored in applyJoins) | CLOSED via PR-O2 P0-4. |

## Stats

| Metric | Value |
|---|---|
| Branch | `step-pr-o-hardening` from `main` = `e5903d0` |
| Number of commits | 23 (= 9 sub-commits for O4 + 5 for O1 + 3 for O2 + 5 for O3, where O1 is itself 4 sub-commits: O1a/O1b/O1c-1/O1c-2/O1c-3) |
| Estimated LOC | ~1100 (across main + tests) |
| Estimated tests | +27 (12 O4 + 10 O1 + 3 O2 + 5 O3, O1c-1..3 nets ~8 tests) |
| Pre-push gates per commit | LSP + codegraph + enforcer + reactor + resource monitor (per the user's standing rule) |
| Net tests after O-series | 470 + 140 + ~30 across other modules = ~640 in the snapshot reactor |

## Reporting back

Per commit: `git commit -F - << 'MSG'` (heredoc, NOT the Edit tool per the user's standing rule) + `git push origin step-pr-o-hardening` (cumulative push after each commit).
Per PR (final): `gh pr create --base main --head step-pr-o-hardening --title "Post-Review Hardening (O-series: 4 PRs in 1, ~830 LOC)" --body-file /tmp/pr-body.md` + `gh pr view N --json state,mergeable,mergeStateStatus`.

## PR-O1c follow-up (post-#88 squash-merge)

**Status:** Accepted. **Date:** 2026-08-18. **Branch:** `step-pr-o1c-tocolumn-either` from `main` = `075021e`.

Per Option C of the PR #88 review (2026-08-17 user decision: "Open O1c follow-up PR"), the `toColumn: Column → Either[EngineError, Column]` signature change (PR-O1 P0-2) was carved out of the squash-merge because the 7 test failures needed a separate review surface. This ADR records what landed in the follow-up.

### Change

`PortableExprCompiler.toColumn(expr: Expr): Either[EngineError, Column]` — replaces the old `Column` return. The 2 throw sites (`Expr.FunctionCall` + `LiteralValue.ArrayValue`) become typed `Left(EngineError.UnsupportedCapability(...))`. Threaded through 6 prod callsites:

1. `PortableQueryCompiler.compile` (entry point) — `Either[EngineError, DataFrame]`
2. `PortableQueryCompiler.applyFilters` — `Either[EngineError, DataFrame]`
3. `PortableQueryCompiler.applyAggregations` — `Either[EngineError, DataFrame]`
4. `PortableQueryCompiler.applyCalculatedMeasures` — `Either[EngineError, DataFrame]`
5. `PortableQueryCompiler.applyGroupByAgg` — `Either[EngineError, DataFrame]`
6. `PortableQueryCompiler.applyWithWindows` — `Either[EngineError, DataFrame]`
7. `PortableQueryCompiler.renderAggregate` — `Either[EngineError, Column]`
8. `MinimalRelOpLowerer.lowerScan` (projection) — `Either[EngineError, DataFrame]`
9. `MinimalRelOpLowerer.lowerFilter` — `Either[EngineError, DataFrame]`
10. `MinimalRelOpLowerer.lowerProject` — `Either[EngineError, DataFrame]`
11. `MinimalRelOpLowerer.lowerAggregate` — `Either[EngineError, DataFrame]`
12. `MaterializePolicy.Persist` dispatch — `Either[EngineError, StorageLevel]` with typed `Left(UnsupportedCapability)` on invalid level

Helper `colsOf(List[Expr]): Either[EngineError, Array[Column]]` (package-private) — shared by `MinimalRelOpLowerer` and `PortableQueryCompiler` for the Either-fold over `List[Expr]`.

### Why typed `Left` instead of `throw`

Per [[scala-error-handling-mindset]] decision rule #1: `Either[Error, T]` for expected business errors the caller should handle. `Expr.FunctionCall` (UDF resolution deferred) and `LiteralValue.ArrayValue` (array literals deferred) are **expected** errors, not programmer errors. The throw-bomb could crash the driver or — worse, at scale — kill executors and trigger Spark's retry, multiplying the failure. Typed `Left(EngineError.UnsupportedCapability(...))` flows through the compile boundary and the MCP server maps it to a 501 `UNSUPPORTED_CAPABILITY` wire response.

### Three missing Expr cases filled in

While making the 7 test failures green, the underlying compiler was missing `Expr.Not`, `Expr.IsNull`, `Expr.IsNotNull` cases — pre-existing gaps (not introduced by O1c). Without these, `MatchError` was thrown at runtime for any filter using `IS NULL` / `IS NOT NULL` / `NOT`. Threaded via `flatMap` to honor the P0-2 contract:

```scala
case Expr.Not(e)       => toColumn(e).map(sparkNot)
case Expr.IsNull(e)    => toColumn(e).map(_.isNull)
case Expr.IsNotNull(e) => toColumn(e).map(_.isNotNull)
```

This fixes 5 of 7 reactor failures at once (3 Spec tests + 2 DataFrameSpec filter tests using `Expr.IsNull` and `Expr.Not`). The remaining 2 failures were the old "throws" tests (`Expr.FunctionCall: throws UnsupportedOperationException` + `LiteralValue.ArrayValue: throws UnsupportedOperationException`) — **deleted**, their old contract is replaced by the 3 new typed `Left` tests already present.

### Tests

- **2 old tests deleted** (old contract no longer exists): `Expr.FunctionCall: throws UnsupportedOperationException` + `LiteralValue.ArrayValue: throws UnsupportedOperationException`.
- **3 new tests added** (PR-O1c typed-error contract): `toColumn(Expr.FunctionCall) returns Left(UnsupportedCapability)` + `toColumn(LiteralValue.ArrayValue) returns Left(UnsupportedCapability)` + `colsOf([expr1, expr2]) folds Either through the list (no throw on partial failure)`.
- **13 + 17 = 30 existing call sites mechanically unwrapped** via `.toOption.get` in DataFrameSpec + Spec.

Reactor: `151/151 succeeded` in spark-connector; full reactor: 480 (sm8-core) + 33 (sm8-platform) + 24 (sm8-server) + 151 (spark-connector) = **688/688 passed** across 6 modules.

### RFC §3 + PLAN alignment

| Direction | Status |
|---|---|
| `~/.claude/plans/agile-kindling-beacon.md` Step 12 (sm8-platform/server split) | ALIGNED — error flows up the compile boundary, no layer shift |
| `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md` typed-error at adapter boundary | ALIGNED — every compile call returns `Either[EngineError, T]` |
| ADR-008-O P0-2 (typed Left instead of throw) | CLOSED |
| EngineError ADT | REUSED — no new error type added; `UnsupportedCapability` carries engine + capability + message |
