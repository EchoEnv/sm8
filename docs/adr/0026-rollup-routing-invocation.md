# ADR-0026: Rollup routing invocation (the missing fold site)

**Status:** Proposed.
**Date:** 2026-09-07.

## Context

Three shipped PRs (`#345` staleness gate, `#346` grain bucketing + coarsening routing, `#347` refusal observability) put the full machinery in place:

- `RollupRewriter.rewrite(plan, model, requestGrain)` exists in core and returns a sealed `RollupRewriteResult` (`Rewritten(plan, name)` or `Unchanged(reason)`).
- The refusal ADT distinguishes recoverable from permanent refusals (`isPermanent`).
- `MetricsSink.recordRollupRewrite` / `recordRollupRefusal(reason)` and `QueryMetrics.snapshot().rollup` wire the counter surface.
- The Prometheus exporter and the `rollup-refusal-observer` plugin surface those counters to operators.
- The connector `lowerScan` already handles a `RelOp.Scan` whose `sourceRef` is `SourceRef.ByName("sales__by_region")` — it reads the rollup table, applies projection, and gates against `RollupSchemaStale`. **No connector change is needed** to serve a rewritten plan.

What is missing: a production call site. `RollupRewriter.rewrite()` has zero production callers today (verified in the `#347` impact analysis). Until the rewriter is invoked, every query falls back to the base table silently — no rewrite, no refusal, no telemetry despite the surface being shipped.

This ADR lands the single fold site that completes the surface end-to-end. It is the smallest change that turns the three shipped PRs into actual query-time rollup serving.

## Decision

**Single fold site**, engine-portable: a new method `EngineProvider.compileSteps(...)` (or an inline block at the existing `SparkEngineProvider.query` site, depending on engine) calls `RollupRewriter.rewrite(relOp, model, request.timeGrain)` immediately after `QueryBuilder.build` and before the connector-specific compile step.

**Why a fold site (not a hook)**:
- ADR-010-a hooks fire at stage boundaries (Pre/Post-Execute). Routing is *part of* compilation, not a side observation of it. A hook would see the already-built plan; we'd need to set `ctx.stop = true` and substitute the rewritten plan, which is exactly the routing decision this PR owns.
- A hook also requires every operator to opt in to the routing-plugin — a regression from the current fail-open base-path default. The fold site is unconditional: if the rewriter says "rewritten", we route; if "unchanged", we keep the base path. No opt-in, no surprise.

**Why the connector site (not the platform EngineService fold)**:
- `EngineService.runQueryWithHooks` lives in the platform and is engine-portable. The rewriter operates on a `RelOp` plan — but the **plan only exists after `QueryBuilder.build`**, which is called by the engine provider inside its `compileSteps` thunk. Threading the rewritten plan back through the platform would require either a new channel on `EngineHookRequest` (turning an HTTP-ish DTO into a rewriter input) or a rewrite callback the provider registers — both are heavier than the natural fold inside the provider's existing thunk.
- The platform CAN observe refusals (`MetricsRegistry.sink().recordRollupRefusal(reason)`) at any seam; the routing decision itself stays in the engine provider where the plan is materialized.

**Per-engine contract (Spark first; Trino/DuckDB follow when their materialization lands)**:
- `SparkEngineProvider.query` wraps the existing `relOp <- QueryBuilder.build(...)` line with `rewrite(relOp, model, request.timeGrain)`. The match on `RollupRewriteResult` records the outcome via `MetricsRegistry.sink()` and feeds the `Right(plan)` side onward to `compileRelOp`.
- The connector-side code is **unchanged**: `lowerScan` already handles `SourceRef.ByName("sales__by_region")`. The only difference is the `sourceRef.table` name, which the rewriter rewrites from `sales_base` → `sales__by_region` (or whichever `RollupSpec` matched).
- Trino / DuckDB rollup invocation lands with their materialization work (a separate ADR).

**Refusal-recording contract**:
- `Rewritten(plan, name)` → `MetricsRegistry.sink().recordRollupRewrite()`.
- `Unchanged(reason)` → `MetricsRegistry.sink().recordRollupRefusal(reason)`. The reason routes to the per-reason counter keyed by `reasonName(reason)`.
- Both calls are no-ops when no sink is registered (the `MetricsSink` default methods are no-ops by contract). Zero overhead in tests.

**Fail-open semantics**: the rewriter returns `Unchanged` (the ORIGINAL plan instance) on every refusal. The provider feeds `Right(relOp)` onward — the same code path as before. The query behaves exactly as today when no rollup matches. Existing e2e specs do not change.

## Layer discipline (RFC §3)

- **Core is unchanged.** The fold happens outside core; the rewriter is invoked, not modified.
- **Adapter does NOT import a plugin impl class.** `RollupRewriter` is in `io.sm8.core.rel` (core); `MetricsRegistry` is in `io.sm8.core.cache` (core); both are SDK-level seams.
- **Platform does NOT import a connector.** The platform records telemetry via the `MetricsRegistry` seam — same as the cache plugin already does. The Spark provider is where the routing decision lives, because that's where the plan is materialized.
- **Hooks fire via HookRunnerOrchestration** (per RULE#1): routing is a fold, not a hook. Hooks observe the rolled-out plan (refusal telemetry flows through `MetricsRegistry` to the existing `rollup-refusal-observer` plugin).

## Alternatives considered

1. **Built-in routing hook.** Rejected: forces every operator to opt in; would surprise existing deployments; hooks can't substitute plans, only set `ctx.stop`, which is the wrong primitive for "swap this scan for a cheaper one".
2. **Platform EngineService fold.** Rejected: would require threading the rewritten plan through `EngineHookRequest` (turning an HTTP DTO into a rewriter carrier) or a rewrite callback. Heavier than the natural fold in the provider's existing thunk.
3. **A new "routing engine"** that wraps `QueryBuilder` and `RollupRewriter`. Rejected: adds a class for no behavioral gain; the existing `compileSteps` thunk is the seam.

## File-level change list

- `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala` — wrap `QueryBuilder.build(...)` with `RollupRewriter.rewrite(...)` + telemetry recording (~25 LOC).
- `connectors/spark-connector/src/test/scala/io/sm8/connectors/spark/RollupRoutingInvocationSpec.scala` (new) — end-to-end parity test: routed ≡ base path for Sum/Count/Avg/Min/Max; coarsening + algebraic coverage; refusal telemetry verified via `QueryMetrics.snapshot().rollup`.
- This ADR.

## Tests

- **Routed path produces identical results to the base path.** A query at the rollup's grain returns the same rows as the equivalent query against the base table, byte-for-byte. (Existing `RollupMaterializerSpec` parity cases become live at query time.)
- **Coarsening path.** A query at a coarser grain than the rollup returns the same rows as a query against an explicit coarser-grain rollup (or against the base path with a manual `date_trunc`).
- **Refusal telemetry.** Each refusal reason increments the right per-reason counter; `recordRollupRewrite` increments the rewrite counter; `RollupCountersSnapshot.rewrites/refusals/refusalsByReason` matches.
- **Fail-open.** An unrecognized-grain request, a non-ByName source, an unsupported aggregate (e.g. Stddev via algebraic state not yet wired), etc. — all return `Unchanged(reason)` and the query completes via the base path unchanged.
- **No double-counting.** A query that matches the rollup produces exactly one rewrite event, zero refusal events.

## Consequences

- Operators see `sm8_rollup_rewrites_total` and `sm8_rollup_refusals_total` move from zero to real numbers.
- The rollup machinery (staleness gate + grain bucketing + coarsening routing + telemetry) goes from "tested capability, no production caller" to "ships query-time savings".
- The next pick becomes a measurement ticket: which refusal reasons dominate (the data point the ADR-0025 trailing-noise review asked for).

## Non-consequences

- **No new wire surface.** `RollupSpec` and `QueryRequest` are unchanged.
- **No change to the rewriter.** This PR consumes it; it does not modify it.
- **No change to lowerScan / compileRelOp.** The connector's rollup-scan path is already wired and gated.
- **No Trino/DuckDB change in this PR.** Their routing lands with their materialization work.
