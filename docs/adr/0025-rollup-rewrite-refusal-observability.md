# ADR-0025: Rollup-rewrite refusal observability

**Status:** Proposed.
**Date:** 2026-09-07.
**Author:** SM8 agent.

## Context

`RollupRewriter.rewrite()` (sm8-core/src/main/scala/io/sm8/core/rel/RollupRewriter.scala:253)
returns a typed `RollupRewriteResult` ADT: `Rewritten(plan, name)` on success,
`Unchanged(reason: RollupRewriteRefusal)` on refusal. The refusal ADT is a sealed
hierarchy of eight machine-readable reasons (`NonCanonicalShape`, `NoGroupSetMatch`,
`UnsplittableAggregate`, `FilterNotEvaluable`, `GrainMismatch`,
`SourceKindUnsupported`, `AlgebraicStateNotWired`) plus the connector-emitted
`SchemaStale` — and the ADT distinguishes recoverable from permanent refusals
so a future observer can tell them apart.

Today the rewriter has **zero production callers** (verified via exhaustive import
search; the function exists as a pure library plus tests, exercised only by
`RollupRewriterSpec` and `RollupMaterializerSpec`). The routing-invocation PR
that will call `rewrite()` from the production query path is a separate ticket
not yet designed.

ADR-0023 §"Explainer/observability" records, verbatim: "the platform-side observer
that consumes `RollupRewriteRefusal` is still a Ticket-6-epic follow-up, not wired
behavior today." This ADR lands that follow-up — the observability surface —
before the routing-invocation PR. Without it, the moment routing is wired, every
refusal becomes invisible: a query that *should* hit a rollup silently falls back
to the base table, with no signal anywhere in the system.

## Decision

Land the observability surface in three pieces, all layer-clean:

1. **core** (sm8-core): the `RollupRewriteRefusal` ADT itself is unchanged.
   Additions: a `reasonName(r)` helper on the companion (stable
   machine-readable label per case, one source of truth for counter keys),
   an `isPermanent(r)` classifier (recoverable vs permanent split), and on
   `MetricsSink` three default (no-op) methods — `recordRollupRewrite()`,
   `recordRollupRefusal(reason)`, `rollupSnapshot()` — plus the
   `RollupCountersSnapshot` ADT, so plugins read counters through the
   core-only `MetricsRegistry` seam without importing the platform.

2. **adapter** (sm8-platform): the record methods live directly on the existing
   `QueryMetrics` object (sm8-platform/src/main/scala/io/sm8/platform/query/QueryMetrics.scala),
   which already implements `MetricsSink`: `recordRollupRewrite()`,
   `recordRollupRefusal(reason)`, and the `rollupSnapshot()` read surface.
   Implementation note: an earlier draft of this ADR described a separate thin
   `RollupRewriteRefusalReporter` object; it was elided because `QueryMetrics`
   already is the platform's sink implementation and an indirection would only
   forward the calls. The routing-invocation change calls the `QueryMetrics`
   methods (or equivalently `MetricsRegistry.sink()`, the registered
   implementation of which is `QueryMetrics` in sm8-server's boot wiring):

3. **plugin** (plugins/rollup-refusal-observer/): a new observer plugin that
   follows the proven `QueryFrequencyObserverPlugin` template. It registers a
   PostExecute hook that publishes the eight refusal counts into
   `context.meta` under a stable key, so the existing meta-inspector surface
   reads them with zero new transport code.

## Layer discipline (RFC §3)

- core stays free of plugin or adapter references (RULE#1).
- `QueryMetrics` (the platform sink) does NOT import any plugin implementation class.
  This preserves the discovery / construction factory seams
  from PR-191: plugins call `EngineFactory.create(plugins)`; metrics are a
  platform-level concern.
- The new observer plugin closes over the same JVM-global counters `QueryMetrics`
  writes to, so plugin removal does not lose counts (the singleton owns them).

## Why a separate observer plugin (not a built-in PostExecute hook)

ADR-010-a puts hook-seam policy in sm8-platform/hooks/ as a typed seam; plugins
opt into observing. Built-in hooks would:
- Force every server to emit rollup-refusal telemetry, even operators who don't
  care (false-positive "rollup broken" alerts);
- Couple the platform to a domain concern it shouldn't know about;
- Make rollup-refusal telemetry impossible to disable in environments that
  disable plugin loading entirely.

The observer-plugin shape mirrors `QueryFrequencyObserverPlugin`: same template,
same JVM-global singleton pattern, same `context.meta` publication surface.
Operators who don't want it simply don't include it in their plugin set.

## What this ADR does NOT do

- It does NOT wire `RollupRewriter.rewrite()` into the production query path.
  That is the routing-invocation ticket (separate ADR when designed).
- It does NOT change the `RollupRewriteResult` / `RollupRewriteRefusal` ADT
  surface. The sealed hierarchy is exhaustive as designed.
- It does NOT add a runtime config flag. The `QueryMetrics` record methods are zero-cost
  when no plugin consults them; the observer plugin is opt-in by inclusion in
  the plugin set.

## Alternatives considered

1. **Inline logging only.** Rejected: logs without aggregation don't surface
   in dashboards; operators still have to grep.
2. **Wire the rewriter now and observe it.** Rejected: PR-338 / PR-340
   history — flipping the routing gate without the full surface is the
   documented failure mode (ADR-0023 §"Supersedes the approach reverted in
   PR-338"). Observer first, routing second.
3. **Built-in PostExecute hook on the dispatcher.** Rejected for the three
   reasons above (coupling, opt-out impossibility, layer violation).

## File-level change list

- sm8-core/src/main/scala/io/sm8/core/rel/RollupRewriter.scala — add
  `reasonName` helper on `RollupRewriteRefusal` (~6 LOC).
- sm8-platform/src/main/scala/io/sm8/platform/query/QueryMetrics.scala —
  add three counter fields and two methods (~12 LOC).
- sm8-platform/src/main/scala/io/sm8/platform/query/QueryMetrics.scala —
  three counter fields, the per-reason bounded map, and two record methods
  plus the `rollupSnapshot()` read surface (~40 LOC).
- plugins/rollup-refusal-observer/ — new module (template: query-frequency-observer-plugin).
  POM + plugin class + spec. ~150 LOC total.
- Docs: this ADR.

## Tests

- core: extend `RollupRewriterSpec` with a `reasonName` exhaustiveness pin.
- sm8-platform: extend `QueryMetricsSpec` with direct assertions for
  `recordRollupRewrite` / `recordRollupRefusal` / `rollupSnapshot` — call each
  method, assert the counter surfaces in `snapshot()` and in
  `rollupSnapshot()`.
- plugins/rollup-refusal-observer: a Spec mirroring
  `QueryFrequencyObserverPluginSpec`'s shape — register the plugin, fire a
  synthetic PostExecute context, assert `context.meta` carries the published
  counts and that the JVM-global counters match.

## Consequences

- Operators gain rollup-refusal telemetry the moment the routing-invocation
  PR ships — no PR-coordination race.
- A future "measure which refusals dominate" ticket reads from
  `QueryMetrics.snapshot()` and the plugin's `context.meta` publication;
  no new transport surface.
- `QueryMetrics` (the platform sink) is the **only** code site the future routing-invocation PR
  must touch to wire observability — that's the contract this ADR pins.
