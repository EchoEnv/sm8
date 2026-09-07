# ADR-0025: Rollup-rewrite refusal observability

**Status:** Proposed.
**Date:** 2026-09-07.
**Author:** SM8 agent.

## Context

`RollupRewriter.rewrite()` (sm8-core/src/main/scala/io/sm8/core/rel/RollupRewriter.scala:253)
returns a typed `RollupRewriteResult` ADT: `Rewritten(plan, name)` on success,
`Unchanged(reason: RollupRewriteRefusal)` on refusal. The refusal ADT is a sealed
hierarchy of seven machine-readable reasons (`NonCanonicalShape`, `NoGroupSetMatch`,
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

1. **core** (sm8-core): no change. The `RollupRewriteRefusal` ADT already exists
   and is exhaustive. Add one helper, `RollupRewriteRefusal.reasonName: String`,
   so observers and metrics need not pattern-match the sealed trait inline
   (one source of truth for human-readable labels).

2. **adapter** (sm8-platform): a new object `RollupRewriteRefusalReporter`
   (sm8-platform/src/main/scala/io/sm8/platform/query/RollupRewriteRefusalReporter.scala)
   that owns three counters on `QueryMetrics`:
   `rollupRewriteRefusalsTotal`, `rollupRewriteRewritesTotal`, and
   `rollupRewriteRefusalPermanentTotal` — the third is the subset whose reason
   is `UnsplittableAggregate` (the only permanent refusal; everything else is
   recoverable). Also a `recordRefusal(reason)` and `recordRewrite()` method
   pair. The reporter is invoked by the future routing-invocation PR at the
   platform fold site (the natural place: alongside the existing
   `cachePolicy`/`decisionHints` fold in
   `EngineService.runQueryWithHooks`).

3. **plugin** (plugins/rollup-refusal-observer/): a new observer plugin that
   follows the proven `QueryFrequencyObserverPlugin` template. It registers a
   PostExecute hook that publishes the seven refusal counts into
   `context.meta` under a stable key, so the existing meta-inspector surface
   reads them with zero new transport code.

## Layer discipline (RFC §3)

- core stays free of plugin or adapter references (RULE#1).
- The adapter-side reporter does NOT import any plugin implementation class —
  it only adds methods to the existing `QueryMetrics` (a typed counter surface
  in sm8-platform). This preserves the discovery / construction factory seams
  from PR-191: plugins call `EngineFactory.create(plugins)`; metrics are a
  platform-level concern.
- The new observer plugin closes over the same JVM-global counters the reporter
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
- It does NOT add a runtime config flag. The reporter methods are zero-cost
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
- sm8-platform/src/main/scala/io/sm8/platform/query/RollupRewriteRefusalReporter.scala —
  new file, ~50 LOC. Thin layer that the future routing-invocation PR calls.
- plugins/rollup-refusal-observer/ — new module (template: query-frequency-observer-plugin).
  POM + plugin class + spec. ~150 LOC total.
- Docs: this ADR.

## Tests

- core: extend `RollupRewriterSpec` with a `reasonName` exhaustiveness pin.
- sm8-platform: extend `QueryMetricsSpec` (if present) or add a new spec for
  `RollupRewriteRefusalReporter` — call each method, assert the counter
  surfaces in `snapshot()`.
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
- The reporter is the **only** code site the future routing-invocation PR
  must touch to wire observability — that's the contract this ADR pins.
