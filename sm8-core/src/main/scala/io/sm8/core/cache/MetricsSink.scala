/*
 * SM8 Core — MetricsSink.
 *
 * Per [[ADR-012-b-followup]] (`docs/adr/0012-b-followup-real-counter-instrumentation.md`)
 * §Decision 2 (opt-in cache-plugin integration) + §Layer discipline:
 * the cache-plugin needs a way to record hit/miss events without
 * importing sm8-platform (the layer rule says cache-plugin depends
 * ONLY on sm8-core).
 *
 * This trait lives in sm8-core and is consumed by the cache-plugin.
 * The implementation (concrete counter holder) lives in sm8-platform
 * (see `sm8-platform/.../QueryMetrics.scala`) and registers itself
 * via `MetricsRegistry` at boot.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct change": the
 * default implementations are no-ops, so plugins can record against
 * the sink BEFORE any registration and produce no overhead if the
 * runtime never wires a sink.
 */
package io.sm8.core.cache

/**
 * Diagnostic counter sink for query-pipeline cache events.
 *
 * Plugins call these methods at hit/miss sites. The default
 * implementations are no-ops so plugin code can call them without
 * guarding — the runtime decides whether recording has any effect.
 *
 * Per [[scala-jvm-safety-mindset]]: implementations must be
 * thread-safe (the cache hook may run on multiple threads).
 */
trait MetricsSink extends Serializable {

  /** Called when a cache read returns `Some(value)` (cache hit). */
  def recordCacheHit(): Unit = ()

  /** Called when a cache read returns `None` (cache miss). */
  def recordCacheMiss(): Unit = ()

  /** Called when a query attempt starts. Default: no-op. */
  def recordInvocation(): Unit = ()

  /** Called when a query attempt completes successfully. Default: no-op. */
  def recordSuccess(): Unit = ()

  /** Called when `RollupRewriter.rewrite` returns `Rewritten(plan,
    * name)` — the query was routed to a rollup. Default: no-op.
    */
  def recordRollupRewrite(): Unit = ()

  /** Called when `RollupRewriter.rewrite` returns `Unchanged(reason)`
    * — the query could not be routed to a rollup. The default
    * implementation is a no-op; the sm8-platform `QueryMetrics`
    * implementation increments the per-reason counter keyed by
    * `RollupRewriteRefusal.reasonName(reason)`.
    *
    * @param reason the typed refusal reason from `RollupRewriter`
    */
  def recordRollupRefusal(reason: io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal): Unit = ()

  /** Read the current rollup-rewrite counters as an immutable
    * snapshot. The default returns a zero snapshot (the no-op
    * sink never observed a refusal). The sm8-platform `QueryMetrics`
    * implementation returns `(rewrites, refusals, refusalsPermanent,
    * refusalsByReason)` so observer plugins can publish the
    * breakdown into `context.meta`.
    *
    * @return the rollup counter snapshot
    */
  def rollupSnapshot(): RollupCountersSnapshot =
    RollupCountersSnapshot.empty
}

/** Immutable snapshot of the rollup-rewrite counters at one
  * instant. `rewrites` is the total `Rewritten` count,
  * `refusals` the total `Unchanged(reason)` count,
  * `refusalsPermanent` the subset whose reason is permanent
  * (only `UnsplittableAggregate` in v1), and `refusalsByReason`
  * the per-reason breakdown keyed by
  * `RollupRewriteRefusal.reasonName`.
  *
  * Lives in `io.sm8.core.cache` (next to `MetricsSink`) so observer
  * plugins can read a stable, plugin-only-friendly surface without
  * importing `sm8-platform`. The sm8-platform `QueryMetrics`
  * implementation is the one that actually populates non-empty
  * values; the no-op default always returns `empty`.
  *
  * @param rewrites          total `Rewritten` results
  * @param refusals          total `Unchanged(reason)` results
  * @param refusalsPermanent permanent-refusal subset
  * @param refusalsByReason  per-reason breakdown
  */
final case class RollupCountersSnapshot(
    rewrites: Long,
    refusals: Long,
    refusalsPermanent: Long,
    refusalsByReason: List[(String, Long)]
) extends Product with Serializable

object RollupCountersSnapshot {

  /** Zero-valued snapshot for the no-op sink.
    *
    * @return the empty snapshot
    */
  def empty: RollupCountersSnapshot =
    RollupCountersSnapshot(rewrites = 0L, refusals = 0L,
                           refusalsPermanent = 0L, refusalsByReason = Nil)
}

object MetricsSink {

  /** No-op sink: every method is a default no-op. Used when no sink has been registered. */
  object NoOp extends MetricsSink
}
