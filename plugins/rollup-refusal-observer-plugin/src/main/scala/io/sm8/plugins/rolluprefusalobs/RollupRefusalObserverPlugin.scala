/*
 * SM8 rollup-refusal-observer Plugin.
 *
 * A PostExecute OBSERVER plugin that publishes the per-reason
 * rollup-rewrite refusal counts (held by the process-wide
 * MetricsSink, e.g. sm8-platform's QueryMetrics) into `context.meta`
 * so the existing meta-inspector surface (`sm8 inspect`) reads them
 * with zero new transport code.
 *
 * ==What this plugin does NOT do==
 *
 * It does not CALL `RollupRewriter.rewrite` and does not own the
 * production routing decision — that is the routing-invocation PR's
 * job (its fold site calls `MetricsRegistry.sink().recordRollupRefusal`
 * / `.recordRollupRewrite`). This plugin is the read/publish half:
 * without it the counters exist but no operator-facing surface shows
 * them per query.
 *
 * ==Where the counts live==
 *
 * The single source of truth is `MetricsRegistry.sink()` — the same
 * JVM-global sink the cache-plugin and the query pipeline write to.
 * This plugin holds NO counters of its own; it only reads a snapshot
 * and publishes. If no sink is registered, the read is a no-op and
 * `context.meta` is left untouched (the MetricsSink default methods
 * are no-ops by contract).
 *
 * ==Closure safety==
 *
 * The hook closes over NOTHING (`closedOverVars` is Nil); the sink
 * is looked up per invocation via the JVM-global registry.
 * Serializable by construction.
 */
package io.sm8.plugins.rolluprefusalobs

import io.sm8.core.cache.MetricsRegistry
import io.sm8.sdk.{Context, Engine, HookStage, Plugin}

/** The plugin: registers one PostExecute observer that snapshots the
  * rollup-rewrite counters from the process-wide MetricsSink and
  * publishes them into `context.meta` under
  * `io.sm8.plugins.rolluprefusalobs:counters`.
  */
final class RollupRefusalObserverPlugin extends Plugin {

  /** Stable plugin identity for diagnostic surfaces.
    *
    * @return the stable plugin name
    */
  override def name: String = "rollup-refusal-observer"

  /** Registers the PostExecute observer (priority 910 — after the
    * query-frequency observer at 900 so both observers attach before
    * the pipeline's terminal hooks).
    *
    * @param engine the engine being configured
    */
  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      hook,
      910
    )
  }

  /** No captured state: the sink is resolved per invocation from the
    * JVM-global MetricsRegistry.
    *
    * @return the captured-variable name list (empty)
    */
  override def closedOverVars: Seq[String] = Seq.empty

  /** The PostExecute observer hook (test seam). Reads the
    * rollup-rewrite counters from the sink snapshot and publishes
    * them into `context.meta`.
    *
    * @return the registered PostHook
    */
  private[rolluprefusalobs] def hook: io.sm8.sdk.PostHook =
    new io.sm8.sdk.PostHook with java.io.Serializable {
      override val name: String = "rollup-refusal-observer"
      override val stage: HookStage = HookStage.PostExecute
      override val priority: Int = 910
      override val runsOnStop: Boolean = true // observer: always fire

      /** Observe one completed invocation: publish the sink's
        * rollup counter snapshot into `context.meta`.
        *
        * @param context the post-execute context
        * @return the context (meta enriched with the counters key)
        */
      override def run(context: Context): Context = {
        val snap = MetricsRegistry.sink().rollupSnapshot()
        context.copy(meta = context.meta +
          ("io.sm8.plugins.rolluprefusalobs:counters" -> snap))
      }
    }
}
