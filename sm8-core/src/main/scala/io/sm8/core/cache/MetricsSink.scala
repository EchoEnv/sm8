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
}

object MetricsSink {

  /** No-op sink: every method is a default no-op. Used when no sink has been registered. */
  object NoOp extends MetricsSink
}
