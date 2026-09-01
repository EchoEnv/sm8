/*
 * SM8 Core — MetricsRegistry.
 *
 * Static holder for the `MetricsSink` implementation. Plugins call
 * `MetricsRegistry.sink().recordCacheHit()` etc.; the deployment
 * module (e.g. `sm8-server`) registers a concrete sink at boot
 * via `MetricsRegistry.register(sink)`.
 *
 * Per [[ADR-012-b-followup]] §Decision 2 (opt-in cache-plugin integration):
 *   - sm8-core ships a `MetricsSink` trait + this registry (no concrete
 *     counter logic — that's in sm8-platform's `QueryMetrics`).
 *   - sm8-platform's `QueryMetrics` implements `MetricsSink`.
 *   - sm8-server wires the sink at boot via `MetricsRegistry.register`.
 *   - cache-plugin calls `MetricsRegistry.sink().recordCacheHit()` etc.
 *
 * If no sink is registered (e.g. tests, or operators who don't want
 * metrics), all sink calls are no-ops (the trait's default methods).
 *
 * Per [[scala-jvm-safety-mindset]]: the static field uses
 * `@volatile` for visibility across threads (a writer thread must
 * see its write from a reader thread without explicit synchronization).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: the sink itself
 * must be `Serializable` (it's wired through the `MetricsSink` trait
 * which already extends `Serializable`).
 */
package io.sm8.core.cache

object MetricsRegistry {

  @volatile
  private var currentSink: MetricsSink = MetricsSink.NoOp

  /** Replace the registered sink with `newSink`. Used by sm8-server at boot.
    *
    * @param newSink the sink to register. Must be non-null; defaults
    *                to the current sink if already registered (does
    *                NOT add — it replaces).
    */
  def register(newSink: MetricsSink): Unit = {
    currentSink = newSink
  }

  /** Read the currently-registered sink. Defaults to a no-op sink.
    *
    * @return the current `MetricsSink` (never null; defaults to
    *         `MetricsSink.NoOp` if no sink has been registered)
    */
  def sink(): MetricsSink = currentSink

  /** Reset to no-op. Useful for tests (no state leaks between test cases). */
  def reset(): Unit = {
    currentSink = MetricsSink.NoOp
  }
}