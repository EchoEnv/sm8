/*
 * SM8 Platform — QueryMetrics.
 *
 * Per [[ADR-012-b-followup]] (`docs/adr/0012-b-followup-real-counter-instrumentation.md`):
 * process-wide counter holder that backs the real values for the
 * `MetricsService.snapshot` handler. Replaces the placeholder zeros from
 * PR-254 (`d0c15ee`) with actual invocation/cache/error counters.
 *
 * Per [[scala-jvm-safety-mindset]]: `AtomicLong` per counter gives
 * concurrent-safe increments without explicit locking. The 6 counters
 * are independent — no cross-counter invariant to maintain.
 *
 * Per [[scala-perf-testing-mindset]]: `AtomicLong.incrementAndGet` is
 * ~10ns per call. The hot path (QueryService.runQuery) calls 3 record
 * methods per request — ~30ns added overhead per request. Below
 * measurement noise.
 *
 * Per [[ADR-012-b-followup]] §Risks (snapshot atomicity): each
 * `get()` is atomic, but the 6 reads in `snapshot()` are not a single
 * transaction. Worst case: counters disagree by 1 across concurrent
 * reads + writes. Acceptable for a diagnostic counter; documented
 * in Scaladoc on `MetricsService.snapshot`.
 */
package io.sm8.platform.query

import java.util.concurrent.atomic.AtomicLong

import io.sm8.core.cache.MetricsSink

/**
 * Process-wide counter holder for query-pipeline metrics.
 *
 * Counters:
 *   - `invocationsTotal`     : total calls to `QueryService.runQuery`
 *                             (incremented at the top of `private def runQuery`)
 *   - `invocationsSucceeded` : calls that returned a `Right[QueryResult]`
 *   - `cacheHits`           : cache-plugin read returned `Some(_)` (opt-in
 *                             via `CachePlugin.withSharedMetrics`)
 *   - `cacheMisses`         : cache-plugin read returned `None` (opt-in)
 *   - `auditSinkUnavailable`: `EngineError.AuditSinkUnavailable` was raised
 *   - `timedOut`            : `EngineError.QueryTimedOut` was raised
 *
 * The `failed` counter exposed in `MetricsSnapshot.invocations.failed`
 * is computed in `snapshot()` as `auditSinkUnavailable + timedOut`
 * (the only 5xx error types currently treated as "failed"; other
 * `EngineError` variants have different semantics). Per the ADR: this
 * keeps the read-side invariant simple (no separate `failed` counter
 * to keep in sync).
 *
 * Thread-safety: `AtomicLong.incrementAndGet()` is atomic per the JDK
 * contract. No explicit locking required.
 */
object QueryMetrics extends MetricsSink {

  private val invocationsTotal      = new AtomicLong(0)
  private val invocationsSucceeded  = new AtomicLong(0)
  private val cacheHits             = new AtomicLong(0)
  private val cacheMisses           = new AtomicLong(0)
  private val auditSinkUnavailable  = new AtomicLong(0)
  private val timedOut              = new AtomicLong(0)

  // -- Per-invocation record methods (called from QueryService.runQuery) --

  /** Called at the top of `QueryService.private def runQuery`. */
  override def recordInvocation(): Unit = invocationsTotal.incrementAndGet()

  /** Called when `runQuery` returns `Right(qr)`. */
  override def recordSuccess(): Unit = invocationsSucceeded.incrementAndGet()

  /** Called inside the `Left(err)` arm for `EngineError.AuditSinkUnavailable`. */
  def recordAuditSinkUnavailable(): Unit = auditSinkUnavailable.incrementAndGet()

  /** Called inside the `Left(err)` arm for `EngineError.QueryTimedOut`. */
  def recordTimedOut(): Unit = timedOut.incrementAndGet()

  // -- Cache record methods (opt-in via MetricsRegistry.sink()) --

  /** Called from `CachePlugin.onPreExecute` when `cache.getJournaled(key)` returns `Some(_)`. */
  override def recordCacheHit(): Unit = cacheHits.incrementAndGet()

  /** Called from `CachePlugin.onPreExecute` when `cache.getJournaled(key)` returns `None`. */
  override def recordCacheMiss(): Unit = cacheMisses.incrementAndGet()

  // -- Snapshot reader (called by MetricsService.snapshotRunner) --

  /**
   * Read all counters into a `MetricsSnapshot` for the wire response.
   *
   * Per [[ADR-012-b-followup]] §Risks (snapshot atomicity): the 6
   * reads are NOT a single atomic transaction. Worst case: counters
   * disagree by 1 between concurrent reads + writes. Acceptable for
   * a diagnostic UI.
   *
   * @param uptimeSeconds  the process uptime in seconds (computed by
   *                       `MetricsService.snapshotRunner` from its captured
   *                       `startedAt: Instant`)
   * @param startedAtIso   the process start time as an ISO-8601 string
   *                       (from `startedAt.toString`)
   * @return the wire-stable `MetricsSnapshot` with all 6 counters read
   */
  def snapshot(uptimeSeconds: Long, startedAtIso: String): MetricsSnapshot =
    MetricsSnapshot(
      startedAt    = startedAtIso,
      uptimeSeconds = uptimeSeconds,
      invocations  = InvocationCounters(
                      total     = invocationsTotal.get,
                      succeeded = invocationsSucceeded.get,
                      // Failed = sum of specific 5xx error counters.
                      // Other EngineError variants have different
                      // semantics and don't count here.
                      failed    = auditSinkUnavailable.get + timedOut.get),
      cache        = CacheCounters(
                      hits   = cacheHits.get,
                      misses = cacheMisses.get),
      errors       = ErrorCounters(
                      auditSinkUnavailable = auditSinkUnavailable.get,
                      timedOut             = timedOut.get)
    )
}