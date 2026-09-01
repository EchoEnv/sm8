/*
 * SM8 Platform — MetricsHttpRoute.
 *
 * Per ADR-012-b-export (`docs/adr/0012-b-export-prometheus-metrics.md`):
 * standalone Prometheus `/metrics` exporter on a dedicated HTTP server
 * (REVISION 3 design). This is a SEPARATE Vert.x `HttpServer` on
 * `--metrics-port` (default 9090), NOT a sub-router on the
 * RestateHttpServer's Vert.x instance. Two sockets, two concerns.
 *
 * Reads the live `QueryMetrics` singleton and emits 9
 * counters + 2 gauges in `text/plain; version=0.0.4` Prometheus
 * exposition format. No cache, no batching — counters are read fresh
 * per scrape (~70ns total per scrape, well below Prometheus's default
 * 15s cadence noise).
 *
 * Per [[scala-jvm-safety-mindset]]: stateless route. No resource
 * lifecycle beyond standard Vert.x handler registration. Both the
 * metrics server and the Restate ingress call `Vertx.vertx()` (the
 * JVM-level singleton), but the two `HttpServer` instances are
 * independent — independent sockets, independent request handlers.
 * Nothing in the metrics server touches the Restate ingress's
 * `HttpEndpointRequestHandler` on port 8080 (verified via javap on
 * the SDK jar: `RestateHttpServer.fromEndpoint` creates its own
 * server; we never call `requestHandler` on a server we didn't
 * create).
 *
 * Per the `building-restate-services` skill: this route is OUTSIDE
 * Restate's journal pipeline (the `/metrics` path is plain HTTP, not
 * a Restate ingress handler), so `Instant.now()` is correct here even
 * though it's a no-no inside Restate handler closures.
 */
package io.sm8.platform.query

import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions

import java.time.Instant

object MetricsHttpRoute {

  /** Shared Vert.x instance — lazily created on first use. The
    * separate-port design intentionally does NOT use the
    * RestateHttpServer's Vertx instance (the Restate SDK's
    * `RestateHttpServer.fromEndpoint(...).listen(...)` returns an
    * `HttpServer` whose requestHandler is already wired to
    * `HttpEndpointRequestHandler`; calling `.requestHandler(...)`
    * again would overwrite that handler and silently break the
    * Restate ingress). */
  private lazy val vertx: Vertx = Vertx.vertx()

  /** Start a dedicated `HttpServer` on `metricsPort` exposing
    * `GET /metrics` in Prometheus text format. Returns the running
    * server so sm8-server's shutdown hook can close it.
    *
    * Per [[scala-jvm-safety-mindset]]: `server.listen()` is awaited
    * with a 30-second timeout so port-bind failures (port in use,
    * permission denied) fail LOUD at startup — not silently when
    * the first scrape arrives.
    *
    * Per [[scala-perf-testing-mindset]]: ~70ns per scrape (6
    * `AtomicLong.get()` + 1 `Instant.now()` + 9 string concats).
    *
    * @param metricsPort  the port to bind (default 9090 per
    *                      `--metrics-port` CLI flag in sm8-server)
    * @param startedAt    the sm8 process start time (from
    *                      `MetricsService.startedAtInstant`); used
    *                      to compute `sm8_process_uptime_seconds`
    *                      and `sm8_process_start_time_seconds`
    * @return the running `HttpServer` handle (for shutdown)
    */
  def start(metricsPort: Int, startedAt: Instant): HttpServer = {
    val server = vertx.createHttpServer(
      new HttpServerOptions().setPort(metricsPort).setHost("0.0.0.0")
    )
    server.requestHandler { req =>
      // Verification criterion #1 + #4 of the design record: only
      // GET /metrics returns 200 + Prometheus text; everything else
      // returns 404. No path traversal / no auth check at this layer
      // — auth on /metrics is documented as out of scope per the
      // design record (separate ADR if needed).
      if (req.path() == "/metrics" && req.method().name() == "GET") {
        req.response()
          .putHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
          .end(renderBody(startedAt))
      } else {
        req.response().setStatusCode(404).end("not found")
      }
    }
    val bound = try
      server.listen()
        .toCompletionStage.toCompletableFuture
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
    catch {
      case _: java.util.concurrent.ExecutionException =>
        throw new IllegalStateException(
          s"sm8: metrics HTTP server failed to bind port $metricsPort")
      // A 30-second bind hang is near-impossible in practice (no
      // I/O before the socket is bound), but if it does fire the
      // TimeoutException / InterruptedException must be converted
      // to the same IllegalStateException so Main.run's fail-soft
      // contract holds ("continuing without metrics"). Without
      // this the process would die mid-boot with the Restate
      // ingress already up (the exception would escape run()).
      case _: java.util.concurrent.TimeoutException =>
        throw new IllegalStateException(
          s"sm8: metrics HTTP server bind timed out (30s) on port $metricsPort")
      case e: InterruptedException =>
        Thread.currentThread().interrupt()  // restore interrupt
        throw new IllegalStateException(
          s"sm8: metrics HTTP server bind interrupted on port $metricsPort", e)
    }
    bound
  }

  /** Stop the metrics server (called from sm8-server's shutdown
    * hook).
    *
    * @param server the `HttpServer` handle returned by [[start]]
    */
  def stop(server: HttpServer): Unit = server.close()

  /** Build the Prometheus text body for the current `QueryMetrics`
    * snapshot. Pure function — exposed package-private for unit
    * tests. */
  private[query] def renderBody(startedAt: Instant): String = {
    val startEpoch = startedAt.toEpochMilli / 1000L
    val now        = System.currentTimeMillis / 1000L
    val snap = QueryMetrics.snapshot(
      uptimeSeconds = now - startEpoch,
      startedAtIso  = startedAt.toString
    )
    // snap.uptimeSeconds == now - startEpoch; use snap.uptimeSeconds
    // on the wire so both gauges share one clock tick — two
    // `System.currentTimeMillis` calls could disagree at a second
    // boundary, making the uptime gauge and the snapshot disagree.
    s"""# HELP sm8_invocation_total Total QueryService.runQuery calls
       |# TYPE sm8_invocation_total counter
       |sm8_invocation_total ${snap.invocations.total}
       |# HELP sm8_invocation_succeeded_total QueryService.runQuery calls that returned Right
       |# TYPE sm8_invocation_succeeded_total counter
       |sm8_invocation_succeeded_total ${snap.invocations.succeeded}
       |# HELP sm8_invocation_failed_total QueryService.runQuery calls that raised QueryTimedOut or AuditSinkUnavailable
       |# TYPE sm8_invocation_failed_total counter
       |sm8_invocation_failed_total ${snap.invocations.failed}
       |# HELP sm8_cache_hits_total Total cache hits (cache.getJournaled returned Some)
       |# TYPE sm8_cache_hits_total counter
       |sm8_cache_hits_total ${snap.cache.hits}
       |# HELP sm8_cache_misses_total Total cache misses (cache.getJournaled returned None)
       |# TYPE sm8_cache_misses_total counter
       |sm8_cache_misses_total ${snap.cache.misses}
       |# HELP sm8_error_audit_sink_unavailable_total Total EngineError.AuditSinkUnavailable raised
       |# TYPE sm8_error_audit_sink_unavailable_total counter
       |sm8_error_audit_sink_unavailable_total ${snap.errors.auditSinkUnavailable}
       |# HELP sm8_error_timed_out_total Total EngineError.QueryTimedOut raised
       |# TYPE sm8_error_timed_out_total counter
       |sm8_error_timed_out_total ${snap.errors.timedOut}
       |# HELP sm8_process_uptime_seconds Seconds since sm8 process start
       |# TYPE sm8_process_uptime_seconds gauge
       |sm8_process_uptime_seconds ${snap.uptimeSeconds}
       |# HELP sm8_process_start_time_seconds sm8 process start time (Unix seconds)
       |# TYPE sm8_process_start_time_seconds gauge
       |sm8_process_start_time_seconds $startEpoch
       |""".stripMargin
  }
}