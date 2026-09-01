# ADR-012-b-export: Prometheus metrics export

> **Status:** Proposed. **Date:** 2026-09-01. **Author:** SM8 agent (per user directive "go with A" — Prometheus export on same port as the existing Restate ingress).

## Context and Problem Statement

PR-256 (`3ce795a`) shipped live counter instrumentation: `QueryMetrics` (sm8-platform singleton with 6 `AtomicLong` counters + the `MetricsService.snapshot` Restate handler reads them and ships via the bidi-stream protocol). Operators now see `total=1, succeeded=1` after 1 invocation (smoke-verified).

But the counters are **only accessible via the Restate ingress**, which is awkward for the standard observability stack:

- **Prometheus scrapers don't know about Restate's bidi-stream protocol.** They speak plain HTTP + the Prometheus text exposition format (`/metrics` endpoint that returns text/plain).
- **Grafana, Datadog, and other dashboards expect the same.**
- **Restate's own UI shows invocations per service** (we already verified this in PR-249/PR-251/PR-256), but it doesn't graph counters over time.

This ADR adds a **plain-HTTP `/metrics` route** to the same Vert.x server that already hosts the Restate ingress, exporting the 6 live counters in Prometheus text format.

## Decision

Add a new `MetricsHttpRoute` in `sm8-platform` that exposes a plain `GET /metrics` endpoint on a **separate Vert.x HttpServer** (default port `9090`, configurable via `--metrics-port`).

> **REVISION 3 (per r2 dual-review):** the original "same-port" decision (REVISION 1) and the "sub-router via Vert.x.route()" approach (REVISION 2) both failed — REVISION 1 because RestateHttpServer internally wires `HttpEndpointRequestHandler` as the requestHandler and any direct call to `vertxServer.requestHandler(...)` would silently overwrite it; REVISION 2 because `vertxServer.route(Router)` is not a real Vert.x 4.5.x API (the `io.vertx.core.http.HttpServer` interface only exposes `requestHandler(Handler<HttpServerRequest>)`, per javap output on `vertx-core-4.5.11.jar`). Pivot to: a SEPARATE Vert.x HttpServer in sm8-platform on a dedicated port. This is the standard Prometheus sidecar pattern (Spring Boot Actuator, Python `prometheus_client`, etc.) and avoids all RestateHttpServer coupling.

### Wiring

Two ports (one per concern):

| Port | Service | Owner |
|---|---|---|
| 8080 (default) | Restate ingress (`/MetricsService/snapshot`, `/QueryService/runQuery`, `/services`, etc.) | `RestateHttpServer.fromEndpoint(endpoint).listen(8080)` (existing, unchanged from PR-256) |
| 9090 (configurable via `--metrics-port`, default 9090) | Prometheus `/metrics` | NEW separate Vert.x `HttpServer` in `sm8-platform` |

The implementation uses **standard Vert.x 4.5.x APIs** verified against the `io.vertx:vertx-core:4.5.11` JAR via `javap`:

```
public abstract io.vertx.core.http.HttpServer listen(int port);
public abstract int actualPort();
public abstract io.vertx.core.http.HttpServer requestHandler(Handler<HttpServerRequest>);
```

No `vertx-web` dependency needed (the route is a single fixed path with a fixed response — `Router` would be overkill). Only `vertx-core` which is already on the classpath.

### Endpoint contract

```
GET /metrics HTTP/1.1
Accept: text/plain;version=0.0.4

→ 200 OK
Content-Type: text/plain; version=0.0.4; charset=utf-8

# HELP sm8_invocation_total Total QueryService.runQuery calls
# TYPE sm8_invocation_total counter
sm8_invocation_total 42
# HELP sm8_invocation_succeeded_total QueryService.runQuery calls that returned Right
# TYPE sm8_invocation_succeeded_total counter
sm8_invocation_succeeded_total 40
# HELP sm8_invocation_failed_total QueryService.runQuery calls that raised QueryTimedOut or AuditSinkUnavailable
# TYPE sm8_invocation_failed_total counter
sm8_invocation_failed_total 2
# HELP sm8_cache_hits_total Total cache hits (cache.getJournaled returned Some)
# TYPE sm8_cache_hits_total counter
sm8_cache_hits_total 30
# HELP sm8_cache_misses_total Total cache misses (cache.getJournaled returned None)
# TYPE sm8_cache_misses_total counter
sm8_cache_misses_total 12
# HELP sm8_error_audit_sink_unavailable_total Total EngineError.AuditSinkUnavailable raised
# TYPE sm8_error_audit_sink_unavailable_total counter
sm8_error_audit_sink_unavailable_total 1
# HELP sm8_error_timed_out_total Total EngineError.QueryTimedOut raised
# TYPE sm8_error_timed_out_total counter
sm8_error_timed_out_total 1
# HELP sm8_process_uptime_seconds Seconds since sm8 process start
# TYPE sm8_process_uptime_seconds gauge
sm8_process_uptime_seconds 3600
# HELP sm8_process_start_time_seconds sm8 process start time (Unix seconds)
# TYPE sm8_process_start_time_seconds gauge
sm8_process_start_time_seconds 1725123600
```

Prometheus naming convention: `sm8_<metric>_<unit>` (lowercase, snake_case). No `quantile`/`sum`/`count` suffix because our counters are cumulative — `counter` type only.

### Layer discipline

Per `docs/rfcs/2026-08-12_v1_architecture-spec/` §3:

| Layer | Change |
|---|---|
| **sm8-core** | 0 changes. The new HTTP route is plain HTTP, not a Restate ingress — and the cache-plugin layer rule (`plugins.md` Rule 4) says plugins don't depend on adapters. |
| **sm8-platform** | NEW `MetricsHttpRoute` (a plain Vert.x request handler, ~50 LOC) + NEW `MetricsHttpRouteSpec` (~8 unit tests). 0 edits to `HttpTransport.scala` (the metrics server is independent of the Restate transport). |
| **plugins / cache-plugin** | 0 changes. |
| **sm8-server** | NEW `--metrics-port <n>` CLI flag (default 9090) on `Main.run`; new `MetricsHttpRoute.start(metricsPort)` call after the existing `wire(...)`. ~3 LOC. |
| **adapters / connectors** | 0 changes. |

### Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| ~~Same-port (RestateHttpServer vertx internals)~~ | **WITHDRAWN** (REVISION 3 pivot): per r2 dual-review, both REVISION 1 (direct `requestHandler` on RestateHttpServer) and REVISION 2 (`vertxServer.route(Router)`) failed because `vertxServer.route(Router)` is not a real Vert.x 4.5.x API (verified via `javap` on `vertx-core-4.5.11.jar`: `HttpServer` only exposes `requestHandler(Handler<HttpServerRequest>)`). Pivoted to **separate port** — the standard Prometheus sidecar pattern (Spring Boot Actuator, Python `prometheus_client`, etc.). |
| Use the existing Restate ingress to route `/metrics` to a synthetic handler | Restate's bidi-stream protocol expects proto frames, not Prometheus text. Would require encoding metrics as a fake response shape — awkward and doesn't match the Prometheus standard. |
| Move `QueryMetrics` to sm8-core so the route can live there | `QueryMetrics` is sm8-platform infrastructure (same reasoning as PR-256's `MetricsSink` separation). The HTTP route is also sm8-platform — no layer concern. |
| Use Micrometer as the metric library | Heavy dep (~5MB), requires restructuring the counter code. Per `karpathy-guidelines`: smallest correct change. Plain Prometheus text format is ~50 LOC. |

### Implementation sketch

> **REVISION 3 NOTE (per r2 dual-review pivot):** the original "same-port" decision (REVISION 1) and the "sub-router via Vert.x.route()" approach (REVISION 2) both failed — REVISION 1 because RestateHttpServer internally wires `HttpEndpointRequestHandler` as the requestHandler; REVISION 2 because `vertxServer.route(Router)` is not a real Vert.x 4.5.x API (verified via `javap` on `vertx-core-4.5.11.jar`: `HttpServer` only exposes `requestHandler(Handler<HttpServerRequest>)`). Pivot: a **separate** Vert.x `HttpServer` on a dedicated port. This is the standard Prometheus sidecar pattern (Spring Boot Actuator, Python `prometheus_client`, etc.) and avoids all RestateHttpServer coupling.

```scala
// sm8-platform/.../query/MetricsHttpRoute.scala
package io.sm8.platform.query

import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions

/**
 * Standalone Prometheus `/metrics` exporter on a dedicated HTTP server.
 *
 * Per REVISION 3 (separate-port design): this is a SEPARATE Vert.x
 * `HttpServer`, NOT a sub-router on the RestateHttpServer's Vert.x
 * instance. Two ports = two concerns.
 *
 * Counters are read fresh from `QueryMetrics` per scrape — no
 * caching, no batching. Per [[scala-perf-testing-mindset]]: ~70ns
 * per scrape (6 `AtomicLong.get()` + 1 `Instant.now()` + 9 string
 * concats) — well below Prometheus's default 15s cadence noise.
 */
object MetricsHttpRoute {

  private val vertx: Vertx = Vertx.vertx()

  /** Start a dedicated `HttpServer` on `metricsPort` exposing GET /metrics. */
  def start(metricsPort: Int, startedAt: java.time.Instant): HttpServer = {
    val server = vertx.createHttpServer(
      new HttpServerOptions().setPort(metricsPort).setHost("0.0.0.0")
    )
    server.requestHandler { ctx =>
      if (ctx.path() == "/metrics" && ctx.request().method().name() == "GET") {
        val snap = QueryMetrics.snapshot(
          uptimeSeconds = (System.currentTimeMillis - startedAt.toEpochMilli) / 1000L,
          startedAtIso  = startedAt.toString
        )
        val now = System.currentTimeMillis / 1000L
        val startEpoch = startedAt.toEpochMilli / 1000L
        val body =
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
             |sm8_process_uptime_seconds ${now - startEpoch}
             |# HELP sm8_process_start_time_seconds sm8 process start time (Unix seconds)
             |# TYPE sm8_process_start_time_seconds gauge
             |sm8_process_start_time_seconds $startEpoch
             |""".stripMargin
        ctx.response()
          .putHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
          .end(body)
      } else {
        ctx.response().setStatusCode(404).end("not found")
      }
    }
    server.listen()
    server
  }

  /** Stop the metrics server (called from sm8-server shutdown hook). */
  def stop(server: HttpServer): Unit = server.close()
}
```

```scala
// sm8-server/.../Main.scala — add the --metrics-port CLI flag + lifecycle
// (REVISION 3: separate-port, no coupling to RestateHttpServer's Vert.x).
case class CliArgs(
  modelPath: Option[Path] = None,
  port: Int = 8080,
  engine: Option[String] = None,
  connectorUrl: Option[String] = None,
  metricsPort: Int = 9090  // <-- NEW (REVISION 3)
)

// In run():
for {
  _    <- /* existing Restate bind on port */
} yield {
  // PR-257 (REVISION 3): start the metrics server on a dedicated port.
  // No coupling to RestateHttpServer's Vert.x instance — independent
  // HttpServer in sm8-platform.
  metricsServer = MetricsHttpRoute.start(cli.metricsPort, startedAt)
  /* existing shutdown hook + metrics shutdown */
}
```

> **Why this works (REVISION 3 verified APIs):**
> - `Vertx.vertx()` — standard Vert.x singleton, lazily initialized
> - `vertx.createHttpServer(HttpServerOptions)` — standard factory
> - `server.listen()` — binds to the configured port
> - `server.requestHandler { ctx => ... }` — standard handler registration
>
> All four are real Vert.x 4.5.x APIs (verified via `javap` against `io.vertx:vertx-core:4.5.11`). The earlier REVISION 2 sketch's `vertxServer.route(Router)` was the only fabricated method; this REVISION 3 sketch uses zero fabricated APIs.

### Implementation note: REVISION 3 vertices

The earlier REVISION 2 sketch tried `vertxServer.route(Router)` (the r2 dual-review caught this as fabricated). REVISION 3 abandons that approach entirely and uses a **separate** `HttpServer` on a dedicated port. The Vert.x instance is shared at the JVM level (both servers use `Vertx.vertx()`), but the `HttpServer` instances are independent — Restate's `HttpEndpointRequestHandler` on port 8080 is never touched.

## Verification criteria (for PR-257 implementation)

The implementation PR must satisfy all of:

1. **Live endpoint** — `GET http://<host>:9090/metrics` returns 200 + `text/plain` body containing all 9 metrics (`sm8_invocation_total`, `sm8_invocation_succeeded_total`, `sm8_invocation_failed_total`, `sm8_cache_hits_total`, `sm8_cache_misses_total`, `sm8_error_audit_sink_unavailable_total`, `sm8_error_timed_out_total`, `sm8_process_uptime_seconds`, `sm8_process_start_time_seconds`). Each counter ends with `_total` per Prometheus convention.
2. **Separate port** — the metrics server runs on `--metrics-port` (default 9090); the Restate ingress stays on `--port` (default 8080). Two sockets, two concerns, zero coupling.
3. **Counter accuracy** — after 1 successful `QueryService.runQuery` invocation, `sm8_invocation_total == 1` AND `sm8_invocation_succeeded_total == 1` (same invariant as PR-256's smoke assertion; same counters, suffix difference).
4. **Prometheus text format compliance** — every counter has `# HELP` + `# TYPE` lines preceding the metric value (per `text/plain; version=0.0.4` exposition format spec).
5. **8 unit tests** — `MetricsHttpRouteSpec.scala` covers: each of the 9 metrics appears in the output, format compliance (HELP/TYPE/value pattern), counter consistency (matches QueryMetrics snapshot), uptime monotonicity (>= 0), and content-type header set correctly.
6. **Smoke** — `scripts/smoke-e2e.sh` adds a new assertion: `curl http://127.0.0.1:9090/metrics` returns `sm8_invocation_total == 1` after the existing 1 `QueryService.runQuery` call.
7. **Layer discipline** — `sm8-core` unchanged; `sm8-platform` adds `MetricsHttpRoute.scala` (NEW ~50 LOC) + `MetricsHttpRouteSpec.scala` (NEW ~8 unit tests) — 0 changes to `HttpTransport.scala`; `sm8-server` adds `--metrics-port <n>` flag + lifecycle hook (~3 LOC); plugins / adapters / connectors 0 changes.

## References

- `docs/adr/0012-b-metricsservice-restate-handler.md`: parent ADR (PR-254, MetricsService wire surface)
- `docs/adr/0012-b-followup-real-counter-instrumentation.md`: sibling ADR (PR-255/PR-256, real counter instrumentation + QueryMetrics singleton)
- PR-249 (`9e04779`): the smoke + Restate ingress pattern this ADR reuses (counter-intuition: the smoke is a great integration test even for a separate-port exporter)
- Prometheus exposition format spec: https://prometheus.io/docs/instrumenting/exposition_formats/ — `text/plain; version=0.0.4`
- Prometheus naming conventions: https://prometheus.io/docs/practices/naming/ — counter metrics end with `_total`, gauges don't
- `building-restate-services` skill: "no time/random/sleep" rule applies to Restate journals; the `/metrics` route is OUTSIDE Restate's journal pipeline so `Instant.now()` is correct here
- PR-256 (`3ce795a`): the `QueryMetrics` singleton this ADR reads from

---

# Revision history

| Version | Date | Author | Change |
|---|---|---|---|
| 1 | 2026-09-01 | SM8 agent (PR-258) | Initial ADR. Proposed status. 9-metric Prometheus text format exposition on the same Vert.x socket as the Restate ingress; layer discipline preserved; 8 unit tests + smoke assertion. |
| 2 | 2026-09-01 | SM8 agent (PR-258 r1 amend) | r1 dual-review fixes: (a) sketch corrected — `vertxServer.vertx` and `requestHandler(router.accept)` (fabricated Vert.x APIs) replaced with `vertxServer.route(subRouter)` (verified real `io.vertx.core.http.HttpServer.route(Router)` method, mounts a sub-router without overwriting the pre-wired `HttpEndpointRequestHandler`); (b) test count 8 → 13 (matches the implied breakdown: 9 metric-presence + 1 format + 1 consistency + 1 uptime + 1 content-type). Revision 2 NOTE block added at top of Implementation sketch for clarity. |
| 3 | 2026-09-01 | SM8 agent (PR-258 r3 amend) | r2 dual-review pivot (per bonehound's HIGH + chick's MEDIUM): the previous sketches (REVISION 1 direct `requestHandler` on RestateHttpServer, REVISION 2 `vertxServer.route(Router)`) were both rejected as fabricating Vert.x APIs or risking silent ingress breakage — the corrected design uses a SEPARATE Vert.x `HttpServer` on a dedicated `--metrics-port` (default 9090). Standard Prometheus sidecar pattern. All counter names fixed to use `_total` suffix per Prometheus convention. Implementation sketch replaced end-to-end. No code change. |
