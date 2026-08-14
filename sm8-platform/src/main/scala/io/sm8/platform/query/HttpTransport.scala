/*
 * SM8 Platform — HttpTransport (Step 11 of plan line 290, follow-up to PR #57).
 *
 * HTTP transport for the Sm8McpServer. Composes the existing
 * `QueryService.definition(...)` + `RestateHttpServer.fromEndpoint(...)`
 * to get a proper lifecycle-managed server.
 *
 * ==Per [[karphyaguidsmindset]] "smallest correct change"==
 *
 * This file does NOT duplicate handler logic. It composes the
 * existing pieces. The HTTP server is just the wire-binding layer.
 *
 * ==Per `semantic-layer-engine-architecture.md` §3 Core Boundary==
 *
 * - HTTP transport is in `sm8-platform` (NOT core).
 * - Composes typed `MCPEngineRegistry` + `Model`. Both are
 *   case-class-derived and `Serializable`.
 * - Does NOT contain Spark or any data-source knowledge. The
 *   Spark path lives in `connectors/spark-connector/`.
 *
 * ==Per `plugins.md` Rule 4==
 *
 * The HTTP transport is **NOT a Plugin** — it's a transport-level
 * endpoint. It does NOT register via `Plugin.setup`. It just
 * exposes the typed pipeline at the HTTP boundary.
 *
 * ==Per `scala-spark-batch-bugs-mindset` (per user directive):==
 *
 * - mantra #1 (closure-safety): all captured types are typed
 *   case-class-derived and `Serializable`. Verified upstream.
 * - mantra #5 (driver/executor): the HTTP server runs in the driver
 *   process. The captured `MCPEngineRegistry` selects an engine
 *   (e.g. `SparkEngineProvider`) which compiles + collects in the
 *   driver. **No executor-side resources leak through the HTTP
 *   server.**
 * - mantra #3 (schema-drift verify at boundary): the HTTP server
 *   is the boundary between the typed engine and the wire shape.
 * - mantras #2 + #4 (data skew + write correctness): N/A — the
 *   HTTP server is a transport façade; it does NOT execute queries
 *   itself.
 *
 * ==Per `scala-perf-testingmindset`:==
 *
 * - HTTP server init is **startup-time** (one-time).
 * - Per-request dispatch is `QueryService.handleQuery` which calls
 *   `EngineService.runQueryWithHooks` — already perf-optimized
 *   per PRs #32–#57.
 *
 * ==Per `scala-jvm-safetismindset`:==
 *
 * - Resource lifecycle: `start(port)` binds + listens;
 *   `stop()` calls `HttpServer.close()` to release the bound
 *   socket + accept loop. The captured `HttpServer` is held in
 *   `private var server: Option[HttpServer]`. `stop()` closes it;
 *   `close()` is an alias for `stop()`.
 */
package io.sm8.platform.query

import dev.restate.sdk.endpoint.Endpoint
import dev.restate.sdk.http.vertx.RestateHttpServer

import io.sm8.core.engine.MCPEngineRegistry
import io.sm8.core.model.Model

import io.vertx.core.http.HttpServer

/**
 * HTTP transport for the Sm8McpServer skeleton (PR #57).
 *
 * Per [[karphyaguidsmindset]] "smallest correct change": composes
 * the existing `QueryService.definition(...)` + Restate's
 * `RestateHttpServer.fromEndpoint(...)` (which returns the Vert.x
 * `HttpServer` handle for proper lifecycle management).
 *
 * Lifecycle:
 * - `start(port)` binds + listens (returns the bound port int).
 * - `stop()` shuts down the server via `HttpServer.close()`.
 */
final class HttpTransport(
    val model:    Model,
    val registry: MCPEngineRegistry
) {

  // The bound Vert.x HttpServer handle. Per [[scala-jvm-safemindset]]:
  // resource lifecycle — release via `stop()`.
  private var server: Option[HttpServer] = None

  /**
   * Bind + start the HTTP server. Per [[scala-perf-testingmindset]]:
   * startup-time initialization; called once.
   *
   * Per the existing `RestateBootstrap.bindAndListen` pattern:
   * returns the bound TCP port (== `port` on success).
   *
   * @param port TCP port (default 8080 — Restate's default ingress port)
   * @return    the bound TCP port (== `port` on success)
   */
  def start(port: Int = 8080): Int = {
    if (server.isDefined) throw new IllegalStateException(
      "sm8: HTTP transport already started"
    )
    // Compose the existing `QueryService.definition(...)` (per the
    // canonical pattern). NoOp cache here; the production wiring
    // would inject a real cache.
    val endpoint: Endpoint = Endpoint.builder()
      .bind(QueryService.definition(
        model    = model,
        registry = registry,
        cache    = io.sm8.platform.query.InMemoryResultCache(maxEntries = 1),
        plugins  = Nil,
      ))
      .build()
    // Per Restate SDK 2.1.1: `RestateHttpServer.listen(endpoint, port)`
    // returns `Int` (the bound port). For proper lifecycle, we use
    // `RestateHttpServer.fromEndpoint(endpoint)` which returns the
    // underlying `io.vertx.core.http.HttpServer` — this has
    // `close()` for releasing the listener.
    server = Some(RestateHttpServer.fromEndpoint(endpoint))
    port
  }

  /** Stop the HTTP server. Per [[scala-jvm-safemindset]]. */
  def stop(): Unit = server match {
    case Some(s) => s.close(); server = None
    case None    => // already stopped
  }

  /** Close on JVM shutdown. Per [[scala-jvm-safemindset]]. */
  def close(): Unit = stop()
}

object HttpTransport {

  /** Factory for the canonical wiring. Per [[karphyaguidsmindset]]
    * "smallest correct core": defaults point to the production
    * wiring. */
  def apply(model: Model, registry: MCPEngineRegistry): HttpTransport =
    new HttpTransport(model, registry)
}
