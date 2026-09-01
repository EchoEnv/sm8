/*
 * SM8 Platform — HttpTransport (Step 11 of plan line 290, per ADR-006).
 *
 * HTTP transport for the SM8 MCP server. Composes the existing
 * `QueryService.definition(...)` + `RestateHttpServer.fromEndpoint(...)`
 * + Vert.x `listen(port)` to get a properly bound, lifecycle-managed
 * server.
 *
 * ==Per karphyaguidsmindset "smallest correct change"==
 *
 * This file does NOT duplicate handler logic. It composes the
 * existing pieces. The HTTP server is just the wire-binding layer.
 *
 * ==Per `semantic-layer-engine-architecture.md` §3 Core Boundary==
 *
 * - HTTP transport is in `sm8-platform` (NOT core).
 * - Composes typed `EngineRegistry` + `Model`. Both are
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
 *   process. The captured `EngineRegistry` selects an engine
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

import io.sm8.core.cache.ResultCache
import io.sm8.core.engine.EngineRegistry
import io.sm8.core.model.Model

import io.vertx.core.http.HttpServer

/**
 * HTTP transport for the SM8 MCP server (per ADR-006).
 *
 * Per karphyaguidsmindset "smallest correct change": composes
 * the existing `QueryService.definition(...)` + Restate's
 * `RestateHttpServer.fromEndpoint(...)` (which returns the Vert.x
 * `HttpServer` handle for proper lifecycle management).
 *
 * Lifecycle:
 * - `start(port)` binds + listens (awaits the bind future; returns
 *   the ACTUAL bound port — real for ephemeral `port = 0`).
 * - `stop()` shuts down the server via `HttpServer.close()`.
 *
 * @param model    the engine-portable model the server serves
 * @param registry the engine registry used for engine selection
 * @param cache    the result cache wired into the query service
 * @param plugins  plugins whose hooks are registered on the query
 *                 service's dispatcher (cache read/write, audit,
 *                 row-cap, broadcast/skew oracle, materialize).
 *                 Default `Nil` keeps the existing 3-arg call sites
 *                 unchanged.
 * @param metaInspectorEngineFn when defined, the `MetaInspectorService`
 *                 is bound on the same endpoint so `sm8 inspect <key>`
 *                 is served; the function returns the most recent
 *                 request's `Context.meta` (the deployment module
 *                 wires it to the hook pipeline).
 */
final class HttpTransport(
    val model:    Model,
    val registry: EngineRegistry,
    val cache:    ResultCache,
    val plugins:  Seq[io.sm8.sdk.Plugin] = Nil,
    val metaInspectorEngineFn: Option[() => Map[String, Any]] = None
) {

  // The bound Vert.x HttpServer handle. Per scala-jvm-safemindset:
  // resource lifecycle — release via `stop()`.
  private var server: Option[HttpServer] = None

  /** The composed Restate endpoint: `QueryService` (with the
    * caller-supplied `plugins` registered on its hook dispatcher)
    * plus `MetaInspectorService` when `metaInspectorEngineFn` is
    * defined. Built once; `start` binds it. Exposed package-private
    * so tests can introspect the bound services without binding a
    * socket.
    *
    * The caller-supplied `plugins` are passed through so their
    * hooks (cache read/write, audit, row-cap, broadcast/skew
    * oracle, materialize) are registered on the dispatcher; if a
    * `metaInspectorEngineFn` is provided, the MetaInspectorService
    * is bound on the same endpoint so `sm8 inspect <key>` is
    * served instead of a 404. */
  private[query] lazy val endpoint: Endpoint = {
    val baseEndpoint = Endpoint.builder()
      .bind(QueryService.definition(
        model    = model,
        registry = registry,
        cache    = cache,
        plugins  = plugins,
      ))
      // Per [[ADR-012-a]] (`docs/adr/0012-a-modelservice-restate-handler.md`):
      // bind ModelService alongside QueryService. ModelService is a
      // stateless reader over the captured `model` reference, so
      // binding it after QueryService doesn't change QueryService's
      // semantics.
      .bind(ModelService.definition(model))
      // Per [[ADR-012-b]] (`docs/adr/0012-b-metricsservice-restate-handler.md`):
      // bind MetricsService. No captured params — the placeholder
      // counters are a constant zero until ADR-012-b-followup
      // instruments the call sites. Same SERVICE+SHARED rationale
      // as MetaInspectorService (state lives in sm8-platform, not
      // in a Restate journal).
      .bind(MetricsService.definition())
    metaInspectorEngineFn match {
      case Some(engineFn) =>
        baseEndpoint
          .bind(MetaInspectorService.definition(model, registry, engineFn))
          .build()
      case None =>
        baseEndpoint.build()
    }
  }

  /**
   * Bind + start the HTTP server. Per [[scala-perf-testing-mindset]]:
   * startup-time initialization; called once.
   *
   * Awaits the Vert.x bind future — bind failures throw
   * `IllegalStateException` (fail loud, per [[scala-jvm-safety-mindset]]).
   *
   * @param port TCP port (default 8080 — Restate's default ingress
   *             port; `0` = ephemeral)
   * @return    the ACTUAL bound TCP port (`actualPort()`)
   */
  def start(port: Int = 8080): Int = {
    if (server.isDefined) throw new IllegalStateException(
      "sm8: HTTP transport already started"
    )
    // Per Restate SDK 2.1.1 + Vert.x 4.5.x: `fromEndpoint(endpoint)`
    // returns an UN-listening Vert.x HttpServer; `.listen(port)` is
    // what actually binds the socket. Await the future so bind
    // failures (port in use, etc.) fail LOUD here (per
    // scala-jvm-safetymindset) — not silently at first request.
    // Return `actualPort()` so `port = 0` (ephemeral) reports the
    // real bound port.
    val vertxServer = RestateHttpServer.fromEndpoint(endpoint)
    val bound = try
      vertxServer.listen(port)
        .toCompletionStage.toCompletableFuture
        .get(30, java.util.concurrent.TimeUnit.SECONDS)
    catch {
      case _: java.util.concurrent.ExecutionException =>
        throw new IllegalStateException(
          s"sm8: HTTP transport failed to bind port $port")
    }
    server = Some(bound)
    bound.actualPort()
  }

  /** Stop the HTTP server. Per scala-jvm-safemindset. */
  def stop(): Unit = server match {
    case Some(s) => s.close(); server = None
    case None    => // already stopped
  }

  /** Close on JVM shutdown. Per scala-jvm-safemindset. */
  def close(): Unit = stop()
}

object HttpTransport {

  /** Factory for the canonical wiring. The caller (deployment
    * module) supplies the cache, plugins, and meta inspector; the
    * new params default to the existing behaviour so existing 3-arg
    * call sites are unchanged. */
  def apply(
      model:    Model,
      registry: EngineRegistry,
      cache:    ResultCache,
      plugins:  Seq[io.sm8.sdk.Plugin] = Nil,
      metaInspectorEngineFn: Option[() => Map[String, Any]] = None
  ): HttpTransport =
    new HttpTransport(model, registry, cache, plugins, metaInspectorEngineFn)
}
