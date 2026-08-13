/*
 * SM8 Platform — Restate HTTP server bootstrap.
 *
 * Per [[karpathy-guidelines-mindset]]: a thin wrapper around
 * `dev.restate.sdk.http.vertx.RestateHttpServer.listen(endpoint,
 * port)`. Exists to encapsulate the static factory + Endpoint
 * construction, so callers (production + tests) don't repeat the
 * boilerplate.
 *
 * Per [[scala-jvm-safety-mindset]] "resource lifecycle": `bindAnd
 * Listen` returns the bound TCP port (fire-and-forget — the
 * server runs in background threads owned by the Restate SDK +
 * Vert.x event-loop). The follow-up PR-C-final-int (Docker-
 * gated integration test) will use Testcontainers' shutdown
 * hook to release the server.
 *
 * Per [[scala-impact-analysis-mindset]] "name every caller": the
 * only callers in the reactor today are absent — production
 * entry-point wiring lands in PR-C-final-int (deferred per plan).
 * This file ships NOW so the follow-up PR doesn't reintroduce
 * boilerplate.
 *
 * ==Why no `bind()` returning the Vert.x HttpServer==
 *
 * `io.vertx:vertx-core` is exposed by `sdk-http-vertx` only at
 * `runtime` scope — not compile. Adding `bind()` here would force
 * us to either bump vertx-core to compile scope (heavier JAR
 * for the public API) or carry a "no return type" abstraction.
 * Per [[karpathy-guidelines-mindset]] "smallest correct change",
 * we ship only `bindAndListen` (returns `Int` — sufficient for
 * the follow-up integration test). When production lifecycle
 * hooks are needed (Spring `@PreDestroy`), the follow-up PR
 * adds `bind(...)` explicitly and bumps vertx-core to compile
 * scope.
 */
package io.sm8.platform.query

import dev.restate.sdk.endpoint.Endpoint
import dev.restate.sdk.http.vertx.RestateHttpServer

import io.sm8.core.engine.MCPEngineRegistry
import io.sm8.core.model.Model

/**
 * Boot the Restate HTTP server with the engine-portable `QueryService`.
 *
 * Static methods only (no state). Per [[karpathy-guidelines-mindset]],
 * the helpers are explicit (no implicit DI, no builder ceremony) —
 * callers pass the (Serializable) `Model` + `MCPEngineRegistry` +
 * `ResultCache` directly.
 */
object RestateBootstrap {

  /**
   * Bind the QueryService on the supplied port and return the bound
   * port int (fire-and-forget). The server runs in background
   * Vert.x event-loop threads; this call BLOCKS until the server
   * is bound (per Restate SDK contract — `RestateHttpServer.listen`
   * is synchronous to bind-and-return-port).
   *
   * Suitable for tests + dev.
   *
   * @param model    the engine-portable model
   * @param registry the engine-portable registry
   * @param cache    the result cache
   * @param port     TCP port (default 8080 — Restate's default
   *                 ingress port)
   * @return         the bound TCP port (== `port` on success)
   */
  def bindAndListen(
      model: Model,
      registry: MCPEngineRegistry,
      cache: ResultCache,
      port: Int = 8080
  ): Int = {
    // Per [[scala-data-driven-refactor-mindset]] "sealed-trait
    // dispatch": `Endpoint.Builder.bind(ServiceDefinition)` is
    // the v2.x-shaped registration API (no `bind(Object)`
    // reflection).
    val endpoint: Endpoint = Endpoint.builder()
      .bind(QueryService.definition(model, registry, cache))
      .build()
    RestateHttpServer.listen(endpoint, port)
  }
}
