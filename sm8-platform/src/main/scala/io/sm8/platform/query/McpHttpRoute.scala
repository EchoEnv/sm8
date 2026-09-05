/*
 * SM8 MCP — McpHttpRoute composition root (ADR-0021 Phase 2b).
 *
 * Wires the 3 collaborators (McpHttpServer / McpSessionRegistry /
 * McpMessageRouter) into the transport-provider contract the SDK
 * expects. Delegates every handler to the collaborators — this class
 * owns no protocol logic, no session state, and no Vert.x lifecycle
 * of its own.
 *
 * Per ADR-0021, the 671-LOC god-class this replaces held 3 unrelated
 * concerns (Vert.x lifecycle, session state, protocol handlers) in
 * one class. Each is now in its own file with a single
 * responsibility.
 *
 * Public API surface preserved:
 *   - `start(port, config, serverName, serverVersion, toolSpecs)`
 *     returns `(HttpServer, McpSyncServer, McpHttpRoute)` — same
 *     signature as before.
 *   - `stop(httpServer, syncServer)` — same signature as before.
 *   - `config` field — same type.
 *   - `setSessionFactory(factory)` — delegates to the server (the
 *     SDK calls this on the provider during buildServer).
 *   - `notifyClients(method, params)` — delegates to the registry.
 *   - `closeGracefully()` — delegates to server + registry.
 */
package io.sm8.platform.query

import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.spec.McpStreamableServerSession
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.spec.McpSchema

import io.vertx.core.http.HttpServer

import reactor.core.publisher.Mono

/** Streamable HTTP MCP transport for sm8 (composition root).
  *
  * Implements the SDK's `McpStreamableServerTransportProvider`
  * interface by delegating to the 3 collaborators:
  *   - `server`   — Vert.x lifecycle (start/stop/buildServer)
  *   - `sessions` — session registry (put/get/remove/notifyClients)
  *   - `router`   — MCP protocol handlers (JSON-RPC dispatch + SSE)
  *
  * Use the `start` factory (object) rather than `new McpHttpRoute(...)`
  * directly — the factory wires the SDK McpSyncServer (which calls
  * `setSessionFactory` on us) BEFORE we bind the Vert.x server.
  *
  * @param config the transport config (endpointPath, disallowDelete)
  */
final class McpHttpRoute private[sm8] (
    val config: McpHttpRoute.Config
) extends McpStreamableServerTransportProvider {

  /** Vert.x lifecycle: server creation, port binding, graceful
    * shutdown, and the SDK `McpSyncServer` construction. */
  private[query] val server = new McpHttpServer(config)

  /** Session registry: `ConcurrentHashMap` keyed by session-id +
    * `notifyClients` fan-out. */
  private[query] val sessions = new McpSessionRegistry

  /** MCP protocol handlers: JSON-RPC dispatch + SSE + transport
    * factory. Receives the session registry + a session-factory
    * lookup function that reads from the server. */
  private[query] val router = new McpMessageRouter(
    sessions,
    config,
    () => server.sessionFactory
  )

  /** SDK callback: `McpServer.sync(this).build()` calls this to wire
    * the session factory back to the transport provider. Delegates to
    * the server's AtomicReference slot. */
  override def setSessionFactory(factory: McpStreamableServerSession.Factory): Unit =
    server.setSessionFactory(factory)

  /** Server-push notification fan-out to every live session.
    * Delegates to the session registry. */
  override def notifyClients(method: String, params: Object): Mono[Void] =
    sessions.notifyClients(method, params)

  /** Graceful shutdown: stop the Vert.x server + clear the session
    * registry. Returns `Mono[Void]` per the SDK contract. */
  override def closeGracefully(): Mono[Void] = {
    server.stop()
    sessions.clear()
    Mono.empty
  }

  /** Entry point wired to the Vert.x server. Delegates to the
    * router's `handleRequest`. */
  private[query] def handleRequest(req: io.vertx.core.http.HttpServerRequest): Unit =
    router.handleRequest(req)
}

object McpHttpRoute {

  /** Disallow-Delete config knob: when true, DELETE short-circuits
    * with 405 Method Not Allowed. Per ADR-014 §Decision the default
    * is false (MCP clients expect DELETE for session cleanup). */
  final case class Config(
      endpointPath:   String  = "/mcp",
      disallowDelete: Boolean = false
  )

  /** Factory: build + start the route + return the bound Vert.x
    * HttpServer + the SDK McpSyncServer. Caller registers the
    * shutdown hook.
    *
    * Mirrors `MetricsHttpRoute.start()` (PR-258): 30s bind timeout,
    * fail-LOUD on bind failure (IllegalStateException), AtomicReference
    * slot pattern at the caller.
    */
  def start(
      port: Int,
      config: Config,
      serverName: String,
      serverVersion: String,
      toolSpecs: Seq[McpServerFeatures.SyncToolSpecification]
  ): (HttpServer, McpSyncServer, McpHttpRoute) = {
    val route = new McpHttpRoute(config)
    val syncServer = route.server.buildServer(route, serverName, serverVersion, toolSpecs)
    val httpServer = route.server.start(port, req => route.handleRequest(req))
    (httpServer, syncServer, route)
  }

  /** Stop the MCP HTTP server (called from the JVM shutdown hook,
    * sequenced inside ONE Runnable per ADR-014 r1 fix). */
  def stop(httpServer: HttpServer, syncServer: McpSyncServer): Unit = {
    try syncServer.closeGracefully()
    catch { case scala.util.control.NonFatal(_) => () }
    try {
      val f = httpServer.close()
      f.toCompletionStage.toCompletableFuture.get(5, java.util.concurrent.TimeUnit.SECONDS)
    } catch { case scala.util.control.NonFatal(_) => () }
  }
}
