/*
 * SM8 MCP — McpHttpServer (ADR-0021 Phase 2b).
 *
 * Vert.x lifecycle for the Streamable HTTP MCP transport: server
 * creation, port binding, graceful shutdown, and the SDK
 * `McpSyncServer` construction (which wires the
 * `setSessionFactory` callback back to the transport provider).
 *
 * Extracted from `McpHttpRoute` per ADR-0021 — Vert.x lifecycle is a
 * separate concern from session state (`McpSessionRegistry`) and MCP
 * protocol handling (`McpMessageRouter`). This class owns no session
 * state and no JSON-RPC parsing; it is pure server lifecycle.
 *
 * The `requestHandler` hook is injected by the composition root
 * (`McpHttpRoute`) so the server doesn't need to know about the
 * protocol router.
 */
package io.sm8.platform.query

import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpStreamableServerSession

import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.Vertx

import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

/** Vert.x lifecycle for the Streamable HTTP MCP transport.
  *
  * @param config the transport config (endpointPath, disallowDelete);
  *               the `endpointPath` is used by the route's request
  *               handler for path matching, not by the server itself.
  */
final class McpHttpServer(val config: McpHttpRoute.Config) {

  private val Log = LoggerFactory.getLogger(getClass)

  /** Vert.x runtime. One per `McpHttpServer` instance — Vert.x
    * instances are heavyweight (own thread pools); a 1:1 mapping with
    * the MCP HTTP server is the documented pattern. */
  private val vertx: Vertx = Vertx.vertx()

  private var boundServer: Option[HttpServer] = None

  /** The SDK injects this when `McpServer.sync(provider).build()` is
    * called. Per javap: `setSessionFactory(McpStreamableServerSession.Factory)`. */
  private val sessionFactoryRef =
    new java.util.concurrent.atomic.AtomicReference[McpStreamableServerSession.Factory]()

  /** SDK callback: `McpServer.sync(provider).build()` calls this to
    * wire the session factory back to the transport provider. */
  def setSessionFactory(factory: McpStreamableServerSession.Factory): Unit =
    sessionFactoryRef.set(factory)

  /** Read the current session factory (used by `McpMessageRouter` to
    * reject `initialize` before `buildServer` has fired). */
  def sessionFactory: Option[McpStreamableServerSession.Factory] =
    Option(sessionFactoryRef.get())

  /** Build the SDK `McpSyncServer`, wiring the given transport
    * provider (the route, which implements
    * `McpStreamableServerTransportProvider`). The SDK calls
    * `provider.setSessionFactory(...)` during this call, so the
    * provider must be alive when `buildServer` runs.
    *
    * `validateToolInputs(false)` is REQUIRED: the SDK's default
    * schema validator loads `com.networknt:json-schema-validator`
    * transitively, which conflicts with sm8-core's `ManifestValidator`
    * (1.5.2 uses `SpecVersion.VersionFlag`; 3.0.6 uses `Dialects`).
    * See `NoopJsonSchemaValidatorSupplier` for the ServiceLoader
    * workaround.
    *
    * @param provider the transport provider (the `McpHttpRoute`
    *                  instance, which extends
    *                  `McpStreamableServerTransportProvider`)
    * @param serverName the MCP server name advertised to clients
    * @param serverVersion the MCP server version advertised to clients
    * @param toolSpecs the tool specifications (from `Sm8ToolHandlers.build`)
    * @return the built `McpSyncServer`
    */
  def buildServer(
      provider: McpStreamableServerTransportProvider,
      serverName: String,
      serverVersion: String,
      toolSpecs: Seq[McpServerFeatures.SyncToolSpecification]
  ): McpSyncServer = {
    McpServer.sync(provider)
      .serverInfo(serverName, serverVersion)
      .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build())
      .tools(toolSpecs: _*)
      .validateToolInputs(false)
      .build()
  }

  /** Start the Vert.x HTTP server bound to `port`.
    *
    * The `requestHandler` is supplied by the composition root (the
    * route's `McpMessageRouter.handleRequest`) so the server doesn't
    * need to know about protocol handling.
    *
    * @param port the TCP port to bind (0.0.0.0)
    * @param requestHandler the per-request handler (from the route's
    *                       `McpMessageRouter`)
    * @return the bound `HttpServer`
    * @throws IllegalStateException if the bind fails (30s timeout)
    */
  def start(
      port: Int,
      requestHandler: io.vertx.core.Handler[io.vertx.core.http.HttpServerRequest]
  ): HttpServer = {
    val server = vertx.createHttpServer(
      new HttpServerOptions().setPort(port).setHost("0.0.0.0")
    )
    server.requestHandler { req =>
      try requestHandler.handle(req)
      catch { case NonFatal(e) =>
        Log.error("unhandled MCP HTTP request error", e)
        try req.response.setStatusCode(500).end(s"internal error: ${e.getClass.getSimpleName}")
        catch { case NonFatal(_) => () }
      }
    }
    val bound = try
      server.listen()
        .toCompletionStage.toCompletableFuture
        .get(30, TimeUnit.SECONDS)
    catch {
      case _: java.util.concurrent.ExecutionException =>
        throw new IllegalStateException(
          s"sm8: MCP HTTP server failed to bind port $port")
    }
    boundServer = Some(bound)
    bound
  }

  /** Graceful shutdown: close the bound Vert.x HTTP server (5s
    * timeout), logging (not throwing) on failure. */
  def stop(): Unit = {
    boundServer.foreach { srv =>
      try srv.close().toCompletionStage.toCompletableFuture.get(5, TimeUnit.SECONDS)
      catch { case NonFatal(e) => Log.warn("MCP HTTP server close failed", e) }
    }
  }
}
