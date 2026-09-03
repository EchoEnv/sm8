/*
 * SM8 Platform — McpHttpRoute.
 *
 * Per ADR-014 (PR-261, merged): Streamable HTTP MCP transport for sm8.
 * A standalone Vert.x HttpServer on its own port (mirror the PR-258
 * MetricsHttpRoute sidecar pattern) that implements the MCP
 * `McpStreamableServerTransportProvider` interface over plain HTTP +
 * SSE. The 5 MCP tools (defined via `SyncToolSpecification` list) are
 * passed in by the caller — this file is transport-only.
 *
 * ==Why a custom Vert.x adapter (not the SDK's Servlet one)==
 *
 * Per ADR-014 §Decision: sm8-server uses vertx-core, not vertx-web
 * or Jakarta Servlet. The SDK ships `HttpServletStreamableServerTransportProvider`
 * which extends `jakarta.servlet.http.HttpServlet` — incompatible
 * with sm8's Vert.x core. Custom adapter: ~250 LOC using
 * `vertx-core.HttpServer.requestHandler` + manual SSE chunked-write.
 *
 * ==APIs verified before writing== (PR-257 r2 catch pattern)
 *
 * - `io.modelcontextprotocol.spec.McpStreamableServerTransportProvider`
 *   (extends McpServerTransportProviderBase): setSessionFactory,
 *   notifyClients, notifyClient (default), closeGracefully, close
 *   (default). All javap-verified on mcp-core-2.0.1.jar.
 * - `io.modelcontextprotocol.spec.McpStreamableServerSession`
 *   methods: getId, sendNotification, delete, listeningStream(transport),
 *   responseStream(request, transport), replay(id), accept(notif/response),
 *   closeGracefully. javap-verified.
 * - `io.modelcontextprotocol.spec.McpStreamableServerTransport`
 *   (per-session): sendMessage(JSONRPCMessage, String) -> Mono<Void>.
 * - `io.modelcontextprotocol.spec.McpStreamableServerSession.Factory`:
 *   startSession(InitializeRequest) -> McpStreamableServerSessionInit(session, initResult Mono).
 * - `io.modelcontextprotocol.spec.McpStreamableServerSession$McpStreamableServerSessionInit`:
 *   record (session, initResult Mono).
 * - `io.modelcontextprotocol.spec.McpSchema.deserializeJsonRpcMessage(mapper, body)`:
 *   static utility for JSON-RPC body parsing.
 *
 * ==Architecture==
 *
 * The class is BOTH the transport provider AND the request router.
 * The outer `McpHttpRoute` implements `McpStreamableServerTransportProvider`;
 * it stores the SDK-provided session factory (set via setSessionFactory
 * when `McpServer.sync(...).build()` is called) and the Vert.x
 * HttpServer. The `start(...)` factory method wires the route handlers
 * and returns the bound server. Lifecycle: install-hook-before-bind
 * + AtomicReference slot, mirror PR-258 MetricsHttpRoute pattern.
 *
 * ==HTTP surface (per ADR-014 §Decision table + the SDK's Servlet impl)==
 *
 * POST /mcp   initialize                -> 200 + result + Mcp-Session-Id header
 * POST /mcp   request                   -> 200 + SSE stream (response(s))
 * POST /mcp   notification              -> 202 Accepted
 * POST /mcp   response                  -> 202 Accepted
 * GET  /mcp                             -> 200 + SSE listening stream (or replay)
 * DELETE /mcp                           -> 200 OK + session closed (or 405 if disallowDelete)
 *
 * Accept-header validation is REQUIRED on every POST (both
 * application/json AND text/event-stream) and on GET
 * (text/event-stream only) per SDK doPost/doGet source.
 *
 * ==Lifecycle (per ADR-014 §Lifecycle + r1 fix)==
 *
 * One shared JVM shutdown Runnable sequences: (a) MCP HTTP
 * teardown (close SSE streams + Vert.x server.close), (b) metrics
 * stop, (c) Restate stop. Sequencing INSIDE one Runnable (JVM hook
 * order is not guaranteed). Mirror PR-258 MetricsHttpRoute pattern.
 *
 * ==Spark closure serialization==
 *
 * No Spark involvement (sm8-platform's Spark surface is elsewhere).
 * The route handlers capture only `vertx`, `endpointPath`, `mapper`,
 * `sessionFactory`, and the `sessions` ConcurrentHashMap — all
 * thread-safe.
 *
 * ==Performance==
 *
 * Each session creates a small thread (the SDK's responseStream
 * uses a per-session Reactor scheduler; the listeningStream uses
 * SSE chunked-write). Sessions are bounded by the SSE connection
 * lifecycle; idle sessions stay in the `sessions` map until DELETE
 * or JVM shutdown. No background timer in v1 (matches SDK's
 * default `keepAliveInterval = null`).
 *
 * ==OOM==
 *
 * `sessions` is a ConcurrentHashMap; entries removed on DELETE +
 * on listening-stream completion. No unbounded buffers: SSE chunks
 * are flushed as written, not accumulated.
 *
 * ==NPE==
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar": all `Option`-like
 * access is explicit. `args.get(key)` returns `null` on miss → we
 * short-circuit. `req.path()` returns `String`, never null. Vert.x
 * response writes return futures we never block on (we register
 * handlers instead).
 *
 * ==building-restate-services==
 *
 * This is OUTSIDE Restate's journal pipeline (plain HTTP server,
 * not a Restate handler). `Instant.now()` is correct here even
 * though it's a no-no inside Restate handler closures.
 */
package io.sm8.platform.query

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.{McpServer, McpServerFeatures, McpSyncServer}
import io.modelcontextprotocol.spec._
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.{HttpServer, HttpServerOptions, HttpServerResponse}
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono

import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.util.control.NonFatal

/** Streamable HTTP MCP transport for sm8. Implements the SDK's
  * `McpStreamableServerTransportProvider` interface over plain Vert.x
  * HTTP + SSE. The 5 tools are passed in by the caller; this class
  * is transport-only. Use the `start` factory (object) rather than
  * `new McpHttpRoute(...)` directly — the factory wires the SDK
  * McpSyncServer (which calls setSessionFactory on us) BEFORE we
  * bind the Vert.x server. */
final class McpHttpRoute private[sm8] (
    val config: McpHttpRoute.Config
) extends McpStreamableServerTransportProvider {

  private val Log = LoggerFactory.getLogger(getClass)

  // The SDK injects this when McpServer.sync(this).build() is called.
  // Per javap: setSessionFactory(McpStreamableServerSession.Factory).
  private val sessionFactoryRef =
    new java.util.concurrent.atomic.AtomicReference[McpStreamableServerSession.Factory]()

  override def setSessionFactory(factory: McpStreamableServerSession.Factory): Unit =
    sessionFactoryRef.set(factory)

  // Active sessions, keyed by session-id. Removed on DELETE or
  // when the listening stream closes.
  private val sessions =
    new ConcurrentHashMap[String, McpStreamableServerSession]()

  private val vertx: Vertx = Vertx.vertx()

  private var boundServer: Option[HttpServer] = None

  /** Build the SDK McpSyncServer (this wires the sessionFactory via
    * setSessionFactory). Call after constructing McpHttpRoute and
    * passing it to McpServer.sync(...).build(). The tools MUST be
    * passed via `sync(this).tools(...)` BEFORE build().
    *
    * `validateToolInputs(false)` is REQUIRED: the SDK's default
    * schema validator loads `com.networknt:json-schema-validator`
    * transitively, which conflicts with sm8-core's `ManifestValidator`
    * (1.5.2 uses `SpecVersion.VersionFlag`; 3.0.6 uses `Dialects`).
    * PR-263 ships with an empty tool list (the 5 PR-260 tools stay
    * wired via the stdio subprocess), so schema validation is
    * unnecessary anyway. If a future PR bridges the 5 tools into
    * this HTTP transport, the bridge PR must either (a) use a
    * MCP-SDK-supplied validator (drop our networknt dep entirely)
    * or (b) wire a custom `JsonSchemaValidatorSupplier` via
    * META-INF/services. See ADR-014 §Decision for the trade-off. */
  def buildServer(
      serverName: String,
      serverVersion: String,
      toolSpecs: Seq[McpServerFeatures.SyncToolSpecification]
  ): McpSyncServer = {
    McpServer.sync(this)
      .serverInfo(serverName, serverVersion)
      .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build())
      .tools(toolSpecs: _*)
      .validateToolInputs(false)
      .build()
  }

  /** Start the Vert.x HTTP server bound to `port`. The McpServer
    * must already be built (caller calls buildServer first) so that
    * setSessionFactory has fired and sessionFactoryRef is non-null. */
  def start(port: Int): HttpServer = {
    val server = vertx.createHttpServer(
      new HttpServerOptions().setPort(port).setHost("0.0.0.0")
    )
    server.requestHandler { req =>
      try handleRequest(req)
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

  override def notifyClients(method: String, params: Object): Mono[Void] =
    Mono.fromRunnable(new java.lang.Runnable {
      def run(): Unit = {
        sessions.values.parallelStream.forEach { session =>
          try session.sendNotification(method, params).block()
          catch { case NonFatal(e) =>
            Log.warn(s"notifyClients: sendNotification failed for session ${session.getId}", e)
          }
        }
      }
    })

  override def closeGracefully(): Mono[Void] = {
    boundServer.foreach { srv =>
      try srv.close().toCompletionStage.toCompletableFuture.get(5, TimeUnit.SECONDS)
      catch { case NonFatal(e) => Log.warn("MCP HTTP server close failed", e) }
    }
    sessions.clear()
    Mono.empty
  }

  /** Per-request routing. Mirrors the SDK's
    * HttpServletStreamableServerTransportProvider.doGet/doPost/doDelete.
    * The Vert.x request handler is async, so body reading for POST
    * must be done via Vert.x's body handler before we can dispatch. */
  private def handleRequest(req: io.vertx.core.http.HttpServerRequest): Unit = {
    if (!req.path().endsWith(config.endpointPath)) {
      req.response.setStatusCode(404).end("not found")
      return
    }
    req.method() match {
      case io.vertx.core.http.HttpMethod.POST  =>
        // Accumulate the body via Vert.x's async handlers, then dispatch
        // synchronously. The body is bounded by the SDK's default 16MB
        // request size (json-schema-validator default); MCP tool calls
        // are < 10 KiB in practice.
        val sb = new java.lang.StringBuilder()
        req.bodyHandler(new io.vertx.core.Handler[Buffer] {
          def handle(buf: Buffer): Unit = {
            // Guard against oversized payloads. 16MB matches the SDK
            // bound; reject earlier so we don't grow the StringBuilder
            // unbounded across chunked uploads.
            if (sb.length + buf.length() > 16L * 1024L * 1024L) {
              try req.response.setStatusCode(413).end("payload too large")
              catch { case NonFatal(_) => () }
              return
            }
            sb.append(buf.toString("UTF-8"))
            try handlePostBody(req, sb.toString)
            catch { case NonFatal(e) =>
              Log.error("MCP HTTP POST error", e)
              try req.response.setStatusCode(500).end(s"internal error: ${e.getClass.getSimpleName}")
              catch { case NonFatal(_) => () }
            }
          }
        })
      case io.vertx.core.http.HttpMethod.GET   => handleGet(req); ()
      case io.vertx.core.http.HttpMethod.DELETE => handleDelete(req); ()
      case _        => req.response.setStatusCode(405).end("method not allowed")
    }
  }

  /** POST handler (called AFTER body has been read by Vert.x).
    * 3 sub-cases (initialize / request / notification+response) per
    * ADR-014 + SDK doPost source. */
  private def handlePostBody(
      req: io.vertx.core.http.HttpServerRequest,
      body: String
  ): Unit = {
    val accept = req.getHeader("Accept")
    if (accept == null || !accept.contains("application/json") || !accept.contains("text/event-stream")) {
      req.response.setStatusCode(400).end(
        """{"error":"Accept must include both application/json and text/event-stream"}"""
      )
      return
    }
    val mapper = mapperFor(req)
    val message = try McpSchema.deserializeJsonRpcMessage(mapper, body)
    catch { case NonFatal(e) =>
      req.response.setStatusCode(400).end(
        s"""{"error":"invalid JSON-RPC: ${e.getClass.getSimpleName}"}"""
      )
      return
    }
    message match {
      case jsonrpcReq: McpSchema.JSONRPCRequest
          if jsonrpcReq.method() == McpSchema.METHOD_INITIALIZE =>
        handleInitialize(req, jsonrpcReq)
      case jsonrpcReq: McpSchema.JSONRPCRequest =>
        handlePostRequest(req, jsonrpcReq)
      case notif: McpSchema.JSONRPCNotification =>
        handlePostNotification(req, notif)
      case resp: McpSchema.JSONRPCResponse =>
        handlePostResponse(req, resp)
      case _ =>
        req.response.setStatusCode(400).end(
          """{"error":"unknown JSON-RPC message type"}"""
        )
    }
  }

  /** POST initialize: 200 OK + InitializeResult body + Mcp-Session-Id
    * header. Mirrors SDK doPost lines 215-444 (verified via source
    * fetch in PR-261).
    *
    * Per the r1 dual-review catch (Q1): MCP spec requires
    * `protocolVersion` and `clientInfo` in the initialize params.
    * The SDK's `startSession(...)` is lenient (accepts null fields)
    * and `convertValue` may return a partial record. We pre-validate
    * explicitly and emit a JSON-RPC `error` (code -32602 INVALID_PARAMS)
    * with the request's `id` if either field is missing. The session
    * IS removed on any failure (don't leak an entry in `sessions`).
    */
  private def handleInitialize(
      req: io.vertx.core.http.HttpServerRequest,
      jsonrpcReq: McpSchema.JSONRPCRequest
  ): Unit = {
    val factory = sessionFactoryRef.get()
    if (factory == null) {
      req.response.setStatusCode(503).end("""{"error":"MCP server not initialized"}""")
      return
    }
    val mapper = mapperFor(req)
    val resp = req.response

    // Pre-validate the params map. The MCP spec requires `protocolVersion`
    // and `clientInfo`; reject with -32602 INVALID_PARAMS if either is
    // missing. The SDK's JSONRPCRequest.params() returns `Object` (the
    // Jackson-decoded Map<String,Object>); cast to Map for the containsKey
    // check. Per r1 dual-review catch (Q1): a missing/partial params map
    // would otherwise throw inside startSession and surface as a raw 500
    // instead of a JSON-RPC error.
    val rawParams = jsonrpcReq.params()
    val params = rawParams match {
      case m: java.util.Map[_, _] @unchecked => m.asInstanceOf[java.util.Map[String, Object]]
      case _ => null
    }
    if (params == null
        || !params.containsKey("protocolVersion")
        || params.get("protocolVersion") == null
        || !params.containsKey("clientInfo")
        || params.get("clientInfo") == null) {
      Log.warn(s"initialize rejected: missing protocolVersion or clientInfo in params: $params")
      val errorJson = mapper.writeValueAsString(
        McpSchema.JSONRPCResponse.error(
          jsonrpcReq.id(),
          new McpSchema.JSONRPCResponse.JSONRPCError(
            -32602,  // INVALID_PARAMS per JSON-RPC 2.0
            "Missing required initialize params: protocolVersion and clientInfo",
            null
          )
        )
      )
      resp.setStatusCode(400)
      resp.putHeader("Content-Type", "application/json")
      resp.end(errorJson)
      return
    }

    val init = try {
      val initReq = mapper.convertValue(
        params,
        new io.modelcontextprotocol.json.TypeRef[McpSchema.InitializeRequest] {}
      )
      factory.startSession(initReq)
    } catch { case NonFatal(e) =>
      Log.warn(s"initialize failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
      val errorJson = mapper.writeValueAsString(
        McpSchema.JSONRPCResponse.error(
          jsonrpcReq.id(),
          new McpSchema.JSONRPCResponse.JSONRPCError(
            -32602,  // INVALID_PARAMS per JSON-RPC 2.0
            s"Invalid initialize params: ${e.getMessage}",
            null
          )
        )
      )
      resp.setStatusCode(400)
      resp.putHeader("Content-Type", "application/json")
      resp.end(errorJson)
      return
    }
    val sessionId = init.session().getId()
    sessions.put(sessionId, init.session())
    val initResult = try init.initResult().block(Duration.ofSeconds(10))
    catch { case NonFatal(e) =>
      Log.warn(s"init.initResult() failed for session $sessionId: ${e.getMessage}")
      sessions.remove(sessionId)
      val errorJson = mapper.writeValueAsString(
        McpSchema.JSONRPCResponse.error(
          jsonrpcReq.id(),
          new McpSchema.JSONRPCResponse.JSONRPCError(
            -32603,  // INTERNAL_ERROR per JSON-RPC 2.0
            s"Init handler error: ${e.getMessage}",
            null
          )
        )
      )
      resp.setStatusCode(500)
      resp.putHeader("Content-Type", "application/json")
      resp.end(errorJson)
      return
    }
    if (initResult == null) {
      req.response.setStatusCode(500).end("""{"error":"init timeout"}""")
      sessions.remove(sessionId)
      return
    }
    val respJson = mapper.writeValueAsString(
      McpSchema.JSONRPCResponse.result(jsonrpcReq.id(), initResult)
    )
    resp.setStatusCode(200)
    resp.putHeader("Content-Type", "application/json")
    resp.putHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID, sessionId)
    resp.end(respJson)
  }

  /** POST JSON-RPC request (not initialize): 200 OK + SSE stream of
    * the response. Mirrors SDK doPost request branch (lines ~470-540). */
  private def handlePostRequest(
      req: io.vertx.core.http.HttpServerRequest,
      jsonrpcReq: McpSchema.JSONRPCRequest
  ): Unit = {
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId)
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    val mapper = mapperFor(req)
    val resp = req.response
    resp.setStatusCode(200)
    resp.putHeader("Content-Type", "text/event-stream")
    resp.putHeader("Cache-Control", "no-cache")
    resp.putHeader("Connection", "keep-alive")
    resp.setChunked(true)

    val sessionTransport = newTransportForResp(mapper, resp, sessionId, withStatusFallback = true)
    try {
      session.responseStream(jsonrpcReq, sessionTransport).block(Duration.ofSeconds(60))
    } catch { case NonFatal(e) =>
      Log.error(s"responseStream failed for session $sessionId", e)
    }
    try resp.end() catch { case NonFatal(_) => () }
  }

  /** POST JSON-RPC notification: 202 Accepted. Mirrors SDK doPost
    * notification branch (lines ~460-470). */
  private def handlePostNotification(
      req: io.vertx.core.http.HttpServerRequest,
      notif: McpSchema.JSONRPCNotification
  ): Unit = {
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId)
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    try session.accept(notif).block(Duration.ofSeconds(10))
    catch { case NonFatal(e) => Log.error(s"accept(notification) failed: $sessionId", e) }
    req.response.setStatusCode(202).end()
  }

  /** POST JSON-RPC response (client→server flow): 202 Accepted.
    * Mirrors SDK doPost response branch (lines ~445-460). */
  private def handlePostResponse(
      req: io.vertx.core.http.HttpServerRequest,
      resp: McpSchema.JSONRPCResponse
  ): Unit = {
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId)
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    try session.accept(resp).block(Duration.ofSeconds(10))
    catch { case NonFatal(e) => Log.error(s"accept(response) failed: $sessionId", e) }
    req.response.setStatusCode(202).end()
  }

  /** GET handler: opens an SSE listening stream (or replays if
    * Last-Event-ID is present). Mirrors SDK doGet source. */
  private def handleGet(req: io.vertx.core.http.HttpServerRequest): Unit = {
    val accept = req.getHeader("Accept")
    if (accept == null || !accept.contains("text/event-stream")) {
      req.response.setStatusCode(400).end("""{"error":"Accept must include text/event-stream"}""")
      return
    }
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId)
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    val lastEventId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.LAST_EVENT_ID)
    val mapper = mapperFor(req)
    val resp = req.response
    resp.setStatusCode(200)
    resp.putHeader("Content-Type", "text/event-stream")
    resp.putHeader("Cache-Control", "no-cache")
    resp.putHeader("Connection", "keep-alive")
    resp.setChunked(true)

    val sessionTransport = newTransportForResp(mapper, resp, sessionId, withStatusFallback = false)
    try {
      if (lastEventId != null && !lastEventId.isBlank) {
        session.replay(lastEventId)
          .toIterable
          .forEach(message => {
            val jsonText = mapper.writeValueAsString(message)
            writeSseEvent(resp, "message", jsonText, lastEventId)
          })
        try resp.end() catch { case NonFatal(_) => () }
      } else {
        // Live listening path: open a stream and block the request
        // handler until the session closes. The SDK's
        // McpStreamableServerSessionStream is a Mono<Void> that
        // completes when the stream is closed.
        val stream = session.listeningStream(sessionTransport)
        stream.closeGracefully().block(Duration.ofMinutes(30))
        try resp.end() catch { case NonFatal(_) => () }
      }
    } catch { case NonFatal(e) =>
      Log.error(s"GET stream failed for session $sessionId", e)
      try resp.end() catch { case NonFatal(_) => () }
    }
  }

  /** DELETE handler: closes the session. Mirrors SDK doDelete source. */
  private def handleDelete(req: io.vertx.core.http.HttpServerRequest): Unit = {
    if (config.disallowDelete) {
      req.response.setStatusCode(405).end("""{"error":"DELETE not allowed (disallowDelete=true)"}""")
      return
    }
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.remove(sessionId)
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    try session.delete().block(Duration.ofSeconds(10))
    catch { case NonFatal(e) => Log.error(s"delete failed for session $sessionId", e) }
    req.response.setStatusCode(200).end()
  }

  /** Build a per-session `McpStreamableServerTransport` that writes
    * SSE events to the given HttpServerResponse. Implements the
    * inherited McpTransport methods (sendMessage(message),
    * closeGracefully(), unmarshalFrom()) which
    * McpStreamableServerTransport inherits but doesn't override. */
  private def newTransportForResp(
      mapper: McpJsonMapper,
      resp:   HttpServerResponse,
      sessionId: String,
      withStatusFallback: Boolean
  ): McpStreamableServerTransport = new McpStreamableServerTransport {
    override def sendMessage(message: McpSchema.JSONRPCMessage): Mono[Void] =
      sendMessage(message, null)
    override def sendMessage(message: McpSchema.JSONRPCMessage, messageId: String): Mono[Void] =
      Mono.fromRunnable(new java.lang.Runnable {
        def run(): Unit = {
          try {
            val jsonText = mapper.writeValueAsString(message)
            writeSseEvent(resp, "message", jsonText, messageId)
          } catch { case NonFatal(e) =>
            Log.error(s"sendMessage failed for session $sessionId", e)
            if (withStatusFallback) {
              try resp.setStatusCode(500).end("session transport error")
              catch { case NonFatal(_) => () }
            }
          }
        }
      })
    override def closeGracefully(): Mono[Void] = Mono.empty
    override def unmarshalFrom[T](data: Object, typeRef: io.modelcontextprotocol.json.TypeRef[T]): T =
      mapper.convertValue(data, typeRef).asInstanceOf[T]
  }

  /** SSE chunked-write for one event. Format (per SDK HttpServlet
    * sendEvent at lines 477-487): id + event + data, terminated by
    * a blank line (\n\n). */
  private def writeSseEvent(
      resp: HttpServerResponse,
      eventType: String,
      data: String,
      id: String
  ): Unit = {
    val buf = new StringBuilder()
    if (id != null) buf.append("id: ").append(id).append('\n')
    buf.append("event: ").append(eventType).append('\n')
    buf.append("data: ").append(data).append("\n\n")
    resp.write(Buffer.buffer(buf.toString))
  }

  /** Lazy MCP JSON mapper (Jackson 3 via McpJsonMapper). One per
    * instance — Vert.x handlers are concurrent so the mapper must
    * be thread-safe (Jackson 3 ObjectMapper is documented
    * thread-safe after configuration). */
  private val mapper: McpJsonMapper =
    new JacksonMcpJsonMapper(tools.jackson.databind.json.JsonMapper.builder().build())

  private def mapperFor(req: io.vertx.core.http.HttpServerRequest): McpJsonMapper = mapper
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
    * slot pattern at the caller. */
  def start(
      port: Int,
      config: Config,
      serverName: String,
      serverVersion: String,
      toolSpecs: Seq[McpServerFeatures.SyncToolSpecification]
  ): (HttpServer, McpSyncServer, McpHttpRoute) = {
    val route = new McpHttpRoute(config)
    val syncServer = route.buildServer(serverName, serverVersion, toolSpecs)
    val httpServer = route.start(port)
    (httpServer, syncServer, route)
  }

  /** Stop the MCP HTTP server (called from the JVM shutdown hook,
    * sequenced inside ONE Runnable per ADR-014 r1 fix). */
  def stop(httpServer: HttpServer, syncServer: McpSyncServer): Unit = {
    try syncServer.closeGracefully()
    catch { case NonFatal(_) => () }
    try {
      val f = httpServer.close()
      f.toCompletionStage.toCompletableFuture.get(5, TimeUnit.SECONDS)
    } catch { case NonFatal(_) => () }
  }
}