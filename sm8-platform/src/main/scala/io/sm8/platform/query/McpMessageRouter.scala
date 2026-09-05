/*
 * SM8 MCP — McpMessageRouter (ADR-0021 Phase 2b).
 *
 * MCP protocol handlers for the Streamable HTTP transport: JSON-RPC
 * message dispatch (initialize / request / notification / response),
 * GET SSE stream handling, DELETE session cleanup, and the per-session
 * `McpStreamableServerTransport` factory that writes SSE events to
 * the client.
 *
 * Extracted from `McpHttpRoute` per ADR-0021 — protocol handling is a
 * separate concern from Vert.x lifecycle (`McpHttpServer`) and session
 * state (`McpSessionRegistry`). This class owns no Vert.x server
 * lifecycle and no session storage; it is pure protocol routing.
 *
 * The route (composition root) injects the registry + config, and
 * exposes `handleRequest` to the Vert.x server.
 */
package io.sm8.platform.query

import io.modelcontextprotocol.spec.McpStreamableServerSession
import io.modelcontextprotocol.spec.McpStreamableServerTransport
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper

import io.vertx.core.Handler
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse

import org.slf4j.LoggerFactory

import java.time.Duration

import reactor.core.publisher.Mono

import scala.util.control.NonFatal

/** MCP protocol handlers for the Streamable HTTP transport.
  *
  * @param sessions the session registry (shared with the route)
  * @param config   the transport config (endpointPath, disallowDelete)
  * @param sessionFactoryFn per-call source of the SDK-injected session
  *                         factory (set via `McpHttpServer.buildServer`
  *                         → `provider.setSessionFactory(...)`); used by
  *                         `handleInitialize` to reject requests before
  *                         the server is built.
  */
final class McpMessageRouter(
    sessions: McpSessionRegistry,
    config: McpHttpRoute.Config,
    sessionFactoryFn: () => Option[McpStreamableServerSession.Factory]
) {

  private val Log = LoggerFactory.getLogger(getClass)

  /** Jackson 3 MCP JSON mapper (thread-safe after construction). */
  private val mapper: McpJsonMapper =
    new JacksonMcpJsonMapper(tools.jackson.databind.json.JsonMapper.builder().build())

  /** Entry point wired to the Vert.x server by the composition root.
    * Routes by HTTP method: POST (body-accumulating), GET, DELETE,
    * others → 405. Rejects paths that don't end in `endpointPath`. */
  def handleRequest(req: HttpServerRequest): Unit = {
    if (!req.path().endsWith(config.endpointPath)) {
      req.response.setStatusCode(404).end("not found")
      return
    }
    req.method() match {
      case io.vertx.core.http.HttpMethod.POST =>
        // Accumulate the body via Vert.x's async handlers, then dispatch
        // synchronously. The body is bounded by the SDK's default 16MB
        // request size (json-schema-validator default); MCP tool calls
        // are < 10 KiB in practice.
        val sb = new java.lang.StringBuilder()
        req.bodyHandler(new Handler[Buffer] {
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
      case io.vertx.core.http.HttpMethod.GET    => handleGet(req); ()
      case io.vertx.core.http.HttpMethod.DELETE => handleDelete(req); ()
      case _         => req.response.setStatusCode(405).end("method not allowed")
    }
  }

  /** POST handler (called AFTER body has been read by Vert.x).
    * 3 sub-cases (initialize / request / notification+response) per
    * ADR-014 + SDK doPost source. */
  private def handlePostBody(
      req: HttpServerRequest,
      body: String
  ): Unit = {
    val accept = req.getHeader("Accept")
    if (accept == null || !accept.contains("application/json") || !accept.contains("text/event-stream")) {
      req.response.setStatusCode(400).end(
        """{"error":"Accept must include both application/json and text/event-stream"}"""
      )
      return
    }
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
    * header. Per the r1 dual-review catch (Q1): MCP spec requires
    * `protocolVersion` and `clientInfo` in the initialize params;
    * reject with -32602 INVALID_PARAMS if either is missing. The
    * session IS removed on any failure (don't leak an entry in
    * `sessions`). */
  private def handleInitialize(
      req: HttpServerRequest,
      jsonrpcReq: McpSchema.JSONRPCRequest
  ): Unit = {
    val factoryOpt = sessionFactoryFn()
    val factory = factoryOpt.getOrElse {
      req.response.setStatusCode(503).end("""{"error":"MCP server not initialized"}""")
      return
    }
    val resp = req.response

    // Pre-validate the params map. The MCP spec requires `protocolVersion`
    // and `clientInfo`; reject with -32602 INVALID_PARAMS if either is
    // missing. Per r1 dual-review catch (Q1): a missing/partial params map
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
    * the response. */
  private def handlePostRequest(
      req: HttpServerRequest,
      jsonrpcReq: McpSchema.JSONRPCRequest
  ): Unit = {
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId).orNull
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
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

  /** POST JSON-RPC notification: 202 Accepted. */
  private def handlePostNotification(
      req: HttpServerRequest,
      notif: McpSchema.JSONRPCNotification
  ): Unit = {
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId).orNull
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    try session.accept(notif).block(Duration.ofSeconds(10))
    catch { case NonFatal(e) => Log.error(s"accept(notification) failed: ${sessionIdOf(req)}", e) }
    req.response.setStatusCode(202).end()
  }

  /** POST JSON-RPC response (client→server flow): 202 Accepted. */
  private def handlePostResponse(
      req: HttpServerRequest,
      resp: McpSchema.JSONRPCResponse
  ): Unit = {
    val sessionId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)
    if (sessionId == null || sessionId.isBlank) {
      req.response.setStatusCode(400).end("""{"error":"missing Mcp-Session-Id header"}""")
      return
    }
    val session = sessions.get(sessionId).orNull
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    try session.accept(resp).block(Duration.ofSeconds(10))
    catch { case NonFatal(e) => Log.error(s"accept(response) failed: ${sessionIdOf(req)}", e) }
    req.response.setStatusCode(202).end()
  }

  /** GET handler: opens an SSE listening stream (or replays if
    * Last-Event-ID is present). */
  private def handleGet(req: HttpServerRequest): Unit = {
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
    val session = sessions.get(sessionId).orNull
    if (session == null) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    val lastEventId = req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.LAST_EVENT_ID)
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

  /** DELETE handler: closes the session. */
  private def handleDelete(req: HttpServerRequest): Unit = {
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
    if (session.isEmpty) {
      req.response.setStatusCode(404).end(s"""{"error":"session not found: $sessionId"}""")
      return
    }
    try session.get.delete().block(Duration.ofSeconds(10))
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

  /** Read the Mcp-Session-Id header as a String (for log messages). */
  private def sessionIdOf(req: HttpServerRequest): String =
    req.getHeader(io.modelcontextprotocol.spec.HttpHeaders.MCP_SESSION_ID)

}
