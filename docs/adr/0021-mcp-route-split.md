# ADR-0021: `McpHttpRoute` + `McpStdioRoute` + `Sm8ToolHandlers` structural split

## Status

Proposed. **Date:** 2026-09-05. **Author:** SM8 agent (per wayfinder map Ticket #3, `docs/wayfinder/2026-09-05-control-plane-robustness.md`).

## Context and Problem Statement

`sm8-platform/src/main/scala/io/sm8/platform/query/McpHttpRoute.scala` is a **671-LOC god-class** doing three unrelated jobs in one class:

1. **Vert.x lifecycle** (lines 136-160, 632-672): owns `private val vertx: Vertx`, `start(port): HttpServer`, `stop(httpServer, syncServer)`, the `AtomicReference<McpStreamableServerSession.Factory>` slot for session-factory injection, the `buildServer(serverName, serverVersion, toolSpecs)` that returns an SDK McpSyncServer, and the object `McpHttpRoute` companion's `start` / `stop` factories.

2. **Session state** (lines 142-146, 270-307): owns `private val sessions: ConcurrentHashMap[String, McpStreamableServerSession]`, the inner anonymous class extending `McpStreamableServerSession` that forwards `sendMessage` and `closeGracefully` to the registry, and the `notifyClients` / lifecycle hooks (`closeGracefully`) for SSE-driven notifications.

3. **MCP protocol handlers** (lines 309-499, 570-625): `handleRequest` / `handlePostBody` / `handleInitialize` / `handlePostRequest` / `handlePostNotification` / `handlePostResponse` / `handleGet` / `handleDelete` / `writeSseEvent` / `mapperFor` — the actual JSON-RPC method dispatch and SSE framing.

`sm8-platform/src/main/scala/io/sm8/platform/mcp/McpStdioRoute.scala` (542 LOC) carries the **same 3-way structure** for the stdio transport, with `INSTANCE_COUNT` JVM-singleton bookkeeping added. Splitting McpHttpRoute without splitting McpStdioRoute would re-introduce drift between the two transports — they need to mirror the split.

`sm8-platform/src/main/scala/io/sm8/platform/mcp/Sm8ToolHandlers.scala` (398 LOC) is structured as 7 nearly-identical `buildXxxTool(client, mapper): SyncToolSpecification` methods. Each tool:
1. builds `McpSchema.Tool` (name + title + description + inputSchema + required list),
2. builds a `BiFunction` callHandler that copies args from `req.arguments()` into a `LinkedHashMap[String, Object]` and calls `callAndWrap(client, mapper, "/path", requestBody)`.

The shape is **highly uniform** — adding tool #8 today means another ~50 LOC of copy-paste boilerplate. The `apply` method enumerates all 7 builders and maps them to `SyncToolSpecification` objects.

Per the architect review at Ticket #3's chart-time (`docs/wayfinder/2026-09-05-control-plane-robustness.md` Ticket #3):
- McpHttpRoute's 671 LOC is the biggest cognitive-load surface for any MCP bugfix; reading it requires holding 3 concerns in mind simultaneously.
- Sm8ToolHandlers's per-tool boilerplate scales linearly with the tool count; a future Ticket #N tool is another ~50 LOC of near-identical code.
- McpStdioRoute's structural mirroring means the same bugfix would need to land in two places without a shared split.

## Decision

Split `McpHttpRoute` into 3 collaborators + extract a `ToolRegistry` helper for `Sm8ToolHandlers` + mirror the McpHttpRoute split on `McpStdioRoute`.

### McpHttpRoute → 3 collaborators + composition root

```scala
// === NEW: McpHttpServer.scala ===
// ~150-200 LOC: Vert.x lifecycle ONLY.
final class McpHttpServer(config: Config) {
  private val vertx: Vertx = Vertx.vertx()
  private val sessionFactoryRef =
    new AtomicReference[McpStreamableServerSession.Factory]()

  def setSessionFactory(factory: McpStreamableServerSession.Factory): Unit =
    sessionFactoryRef.set(factory)

  def buildServer(name: String, version: String,
                   toolSpecs: Seq[SyncToolSpecification]): McpSyncServer = { ... }
  def start(port: Int): HttpServer = { ... }
  def stop(): Unit = { ... }
}

// === NEW: McpSessionRegistry.scala ===
// ~100-150 LOC: session state ONLY.
final class McpSessionRegistry {
  private val sessions = new ConcurrentHashMap[String, McpStreamableServerSession]()

  def get(id: String): Option[McpStreamableServerSession] = Option(sessions.get(id))
  def put(id: String, session: McpStreamableServerSession): Unit = sessions.put(id, session)
  def remove(id: String): Unit = sessions.remove(id)
  def closeAll(): Unit = { ... }

  def newForwardingSession(id: String, exchange: McpSyncServerExchange): McpStreamableServerSession = {
    // the inner anonymous-class-forwarding pattern lives here
    ...
  }
}

// === NEW: McpMessageRouter.scala ===
// ~250-300 LOC: protocol handlers ONLY.
final class McpMessageRouter(sessions: McpSessionRegistry, mapper: McpJsonMapper) {
  private def handleRequest(req: HttpServerRequest): Unit = { ... }
  private def handlePostBody(req: HttpServerRequest, body: String): Unit = { ... }
  private def handleInitialize(...): Unit = { ... }
  private def handlePostRequest(req: HttpServerRequest, jsonrpcReq: JsonRpcRequest): Unit = { ... }
  private def handlePostNotification(req: HttpServerRequest, jsonrpcReq: JsonRpcRequest): Unit = { ... }
  private def handlePostResponse(req: HttpServerRequest, jsonrpcReq: JsonRpcResponse): Unit = { ... }
  private def handleGet(req: HttpServerRequest): Unit = { ... }
  private def handleDelete(req: HttpServerRequest): Unit = { ... }
  private def writeSseEvent(...): Unit = { ... }
  def mapperFor(req: HttpServerRequest): McpJsonMapper = ...
}

// === REFACTORED: McpHttpRoute.scala (≤100 LOC composition root) ===
final class McpHttpRoute(config: Config) extends McpStreamableServerSession.Factory {
  private val server = new McpHttpServer(config)
  private val sessions = new McpSessionRegistry()
  private val router = new McpMessageRouter(sessions, mapper)

  override def setSessionFactory(factory: McpStreamableServerSession.Factory): Unit =
    server.setSessionFactory(factory)
  override def notifyClients(method: String, params: Object): Mono[Void] = { ... }
  override def closeGracefully(): Mono[Void] = { ... }
}

object McpHttpRoute {
  final case class Config(endpointPath: String = "/mcp", disallowDelete: Boolean = false)
  def start(port, config, serverName, serverVersion, toolSpecs): (HttpServer, McpSyncServer, McpHttpRoute) = {
    val route = new McpHttpRoute(config)
    val syncServer = route.server.buildServer(serverName, serverVersion, toolSpecs)
    val httpServer = route.server.start(port)
    (httpServer, syncServer, route)
  }
  def stop(httpServer, syncServer): Unit = {
    try syncServer.closeGracefully() catch ...
    try httpServer.close() ...
  }
}
```

### Sm8ToolHandlers → ToolRegistry helper

```scala
// === NEW: ToolRegistry.scala ===
object ToolRegistry {
  /** A registered tool definition: name, builder (consumes client + mapper). */
  final case class Entry(name: String, build: (HttpIngressClient.Impl, ObjectMapper) => SyncToolSpecification)

  /** Register a tool. Plugin-style mutation; plugins call this from their `setup(engine)`. */
  private val entries = new ConcurrentHashMap[String, Entry]()
  def register(entry: Entry): Unit = entries.put(entry.name, entry)
  def tools: Seq[SyncToolSpecification] = ???

  /** Helper for tool builders: produces a SyncToolSpecification from a name + description + inputSchema + callHandler. */
  def build(
      name: String,
      title: String,
      description: String,
      inputSchema: McpSchema.JsonSchema,
      required: java.util.List[String],
      callHandler: (McpSyncServerExchange, CallToolRequest) => CallToolResult
  ): SyncToolSpecification = { ... }
}

object Sm8ToolHandlers {
  private def registerAll(): Unit = {
    ToolRegistry.register(ToolRegistry.Entry("query", Sm8ToolHandlers.buildQueryTool))
    ToolRegistry.register(ToolRegistry.Entry("list_models", Sm8ToolHandlers.buildListModelsTool))
    // ... etc for all 7 tools ...
  }
  registerAll()

  def build(client: HttpIngressClient.Impl, mapper: ObjectMapper): Seq[SyncToolSpecification] =
    ToolRegistry.tools.map(_.build(client, mapper))

  // Each buildXxxTool becomes ~20-30 LOC: schema + handler body only.
  private def buildQueryTool(client: HttpIngressClient.Impl, mapper: ObjectMapper): SyncToolSpecification =
    ToolRegistry.build(
      name = "query", title = "Run an sm8 query",
      description = "...",
      inputSchema = ...,
      required = List.of("modelName"),
      callHandler = { (exch, req) =>
        val args = req.arguments()
        val requestBody = new java.util.LinkedHashMap[String, Object]()
        copyString(args, "modelName", requestBody)
        copyList(args, "measures", requestBody)
        // ... existing body ...
        callAndWrap(client, mapper, "/QueryService/runQuery", requestBody)
      }
    )
  // ... 6 more buildXxxTool methods (each ~20-30 LOC) ...
}
```

### McpStdioRoute — mirror the split

McpStdioRoute has the SAME 3 concerns (Vert.x-like stdio lifecycle, session state, protocol handlers — adapted for stdio instead of HTTP) but lacks the Vert.x dependency. The mirror split:

- **McpStdioServer** (lifecycle: `INSTANCE_COUNT` JVM-singleton, `buildServer`, `start`, `stop`, `awaitClose`, `signalClose`, `writePartialFrameParseErrorIfPending`)
- **McpStdioSessionRegistry** (or share McpSessionRegistry from above — same session-state semantics regardless of transport)
- **McpStdioMessageRouter** (protocol handlers adapted for stdio framing — needs verification whether the JSON-RPC dispatch can be shared with McpMessageRouter or requires a parallel stdio variant)

**Open question (TBD during implementation)**: can the two `MessageRouter`s share a common base class for JSON-RPC dispatch logic, with only the I/O framing differing? If yes, extract a `JsonRpcDispatcher` base + two thin I/O subclasses. If no (the framing differs too much), duplicate the router and accept the maintenance cost.

### Layer discipline

- All new files: `sm8-platform/src/main/scala/io/sm8/platform/mcp/` (matches the existing ToolHandlers location)
- No sm8-core change (frozen library preserved)
- No change to EngineProvider / HookManager / EngineError / or other public types
- The split does NOT change the wire protocol — the JSON-RPC + SSE shape at the HTTP / stdio boundary is identical to today

## Consequences

- McpHttpRoute becomes 3 small files (each ≤ 300 LOC, single concern) + 1 thin composition root (≤ 100 LOC). Total LOC unchanged or slightly higher; cognitive load drops dramatically.
- A bug in Vert.x lifecycle can be fixed in `McpHttpServer` without reading `McpSessionRegistry` or `McpMessageRouter`.
- A bug in JSON-RPC dispatch can be fixed in `McpMessageRouter` without reading lifecycle code.
- Sm8ToolHandlers's per-tool boilerplate shrinks from ~50 LOC per tool to ~20-30 LOC. Adding tool #8 (and beyond) becomes a 1-line `ToolRegistry.register(...)` + the build-method body.
- McpStdioRoute mirrors the split — drift between the two transports becomes structurally impossible (both use the same McpSessionRegistry + their own thin I/O).
- Open question: can the two MessageRouters share a JsonRpcDispatcher base? If yes, extract it; if no, document the choice.
- **The existing `McpHttpRouteSpec` and `McpStdioRouteSpec` test suites may need minor signature updates** (e.g. constructing `McpHttpRoute` directly vs. constructing `McpHttpRoute` + the 3 collaborators). The behavior under test does NOT change.

## Alternatives Considered

- **Refactor in-place without extracting new files** (rename methods + add comments to demarcate the 3 concerns within the same 671-LOC file). Rejected: the cognitive-load problem is the file size itself; comments don't reduce it.
- **Only extract `McpHttpServer` + `McpMessageRouter`, keep `McpSessionRegistry` inline as a private field.** Rejected: the session-state concern (ConcurrentHashMap + transport factory forwarding) is non-trivial enough (~100-150 LOC after factoring) to deserve its own file. Same class of bugfix-isolation as the other two.
- **Convert `McpHttpRoute` to a trait with 3 implementing mixins** (`McpLifecycle`, `McpSessionState`, `McpProtocol`). Rejected: Scala 2.13 trait linearization with 3 mixins and multiple inheritance is fragile; the concrete-class composition (private fields holding the 3 collaborators) is simpler and equally testable.

## References

- `docs/wayfinder/2026-09-05-control-plane-robustness.md` Ticket #3 — source map
- `sm8-platform/src/main/scala/io/sm8/platform/query/McpHttpRoute.scala:1-671` — current god-class
- `sm8-platform/src/main/scala/io/sm8/platform/mcp/McpStdioRoute.scala:1-542` — stdio mirror to split
- `sm8-platform/src/main/scala/io/sm8/platform/mcp/Sm8ToolHandlers.scala:1-398` — tool builder boilerplate
- `sm8-platform/src/test/scala/io/sm8/platform/query/McpHttpRouteSpec.scala` — existing regression test (must continue to pass)
- `sm8-platform/src/test/scala/io/sm8/platform/mcp/McpStdioRouteSpec.scala` — stdio test (must continue to pass)
- `sm8-platform/src/main/scala/io/sm8/platform/query/MetricsHttpRoute.scala` (PR-258) — reference for the bind/start/stop lifecycle pattern

Local-only `.omp/WATCHDOG.yml` (gitignored) tunes the advisor roster for this project.
