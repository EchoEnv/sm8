/*
 * SM8 Platform — McpStdioRoute.
 *
 * Per the stdio design (a prior PR, merged): in-process stdio MCP transport.
 * Per ADR-0021 Phase 2c, this file is now the LIFECYCLE + COMPOSITION ROOT
 * for the stdio transport; the wire-level machinery (EOF/partial-frame
 * tracking, the observing provider, the latch-on-close transport wrapper,
 * synchronous fd-1 frame writes) lives in `McpStdioTransport`. Mirrors the
 * `McpHttpRoute` 3-way split (Phase 2b), adapted to stdio's actual
 * structure: stdio has NO session bookkeeping and NO JSON-RPC dispatch of
 * its own (the SDK's `StdioServerTransportProvider` owns both), so the
 * honest mirror is lifecycle + transport, not a forced 3-way.
 *
 * Blocks the main thread on a `CountDownLatch` that the SDK's
 * `closeGracefully()` completes on EOF (per the stdio design §Wiring r2 Q1
 * fix — `Thread.currentThread().join()` does not wake on EOF because
 * EOF does not invoke JVM shutdown hooks).
 *
 * ==Why a separate transport (NOT a flag on McpHttpRoute)==
 *
 * The HTTP transport uses `HttpServer` + `requestHandler` (event-
 * driven) and a Vert.x `Vertx` instance. The stdio transport uses
 * a `BufferedReader.readLine` loop (blocking) and `PrintWriter`.
 * Their lifecycles and shutdown semantics are different enough that
 * a single class with two configurations would be awkward. Two
 * small classes that share the same `Sm8ToolHandlers` factory are
 * cleaner — and the HTTP transport's `McpSyncServer` + the stdio
 * transport's `McpSyncServer` are constructed independently
 * (one per process, depending on which flag is set).
 *
 * ==APIs verified before writing== (a prior PR r2 catch pattern)
 *
 * Verified via javap on mcp-core-2.0.1.jar (a prior PR):
 * - `io.modelcontextprotocol.server.transport.StdioServerTransportProvider`
 * ctor takes `McpJsonMapper`; `closeGracefully()` returns
 * `Mono<Void>`; `setSessionFactory(McpStreamableServerSession.Factory)`
 * sets the session factory (the SDK's `McpServer.sync(transport).build()`
 * does this).
 * - `io.modelcontextprotocol.server.McpServer.sync(transport)` returns
 * `SingleSessionSyncSpecification`; `.build()` returns `McpSyncServer`;
 * `.closeGracefully()` is `void` (fires the close signal but does
 * not drain).
 *
 * ==Layer discipline (the layering RFC)==
 *
 * McpStdioRoute is in `sm8-platform` (transport library), NOT
 * `sm8-server` (deployment) and NOT `sm8-mcp` (separate binary).
 * The stdio lifecycle is a transport concern (read stdin, write
 * stdout, exit on EOF); the actual MAIN entry-point is in
 * `sm8-server/Main.scala` per the existing pattern. This matches
 * `McpHttpRoute`'s placement in `sm8-platform`.
 *
 * ==Why a CountDownLatch (not Thread.join)==
 *
 * Per the stdio design §Wiring r2 Q1 (bonehound's catch): EOF on stdin does
 * NOT invoke JVM shutdown hooks. `Thread.currentThread().join()` would
 * therefore never wake. The SDK's `StdioServerTransportProvider`
 * exposes a `closeGracefully(): Mono<Void>` that the SDK fires when
 * the inbound read loop sees EOF (the SDK's `startInboundProcessing`
 * at StdioServerTransportProvider.java:79-105 sets `isClosing=true`
 * when the reader returns null, then closes the session). We wire
 * the `CountDownLatch.countDown()` to fire on the transport's close
 * paths (see `McpStdioTransport`) — the main thread awaits the latch
 * (which DOES wake on `countDown()`, unlike `Thread.join()`).
 *
 * ==Spark closure serialization==
 *
 * No Spark involvement. The transport captures only `serverInfo` and
 * `serverVersion` (Strings), the McpJsonMapper (a Jackson instance,
 * documented thread-safe after configuration), and the
 * `SyncToolSpecification` list (an immutable Seq). All thread-safe.
 *
 * ==Performance==
 *
 * The SDK's stdio read loop is a single thread (BufferedReader on
 * System.in). No background timer in v1. In-flight tool calls
 * return synchronously via the HTTP ingress; concurrent MCP
 * notifications are queued by the SDK. No thread pool — a single
 * session per process is the v1 limit.
 *
 * ==OOM==
 *
 * No unbounded buffers. The transport's chunked-write is per-line
 * and flushed immediately. No accumulation.
 *
 * ==NPE==
 *
 * "Null is a liar": all `Option`-like access is explicit. `args.get(key)`
 * returns `null` on miss → we short-circuit. The MCP SDK's `params()` is
 * `Map<String, Object>` (nullable in some spots) — we null-check before use.
 *
 * ==building-restate-services==
 *
 * This is OUTSIDE Restate's journal pipeline (plain stdin/stdout,
 * not a Restate handler). `Instant.now()` is correct here even
 * though it's a no-no inside Restate handler closures.
 */
package io.sm8.platform.mcp

import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.spec.McpSchema
import org.slf4j.LoggerFactory

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/** In-process stdio MCP transport per the stdio design. Builds the SDK
 * `McpSyncServer` bound to the `McpStdioTransport`'s observing provider,
 * then blocks the main thread on a `CountDownLatch` that the SDK's
 * `closeGracefully()` completes on EOF. The caller (sm8-server's
 * Main) is responsible for the JVM shutdown hook ordering (per
 * the stdio design §Wiring r1 fix — we don't change the existing hook
 * patterns; the stdio transport adds ONE new hook for
 * its own close).
 *
 * @param serverName MCP server name reported in initialize
 * @param serverVersion MCP server version reported in initialize
 * @param toolSpecs the tool specifications built by `Sm8ToolHandlers`
 * @param sessionTimeoutSeconds passed through for API compatibility;
 *        the SDK's stdio transport manages its single session
 *        internally (no route-level session bookkeeping)
 */
final class McpStdioRoute private (
 val serverName: String,
 val serverVersion: String,
 val toolSpecs: Seq[McpServerFeatures.SyncToolSpecification],
 val sessionTimeoutSeconds: Long
) {

 private val Log = LoggerFactory.getLogger(getClass)

 // CountDownLatch: fires when the SDK's inbound read loop sees stdin
 // EOF and calls `session.close()`. The wire-level transport
 // (McpStdioTransport) counts it down from its close paths; we
 // countDown() it here too for stop()/signalClose(). Per the stdio
 // design §Wiring r2 Q1 fix; EOF-detection redesigned in a prior PR.
 private val closeLatch = new CountDownLatch(1)

 // The SDK's stdio transport; constructed with the route so the
 // builder chain can wire it via McpServer.sync(...).build().
 private val transport: McpStdioTransport = new McpStdioTransport(
 trackingInput = new TrackingInputStream(System.in),
 beforeClose = () => writePartialFrameParseErrorIfPending(),
 closeLatch = closeLatch
 )

 // Byte-counting wrapper around System.in: tracks whether the last
 // byte read before EOF was a newline. Exposes partialFramePending so
 // the close path can emit a JSON-RPC ParseError (-32700) to stderr
 // when the host closes stdin mid-frame instead of leaving the host
 // to wonder why the connection dropped silently. Kept as a route
 // field (shared with the transport) because the close path reads
 // its state — and the ParseErrorOnCloseSpec reflection seam pokes
 // this exact field.
 private val trackingInput: TrackingInputStream = transport.trackingInput

 // JSON-RPC ParseError envelope emitted to stderr when the host
 // closes stdin mid-frame (see writePartialFrameParseErrorIfPending).
 private val PartialFrameParseError =
 "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32700,\"message\":\"Parse error: connection closed with partial JSON-RPC frame in input buffer\"},\"id\":null}\n"

 // The built SDK server, set by buildServer() (null until then);
 // stop() closes it.
 private var syncServer: McpSyncServer = _

 /** Build the SDK McpSyncServer (this wires the sessionFactory via
 * setSessionFactory) bound to the transport's observing provider.
 * Call this from the same Runnable that builds the transport
 * (mirrors the pre-split `McpHttpRoute.start` which does
 * `McpServer.sync(this).build()`).
 *
 * Returns the McpSyncServer so the caller can add it to its
 * shutdown hook chain.
 *
 * Per C5-r2-de-M1: the JVM-singleton guard is acquired by `apply`,
 * but the failure-prone work (SDK builder chain, transport ctor)
 * happens HERE, after apply has already returned. If buildServer
 * throws, the guard would leak at 1 and block all future
 * constructions in the JVM. On NonFatal we release the guard before
 * rethrowing so the factory invariant holds end-to-end
 * (apply + buildServer) as a single acquire/release unit.
 */
 def buildServer(): McpSyncServer = {
 try buildServerInner()
 catch {
 case NonFatal(e) =>
 McpStdioRoute.INSTANCE_COUNT.set(0)
 throw e
 }
 }

 private def buildServerInner(): McpSyncServer = {
 val server = McpServer.sync(transport.provider)
 .serverInfo(serverName, serverVersion)
 .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build())
 .tools(toolSpecs: _*)
 // Per the schema-validation design r1 fix: disable schema validation to avoid
 // loading the networknt 3.0.6 transitive dep that conflicts
 // with sm8-core's ManifestValidator on 1.5.2. The sm8-platform
 // META-INF/services override (a prior PR) provides a noop supplier
 // FIRST, so the SDK's default Jackson validator never loads.
 .validateToolInputs(false)
 .build()
 Log.info(s"McpStdioRoute built McpSyncServer; transport class=${transport.getClass.getName}")
 syncServer = server
 server
 }

 /** Block the current thread until the SDK's stdio transport closes
 * (EOF on stdin, OR closeGracefully() called from another thread).
 * Per the stdio design §Wiring r2 Q1 fix: this REPLACES
 * `Thread.currentThread().join()` (which never wakes on EOF because
 * EOF doesn't invoke JVM shutdown hooks). The latch wakes
 * immediately when countDown() fires.
 *
 * @param timeoutSeconds how long to wait for the close signal
 * @return true if the latch reached 0 within `timeoutSeconds`,
 *         false if it timed out
 */
 def awaitClose(timeoutSeconds: Long): Boolean = {
 closeLatch.await(timeoutSeconds, TimeUnit.SECONDS)
 }

 /** Stop the McpStdioRoute's McpSyncServer. Blocks the calling
    * thread until the SDK's underlying transport executors dispose,
    * so the SDK's non-daemon inbound + outbound single-thread
    * executors (verified: StdioServerTransportProvider.java:170
    * `Executors.newSingleThreadExecutor` × 2) do not leak.
    *
    * The `McpSyncServer.closeGracefully()` method runs
    * `asyncServer.closeGracefully().block()` — so the void return
    * already subscribes the SDK's Mono. However it does NOT wait
    * for the outbound scheduler to drain its Reactor thread; we
    * additionally subscribe the underlying transport's
    * `closeGracefully()` Mono and `.block(2s)` it. Without this, 2 of
    * 3 McpStdioRouteSpec tests leak non-daemon threads across JVMs
    * (a prior PR's HIGH-3 fix).
    *
    * Per C5-de-H3 (downgraded to MEDIUM — comment-only): the 2s
    * `.block(2s)` is misleading because `Mono.fromRunnable` fires
    * SYNCHRONOUSLY on subscribe (it just sets `isClosing=true` +
    * `inboundSink.tryEmitComplete()`). The actual executor disposal
    * happens via the SDK's reactor chain: inboundSink.tryEmitComplete
    * → doOnTerminate fires outboundSink.tryEmitComplete + disposes
    * inboundScheduler → outboundFlux completes → its doOnComplete
    * fires outboundScheduler.dispose(). We rely on that chain. The
    * 2s wait is belt-and-suspenders in case the chain is delayed. */
 def stop(): Unit = {
 if (syncServer != null) {
 try syncServer.closeGracefully()
 catch { case NonFatal(_) => () }
 }
 try {
 val drainMono = transport.closeGracefully()
 if (drainMono != null) drainMono.toFuture.get(2L, TimeUnit.SECONDS)
 } catch { case NonFatal(_) => () }
 closeLatch.countDown()
 // a prior PR's de-L6: release the JVM-singleton guard so a subsequent
 // instance can be constructed (tests + repl-friendly).
 McpStdioRoute.INSTANCE_COUNT.set(0)
 }

 /** Signal the close latch without disposing the server. Used by the
 * JVM shutdown hook in `sm8-server/Main.scala` so SIGTERM during
 * `awaitClose(...)` wakes the main thread within milliseconds
 * instead of forcing the operator to wait for the full
 * `timeoutSeconds` window.
 *
 * Per C5-de-H1: before this method existed, SIGTERM during
 * `stdio.awaitClose(timeoutSeconds=30)` parked the main thread on
 * `CountDownLatch.await` for the full 30s because CountDownLatch.await
 * is not interruptible AND the shutdown hook had no path to countDown
 * the latch. The HTTP transport path got its own hook at
 * Main.scala:544-550; this method is the equivalent for stdio.
 *
 * After signaling, the operator is responsible for calling `stop()`
 * (or letting the JVM exit) to dispose the SDK executors. The
 * shutdown hook itself can call `stop()` because `stop()` is idempotent.
 */
 def signalClose(): Unit = {
 closeLatch.countDown()
 }

 /** Test seam: invokes the same close-path check that
 * `McpStdioTransport`'s close paths run before countDown. Lets
 * `McpStdioRouteParseErrorOnCloseSpec` exercise the stderr ParseError
 * envelope end-to-end without spinning up the full
 * SDK builder chain (which requires a live `StdioServerTransportProvider`
 * and a real `BufferedReader` on the inbound loop).
 *
 * Package-private (same access rationale as `HttpIngressClient.baseUrl`):
 * exposed to the mcp package for the regression test only.
 */
 private[mcp] def triggerParseErrorOnClose(): Unit = {
 writePartialFrameParseErrorIfPending()
 }

 /** If the host closed stdin mid-frame, write a JSON-RPC `-32700
 * ParseError` envelope to stderr so the host learns the connection
 * terminated on a malformed frame instead of wondering why the
 * server dropped out silently. No-op when the last byte before EOF
 * was a newline (frame boundary was clean) or when no bytes were
 * read at all (clean shutdown, no in-flight frame).
 *
 * Why stderr not stdout: per MCP spec the host reserves fd 1 at
 * startup for the JSON-RPC stream. Writing a ParseError to stdout
 * mid-EOF would be interleaved with whatever the host expected to
 * receive as a final response, and the host's frame parser may
 * already have given up by then. stderr is the documented
 * diagnostic channel.
 *
 * The envelope itself has `"id": null` because we don't know the
 * id of the malformed frame (the SDK's BufferedReader.readLine()
 * consumed the partial bytes before we could parse them). JSON-RPC
 * 2.0 permits null id on a ParseError notification per §5.1.
 */
 private def writePartialFrameParseErrorIfPending(): Unit = {
 if (trackingInput != null && trackingInput.partialFramePending) {
 try {
 java.lang.System.err.print(PartialFrameParseError)
 java.lang.System.err.flush()
 } catch { case NonFatal(_) => () }
 // Clear after write so a second invocation (theoretical: both
 // close() and closeGracefully() run during a full shutdown)
 // does not emit a duplicate envelope.
 trackingInput.partialFramePending = false
 }
 }
}

object McpStdioRoute {

 // JVM-singleton counter: the stdio transport reads System.in,
 // a single fd-0; constructing two McpStdioRoute instances in the
 // same JVM would interleave their reads and corrupt the JSON-RPC
 // stream. AtomicInteger.compareAndSet(0,1) gives us a cheap
 // monotonic guard with no synchronization story beyond the CAS.
 // a prior PR's de-L6 fix.
 private[mcp] val INSTANCE_COUNT = new java.util.concurrent.atomic.AtomicInteger(0)

 /** Factory: build + return the route. Caller is responsible for
 * calling `buildServer()` (which starts the SDK's read loop)
 * and `awaitClose(...)` to block the main thread.
 *
 * Per the stdio design §Files changed: the 5 tools come from the SHARED
 * `Sm8ToolHandlers` factory in sm8-platform (moved from sm8-mcp
 * by a prior PR cleanup). The factory enforces the JVM-singleton
 * invariant (a prior PR's de-L6).
 *
 * Per C5-de-M3: the constructor previously did not decrement
 * INSTANCE_COUNT if `new McpStdioRoute(...)` threw (e.g., schema
 * validation failure, jackson mapper init failure). The counter
 * would remain at 1 forever, blocking all future McpStdioRoute
 * constructions in the JVM. Wrapped in try/catch to release the
 * guard on construction failure.
 *
 * @param serverName MCP server name reported in initialize
 * @param serverVersion MCP server version reported in initialize
 * @param toolSpecs the tool specifications built by `Sm8ToolHandlers`
 * @param sessionTimeoutSeconds passed through to the route for API
 *        compatibility; unused at the route level (see class scaladoc)
 * @return the new singleton route instance
 */
 def apply(
 serverName: String,
 serverVersion: String,
 toolSpecs: Seq[McpServerFeatures.SyncToolSpecification],
 sessionTimeoutSeconds: Long = 30
): McpStdioRoute = {
 if (!INSTANCE_COUNT.compareAndSet(0, 1)) {
 throw new IllegalStateException(
 "io.sm8.platform.mcp.McpStdioRoute: only one instance per JVM is allowed (stdio transport reads System.in, a single fd-0; concurrent instances would interleave reads and corrupt the JSON-RPC stream). Call stop() on the previous instance before constructing a new one."
 )
 }
 try {
 new McpStdioRoute(serverName, serverVersion, toolSpecs, sessionTimeoutSeconds)
 } catch {
 case NonFatal(e) =>
 INSTANCE_COUNT.set(0)
 throw e
 }
 }
}
