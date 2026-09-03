/*
 * SM8 Platform — McpStdioRoute.
 *
 * Per the stdio design (a prior PR, merged): in-process stdio MCP transport.
 * Mirrors `McpHttpRoute` (a prior PR) but for stdio. Builds the SDK
 * `McpSyncServer` bound to a `StdioServerTransportProvider`, blocks
 * the main thread on a `CountDownLatch` that the SDK's
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
 * the `CountDownLatch.countDown()` to fire on the Mono's `doOnSuccess`
 * — the main thread awaits the latch (which DOES wake on
 * `countDown()`, unlike `Thread.join()`).
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
 * No unbounded buffers. The SDK's outbound SSE chunked-write is
 * per-line and flushed immediately. No accumulation.
 *
 * ==NPE==
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar": all `Option`-like
 * access is explicit. `args.get(key)` returns `null` on miss → we
 * short-circuit. The MCP SDK's `params()` is `Map<String, Object>`
 * (nullable in some spots) — we null-check before use.
 *
 * ==building-restate-services==
 *
 * This is OUTSIDE Restate's journal pipeline (plain stdin/stdout,
 * not a Restate handler). `Instant.now()` is correct here even
 * though it's a no-no inside Restate handler closures.
 */
package io.sm8.platform.mcp

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpServerSession
import io.modelcontextprotocol.spec.McpServerTransport
import io.modelcontextprotocol.spec.McpServerTransportProvider
import org.slf4j.LoggerFactory

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

/** In-process stdio MCP transport per the stdio design. Builds the SDK
 * `McpSyncServer` bound to a `StdioServerTransportProvider`, then
 * blocks the main thread on a `CountDownLatch` that the SDK's
 * `closeGracefully()` completes on EOF. The caller (sm8-server's
 * Main) is responsible for the JVM shutdown hook ordering (per
 * the stdio design §Wiring r1 fix — we don't change the existing a prior PR /
 * a prior PR hook patterns; the stdio transport adds ONE new hook for
 * its own close). */
final class McpStdioRoute private (
 val serverName: String,
 val serverVersion: String,
 val toolSpecs: Seq[McpServerFeatures.SyncToolSpecification],
 val sessionTimeoutSeconds: Long
) {

 private val Log = LoggerFactory.getLogger(getClass)

 // CountDownLatch: fires when the SDK's inbound read loop sees stdin
 // EOF and calls `session.close()`. We countDown() from a recording
 // session factory (see buildServer) so the awaiting main thread wakes
 // on EOF. Per the stdio design §Wiring r2 Q1 fix (bonehound's catch);
 // EOF-detection redesigned in a prior PR.
 private val closeLatch = new CountDownLatch(1)

 // The SDK's stdio transport; constructed in buildServer() so we can
 // wire setSessionFactory via McpServer.sync(...).build().
 private var transport: StdioServerTransportProvider = _
 private var syncServer: McpSyncServer = _

 // Byte-counting wrapper around System.in: tracks whether the last
 // byte read before EOF was a newline. Exposes partialFramePending so
 // the close path can emit a JSON-RPC ParseError (-32700) to stderr
 // when the host closes stdin mid-frame instead of leaving the host
 // to wonder why the connection dropped silently.
 private var trackingInput: TrackingInputStream = _

 // JSON mapper + unbuffered fd-1 sink, created in buildServer(). The
 // LatchOnCloseTransport writes response frames synchronously through
 // these (see its scaladoc, a prior PR fix #2).
 private var JsonMapper: McpJsonMapper = _
 private var Out: java.io.FileOutputStream = _

 /** Build the SDK McpSyncServer (this wires the sessionFactory via
 * setSessionFactory) and the SDK's stdio transport. Call this from
 * the same Runnable that builds the transport (mirrors a prior PR's
 * `McpHttpRoute.start` which does `McpServer.sync(this).build()`).
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
 JsonMapper = new JacksonMcpJsonMapper(
 tools.jackson.databind.json.JsonMapper.builder().build()
)
 Out = new java.io.FileOutputStream(java.io.FileDescriptor.out)
 // a prior PR smoke fix: the SDK's default ctor writes JSON-RPC frames
 // to `System.out` — a PrintStream that BLOCK-BUFFERS whenever fd 1
 // is a pipe (JVM default: line-buffered only for console, unbuffered
 // for files). A piped host (any MCP client spawning us as a
 // subprocess) therefore never sees a response. Worse: per MCP spec
 // the host reserves fd 1 at startup and routes library stdout noise
 // elsewhere; writing through `System.out` (16 KB autoflush PrintStream
 // shared with third-party code) risks interleave with any stray
 // `Console.println`. The fix: pass an unbuffered
 // `FileOutputStream(FileDescriptor.out)` as the sink (each write hits
 // fd 1 directly, no buffering, no interleave). The SDK's session
 // transport wraps the InputStream we pass in its own BufferedReader
 // (verified via javap on StdioMcpSessionTransport), so `System.in`
 // raw is the correct source argument. NOTE: we also write
 // synchronously from LatchOnCloseTransport.sendMessage (see its
 // scaladoc) — this transport arg still matters because the SDK's
 // read loop uses the InputStream we pass.
 trackingInput = new TrackingInputStream(System.in)
 transport = new StdioServerTransportProvider(
 JsonMapper,
 trackingInput,
 Out
)
 val server = McpServer.sync(new EofObservingProvider(transport))
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

 /** Provider wrapper passed to `McpServer.sync(...)`: delegates
 * everything to the real `StdioServerTransportProvider` but
 * intercepts `setSessionFactory` so the session the SDK creates
 * receives our transport wrapper (see `LatchOnCloseTransport`). */
 private class EofObservingProvider(delegate: StdioServerTransportProvider)
 extends McpServerTransportProvider {

 override def setSessionFactory(factory: McpServerSession.Factory): Unit =
 delegate.setSessionFactory(new McpServerSession.Factory {
 override def create(sessionTransport: McpServerTransport): McpServerSession =
 factory.create(new LatchOnCloseTransport(sessionTransport))
 })

 override def notifyClients(method: String, params: Any): reactor.core.publisher.Mono[Void] =
 delegate.notifyClients(method, params)

 override def closeGracefully(): reactor.core.publisher.Mono[Void] =
 delegate.closeGracefully()
 }

 /** Transport wrapper: two responsibilities.
 *
 * 1. EOF detection. `close()` and `closeGracefully()` countDown our
 * latch. The SDK's inbound loop calls `session.close()` on stdin
 * EOF, which runs `onClose.get()` (subscription cleanup) and
 * then `transport.close()` — that's our latch signal. (The old
 * design subscribed the provider's `closeGracefully()` Mono
 * eagerly at build() to "register" a listener — but
 * `Mono.fromRunnable` RUNS AT SUBSCRIPTION, immediately closing
 * the transport and discarding every inbound message. Never
 * subscribe a close Mono to observe it; intercept the close.)
 *
 * 2. SYNCHRONOUS outbound. We write response frames straight to
 * fd 1 inside `sendMessage` instead of emitting into the SDK's
 * outbound sink. The SDK's stdio design runs inbound reads and
 * outbound writes on two independent single-thread executors
 * (verified: StdioMcpSessionTransport ctor, mcp-core 2.0.1) —
 * on EOF the loop's finally sets isClosing=true before the
 * outbound worker has necessarily drained the buffer, and the
 * worker silently DISCARDS every message written after
 * isClosing flips (verified: `if (message != null &&
 * !isClosing.get())` in the outbound handle). Fast clients
 * (pipe scripts, hosts that close stdin immediately after the
 * last request) would lose the final responses. Delegating the
 * real session's `handle` (via the factory wrapper) to run
 * synchronously makes the write happen INSIDE the caller's
 * `sendMessage` invocation, before its Mono is even returned:
 * every response is on the wire before the next step of the
 * request-handling chain, so EOF can never strand a frame.
 * Per C5-r2-arch-001 (correcting the earlier claim): the caller
 * thread is the SDK session's request-handling thread, NOT the
 * inbound loop thread — the inbound loop only reads. The SDK's
 * outbound scheduler is bypassed entirely because our sendMessage
 * returns without enqueueing (the delegate's outbound sink
 * receives nothing; its isClosing race becomes irrelevant). The
 * `Out.synchronized` block serializes writes regardless of which
 * caller thread performs them.
 *
 * `handle` therefore wraps `delegate.handle(msg)` in
 * `.then()` AFTER writing — the SDK's sendMessage is a no-op
 * for us because we intercept the emission here first. */
 private class LatchOnCloseTransport(delegate: McpServerTransport)
 extends McpServerTransport {

 override def sendMessage(message: McpSchema.JSONRPCMessage): reactor.core.publisher.Mono[Void] = {
 // Serialize exactly as the SDK's outbound loop does (writeValueAsString
 // + newline + embedded-newline escaping), then write synchronously.
 val json = JsonMapper.writeValueAsString(message)
 .replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n")
 Out.synchronized {
 Out.write(json.getBytes(java.nio.charset.StandardCharsets.UTF_8))
 Out.write("\n".getBytes(java.nio.charset.StandardCharsets.UTF_8))
 Out.flush()
 }
 reactor.core.publisher.Mono.empty[Void]()
 }

 override def closeGracefully(): reactor.core.publisher.Mono[Void] = {
 writePartialFrameParseErrorIfPending()
 closeLatch.countDown()
 delegate.closeGracefully()
 }

 override def close(): Unit = {
 writePartialFrameParseErrorIfPending()
 closeLatch.countDown()
 delegate.close()
 }

 override def unmarshalFrom[T](data: Any, typeRef: io.modelcontextprotocol.json.TypeRef[T]): T =
 delegate.unmarshalFrom(data, typeRef)
 }



 /** Block the current thread until the SDK's stdio transport closes
 * (EOF on stdin, OR closeGracefully() called from another thread).
 * Per the stdio design §Wiring r2 Q1 fix: this REPLACES
 * `Thread.currentThread().join()` (which never wakes on EOF because
 * EOF doesn't invoke JVM shutdown hooks). The latch wakes
 * immediately when countDown() fires.
 *
 * Returns true if the latch reached 0 within `timeoutSeconds`,
 * false if it timed out.
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
    * (PR-265 HIGH-3 fix).
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
 if (transport != null) {
 try {
 val drainMono = transport.closeGracefully()
 if (drainMono != null) drainMono.toFuture.get(2L, TimeUnit.SECONDS)
 } catch { case NonFatal(_) => () }
 }
 closeLatch.countDown()
 // PR-265 de-L6: release the JVM-singleton guard so a subsequent
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
 * `LatchOnCloseTransport.close()` and `closeGracefully()` run before
 * countDown. Lets `McpStdioRouteParseErrorOnCloseSpec` exercise the
 * stderr ParseError envelope end-to-end without spinning up the full
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
 val envelope =
 "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32700,\"message\":\"Parse error: connection closed with partial JSON-RPC frame in input buffer\"},\"id\":null}\n"
 try {
 java.lang.System.err.print(envelope)
 java.lang.System.err.flush()
 } catch { case NonFatal(_) => () }
 // Clear after write so a second invocation (theoretical: both
 // close() and closeGracefully() run during a full shutdown)
 // does not emit a duplicate envelope.
 trackingInput.partialFramePending = false
 }
 }
}

/** `FilterInputStream` wrapper around `System.in` that tracks whether
 * the last byte read before EOF was a newline. JSON-RPC 2.0 frames
 * are newline-delimited (per MCP stdio framing), so `EOF after a
 * non-newline byte` == `the host closed stdin mid-frame`.
 *
 * Thread-safety: this is single-threaded. The SDK's
 * `StdioMcpSessionTransport` reads from one thread (the inbound
 * single-thread executor); close paths run on the same thread
 * after EOF triggers `session.close()`. No concurrent readers.
 *
 * @param in the underlying input stream (typically `System.in`)
 */
private final class TrackingInputStream(in: java.io.InputStream)
 extends java.io.FilterInputStream(in) {

 // The last byte the most recent read() returned. -1 means no byte
 // yet or EOF already reached. We use it to decide whether EOF
 // landed on a frame boundary (last byte == '\n') or mid-frame
 // (last byte != '\n', or no bytes read at all means "clean EOF
 // with empty buffer").
 // private[mcp] (not private): same access rationale as
 // HttpIngressClient.scala:82-84 — exposed to the mcp package so
 // McpStdioRoute (same package, same file) and TrackingInputStreamSpec
 // (same package, different file) can read the state.
 @volatile private[mcp] var lastByte: Int = -1

 // Flipped to true by read() when EOF is reached and the prior byte
 // was not a newline. Volatile for defense-in-depth: the close path
 // reads this field after EOF, and although in practice both happen
 // on the SDK's inbound single-thread executor, future refactors
 // could move the close to another thread (e.g. the JVM shutdown
 // hook). The volatile guarantees happens-before visibility.
 // private[mcp] — see lastByte above for the access rationale.
 @volatile private[mcp] var partialFramePending: Boolean = false

 override def read(): Int = {
 val b = in.read()
 if (b == -1) {
 if (lastByte != -1 && lastByte != '\n') partialFramePending = true
 } else {
 lastByte = b
 }
 b
 }

 override def read(b: Array[Byte], off: Int, len: Int): Int = {
 val n = in.read(b, off, len)
 if (n == -1) {
 if (lastByte != -1 && lastByte != '\n') partialFramePending = true
 } else if (n > 0) {
 lastByte = b(off + n - 1) & 0xFF
 }
 n
 }
}

object McpStdioRoute {

 // JVM-singleton counter: the stdio transport reads System.in,
 // a single fd-0; constructing two McpStdioRoute instances in the
 // same JVM would interleave their reads and corrupt the JSON-RPC
 // stream. AtomicInteger.compareAndSet(0,1) gives us a cheap
 // monotonic guard with no synchronization story beyond the CAS.
 // PR-265 de-L6 fix.
 private[mcp] val INSTANCE_COUNT = new java.util.concurrent.atomic.AtomicInteger(0)

 /** Factory: build + return the route. Caller is responsible for
 * calling `buildServer()` (which starts the SDK's read loop)
 * and `awaitClose(...)` to block the main thread.
 *
 * Per the stdio design §Files changed: the 5 tools come from the SHARED
 * `Sm8ToolHandlers` factory in sm8-platform (moved from sm8-mcp
 * by a prior PR cleanup). The factory enforces the JVM-singleton
 * invariant (PR-265 de-L6).
 *
 * Per C5-de-M3: the constructor previously did not decrement
 * INSTANCE_COUNT if `new McpStdioRoute(...)` threw (e.g., schema
 * validation failure, jackson mapper init failure). The counter
 * would remain at 1 forever, blocking all future McpStdioRoute
 * constructions in the JVM. Wrapped in try/catch to release the
 * guard on construction failure.
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
