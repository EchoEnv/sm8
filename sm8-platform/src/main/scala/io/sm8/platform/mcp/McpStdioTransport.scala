/*
 * SM8 Platform — McpStdioTransport.
 *
 * The wire-level half of the stdio MCP transport, split out of
 * McpStdioRoute per ADR-0021 Phase 2c. Mirrors the McpHttpRoute
 * 3-way split (Phase 2b) adapted to stdio's actual structure:
 * the stdio transport has NO session bookkeeping and NO JSON-RPC
 * dispatch of its own (the SDK's `StdioServerTransportProvider`
 * owns both — it parses inbound frames, maintains the single
 * session, and routes JSON-RPC methods). What this transport
 * uniquely owns is the framing and EOF machinery:
 *
 * - [[TrackingInputStream]] — byte-counting wrapper around
 *   `System.in` that detects a host closing stdin mid-frame.
 * - `EofObservingProvider` — the provider wrapper passed to
 *   `McpServer.sync(...)`, which intercepts `setSessionFactory`
 *   so every SDK-created session gets a `LatchOnCloseTransport`.
 * - `LatchOnCloseTransport` — per-session transport wrapper that
 *   (1) counts down the route's close latch on close paths (EOF
 *   detection) and (2) writes response frames SYNCHRONOUSLY to
 *   fd 1, bypassing the SDK's outbound scheduler (whose
 *   `isClosing` race would silently discard final responses).
 * - The unbuffered fd-1 sink (`FileOutputStream(FileDescriptor.out)`)
 *   and the Jackson `McpJsonMapper` used to serialize frames.
 *
 * Lifecycle concerns (the JVM-singleton guard, buildServer/stop/
 * awaitClose/signalClose, the SDK `McpServer.sync(...).build()`
 * chain) remain in [[io.sm8.platform.mcp.McpStdioRoute]] — the
 * composition root. The route consults the partial-frame state via
 * its own `trackingInput` field (passed in here) so the
 * `McpStdioRouteParseErrorOnCloseSpec` reflection seam keeps
 * working unchanged.
 *
 * ==Layer discipline (the layering RFC)==
 *
 * sm8-platform (transport library), NOT sm8-server (deployment).
 * Same placement rationale as McpStdioRoute itself.
 *
 * ==Thread-safety==
 *
 * - `TrackingInputStream` is single-threaded (SDK inbound loop
 *   only); its state fields are `@volatile` for the close path.
 * - Frame writes serialize on the `Out` monitor (`Out.synchronized`),
 *   same as before the split.
 * - The close latch is owned by the route; this class only
 *   `countDown()`s it from the SDK's close paths.
 *
 * ==OOM / NPE==
 *
 * No unbounded buffers (per-line synchronous writes, flushed
 * immediately). No nullable SDK returns are dereferenced without
 * a guard (the `beforeClose` callback and latch are constructor
 * params, never null in practice).
 */
package io.sm8.platform.mcp

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema
import io.modelcontextprotocol.spec.McpServerSession
import io.modelcontextprotocol.spec.McpServerTransport
import io.modelcontextprotocol.spec.McpServerTransportProvider

import java.util.concurrent.CountDownLatch
import scala.util.control.NonFatal

/** Wire-level stdio transport: framing, EOF detection, synchronous
 * outbound writes. See the file header for the concern split;
 * lifecycle lives in `McpStdioRoute`.
 *
 * @param trackingInput the byte-counting wrapper around `System.in`;
 *        created and owned by the route (which reads its
 *        `partialFramePending` state on the close path)
 * @param beforeClose callback run at the top of every close path
 *        (`LatchOnCloseTransport.close` / `closeGracefully`) BEFORE
 *        the latch counts down — the route supplies its
 *        partial-frame ParseError emission here
 * @param closeLatch the route's close latch; `countDown()` fires
 *        when the SDK's inbound loop sees stdin EOF (or an explicit
 *        close reaches the wrapped transport)
 */
private[mcp] final class McpStdioTransport(
 val trackingInput: TrackingInputStream,
 val beforeClose: () => Unit,
 val closeLatch: CountDownLatch
) {

 // JSON mapper + unbuffered fd-1 sink. The LatchOnCloseTransport
 // writes response frames synchronously through these. Created at
 // construction so the transport is fully wired before the route
 // hands `provider` to `McpServer.sync(...)`.
 private val JsonMapper: McpJsonMapper = new JacksonMcpJsonMapper(
 tools.jackson.databind.json.JsonMapper.builder().build()
)
 private val Out: java.io.FileOutputStream =
 new java.io.FileOutputStream(java.io.FileDescriptor.out)

 // a prior PR smoke fix (see McpStdioRoute file history): the SDK's
 // default ctor writes JSON-RPC frames to `System.out` — a
 // PrintStream that BLOCK-BUFFERS whenever fd 1 is a pipe. A piped
 // host therefore never sees a response, and writing through
 // `System.out` risks interleave with stray `Console.println`. The
 // fix: pass the unbuffered `FileOutputStream(FileDescriptor.out)`
 // as the sink (each write hits fd 1 directly). The SDK's session
 // transport wraps the InputStream we pass in its own BufferedReader
 // (verified via javap on StdioMcpSessionTransport), so `System.in`
 // raw is the correct source argument. NOTE: responses are also
 // written synchronously from LatchOnCloseTransport.sendMessage
 // (see its scaladoc) — this transport arg still matters because
 // the SDK's read loop uses the InputStream we pass.
 private val delegate: StdioServerTransportProvider =
 new StdioServerTransportProvider(
 JsonMapper,
 trackingInput,
 Out
 )

 private val observingProvider: McpServerTransportProvider =
 new EofObservingProvider(delegate)

 /** The provider to pass to `McpServer.sync(...)` — the
 * `EofObservingProvider` wrapper, so every SDK-created session
 * gets a `LatchOnCloseTransport` (EOF latch + synchronous writes).
 */
 def provider: McpServerTransportProvider = observingProvider

 /** The RAW underlying transport's `closeGracefully()` Mono, for
 * the route's `stop()` drain (a prior PR's fix semantics: block
 * until the SDK's non-daemon inbound + outbound executors
 * dispose). NOT the observing wrapper's — draining must reach
 * the real executors, and the wrapper only delegates anyway.
 */
 def closeGracefully(): reactor.core.publisher.Mono[Void] =
 delegate.closeGracefully()

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
 beforeClose()
 closeLatch.countDown()
 delegate.closeGracefully()
 }

 override def close(): Unit = {
 beforeClose()
 closeLatch.countDown()
 delegate.close()
 }

 override def unmarshalFrom[T](data: Any, typeRef: io.modelcontextprotocol.json.TypeRef[T]): T =
 delegate.unmarshalFrom(data, typeRef)
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
 // McpStdioRoute (which reads partialFramePending on the close
 // path) and TrackingInputStreamSpec (same package, different
 // file) can read the state.
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
