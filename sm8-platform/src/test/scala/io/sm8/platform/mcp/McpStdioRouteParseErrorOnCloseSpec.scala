/*
 * SM8 Platform — McpStdioRouteParseErrorOnCloseSpec.
 *
 * End-to-end coverage for the partial-frame EOF handler. When the
 * host closes stdin mid-frame (no trailing newline), the close path
 * emits a JSON-RPC `-32700 ParseError` envelope to stderr BEFORE
 * countDown. This spec exercises the full wiring —
 * `McpStdioRoute.triggerParseErrorOnClose` — not just the
 * `TrackingInputStream` state machine in isolation (that coverage
 * lives in TrackingInputStreamSpec).
 *
 * The regression vector this guards against: someone deletes one of
 * the two `writePartialFrameParseErrorIfPending()` call sites at
 * McpStdioRoute.scala:299/305 (or the underlying helper). The unit
 * tests would still pass because they exercise `TrackingInputStream`
 * alone; the close path would silently stop emitting. This spec
 * catches that regression by feeding partial-frame bytes through
 * the wrapper and asserting stderr receives the envelope.
 *
 * Test mechanics:
 * 1. Swap `System.err` with a `ByteArrayOutputStream` capture.
 * 2. Construct a `TrackingInputStream` wrapping a `ByteArrayInputStream`
 *    with a deliberately partial frame.
 * 3. Drain the stream so `partialFramePending = true`.
 * 4. Wire it into a `McpStdioRoute` via reflection on the private
 *    `trackingInput` field — the same field the SDK transport sees.
 * 5. Invoke `triggerParseErrorOnClose()` (the package-private test
 *    seam that the production close path uses).
 * 6. Assert the captured stderr contains the ParseError envelope
 *    with id=null and code=-32700.
 * 7. Invoke a SECOND time and assert stderr is UNCHANGED — proves
 *    the idempotency clear (M1 reviewer finding) works.
 */
package io.sm8.platform.mcp

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class McpStdioRouteParseErrorOnCloseSpec
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach {

  private var originalErr: PrintStream = _
  private var errCapture: ByteArrayOutputStream = _

  override def beforeEach(): Unit = {
    originalErr = java.lang.System.err
    errCapture = new ByteArrayOutputStream()
    java.lang.System.setErr(new PrintStream(errCapture, true, "UTF-8"))
  }

  override def afterEach(): Unit = {
    java.lang.System.setErr(originalErr)
  }

  // Reflectively set the private `trackingInput` field on a freshly
  // constructed McpStdioRoute. The field is package-private
  // (`private[mcp]`) — same package access works, but the field name
  // itself is private to the instance so we still need reflection.
  // Drains the wrapped stream so `partialFramePending` reflects the
  // actual bytes consumed (EOF state is set during the last read()
  // call, not at construction time).
  private def installAndDrainTrackingInput(
    route: McpStdioRoute,
    bytes: Array[Byte]
  ): TrackingInputStream = {
    val field = classOf[McpStdioRoute].getDeclaredField("trackingInput")
    field.setAccessible(true)
    val wrapper = new TrackingInputStream(new ByteArrayInputStream(bytes))
    // Drain so EOF is observed and partialFramePending is set based
    // on whether the last byte was '\n' or not.
    while (wrapper.read() != -1) ()
    field.set(route, wrapper)
    wrapper
  }

  test("partial-frame EOF emits JSON-RPC -32700 ParseError envelope to stderr") {
    // Partial JSON: no trailing newline (the host closed stdin mid-frame).
    val partial = "{\"jsonrpc\":\"2.0\",\"meth".getBytes("UTF-8")
    val route = newRoute("sm8-platform-parse-error-on-close")
    installAndDrainTrackingInput(route, partial)
    try {
      route.triggerParseErrorOnClose()
      val stderr = errCapture.toString("UTF-8")
      assert(stderr.contains("\"jsonrpc\":\"2.0\""))
      assert(stderr.contains("\"code\":-32700"))
      assert(stderr.contains("\"id\":null"))
      assert(stderr.contains("Parse error"))
    } finally {
      // Release the JVM-singleton guard so the next test can build.
      route.stop()
    }
  }

  test("clean EOF (frame ends on newline) does NOT emit a ParseError") {
    val clean = "{\"jsonrpc\":\"2.0\"}\n".getBytes("UTF-8")
    val route = newRoute("sm8-platform-parse-error-clean-eof")
    installAndDrainTrackingInput(route, clean)
    try {
      route.triggerParseErrorOnClose()
      val stderr = errCapture.toString("UTF-8")
      assert(stderr.isEmpty, s"expected no stderr output on clean EOF, got: $stderr")
    } finally {
      route.stop()
    }
  }

  test("double-trigger is idempotent: second close emits no additional envelope") {
    val partial = "{\"id\":1,\"meth".getBytes("UTF-8")
    val route = newRoute("sm8-platform-parse-error-double-trigger")
    installAndDrainTrackingInput(route, partial)
    try {
      route.triggerParseErrorOnClose()
      val firstStderr = errCapture.toString("UTF-8")
      assert(firstStderr.contains("\"code\":-32700"))
      // Second invocation: partialFramePending must have been cleared
      // after the first write (M1 idempotency fix), so no new envelope
      // is emitted. Idempotency matters because the SDK may call both
      // close() and closeGracefully() during full shutdown.
      route.triggerParseErrorOnClose()
      val secondStderr = errCapture.toString("UTF-8")
      assert(
        secondStderr == firstStderr,
        s"second trigger must be a no-op; got new output: " +
          secondStderr.drop(firstStderr.length)
      )
    } finally {
      route.stop()
    }
  }

  // Test factory: build a real McpStdioRoute instance without
  // calling buildServer(). We never call buildServer() in this spec
  // because the test only exercises the close-path helper.
  // Empty toolSpecs is fine — the constructor accepts any Seq, and
  // the test path doesn't invoke .tools(...) on the SDK builder.
  private def newRoute(name: String): McpStdioRoute =
    McpStdioRoute(name, "0.0.0-test", Seq.empty, 30L)
}
