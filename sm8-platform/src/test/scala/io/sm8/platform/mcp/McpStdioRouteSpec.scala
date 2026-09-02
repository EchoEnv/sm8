/*
 * SM8 Platform — McpStdioRouteSpec.
 *
 * Per the stdio design verification criteria: unit tests for
 * the in-process stdio MCP transport. PR-264 redesigned the EOF
 * wiring (see McpStdioRoute scaladoc): the latch counts down when
 * the SDK's transport close reaches our LatchOnCloseTransport, which
 * happens on BOTH paths:
 * - stdin EOF: the SDK's inbound loop finally -> session.close() ->
 *   transport.close() (our wrapper) -> countDown
 * - explicit stop(): syncServer.closeGracefully() -> provider ->
 *   session.closeGracefully() -> transport.closeGracefully() (our
 *   wrapper) -> countDown + subscribe + drain of the underlying
 *   transport's closeGracefully() Mono (PR-265 HIGH-3 fix:
 *   stop() now blocks until the SDK's non-daemon inbound +
 *   outbound executors dispose, so tests don't leak threads).
 *
 * The EOF path cannot be exercised in-process (the test JVM's
 * System.in is not closable), so it is covered by
 * scripts/smoke-mcp-stdio.sh (real subprocess stdin + EOF + 5-tool
 * assert + clean exit <15s) and by StdioEndToEndSpec (real
 * subprocess stdin + EOF + assert).
 *
 * 4 tests:
 * 1. buildServer() succeeds (no exception)
 * 2. stop() counts down the latch; awaitClose returns true
 * 3. awaitClose(1) returns false while the session stays open
 * 4. McpStdioRoute instances are JVM-singletons (de-L6 fix;
 *    constructing two in the same JVM throws)
 */
package io.sm8.platform.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class McpStdioRouteSpec extends AnyFunSuite with Matchers {

  // The SDK never calls the ingress client during initialize /
  // tools/list, so an unroutable port is fine for these tests.
  private def newClient(): HttpIngressClient.Impl =
    new HttpIngressClient.Impl(
      ingressUrl = "http://127.0.0.1:65535",
      requestTimeout = Duration.ofSeconds(1)
    )

  private def newRoute(name: String): McpStdioRoute = {
    McpStdioRoute(name, "0.1.0-TEST", Sm8ToolHandlers.build(newClient()))
  }

  test("buildServer() succeeds (no exception)") {
    val route = newRoute("sm8-platform-mcp-spec")
    val server = route.buildServer()
    server should not be null
    // PR-265 HIGH-3: call route.stop() (which now blocks on the
    // SDK transport closeGracefully Mono) — do NOT just call
    // server.closeGracefully(), which leaks the inbound +
    // outbound executors across the JVM.
    route.stop()
  }

  test("stop() counts down the latch; awaitClose returns true") {
    val route = newRoute("sm8-platform-mcp-spec-2")
    route.buildServer()
    // stop() -> McpSyncServer.closeGracefully() -> provider -> real
    // session.closeGracefully() -> onClose cleanup + wrapped
    // transport.closeGracefully() -> countDown + drain. This is
    // the exact chain the JVM shutdown hook uses; the EOF chain
    // reaches the same wrapper via transport.close().
    route.stop()
    route.awaitClose(timeoutSeconds = 3) shouldBe true
  }

  test("awaitClose returns false while the session stays open") {
    val route = newRoute("sm8-platform-mcp-spec-3")
    route.buildServer()
    // No stop(), no EOF (the test JVM's stdin stays open) -> the
    // latch must NOT fire within the timeout. Then stop() with
    // the same drain semantics as test2.
    route.awaitClose(timeoutSeconds = 1) shouldBe false
    route.stop()
  }

  test("McpStdioRoute is a JVM-singleton (only one instance per process)") {
    // Per de-L6: a single session per process is the v1 limit (the
    // stdio transport reads from System.in, a single fd-0). Two
    // concurrent McpStdioRoute instances would interleave reads and
    // corrupt the JSON-RPC stream. Enforce this at the factory.
    // Note: ScalaTest does NOT guarantee test ordering, so we
    // explicitly reset the singleton counter at the start of this
    // test to give it a clean slate.
    McpStdioRoute.INSTANCE_COUNT.set(0)
    val route1 = newRoute("sm8-platform-mcp-spec-singleton")
    route1.buildServer()
    try {
      val thrown = try {
        val route2 = newRoute("sm8-platform-mcp-spec-singleton-2")
        route2.buildServer()
        // If we get here, the singleton guard did NOT throw — test fails.
        null
      } catch { case e: IllegalStateException => e }
      assert(thrown != null, "expected IllegalStateException when constructing a second McpStdioRoute instance")
      assert(thrown.getMessage.contains("only one instance per JVM"),
        s"unexpected message: ${thrown.getMessage}")
    } finally {
      route1.stop()
    }
  }
}
