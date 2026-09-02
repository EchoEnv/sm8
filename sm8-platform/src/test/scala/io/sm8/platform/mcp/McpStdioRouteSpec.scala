/*
 * SM8 Platform — McpStdioRouteSpec.
 *
 * Per the stdio design (a prior PR, merged) verification criteria: unit tests for
 * the in-process stdio MCP transport. a prior PR redesigned the EOF
 * wiring (see McpStdioRoute scaladoc): the latch counts down when
 * the SDK's transport close reaches our LatchOnCloseTransport, which
 * happens on BOTH paths:
 * - stdin EOF: the SDK's inbound loop finally -> session.close() ->
 * transport.close() (our wrapper) -> countDown
 * - explicit stop(): syncServer.closeGracefully() -> provider ->
 * session.closeGracefully() -> transport.closeGracefully() (our
 * wrapper) -> countDown
 *
 * The EOF path cannot be exercised in-process (the test JVM's
 * System.in is not closable), so it is covered by
 * scripts/smoke-mcp-stdio.sh (real subprocess stdin + EOF + 5-tool
 * assert + clean exit <10s).
 *
 * 3 tests:
 * 1. buildServer() succeeds (no exception)
 * 2. stop() counts down the latch; awaitClose returns true
 * 3. awaitClose(1) returns false while the session stays open
 */
package io.sm8.platform.mcp

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class McpStdioRouteSpec extends AnyFunSuite with Matchers {

 private def newRoute(name: String): McpStdioRoute = {
 // The SDK never calls the ingress client during initialize /
 // tools/list, so an unroutable port is fine for these tests.
 val client = new HttpIngressClient.Impl(
 ingressUrl = "http://127.0.0.1:65535",
 requestTimeout = Duration.ofSeconds(1)
)
 McpStdioRoute(name, "0.1.0-TEST", Sm8ToolHandlers.build(client))
 }

 test("buildServer() succeeds (no exception)") {
 val route = newRoute("sm8-platform-mcp-spec")
 val server = route.buildServer()
 server should not be null
 server.closeGracefully()
 }

 test("stop() counts down the latch; awaitClose returns true") {
 val route = newRoute("sm8-platform-mcp-spec-2")
 route.buildServer()
 // stop() -> McpSyncServer.closeGracefully() -> provider -> real
 // session.closeGracefully() -> onClose cleanup + wrapped
 // transport.closeGracefully() -> countDown. This is the exact
 // chain the JVM shutdown hook uses; the EOF chain reaches the
 // same wrapper via transport.close().
 route.stop()
 route.awaitClose(timeoutSeconds = 3) shouldBe true
 }

 test("awaitClose returns false while the session stays open") {
 val route = newRoute("sm8-platform-mcp-spec-3")
 route.buildServer()
 // No stop(), no EOF (the test JVM's stdin stays open) -> the
 // latch must NOT fire within the timeout.
 route.awaitClose(timeoutSeconds = 1) shouldBe false
 route.stop()
 }
}
