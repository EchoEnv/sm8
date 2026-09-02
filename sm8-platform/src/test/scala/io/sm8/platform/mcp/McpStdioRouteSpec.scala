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

import io.modelcontextprotocol.server.McpServerFeatures
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.util.control.NonFatal

class McpStdioRouteSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  // Per C5-de-M5: previously tests relied on test ordering (1, 2, 3, 4)
  // and test 4 directly poked `INSTANCE_COUNT.set(0)` to recover from a
  // failed prior run. ScalaTest does NOT guarantee ordering, so this
  // was fragile. Reset the counter before every test to give each one
  // a clean slate.
  override def beforeEach(): Unit = {
    McpStdioRoute.INSTANCE_COUNT.set(0)
  }

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
    //
    // Per C5-de-M5: the singleton counter is now reset in
    // beforeEach() (BeforeAndAfterEach trait), so this test no longer
    // needs to poke INSTANCE_COUNT directly.
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

  // Per C5-de-H1: a new method `signalClose()` was added so the JVM
  // shutdown hook can wake `awaitClose(...)` within milliseconds
  // instead of waiting for the full timeout. This test asserts the
  // contract: signalClose() must wake a blocked awaitClose within
  // milliseconds, even without calling stop().
  test("signalClose() wakes awaitClose() without invoking stop()") {
    val route = newRoute("sm8-platform-mcp-spec-signal-close")
    route.buildServer()
    try {
      val t0 = System.currentTimeMillis()
      // Run awaitClose on a background thread (it would otherwise
      // block for the full timeout).
      val awaiter = new Thread(() => route.awaitClose(timeoutSeconds = 30))
      awaiter.start()
      Thread.sleep(100) // give awaiter time to actually start waiting
      route.signalClose()
      awaiter.join(5000)
      val elapsed = System.currentTimeMillis() - t0
      assert(!awaiter.isAlive, s"awaiter thread should have terminated; elapsed=${elapsed}ms")
      assert(elapsed < 1000, s"signalClose() should wake within milliseconds, took ${elapsed}ms")
    } finally {
      route.stop()
    }
  }

  // Per C5-de-M3 + C5-r2-de-M1: the factory must release the JVM-singleton
  // guard if the underlying `new McpStdioRoute(...)` throws. The current
  // constructor doesn't itself throw (it's only field init), but the
  // apply() guard exists for FUTURE invariants (e.g., constructor-time
  // validation). This test verifies the guard is wired correctly by:
  // (a) constructing an instance (counter goes 0 -> 1),
  // (b) verifying the counter is at 1,
  // (c) calling stop() which resets the counter via the test-clean path,
  // (d) verifying a fresh construction succeeds (counter back to 1 then
  //     0 after stop()).
  //
  // The actual NonFatal catch path is hard to trigger in pure unit tests
  // because the constructor has no throw paths today. We assert the
  // invariant empirically: the singleton guard is RELEASED by stop() so
  // a subsequent apply() succeeds (this is the same code path the catch
  // block invokes: `INSTANCE_COUNT.set(0)`).
  test("apply() releases INSTANCE_COUNT after stop() (proves the guard release path)") {
    val route1 = newRoute("sm8-platform-mcp-spec-factory-1")
    route1.buildServer()
    // After construction, the guard is at 1 (CAS succeeded).
    assert(McpStdioRoute.INSTANCE_COUNT.get == 1,
      s"counter should be 1 after construction, was ${McpStdioRoute.INSTANCE_COUNT.get}")
    route1.stop()
    // After stop(), the guard is released back to 0.
    assert(McpStdioRoute.INSTANCE_COUNT.get == 0,
      s"counter should be 0 after stop(), was ${McpStdioRoute.INSTANCE_COUNT.get}")
    // A fresh apply() now succeeds.
    val route2 = newRoute("sm8-platform-mcp-spec-factory-2")
    route2.buildServer()
    try {
      assert(McpStdioRoute.INSTANCE_COUNT.get == 1)
    } finally {
      route2.stop()
    }
  }

  // Per C5-r2-de-M1: exercise the REAL release path. `apply()` acquires
  // the guard but the failure-prone SDK builder chain runs in
  // `buildServer()` (after apply returns). buildServer now catches
  // NonFatal and releases the guard before rethrowing. We force a
  // buildServer failure with a null tool spec (the SDK's builder
  // chain NPEs on it) and assert:
  // (a) buildServer throws,
  // (b) the guard is released (counter back to 0),
  // (c) a subsequent valid apply + buildServer succeeds.
  test("buildServer() releases INSTANCE_COUNT when the SDK builder throws") {
    val badRoute = McpStdioRoute(
      "sm8-platform-mcp-spec-bad", "0.1.0-TEST",
      Seq(null.asInstanceOf[McpServerFeatures.SyncToolSpecification])
    )
    val thrown = try {
      badRoute.buildServer()
      None
    } catch { case NonFatal(e) => Some(e) }
    assert(thrown.isDefined, "expected buildServer() to throw for a null tool spec")
    assert(McpStdioRoute.INSTANCE_COUNT.get == 0,
      s"counter should be released after buildServer failure, was ${McpStdioRoute.INSTANCE_COUNT.get}")
    // The JVM is not poisoned: a fresh valid construction succeeds.
    val goodRoute = newRoute("sm8-platform-mcp-spec-after-failure")
    goodRoute.buildServer()
    try {
      assert(McpStdioRoute.INSTANCE_COUNT.get == 1)
    } finally {
      goodRoute.stop()
    }
  }
}
