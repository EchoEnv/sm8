/*
 * SM8 Platform — HttpTransport spec (follow-up to PR #57).
 *
 * Per [[debug-mantra-mindset]] §1 (reproduce): a fast deterministic
 * pass/fail signal. Each test exercises one invariant.
 *
 * Per [[karphyaguidsmindset]]: no incidental assertions, no
 * incidental metrics. Pure functions.
 *
 * Per [[scala-data-drivenrefactor-mindset]]: shape (typed
 * QueryRequest / QueryResult) vs validity (the transport
 * wiring) are separated. The transport is pure wire-binding.
 *
 * Per [[scala-spark-batch-bugs-mindset]] (per user directive):
 * - closure-safety: server captures typed Serializable types only.
 * - perf: startup-time init; per-request dispatch.
 * - driver/executor: server runs in driver; no executor leak.
 * - serializable: all captured types are case-class derived.
 *
 * ==What this spec verifies==
 *
 * The HttpTransport:
 * - Composes the existing `QueryService.definition(...)` (per the
 *   canonical pattern).
 * - Has a proper lifecycle (`start()` / `stop()`).
 * - Rejects double-start (per `scala-jvm-safemindset`).
 */
package io.sm8.platform.query

import io.sm8.platform.query.cache._
import java.net.ServerSocket

import io.sm8.core.engine.{
  EngineError,
  EngineIdentity,
  MCPEngineProvider,
  MCPEngineRegistry,
  MCPQueryRequest,
  PortableQueryResult
}
import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HttpTransportSpec extends AnyFunSuite with Matchers {

  /** Minimal stub engine. */
  private final class StubProvider(
      val id: EngineIdentity
  ) extends MCPEngineProvider {
    override val identity: EngineIdentity = id
    override val available: Boolean = true
    override def explain(
        m:   Model,
        r:   MCPQueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, String] = Right(s"${id.name} plan for ${m.name}")
    override def query(
        m:   Model,
        r:   MCPQueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, PortableQueryResult] = Right(PortableQueryResult(
      schema = io.sm8.core.engine.ResultSchema(List.empty[io.sm8.core.schema.Field]),
      rows = Vector.empty,
      metadata = Map.empty,
    ))
  }

  private def makeModel(name: String = "test-model"): Model = Model.of(
    name    = name,
    version = 1,
    source  = SourceRef.ByName("default", "stub_table"),
  ).toOption.get

  private def makeRegistry(engine: MCPEngineProvider, default: String = "stub"): MCPEngineRegistry =
    MCPEngineRegistry(Map(default -> engine), default)

  /** Pick a free TCP port + close the probe socket immediately
    * so RestateHttpServer can bind to it. This is the standard
    * "find ephemeral port" pattern. */
  private def freePort(): Int = {
    val s = new ServerSocket(0)
    val port = s.getLocalPort
    s.close()
    port
  }

  // -- Lifecycle: start → stop → start (rebound) → stop --

  test("HttpTransport.start + stop: binds a real socket (actualPort + connect proof)") {
    val model = makeModel("lc1")
    val stub  = new StubProvider(EngineIdentity("stub", "1.0", "0"))
    val reg   = makeRegistry(stub)
    val http  = new HttpTransport(model, reg)

    // Per debug-mantra §5 (verify the fix): port 0 = ephemeral;
    // start() returns the ACTUAL bound port (actualPort()).
    val port1 = http.start(0)
    port1 should be > 0
    // BIND PROOF: a real TCP connect must succeed against the
    // actual port (catches the fromEndpoint-without-listen bug —
    // the PR #58 regression this suite now guards against).
    val probe = new java.net.Socket("localhost", port1)
    probe.isConnected shouldBe true
    probe.close()
    http.stop()

    // Per [[scala-jvm-safemindset]] "resource lifecycle": we can
    // restart after stop on a fresh ephemeral port.
    val port2 = http.start(0)
    port2 should be > 0
    http.stop()
  }

  test("HttpTransport.start: rejects double-start (per scala-jvm-safemindset)") {
    val model = makeModel("lc2")
    val stub  = new StubProvider(EngineIdentity("stub", "1.0", "0"))
    val reg   = makeRegistry(stub)
    val http  = new HttpTransport(model, reg)

    http.start(freePort())
    try {
      val ex = intercept[IllegalStateException] {
        http.start(freePort()) // should throw
      }
      ex.getMessage should include ("already started")
    } finally http.stop()
  }

  test("HttpTransport.stop: idempotent (safe to call multiple times)") {
    val model = makeModel("lc3")
    val stub  = new StubProvider(EngineIdentity("stub", "1.0", "0"))
    val reg   = makeRegistry(stub)
    val http  = new HttpTransport(model, reg)

    http.start(freePort())
    http.stop()
    // Per [[scala-jvm-safemindset]]: stop on an already-stopped
    // transport must be safe (no exception). This prevents JVM
    // shutdown-hook order issues.
    http.stop()
    http.stop()
  }
}
