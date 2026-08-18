/*
 * SM8 in-memory Connector — Replay-safety / determinism spec (PR-F per ADR-007).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #5 (driver-vs-executor
 * asymmetry) and Restate-journal-replay-safety: the engine must be
 * deterministic for the same input — no time-dependent sources, no
 * accumulator mutation, no random.
 *
 * For the in-memory engine: the `query` implementation is a pure
 * function returning `Right(PortableQueryResult(...))` with no
 * captured state, no IO, no time. Determinism is structurally
 * guaranteed by construction. This spec asserts the contract
 * explicitly.
 *
 * Per ADR-007 §PR-F: per-provider strategy. For InMemory, the
 * check is `a shouldBe b` on the `PortableQueryResult` case
 * class (structural equality).
 *
 * Per [[scala-jvm-safety-mindset]]: no static / ThreadLocal state;
 * per [[scala-perf-testing-mindset]]: no allocations beyond the
 * PortableQueryResult itself.
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{EngineContext, EngineProvider, QueryRequest}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InMemoryEngineReplaySafetySpec extends AnyFunSuite with Matchers {

  test("query is deterministic for identical input (Restate-replay-safe)") {
    val provider = new InMemoryEngineProvider()
    val request = QueryRequest(model = "test", limit = Some(100L))
    val ctx = EngineContext.defaultContext

    val a = provider.query(null, request, ctx)
    val b = provider.query(null, request, ctx)

    // The Either return + case-class PortableQueryResult are
    // structurally equal. No time, no random, no IO involved.
    a shouldBe b
  }

  test("query is deterministic across many invocations (100 calls)") {
    val provider = new InMemoryEngineProvider()
    val request = QueryRequest(model = "test")
    val ctx = EngineContext.defaultContext

    val first = provider.query(null, request, ctx)
    for (_ <- 1 to 100) {
      provider.query(null, request, ctx) shouldBe first
    }
  }

  test("query does NOT mutate the EngineContext (engine is a pure function)") {
    val provider = new InMemoryEngineProvider()
    val request = QueryRequest(model = "test")
    val original = EngineContext.defaultContext
    val ctx = original.copy()  // defensive copy

    provider.query(null, request, ctx)
    ctx shouldBe original  // unchanged
  }
}
