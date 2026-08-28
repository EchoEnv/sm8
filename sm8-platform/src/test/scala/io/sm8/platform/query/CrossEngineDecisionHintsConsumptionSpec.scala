/*
 * SM8 Platform — cross-engine DecisionHints consumption spec.
 *
 * The ADR-009-d addendum (item 13) makes the engine-portable decision
 * contract explicit: every `EngineProvider` MUST either consume each
 * `DecisionHints` field for which its engine has a native config, or
 * return `EngineError.UnsupportedCapability` naming the unsupported
 * capability. A silent drop (decision produced but ignored with no
 * error and no log) is a contract violation of the same severity as a
 * fold miss in the platform.
 *
 * This spec drives the REAL platform fold (`EngineService.runQueryWithHooks`)
 * so that step-3 of the pipeline — the provider's own read of the typed
 * `EngineContext.decisionHints` — is exercised, not just `DecisionHints`
 * in isolation. It covers ONLY the cross-engine boundary (plugin -> fold
 * -> non-engine adapter):
 *
 *  - Test 1: a broadcast plugin arms `broadcastArmed`/`broadcastThresholdBytes`
 *    against an `InMemoryEngineProvider` query. The silent-empty success
 *    BEFORE this change is now a typed `UnsupportedCapability`.
 *  - Test 2 (referenced, not duplicated): the platform fold reaching
 *    `StubProvider` with non-empty `decisionHints` in `capturedCtx` is
 *    already asserted by `EngineServiceRunQueryWithHooksSpec` (the
 *    "oracle-wired path" tests at that spec's `StubProvider.capturedCtx`).
 *  - Test 3: no oracle wired, run against InMemory — `Right(empty)`
 *    unchanged (no regression on the bare path).
 */
package io.sm8.platform.query

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.engine.{
  EngineContext,
  EngineError,
  EngineRegistry,
  PortableQueryResult,
  ResultRow
}
import io.sm8.core.model.{
  AuditPolicy,
  CachePolicy,
  MaterializePolicy,
  Model,
  ModelPolicyDefaults,
  ModelStatus,
  SourceRef
}
import io.sm8.core.rel.JoinKind
import io.sm8.core.model.JoinSpec
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.core.cache.ResultCache
import io.sm8.platform.query.hooks.EngineHookDispatcher
import io.sm8.connectors.inmemory.InMemoryEngineProvider  // ADR-009-d: test-scope only — see sm8-platform/pom.xml `<scope>test</scope>` declaration. In-memory is the cheapest realizable cross-engine DecisionHints consumer (its provider has no remote to set up).

class CrossEngineDecisionHintsConsumptionSpec extends AnyFunSuite with Matchers {

  private val dummyModel: Model = Model(
    name = "m",
    version = 1,
    description = None,
    dimensions = Nil,
    measures = Nil,
    defaultPolicies = ModelPolicyDefaults(
      MaterializePolicy.None,
      CachePolicy.NoCache,
      AuditPolicy.NoAudit),
    source = SourceRef.ByName(table = "t"),
    status = ModelStatus.Draft,
    filters = Nil
  )

  /** A small join (est = 1M rows, <= the broadcast 10M arm threshold):
    * arms the BroadcastStub and writes a 10 MiB byte budget. */
  private val modelWithSmallJoin: Model = dummyModel.copy(
    joins = List(JoinSpec(
      name          = "orders.customers",
      rightModel    = "customers",
      kind          = JoinKind.Inner,
      keys          = List("region" -> "region"),
      estimatedRows = Some(1_000_000L))))

  private def inMemoryRegistry: io.sm8.core.engine.EngineRegistry = {
    val inMemory = new InMemoryEngineProvider
    EngineRegistry(Map("in-memory" -> inMemory), default = "in-memory")
  }

  /** Register the real BroadcastStub + SkewStub as the platform wires
    * them — their PreExecute hooks write the arm keys into
    * Context.meta; the engineExecutor fold builds DecisionHints. */
  private def dispatcherWithDecisionPlugins: EngineHookDispatcher = {
    val engineImpl = new io.sm8.core.EngineImpl
    engineImpl.use(new io.sm8.plugins.broadcast.BroadcastStub)
    engineImpl.use(new io.sm8.plugins.skew.SkewStub)
    EngineHookDispatcher(engineImpl.hooks)
  }

  // Test 2 (the fold reaching the engine) already exists in
  // EngineServiceRunQueryWithHooksSpec ("oracle-wired path" tests assert
  // StubProvider.capturedCtx.decisionHints is non-empty after the fold);
  // it is NOT duplicated here per instruction.

  test("cross-engine: broadcast decision against InMemory yielded a TYPED UnsupportedCapability, never a silent empty success") {
    val dispatcher = dispatcherWithDecisionPlugins
    val registry   = inMemoryRegistry
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "in-memory"),
      model      = modelWithSmallJoin,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    // The fold ran; the broadcast plugin armed broadcastArmed=Some(true)
    // and broadcastThresholdBytes=Some(10485760). In-memory cannot honor
    // the decision — before this PR it returned Right(empty); the silent
    // drop is now an explicit typed error.
    out.isLeft shouldBe true
    val err = out.swap.getOrElse(fail("expected Left(UnsupportedCapability), got Right"))
    err shouldBe a [EngineError.UnsupportedCapability]
    val uc: EngineError.UnsupportedCapability =
      err.asInstanceOf[EngineError.UnsupportedCapability]
    uc.engine shouldBe "in-memory-connector"
    uc.capability should not be empty
    // deterministically the first non-None field (broadcastArmed)
    uc.capability shouldBe "sm8.broadcast.arm"
  }

  test("ADR-009-d cross-engine: InMemoryEngineProvider honors an empty (no-oracle) fold — Right(empty) unchanged") {
    // No broadcast/skew plugins registered: the fold builds
    // DecisionHints(all-None); InMemory accepts it and returns the
    // same empty PortableQueryResult as before this PR.
    val engineImpl = new io.sm8.core.EngineImpl
    val dispatcher = EngineHookDispatcher(engineImpl.hooks)
    val registry   = inMemoryRegistry
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "in-memory"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    val qr = out.toOption.getOrElse(fail(s"expected Right, got $out"))
    qr.rows shouldBe empty
    qr.rowCount shouldBe 0L
  }
}