/*
 * SM8 Platform — JoinPathPreHookCycleDetectionSpec (ADR-010-a v0.3).
 *
 * Regression test for the silent-no-op defect discovered in the
 * 2026-08-26 dual senior codebase review:
 * `EngineHookDispatcher.run` hardcoded `PipelineStage.Execute`,
 * so `JoinPathPreHook` (registered at `HookStage.PreResolve`) was
 * silently inert in production since PR #32 (commit `daac360`,
 * 2026-08-14). The cycle validator never ran; users got a generic
 * `ProviderInvocationFailed("NoResult")` for cycle models instead
 * of the typed `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)`.
 *
 * After the orchestrator fix, the cycle validator IS invoked on
 * every request, and v0.3 typed-error surfacing ensures the typed
 * `EngineError` reaches the caller via `Left(...)`.
 *
 * Per [[debug-mantra-mindset]] §5 (verify): this spec exercises the
 * END-TO-END path through `EngineService.runQueryWithHooks` (the
 * production entry point) — not `hook.run(...)` direct calls. The
 * 1163-test baseline was a false green because existing specs
 * called hooks directly; this spec uses the production seam.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct change):
 * the cycle model is minimal — a single self-referencing
 * `CalculatedMeasure` is enough to trip the cycle detector.
 */
package io.sm8.platform.query

import io.sm8.core.cache._
import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineRegistry,
  PortableQueryResult}
import io.sm8.core.engine.{ QueryRequest => CoreQueryRequest }
import io.sm8.core.expr.Expr
import io.sm8.core.model.{CalculatedMeasure, Dimension, MaterializePolicy, CachePolicy, AuditPolicy,
  Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.SealedDataType
import io.sm8.platform.query.hooks.{EngineHookDispatcher, HookRunnerOrchestration}
import io.sm8.plugins.semanticgraph.SemanticGraphPlugin

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JoinPathPreHookCycleDetectionSpec extends AnyFunSuite with Matchers {

  // Minimal stub provider — the cycle model never reaches the
  // engine because the PreResolve hook short-circuits.
  private final class NeverQueryProvider extends EngineProvider {
    override val identity: EngineIdentity = EngineIdentity("never", "0.0.0", "0.0.0")
    override val available: Boolean = true
    override def query(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext): Either[EngineError, PortableQueryResult] =
      fail("engine must NOT be queried — the cycle validator short-circuits the pipeline")
    override def explain(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext): Either[EngineError, String] = Right("never")
  }

  /**
   * Build a cycle model: a `CalculatedMeasure` that references
   * itself via `MeasureRef`. `SemanticGraphBuilder` adds the
   * self-loop edge `(model.name, "self") -> (model.name, "self")`,
   * which JGraphT's `CycleDetector` reports as a real cycle.
   *
   * Per ADR-008-AI v1.1, this is the canonical cycle pattern
   * (`bad = bad + 1`).
   */
  private val cycleModel: Model = Model.of(
    name    = "cycle_fixture",
    version = 1,
    description = None,
    dimensions = List(
      Dimension(name = "amount", expr = Expr.FieldRef("amount"),
        dataType = Some(SealedDataType.Int))
    ),
    measures = Nil,
    defaultPolicies = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache       = CachePolicy.NoCache,
      audit       = AuditPolicy.NoAudit
    ),
    source = SourceRef.byName("in-memory", "x"),
    status = ModelStatus.Draft,
    filters = Nil,
    calculatedMeasures = List(
      CalculatedMeasure(
        name = "self",
        // Self-referencing MeasureRef — the cycle.
        expr = Expr.MeasureRef("self")
      )
    ),
    joins = Nil
  ).toOption.get

  /**
   * Build the production-shape orchestrator: EngineImpl +
   * SemanticGraphPlugin registered, then EngineHookDispatcher +
   * HookRunnerOrchestration wrapping it.
   */
  private def orchestratorFor(engine: io.sm8.core.EngineImpl): HookRunnerOrchestration =
    HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))

  test("cycle model: typed Left(EngineError.UnsupportedCapability(\"SemanticGraph.cycle\", ...)) " +
       "— NOT ProviderInvocationFailed(\"NoResult\")") {
    // Wire the orchestrator + cycle plugin + a never-query provider.
    // The orchestrator drives the PreResolve stage where the
    // JoinPathPreHook fires.
    val engine = new io.sm8.core.EngineImpl
    new SemanticGraphPlugin().setup(engine)
    val orchestrator = orchestratorFor(engine)

    val provider = new NeverQueryProvider
    val registry = EngineRegistry(
      Map("never" -> provider),
      default = "never"
    )

    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("cycle_fixture", Nil, Nil, "", "never"),
      model      = cycleModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = orchestrator
    )

    // The typed error reaches the caller.
    out.isLeft shouldBe true
    val err = out.left.get
    err shouldBe a [EngineError.UnsupportedCapability]
    val typed = err.asInstanceOf[EngineError.UnsupportedCapability]
    typed.capability shouldBe "SemanticGraph.cycle"
    typed.engine     shouldBe "semantic-graph-plugin"
    typed.message    should include ("cycle_fixture")
  }

  test("cycle model: NOT ProviderInvocationFailed(\"NoResult\") — the v0.3 typed-error surfacing " +
       "surfaces the typed error instead of the generic NoResult") {
    // Per ADR-010-a v0.3: without the typed-error surfacing, the
    // orchestrator's `finalCtx.result` would be `None` (the
    // cycle pre-hook short-circuited before the executor ran) and
    // the old fall-through would return `ProviderInvocationFailed("NoResult")`.
    // With v0.3 surfacing, the typed `EngineError` from
    // `ctx.meta("semanticGraphError")` reaches the caller instead.
    val engine = new io.sm8.core.EngineImpl
    new SemanticGraphPlugin().setup(engine)
    val orchestrator = orchestratorFor(engine)

    val provider = new NeverQueryProvider
    val registry = EngineRegistry(Map("never" -> provider), "never")

    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("cycle_fixture", Nil, Nil, "", "never"),
      model      = cycleModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = orchestrator
    )

    out.isLeft shouldBe true
    val err = out.left.get
    // The typed error must NOT be the generic NoResult.
    err match {
      case _: EngineError.ProviderInvocationFailed => fail(
        s"got generic NoResult; expected typed UnsupportedCapability. err=$err")
      case _: EngineError.UnsupportedCapability    => // success
      case _ => fail(s"unexpected error type: $err")
    }
  }
}