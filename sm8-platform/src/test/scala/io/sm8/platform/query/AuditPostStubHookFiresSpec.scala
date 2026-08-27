/*
 * SM8 Platform — AuditPostStubHookFiresSpec (ADR-010-a v0.3).
 *
 * Regression test for the silent-no-op defect discovered in the
 * 2026-08-26 dual senior codebase review: `AuditPostStubHook`
 * (registered at `HookStage.PostFormat`, priority 150) was
 * silently inert in production since PR #32 (commit `daac360`,
 * 2026-08-14). The audit counter never incremented; production
 * had no audit trail.
 *
 * After the orchestrator fix, the PostFormat stage is driven on
 * every request, and `AuditStub.fires` increments.
 *
 * Per [[debug-mantra-mindset]] §5 (verify): the assertion is on
 * the counter value (`AuditStub.fires.get()`) after a real
 * `runQueryWithHooks` call through the production entry point.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct change):
 * no new audit-plugin code — this is purely the regression test
 * that proves the existing `AuditPostStubHook` now fires.
 */
package io.sm8.platform.query

import io.sm8.core.cache._
import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineRegistry,
  PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.sm8.core.engine.{ QueryRequest => CoreQueryRequest }
import io.sm8.core.expr.Expr
import io.sm8.core.model.{CalculatedMeasure, Dimension, MaterializePolicy, CachePolicy, AuditPolicy,
  Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.platform.query.hooks.{EngineHookDispatcher, HookRunnerOrchestration}
import io.sm8.plugins.audit.AuditStub

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AuditPostStubHookFiresSpec extends AnyFunSuite with Matchers {

  // A trivial engine provider (returns a single-row PQR so the
  // happy path completes — PostFormat must fire AFTER the executor).
  private final class TrivialProvider extends EngineProvider {
    override val identity: EngineIdentity = EngineIdentity("trivial", "0.0.0", "0.0.0")
    override val available: Boolean = true
    override def query(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext): Either[EngineError, PortableQueryResult] =
      Right(PortableQueryResult(
        schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int))),
        rows   = Vector(ResultRow(
          values = List(ResultValue.IntV(1L)),
          schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int)))
        ))
      ))
    override def explain(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext): Either[EngineError, String] = Right("trivial")
  }

  private val model: Model = Model.of(
    name = "audit_fixture",
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
        name = "total",
        expr = Expr.Add(
          Expr.FieldRef("amount"),
          Expr.Literal(io.sm8.core.expr.LiteralValue.LongValue(0L), SealedDataType.Int)
        )
      )
    ),
    joins = Nil
  ).toOption.get

  test("runQueryWithHooks: PostFormat fires — AuditStub.fires.get() > 0") {
    // Per ADR-010-a v0.3: the orchestrator drives all 4 stages,
    // including the Format stage where AuditPostStubHook is
    // registered at priority 150. Before the fix, the audit
    // counter never incremented (dispatcher hardcoded
    // PipelineStage.Execute).
    val auditStub = new AuditStub
    val engine = new io.sm8.core.EngineImpl
    auditStub.setup(engine)
    val orchestrator = HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))

    val registry = EngineRegistry(
      Map("trivial" -> new TrivialProvider),
      default = "trivial"
    )

    // Sanity: counter starts at 0.
    auditStub.fires.get() shouldBe 0

    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("audit_fixture", Nil, Nil, "", "trivial"),
      model      = model,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = orchestrator
    )

    out.isRight shouldBe true
    // AuditPostStubHook fired on the Format stage.
    auditStub.fires.get() should be > 0
  }
}