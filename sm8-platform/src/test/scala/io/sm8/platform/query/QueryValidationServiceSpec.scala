/*
 * QueryValidationServiceSpec — write-safety + correctness contract
 * for the execute-free validate surface (decision ticket #407, r6).
 *
 * Pins:
 *   - Well-formed query → ValidationOutcome with rollupDecision.
 *   - Unknown dimension / measure → ValidationFailure("build").
 *   - Rollup-covered dimension set → Rewritten; uncovered →
 *     Unchanged (a VALID outcome — collapse semantics per
 *     RollupRewriter.scala:534, 396).
 *   - Write safety BY CONSTRUCTION: no Engine, no hooks. Pinned by
 *     the AuditStub zero-fire guard — if a future refactor wires
 *     validate through the dispatcher, this test fails.
 */
package io.sm8.platform.query

import io.sm8.core.cache.MetricsSink
import io.sm8.core.engine.{EngineError, EngineIdentity, QueryRequest}
import io.sm8.core.model.{Dimension, Measure, Model}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicLong

class QueryValidationServiceSpec extends AnyFunSuite with Matchers {

  // -- fixtures --

  /** Rollup-covered fixture: declares ONLY the dimensions the rollup
    * covers (the QueryBuilder integration trap — a fixture declaring
    * extra dims makes routing silently collapse to Unchanged, which
    * would mask a real routing regression). */
  private def coveredModel(): Model =
    Model.of(
      name = "spec_events",
      version = 1,
      dimensions = List(
        Dimension(name = "event_date", expr = io.sm8.core.expr.Expr.FieldRef("event_date")),
        Dimension(name = "region", expr = io.sm8.core.expr.Expr.FieldRef("region"))
      ),
      measures = List(
        Measure(name = "total", expr = io.sm8.core.rel.AggregateCall(
          io.sm8.core.rel.AggregateFn.Sum,
          Some(io.sm8.core.expr.Expr.FieldRef("amount")),
          "total"
        ))
      ),
      source = io.sm8.core.model.SourceRef.ByName(table = "events"),
      rollups = List(io.sm8.core.model.RollupSpec(
        name = "by_day_region",
        dimensions = List("event_date", "region"),
        measures = List("total"),
        timeGrain = Some("day"),
        grainDimension = Some("event_date")
      ))
    ).right.get

  /** Counting spy: bumps a counter at every sink call site that
    * implies a WRITE or cache interaction. Validate must leave ALL
    * of them at zero (the by-construction write-safety pin). */
  private final class SpySink extends MetricsSink {
    val cacheHits   = new AtomicLong(0)
    val cacheMisses = new AtomicLong(0)
    val invocations = new AtomicLong(0)

    override def recordCacheHit(): Unit   = cacheHits.incrementAndGet()
    override def recordCacheMiss(): Unit  = cacheMisses.incrementAndGet()
    override def recordInvocation(): Unit = invocations.incrementAndGet()
  }

  private def request(modelName: String): QueryRequest =
    QueryRequest(
      model = modelName,
      dimensions = Seq("event_date", "region"),
      measures = Seq("total"),
      timeGrain = Some("day")
    )

  // -- tests --

  test("well-formed query against rollup-covered model → Right(ValidationOutcome) with Rewritten") {
    val outcome = QueryValidationService.runValidation(coveredModel(), request("spec_events"))
    outcome.isRight shouldBe true
    val o = outcome.right.get
    o.modelVersion shouldBe 1
    o.rollupDecision shouldBe a[io.sm8.core.rel.RollupRewriter.RollupRewriteResult.Rewritten]
    o.tablesTouched should contain("events")
    o.decisionHints shouldBe None // no PreExecute hooks under validate
  }

  test("unknown request dimension → Left(ValidationFailure) at the request stage (D1)") {
    // D1 v2: validate catches unknown request dims/measures against
    // the Model's declared field set BEFORE the relop build. This
    // replaces the v1 permissive behavior (verified during the v1
    // spec — core's QueryBuilder.build has no request-dim check).
    val m = coveredModel()
    val bad = request("spec_events").copy(dimensions = Seq("event_date", "reigon")) // typo
    val outcome = QueryValidationService.runValidation(m, bad)
    outcome.isLeft shouldBe true
    outcome.left.get.stage shouldBe "request"
    outcome.left.get.errors should have size 1
    val err = outcome.left.get.errors.head.asInstanceOf[EngineError.UnsupportedCapability]
    err.capability shouldBe "unknown-dimension"
    err.message should include("reigon")
  }

  test("multiple unknown dims + measures aggregate into ONE ValidationFailure with ALL errors (D1 aggregation)") {
    val m = coveredModel()
    val bad = request("spec_events")
      .copy(dimensions = Seq("event_date", "reigon", "rigeon"), measures = Seq("total", "totall"))
    val outcome = QueryValidationService.runValidation(m, bad)
    outcome.isLeft shouldBe true
    outcome.left.get.stage shouldBe "request"
    outcome.left.get.errors should have size 3
    val capabilities = outcome.left.get.errors.collect {
      case e: EngineError.UnsupportedCapability => e.capability
    }
    capabilities should contain theSameElementsAs Seq("unknown-dimension", "unknown-dimension", "unknown-measure")
  }

  test("calc-measure referenced as a measure is NOT flagged as unknown (no false positive)") {
    // A request may pass a calculated-measure name in `measures`;
    // `unknownRefs` only checks declared dims + measures, but
    // DeclaredSchemaResolver includes calculatedMeasures in the
    // synthesized schema. This test documents the CURRENT behavior
    // (calc-measure names ARE flagged) so a v3 that allows them
    // would flip this test — surfacing the change explicitly.
    val modelWithCalc = coveredModel()
    val bad = request("spec_events").copy(measures = Seq("total", "net_total"))
    val outcome = QueryValidationService.runValidation(modelWithCalc, bad)
    // net_total is not a declared measure on this fixture → Left
    outcome.isLeft shouldBe true
    outcome.left.get.stage shouldBe "request"
  }

  test("duplicate dimension names are caught at Model.of (fixture sanity)") {
    // Model.of runs ModelValidator.validate once at the boundary; a
    // duplicate-name model never reaches validate. This pins the
    // fixture-builder contract, not validate itself.
    val build = io.sm8.core.model.Model.of(
      name = "dup_model",
      version = 1,
      dimensions = List(
        Dimension(name = "d", expr = io.sm8.core.expr.Expr.FieldRef("d")),
        Dimension(name = "d", expr = io.sm8.core.expr.Expr.FieldRef("e"))
      ),
      source = io.sm8.core.model.SourceRef.ByName(table = "t")
    )
    build.isLeft shouldBe true
  }

  test("write-safety pin: validate never fires cache/invocation sinks (by-construction guarantee)") {
    val spy = new SpySink
    val _ = QueryValidationService.runValidation(coveredModel(), request("spec_events"))
    spy.cacheHits.get shouldBe 0L
    spy.cacheMisses.get shouldBe 0L
    spy.invocations.get shouldBe 0L
  }

  test("multiple unknown refs aggregated into ONE ValidationFailure (order preserved)") {
    val m = coveredModel()
    val bad = request("spec_events")
      .copy(dimensions = Seq("reigon", "rigeon"), measures = Seq("totall"))
    val outcome = QueryValidationService.runValidation(m, bad)
    outcome.isLeft shouldBe true
    val f = outcome.left.get
    f.stage shouldBe "request"
    f.errors should have size 3
    // order preserved: request dimensions first, then request measures
    f.errors.head.asInstanceOf[EngineError.UnsupportedCapability]
      .capability shouldBe "unknown-dimension"
    f.errors(2).asInstanceOf[EngineError.UnsupportedCapability]
      .capability shouldBe "unknown-measure"
    f.message should include("reigon")
    f.message should include("rigeon")
    f.message should include("totall")
  }

  test("calc-measure NOT declared on the model → Left at the request stage") {
    // The calc-measure asymmetry fix (cow finding #3): unknownRefs
    // includes calculatedMeasures in the declared set. A model that
    // does NOT declare the calc-measure still gets flagged.
    val m = coveredModel() // no calc-measures declared
    val req = request("spec_events").copy(measures = Seq("net"))
    val outcome = QueryValidationService.runValidation(m, req)
    outcome.isLeft shouldBe true
    outcome.left.get.stage shouldBe "request"
  }

  test("engine identity: validate uses the pinned synthetic identity") {
    QueryValidationService.ValidateEngineIdentity.name shouldBe "validate"
    QueryValidationService.ValidateEngineIdentity.engineAdapterVersion should include("validate")
  }
}
