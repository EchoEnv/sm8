/*
 * DecomposabilityAuditSpec — the Gate B item 2 audit contract tests
 * (ADR-0029 §Gate B item 2; ADR-0030 §D2-7; ADR-0031 §D1/§D6).
 *
 * Pins:
 *   - Every AggregateFn case classifies into the expected
 *     Decomposability class in the audit output (compiler-exhaustive
 *     via AggregateFn.decomposability; this spec pins the AUDIT view).
 *   - deltaCombinable is true exactly for Additive + Algebraic.
 *   - cascadeEligible is Some(true) only when the class cascades AND
 *     the source carries partial state; Some(false) with a reason
 *     otherwise; None when no cascade source is declared.
 *   - The audit is total over all 16 AggregateFn cases (no silently
 *     skipped case — the F1 lesson from the ADR-0029 review).
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr
import io.sm8.core.model._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DecomposabilityAuditSpec extends AnyFunSuite with Matchers {

  private def call(fn: AggregateFn, field: String = "amount") =
    io.sm8.core.rel.AggregateCall(fn = fn, input = Some(Expr.FieldRef(field)))

  private def fixtureModel(measureFns: AggregateFn*): Model =
    Model.of(
      name = "audit_model",
      version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = measureFns.toList.zipWithIndex.map { case (fn, i) =>
        Measure(s"m$i", call(fn))
      },
      source = SourceRef.ByName(table = "audit_base")
    ) match {
      case Right(m) => m
      case Left(e)  => fail(s"fixture model invalid: $e")
    }

  private def specWith(measureFns: AggregateFn*): RollupSpec =
    RollupSpec("by_region", List("region"),
      measureFns.toList.zipWithIndex.map { case (fn, i) => s"m$i" })

  /** All 16 AggregateFn cases — the audit must be TOTAL over these. */
  private val allFns: List[AggregateFn] = List(
    AggregateFn.Sum, AggregateFn.Count, AggregateFn.Min, AggregateFn.Max,
    AggregateFn.Avg, AggregateFn.StddevSample, AggregateFn.StddevPopulation,
    AggregateFn.VarianceSample, AggregateFn.VariancePopulation,
    AggregateFn.CountDistinct, AggregateFn.ApproxPercentile,
    AggregateFn.Median, AggregateFn.PercentileContinuous,
    AggregateFn.PercentileDiscrete, AggregateFn.First, AggregateFn.Last)

  test("audit is total: every AggregateFn case produces an audit row") {
    val model = fixtureModel(allFns: _*)
    val spec = specWith(allFns: _*)
    val rows = DecomposabilityAudit.auditRollup(spec, model, None, sourceHasPartialStateFor = (_, _) => false)
    rows should have size allFns.size.toLong
    rows.map(_.fn) should contain theSameElementsAs allFns
  }

  test("deltaCombinable is true exactly for Additive + Algebraic classes") {
    val model = fixtureModel(allFns: _*)
    val spec = specWith(allFns: _*)
    val rows = DecomposabilityAudit.auditRollup(spec, model, None, sourceHasPartialStateFor = (_, _) => false)
    val deltaFns = rows.filter(_.deltaCombinable).map(_.fn).toSet
    deltaFns shouldBe Set(
      AggregateFn.Sum, AggregateFn.Count, AggregateFn.Min, AggregateFn.Max,
      AggregateFn.Avg, AggregateFn.StddevSample, AggregateFn.StddevPopulation,
      AggregateFn.VarianceSample, AggregateFn.VariancePopulation)
    // Positional/Holistic/Approximable carry the exclusion reason.
    rows.filterNot(_.deltaCombinable).foreach { r =>
      r.exclusionReason shouldBe defined
      r.exclusionReason.get should include("never delta-merges")
    }
  }

  test("cascadeEligible is None when no cascade source is declared") {
    val model = fixtureModel(AggregateFn.Sum, AggregateFn.Avg)
    val spec = specWith(AggregateFn.Sum, AggregateFn.Avg)
    val rows = DecomposabilityAudit.auditRollup(spec, model, None, sourceHasPartialStateFor = (_, _) => false)
    rows.foreach(r => r.cascadeEligible shouldBe None)
  }

  test("cascadeEligible honors partial-state presence on the source") {
    val model = fixtureModel(AggregateFn.Sum, AggregateFn.Avg)
    val spec = specWith(AggregateFn.Sum, AggregateFn.Avg)
    val srcSpec = specWith(AggregateFn.Sum) // source only declares Sum

    // Source WITH all required prefixes present for "amount": Sum and
    // Avg both cascade. Prefix-aware: returns true only when the
    // requested prefix is in the present set.
    val presentAll = Set("sum__", "count__", "m2__", "min__", "max__")
    val withFullState = DecomposabilityAudit.auditRollup(spec, model, Some(srcSpec),
      sourceHasPartialStateFor = (field, prefix) =>
        field == "amount" && presentAll.contains(prefix))
    withFullState.foreach { r =>
      r.cascadeEligible shouldBe Some(true)
      r.exclusionReason shouldBe None
    }

    // Source with only sum__amount present (R1 heron HIGH): Sum (needs
    // only sum__) cascades; Avg (needs count__+sum__+m2__) refuses
    // with the missing-m2 reason named in the message.
    val presentSumOnly = Set("sum__")
    val sumOnly = DecomposabilityAudit.auditRollup(spec, model, Some(srcSpec),
      sourceHasPartialStateFor = (field, prefix) =>
        field == "amount" && presentSumOnly.contains(prefix))
    val sumRow = sumOnly.find(_.measureName == "m0").get
    val avgRow = sumOnly.find(_.measureName == "m1").get
    sumRow.cascadeEligible shouldBe Some(true)
    avgRow.cascadeEligible shouldBe Some(false)
    avgRow.exclusionReason.get should include("m2__")
    avgRow.exclusionReason.get should include("amount")

    // Source with NO partial state at all: nothing cascades.
    val withoutState = DecomposabilityAudit.auditRollup(spec, model, Some(srcSpec),
      sourceHasPartialStateFor = (_, _) => false)
    withoutState.foreach { r =>
      r.cascadeEligible shouldBe Some(false)
      r.exclusionReason shouldBe defined
      r.exclusionReason.get should include("missing partials")
    }
  }

  test("non-cascadable classes refuse cascade with class-specific reason") {
    // R1 heron LOW: exercise every non-cascadable case, not three
    // samples. Positional (First, Last), Holistic (Median,
    // PercentileContinuous, PercentileDiscrete), Approximable
    // (CountDistinct, ApproxPercentile) — 7 cases total.
    val nonCascadable: List[AggregateFn] = List(
      AggregateFn.Median, AggregateFn.PercentileContinuous,
      AggregateFn.PercentileDiscrete,
      AggregateFn.First, AggregateFn.Last,
      AggregateFn.CountDistinct, AggregateFn.ApproxPercentile)
    val model = fixtureModel(nonCascadable: _*)
    val spec = specWith(nonCascadable: _*)
    val srcSpec = specWith(nonCascadable: _*)
    val rows = DecomposabilityAudit.auditRollup(spec, model, Some(srcSpec),
      sourceHasPartialStateFor = (_, _) => true)
    rows should have size nonCascadable.size.toLong
    rows.foreach { r =>
      r.cascadeEligible shouldBe Some(false)
      r.exclusionReason shouldBe defined
      r.exclusionReason.get should include("never cascades")
    }
  }

  test("auditModel enumerates every rollup on the model") {
    val model = Model.of(
      name = "multi_rollup",
      version = 1,
      dimensions = List(Dimension.field("region", "region")),
      measures = List(
        Measure("cnt", call(AggregateFn.Count)),
        Measure("med", call(AggregateFn.Median))),
      rollups = List(
        RollupSpec("r_additive", List("region"), List("cnt")),
        RollupSpec("r_holistic", List("region"), List("med"))),
      source = SourceRef.ByName(table = "multi_base")
    ) match {
      case Right(m) => m
      case Left(e)  => fail(s"fixture model invalid: $e")
    }
    val rows = DecomposabilityAudit.auditModel(model)
    rows should have size 2L
    rows.map(_.rollupName).toSet shouldBe Set("r_additive", "r_holistic")
    rows.find(_.rollupName == "r_additive").get.deltaCombinable shouldBe true
    rows.find(_.rollupName == "r_holistic").get.deltaCombinable shouldBe false
  }
  test("per-measure partial-state check: Sum cascades, Avg refuses, when source lacks m2") {
    val model = fixtureModel(AggregateFn.Sum, AggregateFn.Avg)
    val spec = specWith(AggregateFn.Sum, AggregateFn.Avg)
    val srcSpec = specWith(AggregateFn.Sum)
    // Source carries sum__F but NOT m2__F / count__F (R1 heron HIGH:
    // the Boolean sourceHasPartialState was too coarse — Avg needs the
    // Welford triple, Sum needs only sum__F).
    val src = DecomposabilityAudit.auditRollup(spec, model, Some(srcSpec),
      sourceHasPartialStateFor = (field, prefix) => prefix == "sum__")
    val byMeasure = src.map(r => r.measureName -> r).toMap
    byMeasure("m0").cascadeEligible shouldBe Some(true)   // Sum: sum__ present
    byMeasure("m1").cascadeEligible shouldBe Some(false)  // Avg: m2/count missing
    byMeasure("m1").exclusionReason.get should include("partial")
  }

}
