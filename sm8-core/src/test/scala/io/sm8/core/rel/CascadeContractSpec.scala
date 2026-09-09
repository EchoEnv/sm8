/*
 * SM8 Core — CascadeContractSpec (ADR-0031 §Tests, core half).
 *
 * Covers the D1 eligibility predicate matrix (5 Decomposability
 * classes × coarsen/not-coarsen × partial-state present/absent),
 * the D2 structural coarsening cases, and the D4 DAG enforcement
 * surface via ModelValidator.validateCascadeDag (self-cycle,
 * two-node cycle, three-node cycle, valid three-level DAG).
 *
 * The connector-half tests (staleness via watermark, Welford
 * cross-group merge tripwire, snapshot pinning) live with the
 * connector PR per the D5 declaration/resolution split.
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr
import io.sm8.core.model.{
  Dimension, Measure, Model, ModelValidator, RollupSpec, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CascadeContractSpec extends AnyFunSuite with Matchers {

  import CascadeContract.CascadeVerdict

  // -- fixtures --

  private def measure(name: String, fn: AggregateFn, field: String = "amount"): Measure =
    Measure(name, AggregateCall(fn, Some(Expr.FieldRef(field)), name))

  private val mSum    = measure("total", AggregateFn.Sum)
  private val mCount  = Measure("n", AggregateCall(AggregateFn.Count, None, "n"))
  private val mMin    = measure("lo", AggregateFn.Min)
  private val mMax    = measure("hi", AggregateFn.Max)
  private val mAvg    = measure("avg_amt", AggregateFn.Avg)
  private val mStddev = measure("sd", AggregateFn.StddevSample)
  private val mMedian = measure("med", AggregateFn.Median)
  private val mFirst  = measure("first_amt", AggregateFn.First)

  private val allMeasures: Map[String, Measure] =
    List(mSum, mCount, mMin, mMax, mAvg, mStddev, mMedian, mFirst)
      .map(m => m.name -> m).toMap

  /** Hourly source: day-grain-dimension axis, all region+hour dims. */
  private def hourly(measures: List[Measure]): RollupSpec = RollupSpec(
    name = "hourly",
    dimensions = List("order_date_hour", "region"),
    measures = measures.map(_.name),
    timeGrain = Some("hour"),
    grainDimension = Some("order_date_hour"))

  private def daily(measures: List[Measure]): RollupSpec = RollupSpec(
    name = "daily",
    dimensions = List("order_date_day", "region"),
    measures = measures.map(_.name),
    timeGrain = Some("day"),
    grainDimension = Some("order_date_day"))

  // -- D1: eligibility predicate matrix --

  test("D1: Additive (Sum, Count) cascades hourly→daily") {
    val src = hourly(List(mSum, mCount))
    val tgt = daily(List(mSum, mCount))
    CascadeContract.eligibility(tgt, src, allMeasures) shouldBe
      CascadeVerdict.Eligible
  }

  test("D1: Algebraic (Avg, Stddev) cascades when source carries Welford states") {
    val src = hourly(List(mSum, mCount, mAvg, mStddev))
    val tgt = daily(List(mAvg, mStddev))
    // Source carries the Welford triple columns for 'amount'.
    val srcCols = Set("count__amount", "sum__amount", "m2__amount")
    CascadeContract.eligibility(tgt, src, allMeasures, srcCols) shouldBe
      CascadeVerdict.Eligible
  }

  test("D1: partial-state-absent — Avg on target without m2 on source is PartialStateMissing") {
    val src = hourly(List(mSum, mCount, mAvg))
    val tgt = daily(List(mAvg))
    // Source physically carries ONLY sum+count (no m2__) — the
    // naive pre-Welford shape. The cascade must refuse the Avg arm.
    val srcCols = Set("sum__amount", "count__amount")
    CascadeContract.eligibility(tgt, src, allMeasures, srcCols) match {
      case CascadeVerdict.PartialStateMissing(m, req, found) =>
        m shouldBe "avg_amt"
        req shouldBe Set("count__amount", "sum__amount", "m2__amount")
        found shouldBe Set("sum__amount", "count__amount")
      case other => fail(s"expected PartialStateMissing, got $other")
    }
  }

  test("D1: Min/Max (binary-reducible Additive) cascade-eligible structurally") {
    val src = hourly(List(mSum, mMin, mMax))
    val tgt = daily(List(mMin, mMax))
    val srcCols = Set("min__amount", "max__amount")
    CascadeContract.eligibility(tgt, src, allMeasures, srcCols) shouldBe
      CascadeVerdict.Eligible
    // The MERGE expression distinction (binary re-application, NOT
    // summation) is connector-side; core only certifies the shape.
  }

  test("D1: Holistic (Median) alone → PartiallyEligible (whole target non-cascading)") {
    val src = hourly(List(mSum, mMedian))
    val tgt = daily(List(mMedian))
    CascadeContract.eligibility(tgt, src, allMeasures) match {
      case CascadeVerdict.PartiallyEligible(non) => non shouldBe Set("med")
      case other => fail(s"expected PartiallyEligible, got $other")
    }
  }

  test("D1: Positional (First) never cascades") {
    val src = hourly(List(mSum, mFirst))
    val tgt = daily(List(mFirst))
    CascadeContract.eligibility(tgt, src, allMeasures) match {
      case CascadeVerdict.PartiallyEligible(non) => non shouldBe Set("first_amt")
      case other => fail(s"expected PartiallyEligible, got $other")
    }
  }

  test("D1: mixed eligible + non-eligible → PartiallyEligible with the non-set") {
    val src = hourly(List(mSum, mCount, mMedian))
    val tgt = daily(List(mSum, mCount, mMedian))
    CascadeContract.eligibility(tgt, src, allMeasures) match {
      case CascadeVerdict.PartiallyEligible(non) => non shouldBe Set("med")
      case other => fail(s"expected PartiallyEligible, got $other")
    }
  }

  test("D1: unknown measure name on target degrades to non-cascading (fail-safe)") {
    val src = hourly(List(mSum))
    val tgt = daily(List(mSum, mMedian)).copy(
      measures = List("total", "ghost_measure"))
    CascadeContract.eligibility(tgt, src, allMeasures) match {
      case CascadeVerdict.PartiallyEligible(non) =>
        non shouldBe Set("ghost_measure")
      case other => fail(s"expected PartiallyEligible, got $other")
    }
  }

  // -- D1: dimensions containment --

  test("D1: target dimension missing on source → DimensionsNotContained") {
    val src = RollupSpec(
      name = "hourly", dimensions = List("order_date_hour"),
      measures = List("total"), timeGrain = Some("hour"),
      grainDimension = Some("order_date_hour"))
    val tgt = daily(List(mSum)) // carries 'region'
    CascadeContract.eligibility(tgt, src, allMeasures) match {
      case CascadeVerdict.DimensionsNotContained(missing) =>
        missing shouldBe Set("region")
      case other => fail(s"expected DimensionsNotContained, got $other")
    }
  }

  // -- D2: structural grain coarsening --

  test("D2: hour→day coarsens (same grain axis)") {
    CascadeContract.grainCoarsens(daily(List(mSum)), hourly(List(mSum))) shouldBe None
  }

  test("D2: same-grain (hour→hour) does NOT coarsen") {
    CascadeContract.grainCoarsens(hourly(List(mSum)), hourly(List(mSum))) should be(
      defined)
  }

  test("D2: day→hour is a REFINEMENT, not a coarsening (direction error)") {
    CascadeContract.grainCoarsens(hourly(List(mSum)), daily(List(mSum))) should be(
      defined)
  }

  test("D2: different grain axes (day over a different grainDimension) fail") {
    val srcOtherAxis = RollupSpec(
      name = "hourly_w", dimensions = List("ship_date_hour", "region"),
      measures = List("total"), timeGrain = Some("hour"),
      grainDimension = Some("ship_date_hour"))
    CascadeContract.grainCoarsens(daily(List(mSum)), srcOtherAxis) should be(
      defined)
  }

  test("D2: grained target over ungrained source fails (loses whole-table semantics)") {
    val ungrained = RollupSpec(
      name = "by_region", dimensions = List("region"),
      measures = List("total"))
    CascadeContract.grainCoarsens(daily(List(mSum)), ungrained) should be(
      defined)
  }

  test("D2: unknown grain labels fail loud (not silently unequal ordinals)") {
    val src = RollupSpec(
      name = "hourly", dimensions = List("order_date_hour", "region"),
      measures = List("total"), timeGrain = Some("fourtnight"),
      grainDimension = Some("order_date_hour"))
    val tgt = RollupSpec(
      name = "daily", dimensions = List("order_date_day", "region"),
      measures = List("total"), timeGrain = Some("month"),
      grainDimension = Some("order_date_day"))
    CascadeContract.grainCoarsens(tgt, src).getOrElse("") should include(
      "not a known grain vocabulary")
  }

  test("D2: ungrained→ungrained with dim-set drop coarsens (non-time-grain rule)") {
    val src = RollupSpec(
      name = "region_hour", dimensions = List("region", "carrier"),
      measures = List("total"))
    val tgt = RollupSpec(
      name = "region", dimensions = List("region"),
      measures = List("total"))
    CascadeContract.grainCoarsens(tgt, src) shouldBe None
  }

  test("D2: ungrained target with MORE dims than source fails") {
    val src = RollupSpec(
      name = "region", dimensions = List("region"),
      measures = List("total"))
    val tgt = RollupSpec(
      name = "region_carrier", dimensions = List("region", "carrier"),
      measures = List("total"))
    CascadeContract.grainCoarsens(tgt, src).getOrElse("") should include(
      "must not increase the dimension set")
  }

  // -- D4: DAG enforcement via ModelValidator --

  private def modelWithRollups(rollups: List[RollupSpec]): Model =
    Model.of(
      name = "cascadem",
      version = 1,
      source = SourceRef.ByName(table = "events"),
      dimensions = List(
        Dimension.field("order_date_hour", "order_date_hour",
          io.sm8.core.schema.SealedDataType.Timestamp),
        Dimension.field("order_date_day", "order_date_day",
          io.sm8.core.schema.SealedDataType.Date),
        Dimension.field("order_date_week", "order_date_week",
          io.sm8.core.schema.SealedDataType.Date),
        Dimension.field("region", "region")),
      measures = List(
        Measure("total", AggregateCall(AggregateFn.Sum,
          Some(Expr.FieldRef("amount")), "total")),
        Measure("avg_amt", AggregateCall(AggregateFn.Avg,
          Some(Expr.FieldRef("amount")), "avg_amt"))),
      rollups = rollups).toOption.get

  private val hourlyDecl = RollupSpec(
    name = "hourly", dimensions = List("order_date_hour", "region"),
    measures = List("total"), timeGrain = Some("hour"),
    grainDimension = Some("order_date_hour"))
  private val dailyDecl = RollupSpec(
    name = "daily", dimensions = List("order_date_day", "region"),
    measures = List("total"), timeGrain = Some("day"),
    grainDimension = Some("order_date_day"),
    cascadeSource = Some("hourly"))
  private val weeklyDecl = RollupSpec(
    name = "weekly", dimensions = List("order_date_week", "region"),
    measures = List("total"), timeGrain = Some("week"),
    grainDimension = Some("order_date_week"),
    cascadeSource = Some("daily"))

  private def cascadeErrorsOf(rollups: List[RollupSpec]): List[String] = {
    // Route through Model.of — the PUBLIC deployment-time path
    // (validateCascadeDag is private[model]; ADR-0031 D4: refusal
    // happens at load, which IS Model.of). The error ADT is sealed
    // with case classes that may not expose their msgs directly;
    // stringify the Left to grep for cascade-related lines.
    // modelWithRollups .get-s the Either — for the ERROR path we
    // need the raw Model.of result. Inline it here.
    val res = Model.of(
      name = "cascadem",
      version = 1,
      source = SourceRef.ByName(table = "events"),
      dimensions = List(
        Dimension.field("order_date_hour", "order_date_hour",
          io.sm8.core.schema.SealedDataType.Timestamp),
        Dimension.field("order_date_day", "order_date_day",
          io.sm8.core.schema.SealedDataType.Date),
        Dimension.field("order_date_week", "order_date_week",
          io.sm8.core.schema.SealedDataType.Date),
        Dimension.field("region", "region")),
      measures = List(
        Measure("total", AggregateCall(AggregateFn.Sum,
          Some(Expr.FieldRef("amount")), "total")),
        Measure("avg_amt", AggregateCall(AggregateFn.Avg,
          Some(Expr.FieldRef("amount")), "avg_amt"))),
      rollups = rollups)
    res match {
      case scala.util.Right(_) => Nil
      case scala.util.Left(err) =>
        val s = err.toString
        val lines = s.split(java.lang.System.lineSeparator).toList
        lines.filter(_.contains("cascade"))
    }
  }

  test("D4: valid three-level DAG (base→hourly→daily→weekly) accepted") {
    cascadeErrorsOf(List(hourlyDecl, dailyDecl, weeklyDecl)) shouldBe Nil
  }

  test("D4: self-cycle (A from A) refused") {
    val selfRef = hourlyDecl.copy(cascadeSource = Some("hourly"))
    val errs = cascadeErrorsOf(List(selfRef))
    errs.exists(_.contains("names itself")) shouldBe true
  }

  test("D4: two-node cycle (A from B, B from A) refused") {
    val a = RollupSpec(
      name = "ra", dimensions = List("order_date_day", "region"),
      measures = List("total"), timeGrain = Some("day"),
      grainDimension = Some("order_date_day"),
      cascadeSource = Some("rb"))
    val b = RollupSpec(
      name = "rb", dimensions = List("order_date_hour", "region"),
      measures = List("total"), timeGrain = Some("hour"),
      grainDimension = Some("order_date_hour"),
      cascadeSource = Some("ra"))
    val errs = cascadeErrorsOf(List(a, b))
    // SchemaValidation's toString may put all messages on ONE line
    // (comma-separated) — count OCCURRENCES, not lines.
    val cycleHits = errs.mkString(" ").split("forms a cycle").length - 1
    cycleHits shouldBe 2 // both nodes report
  }

  test("D4: three-node cycle refused") {
    val c1 = hourlyDecl.copy(name = "c1", cascadeSource = Some("c3"))
    val c2 = dailyDecl.copy(name = "c2", cascadeSource = Some("c1"))
    val c3 = weeklyDecl.copy(name = "c3", cascadeSource = Some("c2"))
    val errs = cascadeErrorsOf(List(c1, c2, c3))
    val cycleHits = errs.mkString(" ").split("forms a cycle").length - 1
    cycleHits shouldBe 3
  }

  test("D4: cascadeSource naming an unknown rollup refused at load") {
    val orphan = dailyDecl.copy(cascadeSource = Some("nonexistent"))
    val errs = cascadeErrorsOf(List(hourlyDecl, orphan))
    errs.exists(_.contains("names no rollup on this model")) shouldBe true
  }

  test("D4: structurally-ineligible cascade (same-grain) refused at load, not refresh") {
    val src = hourlyDecl
    val tgt = RollupSpec(
      name = "hourly2", dimensions = List("order_date_hour", "region"),
      measures = List("total"), timeGrain = Some("hour"),
      grainDimension = Some("order_date_hour"),
      cascadeSource = Some("hourly"))
    val errs = cascadeErrorsOf(List(src, tgt))
    errs.exists(_.contains("fails grain coarsening")) shouldBe true
  }

  test("D1 via validator: mixed measures load with a WARNING line (not a refusal)") {
    val src = hourlyDecl.copy(measures = List("total", "avg_amt"))
    val tgt = RollupSpec(
      name = "daily", dimensions = List("order_date_day", "region"),
      measures = List("total", "med"),
      timeGrain = Some("day"), grainDimension = Some("order_date_day"),
      cascadeSource = Some("hourly"))
    // WARNING must NOT refuse: the model loads (Right) despite the
    // mixed-measure declaration (narwhal F1 — the old code put the
    // warning into errs and silently refused the whole model).
    val res = Model.of(
      name = "cascadem",
      version = 1,
      source = SourceRef.ByName(table = "events"),
      dimensions = List(
        Dimension.field("order_date_hour", "order_date_hour",
          io.sm8.core.schema.SealedDataType.Timestamp),
        Dimension.field("order_date_day", "order_date_day",
          io.sm8.core.schema.SealedDataType.Date),
        Dimension.field("region", "region")),
      measures = List(
        Measure("total", AggregateCall(AggregateFn.Sum,
          Some(Expr.FieldRef("amount")), "total")),
        Measure("avg_amt", AggregateCall(AggregateFn.Avg,
          Some(Expr.FieldRef("amount")), "avg_amt")),
        Measure("med", AggregateCall(AggregateFn.Median,
          Some(Expr.FieldRef("amount")), "med"))),
      rollups = List(src, tgt))
    if (res.isLeft) println("MIXED-MEASURES DEBUG: " + res)
    // Debug: surface the Left error if the load refuses.
    res match {
      case scala.util.Right(_) => ()
      case scala.util.Left(err) =>
        fail(s"expected Right (warning must not refuse), got Left: $err")
    }
    res.isRight shouldBe true
  }
}
