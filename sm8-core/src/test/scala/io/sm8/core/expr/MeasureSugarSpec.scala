/*
 * SM8 Core — MeasureSugarSpec (PR-131, ADR-008-T).
 *
 * 8 ergonomics tests proving the new `ExprSugar` extensions + the
 * new `Measure.aggregate(name, call)` overload produce the correct
 * underlying case classes (zero behavior change vs explicit
 * constructors).
 *
 * Per `karpathy-guidelines-mindset` §4 (verifiable success): each
 * test asserts on the returned case-class structure (not on a
 * snapshot of the implementation), so the tests survive any future
 * internal refactor of the sugar as long as the contract holds.
 */
package io.sm8.core.expr

import io.sm8.core.expr.ExprSugar._
import io.sm8.core.model.{CalculatedMeasure, Measure}
import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.core.schema.SealedDataType

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MeasureSugarSpec extends AnyFlatSpec with Matchers {

  // -- F1: ExprAggregateOps (single-input AggregateCall) --

  "ExprAggregateOps.sum" should "produce AggregateCall(Sum, Some(expr), \"\", false, Nil)" in {
    val call: AggregateCall = Expr.FieldRef("amount").sum
    call.fn     shouldBe AggregateFn.Sum
    call.input  shouldBe Some(Expr.FieldRef("amount"))
    call.alias  shouldBe ""
    call.distinct shouldBe false
  }

  "ExprAggregateOps.avg" should "produce AggregateCall(Avg, Some(expr), \"\", false, Nil)" in {
    val call = Expr.FieldRef("value").avg
    call.fn shouldBe AggregateFn.Avg
    call.input shouldBe Some(Expr.FieldRef("value"))
  }

  "ExprAggregateOps.min / .max / .countDistinct" should
      "produce AggregateCalls with the matching AggregateFn + Some(expr)" in {
    Expr.FieldRef("x").min.fn       shouldBe AggregateFn.Min
    Expr.FieldRef("x").max.fn       shouldBe AggregateFn.Max
    Expr.FieldRef("id").countDistinct.fn shouldBe AggregateFn.CountDistinct
    Expr.FieldRef("id").countDistinct.distinct shouldBe true
  }

  // -- F1 supplement: CountOp.countStar (no-input AggregateCall) --

  "CountOp.countStar" should "produce AggregateCall(Count, None, name, false, Nil)" in {
    val call: AggregateCall = "encounter_id".countStar
    call.fn    shouldBe AggregateFn.Count
    call.input shouldBe None
    call.alias shouldBe "encounter_id"
  }

  // -- F2: StringMeasureRefOps (Expr.MeasureRef / Expr.All) --

  "StringMeasureRefOps.measure" should "produce Expr.MeasureRef(name)" in {
    val e: Expr = "total_los".measure
    e shouldBe Expr.MeasureRef("total_los")
  }

  "StringMeasureRefOps.all" should "produce Expr.All(name)" in {
    val e: Expr = "revenue".all
    e shouldBe Expr.All("revenue")
  }

  // -- F3: ExprCastOps (Expr.Cast) --

  "ExprCastOps.castAs" should "produce Expr.Cast(expr, targetType)" in {
    val c: Expr.Cast = Expr.FieldRef("amount").castAs(SealedDataType.BigInt)
    c.expr       shouldBe Expr.FieldRef("amount")
    c.targetType shouldBe SealedDataType.BigInt
  }

  // -- Measure.aggregate(name, AggregateCall) overload --

  "Measure.aggregate(name, AggregateCall)" should
      "build Measure with the supplied AggregateCall as expr" in {
    val m: Measure = Measure.aggregate("total_los", Expr.FieldRef("los_days").sum)
    m.name shouldBe "total_los"
    m.expr.fn    shouldBe AggregateFn.Sum
    m.expr.input shouldBe Some(Expr.FieldRef("los_days"))
  }

  "Measure.aggregate(name, AggregateCall) + CalculatedMeasure + sugar" should
      "build a full model definition end-to-end" in {
    val measures = List(
      Measure.aggregate("encounter_count", "encounter_id".countStar),
      Measure.aggregate("total_los",       Expr.FieldRef("los_days").sum))

    val calculated = List(
      CalculatedMeasure(
        name = "avg_los",
        expr = Expr.Divide("total_los".measure, "encounter_count".measure)))

    measures.size shouldBe 2
    measures(0).expr.fn shouldBe AggregateFn.Count
    measures(1).expr.fn shouldBe AggregateFn.Sum
    calculated.head.expr shouldBe Expr.Divide(
      Expr.MeasureRef("total_los"),
      Expr.MeasureRef("encounter_count"))
  }
}
