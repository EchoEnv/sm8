/*
 * SM8 Core — QueryBuilderDetectCalcCyclesSpec.
 *
 * Direct tests for `QueryBuilder.detectCalcCycles` (the calculated-measure
 * DAG cycle-detection walker). Per the design contract, the algorithm must
 * detect self-cycles, transitive cycles, and report the cycle path; must
 * NOT report a false-positive cycle for any acyclic DAG.
 *
 * Per `debug-mantra-mindset`: each test asserts the EVALUATED result
 * of the algorithm (the typed `Either` value), not the internal state.
 *
 * Per `scala-bug-hunting-mindset`: Test 3 (the 2-calc linear chain) is
 * the falsification test. Before the fix at `QueryBuilder.scala:263-299`,
 * the iterative DFS reported a false-positive cycle for any linear chain
 * because the `nil → Black` transition only fired on the first White→Gray
 * transition. The fix introduces a 3-tuple stack frame `(name, remaining,
 * isContinuation)` where the `isContinuation` flag distinguishes the
 * initial frame from the post-child continuation frame.
 */
package io.sm8.core.query

import io.sm8.core.engine.EngineError.UnsupportedCapability
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.CalculatedMeasure
import io.sm8.core.schema.SealedDataType

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class QueryBuilderDetectCalcCyclesSpec extends AnyFlatSpec with Matchers {

  // -- Test 1: empty calculatedMeasures list --

  "QueryBuilder.detectCalcCycles" should "return Right(Unit) for an empty calculatedMeasures list" in {
    QueryBuilder.detectCalcCycles(Nil) shouldBe Right(())
  }

  // -- Test 2: single calc with no measure refs --

  it should "return Right(Unit) for a single calc with no measure refs" in {
    val calc = CalculatedMeasure(
      name = "alone",
      expr = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)
    )
    QueryBuilder.detectCalcCycles(List(calc)) shouldBe Right(())
  }

  // -- Test 3: 2-calc linear chain (a references b) --
  // FALSIFICATION TEST: pre-fix, the algorithm reported a false-positive
  // cycle for any linear chain. This test would have failed on v1.0
  // (and before the fix); passes on v1.1.

  it should "return Right(Unit) for a 2-calc linear chain (a references b)" in {
    // "a" references "b"; "b" is a leaf (literal). The DAG is a -> b (acyclic).
    val a = CalculatedMeasure(
      name = "a",
      expr = Expr.Alias("a", Expr.MeasureRef("b"))
    )
    val b = CalculatedMeasure(
      name = "b",
      expr = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)
    )
    QueryBuilder.detectCalcCycles(List(a, b)) shouldBe Right(())
  }

  // -- Test 4: self-cycle (a references a) --

  it should "return Left(UnsupportedCapability) for a self-cycle (a references a)" in {
    val a = CalculatedMeasure(
      name = "a",
      expr = Expr.Alias("a", Expr.MeasureRef("a"))
    )
    val out = QueryBuilder.detectCalcCycles(List(a))
    out.isLeft shouldBe true
    val err: UnsupportedCapability = out.left.toOption.get.asInstanceOf[UnsupportedCapability]
    err.engine shouldBe "query-builder"
    err.capability shouldBe "CalculatedMeasure.cycle"
    err.message should include ("Cycle in calculated-measure DAG:")
    err.message should include ("a")
  }

  // -- Test 5: 2-calc cycle (a references b; b references a) --
  // REPLACES the v1.0 3-calc cycle test (which had a brittle order-dependent
  // .reverse assertion). The 2-calc cycle produces a deterministic cycle
  // path (order-independent substring assertion).

  it should "return Left(UnsupportedCapability) for a 2-calc cycle (a references b; b references a)" in {
    val a = CalculatedMeasure(
      name = "a",
      expr = Expr.Alias("a", Expr.MeasureRef("b"))
    )
    val b = CalculatedMeasure(
      name = "b",
      expr = Expr.Alias("b", Expr.MeasureRef("a"))
    )
    val out = QueryBuilder.detectCalcCycles(List(a, b))
    out.isLeft shouldBe true
    val err: UnsupportedCapability = out.left.toOption.get.asInstanceOf[UnsupportedCapability]
    err.engine shouldBe "query-builder"
    err.capability shouldBe "CalculatedMeasure.cycle"
    err.message should include ("Cycle in calculated-measure DAG:")
    err.message should include ("a")
    err.message should include ("b")
  }
}
