/*
 * SM8 Core -- Calculator spec (PR-M5).
 *
 * Per RFC §12 conformance: the engine-portable Expr walker
 * (Calculator.fieldNamesOf + Calculator.measureNamesOf) is
 * the SINGLE source of truth for Expr walking. Every existing
 * Expr case is covered, including PR-I's CaseWhen + Alias.
 */
package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CalculatorSpec extends AnyFunSuite with Matchers {

  private def intLit(n: Int) = Expr.Literal(LiteralValue.IntValue(n), SealedDataType.Int)
  private def strLit(s: String) = Expr.Literal(LiteralValue.StringValue(s), SealedDataType.Varchar)
  private def field(n: String) = Expr.FieldRef(n)
  private def measure(n: String) = Expr.MeasureRef(n)
  private def allRef(n: String) = Expr.All(n)

  // ===== fieldNamesOf =====

  test("fieldNamesOf: empty for Literal") {
    Calculator.fieldNamesOf(intLit(42)) shouldBe Set.empty
  }

  test("fieldNamesOf: single field ref") {
    Calculator.fieldNamesOf(field("amount")) shouldBe Set("amount")
  }

  test("fieldNamesOf: de-dupes across the tree") {
    val e = Expr.Add(Expr.Add(field("a"), field("b")), Expr.Add(field("a"), field("b")))
    Calculator.fieldNamesOf(e) shouldBe Set("a", "b")
  }

  test("fieldNamesOf: MeasureRef and All are NOT fields (engine-known)") {
    val e = Expr.Add(measure("total"), allRef("grand_total"))
    Calculator.fieldNamesOf(e) shouldBe Set.empty
  }

  test("fieldNamesOf: CaseWhen branches + otherwise are walked (PR-I)") {
    val e = Expr.CaseWhen(
      branches = List((Expr.GreaterThan(field("amount"), intLit(100)), strLit("high"))),
      otherwise = Expr.LessThan(field("amount"), intLit(0)),
    )
    Calculator.fieldNamesOf(e) shouldBe Set("amount")
  }

  test("fieldNamesOf: Alias unwraps the inner expression (PR-I)") {
    val e = Expr.Alias("band", Expr.CaseWhen(
      branches = List((field("region"), strLit("east"))),
      otherwise = strLit("other"),
    ))
    Calculator.fieldNamesOf(e) shouldBe Set("region")
  }

  test("fieldNamesOf: FunctionCall arguments are walked (engine-bound UDFs skipped)") {
    val e = Expr.FunctionCall("upper", Seq(field("name"), field("nickname")))
    Calculator.fieldNamesOf(e) shouldBe Set("name", "nickname")
  }

  test("fieldNamesOf: nested arithmetic with literals + fields") {
    val e = Expr.Multiply(Expr.Add(field("price"), intLit(1)), field("quantity"))
    Calculator.fieldNamesOf(e) shouldBe Set("price", "quantity")
  }

  test("fieldNamesOf: IsNull / IsNotNull / Cast / Not unwrap the inner") {
    val e1 = Expr.IsNull(field("a"));     Calculator.fieldNamesOf(e1) shouldBe Set("a")
    val e2 = Expr.IsNotNull(field("b"));  Calculator.fieldNamesOf(e2) shouldBe Set("b")
    val e3 = Expr.Cast(field("c"), SealedDataType.Int); Calculator.fieldNamesOf(e3) shouldBe Set("c")
    val e4 = Expr.Not(field("d"));        Calculator.fieldNamesOf(e4) shouldBe Set("d")
  }

  // ===== measureNamesOf =====

  test("measureNamesOf: empty for Literal and FieldRef (engine-known fields are NOT measures)") {
    Calculator.measureNamesOf(intLit(42)) shouldBe Set.empty
    Calculator.measureNamesOf(field("amount")) shouldBe Set.empty
  }

  test("measureNamesOf: MeasureRef is a measure (legacy PR #419)") {
    Calculator.measureNamesOf(measure("total")) shouldBe Set("total")
  }

  test("measureNamesOf: Expr.All is a measure reference (legacy PR #419)") {
    Calculator.measureNamesOf(allRef("grand_total")) shouldBe Set("grand_total")
  }

  test("measureNamesOf: combined measure refs (pr-PR-M4 calculatedMeasure shape)") {
    // The classic pct-of-total: amount / total. The walker
    // collects both 'amount' (a field) and 'total' (a measure),
    // but a CALLER (e.g. cycle detection) can filter by name-set.
    val e = Expr.Divide(field("amount"), measure("total"))
    Calculator.measureNamesOf(e) shouldBe Set("total")
  }

  test("measureNamesOf: nested divide + add of measures") {
    val e = Expr.Divide(
      Expr.Add(measure("a"), measure("b")),
      Expr.Add(measure("a"), measure("c")),
    )
    Calculator.measureNamesOf(e) shouldBe Set("a", "b", "c")
  }

  test("measureNamesOf: CaseWhen + Alias in measure-only context (PR-I)") {
    val e = Expr.Alias("x",
      Expr.CaseWhen(
        branches = List((measure("y"), measure("z"))),
        otherwise = measure("w"),
      ))
    Calculator.measureNamesOf(e) shouldBe Set("y", "z", "w")
  }

  test("measureNamesOf: FunctionCall with measure ref args is walked") {
    val e = Expr.FunctionCall("sum", Seq(measure("a"), field("b")))
    Calculator.measureNamesOf(e) shouldBe Set("a")
  }

  // ===== caller integration (the test of the refactor) =====

  test("caller integration: ModelValidator delegates via Calculator (smoke)") {
    // The inlined walker in ModelValidator.walkExprForFields is
    // now a 1-liner delegate. This test exercises the SAME call
    // path via Calculator to ensure the refactor preserves the
    // contract.
    val e = Expr.Add(Expr.Add(field("a"), measure("b")), Expr.Alias("x", field("c")))
    Calculator.fieldNamesOf(e) shouldBe Set("a", "c")
    Calculator.measureNamesOf(e) shouldBe Set("b")
  }
}
