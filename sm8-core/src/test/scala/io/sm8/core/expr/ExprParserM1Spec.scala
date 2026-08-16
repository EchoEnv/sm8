/*
 * SM8 Core -- ExprParser PR-M1 extension spec (ADR-008-L Appendix
 * GAP 1: CaseWhen / Alias / All / MeasureRef grammar).
 *
 * Per [[debug-mantra-mindset]] SS1: each test asserts the parsed
 * AST shape (typed, not string fragments).
 *
 * Per [[scala-data-driven-refactor-mindset]]: the parser produces
 * the sealed-trait Expr family at the boundary -- no string
 * substitution, no silent defaulting. Unknown forms fail loud
 * with typed ExprParseError.
 */
package io.sm8.core.expr

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExprParserM1Spec extends AnyFunSuite with Matchers {

  private def ok(s: String): Expr =
    ExprParser.parseExpr(s).fold(e => fail(s"parse failed: $e"), identity)

  private def intLit(n: Int): Expr =
    Expr.Literal(LiteralValue.IntValue(n), io.sm8.core.schema.SealedDataType.Int)

  private def strLit(s: String): Expr =
    Expr.Literal(LiteralValue.StringValue(s), io.sm8.core.schema.SealedDataType.Varchar)

  private def boolLit(b: Boolean): Expr =
    Expr.Literal(LiteralValue.BoolValue(b), io.sm8.core.schema.SealedDataType.Boolean)

  // ===== CASE WHEN =====

  test("CASE WHEN single branch with ELSE parses to CaseWhen") {
    ok("CASE WHEN amount > 100 THEN 'high' ELSE 'low' END") shouldBe
      Expr.CaseWhen(
        branches  = List((
          Expr.GreaterThan(Expr.FieldRef("amount"), intLit(100)),
          strLit("high"))),
        otherwise = strLit("low"),
      )
  }

  test("case-when keywords are case-insensitive") {
    ok("case when amount > 100 then 'high' else 'low' end") shouldBe
      ok("CASE WHEN amount > 100 THEN 'high' ELSE 'low' END")
  }

  test("CASE WHEN multiple branches keep left-to-right order") {
    ok("CASE WHEN a > 2 THEN 1 WHEN a > 1 THEN 2 ELSE 3 END") shouldBe
      Expr.CaseWhen(
        branches = List(
          (Expr.GreaterThan(Expr.FieldRef("a"), intLit(2)), intLit(1)),
          (Expr.GreaterThan(Expr.FieldRef("a"), intLit(1)), intLit(2)),
        ),
        otherwise = intLit(3),
      )
  }

  test("CASE WHEN missing ELSE lowers to NullValue literal (SQL semantics)") {
    ok("CASE WHEN a > 1 THEN 1 END") shouldBe
      Expr.CaseWhen(
        branches  = List((Expr.GreaterThan(Expr.FieldRef("a"), intLit(1)), intLit(1))),
        otherwise = Expr.Literal(LiteralValue.NullValue, io.sm8.core.schema.SealedDataType.Varchar),
      )
  }

  test("CASE WHEN without END is a typed error (fail loud)") {
    ExprParser.parseExpr("CASE WHEN a > 1 THEN 1").isLeft shouldBe true
  }

  test("CASE WHEN without any WHEN branch is a typed error") {
    ExprParser.parseExpr("CASE ELSE 1 END").isLeft shouldBe true
  }

  test("CASE WHEN branch conditions compose with AND/OR (full expression grammar)") {
    ok("CASE WHEN a > 1 AND b < 2 THEN 1 ELSE 0 END") shouldBe
      Expr.CaseWhen(
        branches = List((
          Expr.And(
            Expr.GreaterThan(Expr.FieldRef("a"), intLit(1)),
            Expr.LessThan(Expr.FieldRef("b"), intLit(2))),
          intLit(1))),
        otherwise = intLit(0),
      )
  }

  // ===== AS alias (vs AS type) =====

  test("expr AS aliasName (non-type) parses to Expr.Alias") {
    ok("amount AS total") shouldBe
      Expr.Alias(name = "total", expr = Expr.FieldRef("amount"))
  }

  test("expr AS known-type still parses to Expr.Cast (types win over aliases)") {
    ok("amount AS INT") shouldBe
      Expr.Cast(Expr.FieldRef("amount"), io.sm8.core.schema.SealedDataType.Int)
  }

  test("AS alias is case-insensitive and applies after a full primary") {
    ok("(a + b) AS combined") shouldBe
      Expr.Alias("combined", Expr.Add(Expr.FieldRef("a"), Expr.FieldRef("b")))
  }

  test("AS with empty trailing token is a typed error") {
    ExprParser.parseExpr("amount AS ").isLeft shouldBe true
  }

  // ===== all() / measure() rewrite (legacy CalcExpr DSL) =====

  test("all(name) rewrites to Expr.All (percent-of-total)") {
    ok("all(total_amount)") shouldBe Expr.All("total_amount")
  }

  test("measure(name) rewrites to Expr.MeasureRef") {
    ok("measure(revenue)") shouldBe Expr.MeasureRef("revenue")
  }

  test("all/measure rewrite is case-insensitive") {
    ok("ALL(total)") shouldBe Expr.All("total")
    ok("Measure(revenue)") shouldBe Expr.MeasureRef("revenue")
  }

  test("arbitrary function calls still parse to FunctionCall (no over-rewrite)") {
    ok("upper(name)") shouldBe
      Expr.FunctionCall("upper", Seq(Expr.FieldRef("name")))
  }

  test("all(x, y) with 2 args stays a FunctionCall (rewrite is single-arg only)") {
    ok("all(x, y)") shouldBe
      Expr.FunctionCall("all", Seq(Expr.FieldRef("x"), Expr.FieldRef("y")))
  }

  // ===== composition =====

  test("calculated-measure shape: amount / all(total) parses end-to-end") {
    ok("amount / all(total)") shouldBe
      Expr.Divide(Expr.FieldRef("amount"), Expr.All("total"))
  }

  test("calculated-measure shape with CASE WHEN + alias composes") {
    ok("CASE WHEN amount > 0 THEN amount ELSE 0 END AS clamped") shouldBe
      Expr.Alias(
        name = "clamped",
        expr = Expr.CaseWhen(
          branches  = List((
            Expr.GreaterThan(Expr.FieldRef("amount"), intLit(0)),
            Expr.FieldRef("amount"))),
          otherwise = intLit(0),
        ))
  }
}
