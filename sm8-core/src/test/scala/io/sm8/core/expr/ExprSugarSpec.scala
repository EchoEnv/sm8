/*
 * SM8 Core -- ExprSugarSpec.
 *
 * Verifies every sugar extension method produces the same `Expr`
 * case class as the explicit constructor. Each test asserts on
 * the AST shape via `shouldBe` equality with the explicit
 * constructor invocation; the sugar is the canonical path, not
 * a parallel one.
 */

package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExprSugarSpec extends AnyFunSuite with Matchers {

  import ExprSugar._

  test("ExprSugar: === produces Expr.Equal") {
    val sugar = "x".asField === 1.asInt
    val explicit = Expr.Equal(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    sugar shouldBe explicit
  }

  test("ExprSugar: !== produces Expr.NotEqual") {
    val sugar = "x".asField !== 0.asInt
    val explicit = Expr.NotEqual(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))
    sugar shouldBe explicit
  }

  test("ExprSugar: <, <=, >, >= produce LessThan / LessOrEqual / GreaterThan / GreaterOrEqual") {
    val lt = "x".asField < 10.asInt
    val le = "x".asField <= 10.asInt
    val gt = "x".asField > 10.asInt
    val ge = "x".asField >= 10.asInt

    lt shouldBe Expr.LessThan(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(10), SealedDataType.Int))
    le shouldBe Expr.LessOrEqual(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(10), SealedDataType.Int))
    gt shouldBe Expr.GreaterThan(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(10), SealedDataType.Int))
    ge shouldBe Expr.GreaterOrEqual(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(10), SealedDataType.Int))
  }

  test("ExprSugar: +, -, *, /, % produce Add / Subtract / Multiply / Divide / Modulo") {
    val a = "x".asField + 1.asInt
    val b = "x".asField - 1.asInt
    val c = "x".asField * 2.asInt
    val d = "x".asField / 2.asInt
    val e = "x".asField % 2.asInt

    a shouldBe Expr.Add(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    b shouldBe Expr.Subtract(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    c shouldBe Expr.Multiply(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    d shouldBe Expr.Divide(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    e shouldBe Expr.Modulo(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
  }

  test("ExprSugar: &&, ||, ! produce And / Or / Not") {
    val andExpr = ("a".asField === 1.asInt) && ("b".asField === 2.asInt)
    val orExpr  = ("a".asField === 1.asInt) || ("b".asField === 2.asInt)
    val notExpr  = !("a".asField === 1.asInt)

    andExpr shouldBe Expr.And(
      Expr.Equal(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
      Expr.Equal(Expr.FieldRef("b"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    )
    orExpr shouldBe Expr.Or(
      Expr.Equal(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
      Expr.Equal(Expr.FieldRef("b"), Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    )
    notExpr shouldBe Expr.Not(
      Expr.Equal(Expr.FieldRef("a"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))
    )
  }

  test("ExprSugar: .asVarchar / .asInt / .asLong / .asDouble / .asBool produce typed Expr.Literal") {
    "x".asVarchar   shouldBe Expr.Literal(LiteralValue.StringValue("x"), SealedDataType.Varchar)
    42.asInt       shouldBe Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
    99L.asLong     shouldBe Expr.Literal(LiteralValue.LongValue(99L), SealedDataType.BigInt)
    3.14.asDouble  shouldBe Expr.Literal(LiteralValue.DoubleValue(3.14), SealedDataType.Double)
    true.asBool    shouldBe Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)
  }

  test("ExprSugar: .asField produces Expr.FieldRef") {
    "discharge_status".asField shouldBe Expr.FieldRef("discharge_status")
  }

  test("ExprSugar: List((Expr, Expr)) sugar via parenthesized Expr.Equal -> Expr.Literal") {
    // The parenthesized form `("a".asField === "b".asVarchar) -> c.asInt`
    // wraps the single `Expr` returned by `===` into a 2-tuple --
    // the canonical `Expr.CaseWhen` branch shape.
    val condition = "discharge_status".asField === "expired".asVarchar
    val thenBranch = 1.asInt
    val elseBranch = 0.asInt

    val branches = List(condition -> thenBranch)

    branches.head shouldBe (
      Expr.Equal(
        Expr.FieldRef("discharge_status"),
        Expr.Literal(LiteralValue.StringValue("expired"), SealedDataType.Varchar)
      ),
      Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)
    )

    val sugarExpr = Expr.CaseWhen(branches = branches, otherwise = elseBranch)
    val explicitExpr = Expr.CaseWhen(
      branches = List(
        Expr.Equal(
          Expr.FieldRef("discharge_status"),
          Expr.Literal(LiteralValue.StringValue("expired"), SealedDataType.Varchar)
        ) -> Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)
      ),
      otherwise = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int)
    )
    sugarExpr shouldBe explicitExpr
  }

  test("ExprSugar: ExprTuple.->(thenBranch) produces (Expr, Expr) for single-condition CaseWhen") {
    // Single-Expr form: `cond.->(thenBranch)` where `cond` is a
    // single `Expr` (not a wrapped tuple). The shadowing
    // `ExprTuple.->` is needed because `Any.->` is deprecated in
    // Scala 2.13.18+.
    val condition: Expr = "x".asField === 1.asInt
    val thenBranch: Expr = 2.asInt

    val tuple: (Expr, Expr) = condition.->(thenBranch)
    tuple shouldBe (condition, thenBranch)

    val expr = Expr.CaseWhen(branches = List(tuple), otherwise = 3.asInt)
    expr shouldBe Expr.CaseWhen(
      branches = List(
        Expr.Equal(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)) -> Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)
      ),
      otherwise = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int)
    )
  }
}
