/*
 * SM8 Core -- ExprSugarSpec (PR-35, ADR-008-S v1.3).
 *
 * Per [[karpathy-guidelines-mindset]] SS4 (Goal-Driven Execution):
 * the spec verifies all sugar extension methods produce the
 * correct Expr case class (the canonical constructor path).
 *
 * Per [[scala-bug-hunting-mindset]] SS1 (trust compiler, not
 * runtime): each test asserts on the AST shape (`shouldBe`
 * equality with the explicit constructor invocation).
 *
 * Per [[scala-data-driven-refactor-mindset]] SS1 (data is data)
 * + SS3 (sealed over Map): the sugar returns existing sealed
 * case classes (no Map-based dispatch, no runtime reflection).
 */
package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExprSugarSpec extends AnyFunSuite with Matchers {

  // Per the closure-safety spec pattern -- sugar imported at spec level.
  import ExprSugar._

  // === Test 1: Binary comparison ===

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

  // === Test 2: Comparison operators ===

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

  // === Test 3: Arithmetic ===

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

  // === Test 4: Boolean logic ===

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

  // === Test 5: Literal helpers ===

  test("ExprSugar: .asVarchar / .asInt / .asLong / .asDouble / .asBool produce typed Expr.Literal") {
    "x".asVarchar   shouldBe Expr.Literal(LiteralValue.StringValue("x"), SealedDataType.Varchar)
    42.asInt       shouldBe Expr.Literal(LiteralValue.IntValue(42), SealedDataType.Int)
    99L.asLong     shouldBe Expr.Literal(LiteralValue.LongValue(99L), SealedDataType.BigInt)
    3.14.asDouble  shouldBe Expr.Literal(LiteralValue.DoubleValue(3.14), SealedDataType.Double)
    true.asBool    shouldBe Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)
  }

  // === Test 6: FieldRef helper ===

  test("ExprSugar: .asField produces Expr.FieldRef") {
    "discharge_status".asField shouldBe Expr.FieldRef("discharge_status")
  }

  // === Test 7: CaseWhen tuple sugar (the killer demo, used in PR-34 migration) ===

  test("ExprSugar: List((Expr, Expr)) sugar via parenthesized Expr.Equal -> Expr.Literal") {
    // Per data-eng review NIT #3: the PR-34 migration uses the
    // parenthesized form `("discharge_status".asField === "expired".asVarchar) -> 1.asInt`
    // because `===` returns a single Expr, then `(...)` wraps to
    // `(Expr, Expr)` for the tuple. This is the actual CaseWhen
    // sugar pattern used in the example.
    val condition = "discharge_status".asField === "expired".asVarchar
    val thenBranch = 1.asInt
    val elseBranch = 0.asInt

    val branches = List(condition -> thenBranch)

    // Verify the sugar produces the right tuple shape.
    branches.head shouldBe (
      Expr.Equal(
        Expr.FieldRef("discharge_status"),
        Expr.Literal(LiteralValue.StringValue("expired"), SealedDataType.Varchar)
      ),
      Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)
    )

    // Verify the full Expr.CaseWhen compiles + equals the explicit form.
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

  // === Test 8: ExprTuple implicit class (the single-Expr -> thenBranch form) ===

  test("ExprSugar: ExprTuple.->(thenBranch) produces (Expr, Expr) for single-condition CaseWhen") {
    // Per data-eng review NIT #3: the ExprTuple implicit class IS
    // the canonical sugar for `cond -> thenBranch` when cond is a
    // SINGLE Expr (not a (Expr, Expr) tuple). The PR-34 migration
    // uses the parenthesized form; this test exercises the
    // single-Expr form via ExprTuple to prove it's not dead code.
    val condition: Expr = "x".asField === 1.asInt
    val thenBranch: Expr = 2.asInt

    // ExprTuple.-> returns (Expr, Expr) -- the canonical CaseWhen shape.
    val tuple: (Expr, Expr) = condition.->(thenBranch)
    tuple shouldBe (condition, thenBranch)

    // Use in a CaseWhen.
    val expr = Expr.CaseWhen(branches = List(tuple), otherwise = 3.asInt)
    expr shouldBe Expr.CaseWhen(
      branches = List(
        Expr.Equal(Expr.FieldRef("x"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)) -> Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)
      ),
      otherwise = Expr.Literal(LiteralValue.IntValue(3), SealedDataType.Int)
    )
  }
}
