/*
 * SM8 Core -- ExprSugar (PR-35, ADR-008-S v1.3).
 *
 * Per [[karpathy-app-design-mindset]] §3.1 (Protocols before
 * Implementations): the sugar is at the CONSUMER side (examples,
 * plugins). The Expr AST itself is unchanged. Every sugar
 * method RETURNS an existing sealed case class (verified via
 * [[scala-bug-hunting-mindset]] §3 -- the sealed Expr ADT is
 * preserved).
 *
 * Per [[scala-jvm-safety-mindset]] §1 (zero-allocation when
 * possible): all extension classes `extends AnyVal` -- the
 * compiler inlines the method call to a direct constructor
 * invocation, no wrapper object allocated.
 *
 * Per [[scala-spark-batch-bugs-mindset]] §1 (closure-safety --
 * the user's explicit concern): each sugar method returns the
 * same Expr case class as the explicit constructor; closure
 * safety is inherited from the existing Expr closure-safety
 * (the sugar adds NO new state, NO new class). The closure-safety
 * spec [[ExprSugarClosureSafetySpec]] has 3 tests in the
 * PR-16/17/20/25 pattern.
 *
 * Per [[scala-data-driven-refactor-mindset]] §1 (data is data)
 * + §3 (sealed over Map): the sugar is the canonical constructor
 * path; no Map-based dispatch, no runtime reflection.
 *
 * Per [[karpathy-guidelines-mindset]] §2 (simplicity first):
 * sugar over EXISTING Expr cases only (NO new ADT cases).
 *
 * Per [[karpathy-impact-analysis-mindset]] §3 (binary compat):
 * pre-1.0 churn permitted per ADR-008-P §E2. Sugar is in a NEW
 * object; no class field changes; no signature changes. Every
 * existing `Expr.Equal(Expr.FieldRef(...), Expr.Literal(...))`
 * caller compiles + runs UNCHANGED.
 *
 * Per ADR-008-O §"Cross-cutting principles" #1 (RFC §3 layer
 * ownership): the sugar is at sm8-core/expr (the PROTOCOL
 * layer); no engine-adapter changes. Every engine adapter
 * (Spark, Trino, in-memory, future ORM) consumes the unchanged
 * Expr AST.
 *
 * Per ADR-008-S §"Out of scope" mandate: **any future PR adding
 * new Expr ADT cases MUST extend `ExprSugarClosureSafetySpec`
 * with a 3-test block** in the PR-16/17/20/25 pattern.
 */
package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

/** Expr ergonomics sugar (PR-35, ADR-008-S v1.3).
 *
 * Per [[karpathy-guidelines-mindset]] §2: this object provides
 * 21 sugar extension methods over EXISTING Expr case classes (comparison 6 + arithmetic 5 + boolean 3 + literal 5 + FieldRef 1 + CaseWhen tuple 1).
 * Import it where needed:
 * {{{
 *   import io.sm8.core.expr.ExprSugar._
 *   val expr: Expr = "discharge_status".asField === "expired".asVarchar
 * }}}
 */
object ExprSugar {

  // ---- Binary comparison ----
  implicit class ExprComparisonOps(val left: Expr) extends AnyVal {
    def ===(right: Expr): Expr.Equal          = Expr.Equal(left, right)
    def !==(right: Expr): Expr.NotEqual       = Expr.NotEqual(left, right)
    def <(right: Expr):  Expr.LessThan        = Expr.LessThan(left, right)
    def <=(right: Expr): Expr.LessOrEqual     = Expr.LessOrEqual(left, right)
    def >(right: Expr):  Expr.GreaterThan     = Expr.GreaterThan(left, right)
    def >=(right: Expr): Expr.GreaterOrEqual  = Expr.GreaterOrEqual(left, right)
  }

  // ---- Arithmetic ----
  implicit class ExprArithOps(val left: Expr) extends AnyVal {
    def +(right: Expr): Expr.Add      = Expr.Add(left, right)
    def -(right: Expr): Expr.Subtract = Expr.Subtract(left, right)
    def *(right: Expr): Expr.Multiply = Expr.Multiply(left, right)
    def /(right: Expr): Expr.Divide   = Expr.Divide(left, right)
    def %(right: Expr): Expr.Modulo   = Expr.Modulo(left, right)
  }

  // ---- Boolean logic ----
  implicit class ExprLogicOps(val left: Expr) extends AnyVal {
    def &&(right: Expr): Expr.And = Expr.And(left, right)
    def ||(right: Expr): Expr.Or  = Expr.Or(left, right)
    def unary_! : Expr.Not       = Expr.Not(left)
  }

  // ---- Literal helpers ----
  implicit class StringLit(val s: String) extends AnyVal {
    def asVarchar: Expr = Expr.Literal(
      LiteralValue.StringValue(s), SealedDataType.Varchar)
  }
  implicit class IntLit(val n: Int) extends AnyVal {
    def asInt: Expr = Expr.Literal(
      LiteralValue.IntValue(n), SealedDataType.Int)
  }
  implicit class LongLit(val n: Long) extends AnyVal {
    def asLong: Expr = Expr.Literal(
      LiteralValue.LongValue(n), SealedDataType.BigInt)
  }
  implicit class DoubleLit(val d: Double) extends AnyVal {
    def asDouble: Expr = Expr.Literal(
      LiteralValue.DoubleValue(d), SealedDataType.Double)
  }
  implicit class BoolLit(val b: Boolean) extends AnyVal {
    def asBool: Expr = Expr.Literal(
      LiteralValue.BoolValue(b), SealedDataType.Boolean)
  }

  // ---- FieldRef helper ----
  implicit class FieldRefSugar(val name: String) extends AnyVal {
    def asField: Expr = Expr.FieldRef(name)
  }

  // ---- CaseWhen tuple sugar ----
  // `cond -> thenBranch` parses as `cond.->(thenBranch)`, which
  // returns `(cond, thenBranch)`. The implicit conversion gives
  // it the right type for `List((Expr, Expr))`.
  implicit class ExprTuple(val cond: Expr) extends AnyVal {
    def ->(thenBranch: Expr): (Expr, Expr) = (cond, thenBranch)
  }
}
