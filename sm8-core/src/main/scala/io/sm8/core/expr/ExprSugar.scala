/*
 * SM8 Core -- ExprSugar.
 *
 * Sugar over EXISTING `Expr` ADT cases. No new ADT cases; every
 * method returns the same sealed case class the explicit constructor
 * would call. Sugar is at the consumer side (examples, plugins);
 * every engine adapter consumes the unchanged `Expr` AST.
 */
package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

/** Provides syntactic sugar for constructing `Expr` case classes.
 *
 * Import the object where the sugar is wanted; the import
 * brings every extension method into scope:
 *
 * @example
 * {{{
 * import io.sm8.core.expr.ExprSugar._
 * val condition = "discharge_status".asField === "expired".asVarchar
 * val measure = Expr.CaseWhen(
 * branches = List(condition -> 1.asInt),
 * otherwise = 0.asInt,
 * )
 * }}}
 */
object ExprSugar {

 /** Binary comparison operators on `Expr`. */
 implicit class ExprComparisonOps(val left: Expr) extends AnyVal {
 def ===(right: Expr): Expr.Equal   = Expr.Equal(left, right)
 def !==(right: Expr): Expr.NotEqual  = Expr.NotEqual(left, right)
 def <(right: Expr): Expr.LessThan  = Expr.LessThan(left, right)
 def <=(right: Expr): Expr.LessOrEqual  = Expr.LessOrEqual(left, right)
 def >(right: Expr): Expr.GreaterThan  = Expr.GreaterThan(left, right)
 def >=(right: Expr): Expr.GreaterOrEqual = Expr.GreaterOrEqual(left, right)
 }

 /** Arithmetic operators on `Expr`. */
 implicit class ExprArithOps(val left: Expr) extends AnyVal {
 def +(right: Expr): Expr.Add  = Expr.Add(left, right)
 def -(right: Expr): Expr.Subtract = Expr.Subtract(left, right)
 def *(right: Expr): Expr.Multiply = Expr.Multiply(left, right)
 def /(right: Expr): Expr.Divide = Expr.Divide(left, right)
 def %(right: Expr): Expr.Modulo = Expr.Modulo(left, right)
 }

 /** Boolean logic operators on `Expr`. */
 implicit class ExprLogicOps(val left: Expr) extends AnyVal {
 def &&(right: Expr): Expr.And = Expr.And(left, right)
 def ||(right: Expr): Expr.Or = Expr.Or(left, right)
 def unary_! : Expr.Not  = Expr.Not(left)
 }

 /** Lift a `String` into a `varchar`-typed `Expr.Literal`. */
 implicit class StringLit(val s: String) extends AnyVal {
 def asVarchar: Expr = Expr.Literal(
  LiteralValue.StringValue(s), SealedDataType.Varchar)
 }

 /** Lift an `Int` into an `int`-typed `Expr.Literal`. */
 implicit class IntLit(val n: Int) extends AnyVal {
 def asInt: Expr = Expr.Literal(
  LiteralValue.IntValue(n), SealedDataType.Int)
 }

 /** Lift a `Long` into a `bigint`-typed `Expr.Literal`. */
 implicit class LongLit(val n: Long) extends AnyVal {
 def asLong: Expr = Expr.Literal(
  LiteralValue.LongValue(n), SealedDataType.BigInt)
 }

 /** Lift a `Double` into a `double`-typed `Expr.Literal`. */
 implicit class DoubleLit(val d: Double) extends AnyVal {
 def asDouble: Expr = Expr.Literal(
  LiteralValue.DoubleValue(d), SealedDataType.Double)
 }

 /** Lift a `Boolean` into a `boolean`-typed `Expr.Literal`. */
 implicit class BoolLit(val b: Boolean) extends AnyVal {
 def asBool: Expr = Expr.Literal(
  LiteralValue.BoolValue(b), SealedDataType.Boolean)
 }

 /** Lift a column name into an `Expr.FieldRef`. */
 implicit class FieldRefSugar(val name: String) extends AnyVal {
 def asField: Expr = Expr.FieldRef(name)
 }

 /** Build a `(condition, thenBranch)` tuple for `Expr.CaseWhen` branches.
 *
 * Shadows `Any.->` (deprecated in Scala 2.13.18+) so callers
 * writing `cond -> thenBranch` against a single `Expr` get a
 * non-deprecated `(Expr, Expr)` pair without the warning.
 */
 implicit class ExprTuple(val cond: Expr) extends AnyVal {
 def ->(thenBranch: Expr): (Expr, Expr) = (cond, thenBranch)
 }
}
