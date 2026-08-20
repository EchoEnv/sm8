/*
 * SM8 Core -- ExprSugar.
 *
 * Sugar over EXISTING `Expr` ADT cases. No new ADT cases; every
 * method returns the same sealed case class the explicit constructor
 * would call. Sugar is at the consumer side (examples, plugins);
 * every engine adapter consumes the unchanged `Expr` AST.
 */
package io.sm8.core.expr

import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.core.schema.SealedDataType


/** Provides syntactic sugar for constructing `Expr` and
 * `AggregateCall` values.
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
/** Wrap an `Expr` in a single-input `AggregateCall` (the common
 * `SUM(x) AS total` / `AVG(x) AS avg` shape). Sugar returns the
 * existing `AggregateCall` case class — zero new ADT cases.
 *
 * Pair with `Measure.aggregate(name, call)` for a fully infix
 * model definition.
 *
 * @example
 * {{{
 * import io.sm8.core.expr.ExprSugar._
 * Measure.aggregate("total_los", "los_days".asField.sum)
 * Measure.aggregate("avg_los",   "los_days".asField.avg)
 * }}}
 */
implicit class ExprAggregateOps(val left: Expr) extends AnyVal {
 def sum:           AggregateCall = AggregateCall(AggregateFn.Sum,           Some(left))
 def avg:           AggregateCall = AggregateCall(AggregateFn.Avg,           Some(left))
 def min:           AggregateCall = AggregateCall(AggregateFn.Min,           Some(left))
 def max:           AggregateCall = AggregateCall(AggregateFn.Max,           Some(left))
 def countDistinct: AggregateCall = AggregateCall(AggregateFn.CountDistinct, Some(left), distinct = true)
}

/** Build a `COUNT(*)`-shaped `AggregateCall` (no input expression).
 *
 * Returns the existing `AggregateCall` case class with
 * `input = None` and the receiver as the alias — equivalent to
 * `Measure.aggregate(name, AggregateFn.Count, ???)` where the
 * third arg is conventionally `1.asInt` but engine-lowered as
 * `COUNT(*)` (no input).
 *
 * Method name is `countStar` (not `count`) to avoid shadowing
 * `scala.collection.StringOps.count(p: Char => Boolean)`.
 *
 * @example
 * {{{
 * import io.sm8.core.expr.ExprSugar._
 * Measure.aggregate("encounter_count", "encounter_id".countStar)
 * }}}
 */
implicit class CountOp(val name: String) extends AnyVal {
 def countStar: AggregateCall = AggregateCall(AggregateFn.Count, None, name)
}

/** Reference a sibling measure from inside a `CalculatedMeasure`
 * or `Expr.CaseWhen` branch.
 *
 * Both `name.measure` and `name.all` return the existing
 * `Expr.MeasureRef(name)` / `Expr.All(name)` case classes — zero
 * new ADT cases. Engine-agnostic; Spark lowers `MeasureRef` to
 * `Column = functions.col(measureName)` and `All` to the percent
 * of total.
 *
 * @example
 * {{{
 * import io.sm8.core.expr.ExprSugar._
 * CalculatedMeasure(
 *   name = "avg_los",
 *   expr = Expr.Divide("total_los".measure, "encounter_count".measure))
 * }}}
 */
implicit class StringMeasureRefOps(val name: String) extends AnyVal {
 def measure: Expr = Expr.MeasureRef(name)
 def all:      Expr = Expr.All(name)
}

/** Wrap an `Expr` in an `Expr.Cast` to a target `SealedDataType`
 * (or one of the convenience per-type shortcuts).
 *
 * Sugar returns the existing `Expr.Cast(expr, targetType)` case
 * class — zero new ADT cases. The `.asInt / .asLong / .asDouble /
 * .asBool / .asVarchar` names intentionally differ from the
 * literal-lift shortcuts (`IntLit.asInt` returns `Expr.Literal`,
 * not `Expr.Cast`) — different receiver types resolve without
 * ambiguity.
 *
 * @example
 * {{{
 * import io.sm8.core.expr.ExprSugar._
 * "amount".asField.asLong     // Expr.Cast(FieldRef("amount"), BigInt)
 * "flag".asField.castAs(SealedDataType.Boolean)
 * }}}
 */
implicit class ExprCastOps(val e: Expr) extends AnyVal {
 def castAs(t: SealedDataType): Expr.Cast = Expr.Cast(e, t)
 def asInt:     Expr.Cast = Expr.Cast(e, SealedDataType.Int)
 def asLong:    Expr.Cast = Expr.Cast(e, SealedDataType.BigInt)
 def asDouble:  Expr.Cast = Expr.Cast(e, SealedDataType.Double)
 def asBool:    Expr.Cast = Expr.Cast(e, SealedDataType.Boolean)
 def asVarchar: Expr.Cast = Expr.Cast(e, SealedDataType.Varchar)
}

}
