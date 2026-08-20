/*
 * SM8 Core -- TypedMeasureBridge (PR-26, ADR-008-R SSMeasureBridge).
 *
 * the PR-23 example migration follow-up: provide the typed-witness
 * to un-typed Measure bridge so that the typed DSL can construct a
 * `Model.of(...)` call WITHOUT losing the phantom `[M]` identity.
 *
 * Implementations): this is a Protocol bridge in core. The
 * `TypedMeasure[M]` witness (PR-16) carries the measure identity at
 * the type level; the bridge produces both the un-typed
 * `Measure(name, expr: AggregateCall)` shape (for `Model.of`) AND the
 * typed `TypedAggregateCall[M]` shape (for `QueryBuilderDsl.aggregate`).
 *
 * runtime): the phantom `[M]` is preserved at construction (the
 * witness carries it; the bridge erases at the variance boundary
 * via the well-tested `Measure(...)` / `TypedAggregateCall.of[M](...)`
 * factories).
 *
 * user's explicit concern): the bridge is a pure function -- NO
 * captured SparkSession / DataFrame / Iterator.
 */
package io.sm8.core.model

import io.sm8.core.expr.Expr
import io.sm8.core.rel.{AggregateCall, AggregateFn}

/**
 * Bridge extension on [[TypedMeasure]] that produces BOTH the
 * un-typed `Measure` shape (for `Model.of`) AND the typed
 * `TypedAggregateCall[M]` shape (for `QueryBuilderDsl.aggregate`).
 *
 * the bridge is a pure-function derivation -- no method beyond the
 * two `toMeasure` / `toAggregateCall` accessors.
 */
object TypedMeasureBridge {

 implicit class TypedMeasureToUntyped[M](val typedMeasure: TypedMeasure[M]) extends AnyVal {

 /** Convert the typed witness into the un-typed `Measure(name,
  * expr: AggregateCall)` shape that `Model.of` accepts.
  *
  * the input-column choice matches the legacy PR-K convention:
  * a non-empty `fieldName` produces `Expr.FieldRef(name)`; the
  * default Count case produces `None` (COUNT(*)).
  *
  * exhaustive): `AggregateFn` sealed ADT (6 cases) is matched
  * here. */
 def toMeasure: Measure = {
  val name: String = typedMeasure.name
  val fn: AggregateFn = typedMeasure.aggregateFn
  val inputExpr: Option[Expr] =
  if (typedMeasure.fieldName == "*") None
  else Some(Expr.FieldRef(typedMeasure.fieldName))

  val aggregateCall: AggregateCall = fn match {
  case AggregateFn.Count =>
   AggregateCall(fn = fn, input = None, alias = name)
  case AggregateFn.CountDistinct =>
   AggregateCall(
   fn = fn,
   input = Some(Expr.FieldRef(typedMeasure.fieldName)),
   alias = name,
   distinct = true,
   )
  case other =>
   AggregateCall(
   fn = other,
   input = inputExpr,
   alias = name,
   )
  }
  Measure(name = name, expr = aggregateCall)
 }

 /** Convert the typed witness into the typed
  * `TypedAggregateCall[M]` shape that `QueryBuilderDsl.aggregate`
  * accepts.
  *
  * phantom `[M]` is preserved at construction.
  *
  * the user's explicit concern): pure-function derivation. */
 def toAggregateCall: io.sm8.core.rel.TypedAggregateCall[M] =
  io.sm8.core.rel.TypedAggregateCall.of[M](
  name = typedMeasure.name,
  fn = typedMeasure.aggregateFn,
  input =
   if (typedMeasure.fieldName == "*") None
   else Some(Expr.FieldRef(typedMeasure.fieldName)),
  distinct = (typedMeasure.aggregateFn == AggregateFn.CountDistinct),
  )
 }
}
