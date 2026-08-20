/*
 * SM8 Core — TypedAggregateCall phantom-typed witness (PR-17, ADR-008-R).
 *
 * The phantom-typed wrapper around the existing `AggregateCall`. The
 * phantom type parameter `[M]` carries the measure identity at the type
 * level — a typo at the call site (e.g. `Refs.patienId` vs
 * `Refs.patientCount`) is a COMPILE error, not a runtime error.
 *
 * Per ADR-008-R §"PR-17 Core types": this trait is the PROTOCOL in
 * core. The witness INSTANCE lives in the consumer's code (`object Refs
 * {. }` in a plugin or example) — NOT in method-local scope (which
 * would capture the enclosing scope and break Spark closure-serialization
 * with `NotSerializableException` at executor startup).
 *
 * Per 
 * implementations): the typed builder sits next to the data, behavior
 * lives elsewhere. The phantom `[M]` is purely type-level (zero runtime
 * cost per 
 * case-class `Impl` allocates once at query-build time, driver-side).
 *
 * Per 
 * explicit concern): this trait extends `Serializable` (verified by the
 * closure-safety spec). The case-class `Impl` form (vs. the anonymous-
 * class form that broke in PR-16) preserves the `name` field through
 * `ObjectOutputStream` round-trip — see `TypedAggregateCallSpec`.
 *
 * Per 
 * carrier, no methods beyond derived accessors. §2 (shape vs validity
 * separate): the case-class constructor is unconditional; the typed
 * builder factory validates at the boundary.
 */
package io.sm8.core.rel

import io.sm8.core.expr.{Expr, LiteralValue}

/**
 * Phantom-typed witness for an aggregate measure. The phantom `[M]`
 * carries the measure identity at the type level.
 *
 * Per ADR-008-Q §PR-16 (closure-safety contract): the witness MUST be
 * defined at `object` level (singleton, class-load time) for Spark
 * closure-safety. Method-local definitions capture the enclosing scope
 * (which may include non-Serializable locals — e.g. a `SparkSession`)
 * and break Spark closure serialization at executor startup.
 *
 * Per 
 * the case class `Impl` form provides a proper equals/hashCode + Java
 * getters (per PR-16 lesson — the anonymous-class form returned `null`
 * from `ObjectOutputStream` round-trip because Scala doesn't generate
 * Java getters for `def` without parens).
 */
sealed trait TypedAggregateCall[M] extends Serializable {
 def name: String
 def fn: AggregateFn
 def input: Option[Expr]
 def distinct: Boolean
 def arguments: List[LiteralValue]

 /** Underlying untyped `AggregateCall` — the engine adapter consumes this
 * via the existing typed path (per PR-K / ADR-008-L). */
 def toAggregateCall: AggregateCall =
 AggregateCall(
  fn  = fn,
  input  = input,
  alias  = name,
  distinct = distinct,
  arguments = arguments
 )
}

object TypedAggregateCall {

 /** Internal case-class implementation. Per PR-16 lesson: case class
 * (not anonymous-class) so the `name` field has a proper Java
 * getter, survives `ObjectOutputStream` round-trip, and Spark closure
 * serialization. */
 private final case class Impl[M](
  theName:  String,
  theFn:  AggregateFn,
  theInput: Option[Expr],
  theDistinct: Boolean,
  theArgs:  List[LiteralValue]
 ) extends TypedAggregateCall[M] {
 override def name: String      = theName
 override def fn: AggregateFn     = theFn
 override def input: Option[Expr]    = theInput
 override def distinct: Boolean     = theDistinct
 override def arguments: List[LiteralValue]  = theArgs
 }

 /** Generic factory. The phantom `[M]` is the witness identity. */
 def of[M](
  name:  String,
  fn:  AggregateFn,
  input:  Option[Expr]  = None,
  distinct: Boolean    = false,
  arguments: List[LiteralValue] = Nil
 ): TypedAggregateCall[M] =
 Impl[M](theName = name, theFn = fn, theInput = input, theDistinct = distinct, theArgs = arguments)

 /** Specialized factories reusing the PR-16 `TypedMeasure` pattern. */
 def count[M](name: String): TypedAggregateCall[M] =
 of[M](name = name, fn = AggregateFn.Count)

 def sum[M](name: String, fieldName: String = "amount"): TypedAggregateCall[M] =
 of[M](name = name, fn = AggregateFn.Sum, input = Some(Expr.FieldRef(fieldName)))

 def avg[M](name: String, fieldName: String = "value"): TypedAggregateCall[M] =
 of[M](name = name, fn = AggregateFn.Avg, input = Some(Expr.FieldRef(fieldName)))

 def min[M](name: String, fieldName: String = "value"): TypedAggregateCall[M] =
 of[M](name = name, fn = AggregateFn.Min, input = Some(Expr.FieldRef(fieldName)))

 def max[M](name: String, fieldName: String = "value"): TypedAggregateCall[M] =
 of[M](name = name, fn = AggregateFn.Max, input = Some(Expr.FieldRef(fieldName)))

 def countDistinct[M](name: String, fieldName: String = "id"): TypedAggregateCall[M] =
 of[M](name = name, fn = AggregateFn.CountDistinct, input = Some(Expr.FieldRef(fieldName)), distinct = true)
}
