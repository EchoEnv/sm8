/*
 * SM8 Core — TypedMeasure phantom-typed witness (PR-16, ADR-008-Q §PR-16).
 *
 * Per `karpathy-app-designmindset` §3.1 (Protocols before implementations):
 * the trait is the Protocol in core. The witness INSTANCE lives in the
 * consumer's code (e.g. `object Refs {... }` in a plugin).
 *
 * Per `scala-spark-batch-bugs-mindset` §1 (closure-safety) + ADR-008-Q §C9
 * (Restate forward-looking): `extends Serializable` at both trait + impl.
 */
package io.sm8.core.model

import io.sm8.core.expr.Expr
import io.sm8.core.rel.AggregateFn

/**
 * Phantom-typed witness for a `Measure`. The phantom `[M]` carries the
 * measure identity at the type level.
 *
 * Per `scala-data-driven-refactor-mindset` §1 (data is data): pure data
 * carrier. The fields are the ONLY data; the methods are derived.
 */
sealed trait TypedMeasure[M] extends Serializable {
 def name: String
 def aggregateFn: AggregateFn
 def fieldName: String
 def asFieldRef: Expr = Expr.FieldRef(fieldName)
}

object TypedMeasure {

 /** Internal case-class implementation. Provides clean `equals`,
 * `hashCode`, `toString`, and Serializable round-trip via case-class
 * derivation. */
 private final case class Impl[M](
  _name:  String,
  _aggregateFn: AggregateFn,
  _fieldName: String
 ) extends TypedMeasure[M] {
 override def name: String  = _name
 override def aggregateFn: AggregateFn = _aggregateFn
 override def fieldName: String = _fieldName
 }

 /**
 * `AggregateFn.Count` measure factory (no field needed; COUNT(*)).
 */
 def count[M](name: String): TypedMeasure[M] =
 Impl[M](_name = name, _aggregateFn = AggregateFn.Count, _fieldName = "*")

 /**
 * `AggregateFn.Sum` measure factory. Default `fieldName = "amount"`.
 */
 def sum[M](name: String, fieldName: String): TypedMeasure[M] =
 Impl[M](_name = name, _aggregateFn = AggregateFn.Sum, _fieldName = fieldName)
 def sum[M](name: String): TypedMeasure[M] = sum[M](name, "amount")

 /**
 * `AggregateFn.Avg` measure factory.
 */
 def avg[M](name: String, fieldName: String): TypedMeasure[M] =
 Impl[M](_name = name, _aggregateFn = AggregateFn.Avg, _fieldName = fieldName)
 def avg[M](name: String): TypedMeasure[M] = avg[M](name, "value")

 /**
 * `AggregateFn.Min` measure factory.
 */
 def min[M](name: String, fieldName: String): TypedMeasure[M] =
 Impl[M](_name = name, _aggregateFn = AggregateFn.Min, _fieldName = fieldName)
 def min[M](name: String): TypedMeasure[M] = min[M](name, "value")

 /**
 * `AggregateFn.Max` measure factory.
 */
 def max[M](name: String, fieldName: String): TypedMeasure[M] =
 Impl[M](_name = name, _aggregateFn = AggregateFn.Max, _fieldName = fieldName)
 def max[M](name: String): TypedMeasure[M] = max[M](name, "value")

 /**
 * `AggregateFn.CountDistinct` measure factory.
 */
 def countDistinct[M](name: String, fieldName: String): TypedMeasure[M] =
 Impl[M](_name = name, _aggregateFn = AggregateFn.CountDistinct, _fieldName = fieldName)
 def countDistinct[M](name: String): TypedMeasure[M] = countDistinct[M](name, "id")
}
