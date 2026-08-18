/*
 * SM8 Core — TypedDimension phantom-typed witness (PR-16, ADR-008-Q §PR-16).
 *
 * Per `karpathy-app-designmindset` §3.1 (Protocols before
 * implementations): this trait is the Protocol in core. The witness
 * INSTANCE lives in the consumer's code (e.g. `object Refs { ... }`
 * in a plugin or example) — NOT in method-local scope (which would
 * capture the enclosing scope and break Spark closure-serialization).
 *
 * Per ADR-008-Q §C3 (wire-shape decision): this is the ADDITIVE PARALLEL
 * surface to the existing `Dimension(name, expr, dataType)` data type.
 *
 * Per ADR-008-Q §C9 (Restate forward-looking): `extends Serializable` is
 * required for `Restate.run` journal capture.
 *
 * Per `scala-jvm-safety-mindset` §2 + `scala-spark-batch-bugs-mindset` §1
 * (closure-safety): the witness holds no resources + no mutable state; it
 * `extends Serializable` at both trait and case-class level for safe
 * `ObjectOutputStream` round-trip + Restate journal capture.
 */
package io.sm8.core.model

import io.sm8.core.expr.Expr

/** Phantom-typed witness for a `Dimension`. The phantom `[D]` carries
  * the dimension identity at the type level — a typo at the call
  * site (`Refs.patienId` instead of `Refs.patientId`) is a COMPILE
  * error, not a runtime error.
  *
  * Per [[scala-data-driven-refactor-mindset]] §1 (data is data): pure
  * data carrier. The fields are the ONLY data; the methods are derived.
  */
sealed trait TypedDimension[D] extends Serializable {
  def name: String
  def fieldName: String
  def asFieldRef: Expr = Expr.FieldRef(fieldName)
}

object TypedDimension {

  /**
   * Internal case-class implementation. Uses a case class (per
   * [[scala-data-driven-refactor-mindset]] §2 "shape vs validity
   * separate": the case class constructor is unconditional; validity
   * happens at the `of` factory boundary — but for phantom-typed
   * witnesses, validity IS just compile-time, so no runtime check
   * is needed beyond the type system).
   *
   * `extends TypedDimension[D]` + `extends Product with Serializable`
   * is satisfied via case-class derivation.
   */
  private final case class Impl[D](
      _name:      String,
      _fieldName: String
  ) extends TypedDimension[D] {
    override def name: String      = _name
    override def fieldName: String = _fieldName
  }

  /**
   * The ONLY way to instantiate a `TypedDimension[D]`. Per ADR-008-Q
   * §C9 + `scala-spark-batch-bugs-mindset` §1: the witness MUST be
   * defined at `object` level for Spark closure-safety.
   */
  def of[D](name: String, fieldName: String): TypedDimension[D] =
    Impl[D](_name = name, _fieldName = fieldName)

  /** Convenience overload: `fieldName` defaults to `name` (per ADR spec). */
  def of[D](name: String): TypedDimension[D] = of(name, name)
}
