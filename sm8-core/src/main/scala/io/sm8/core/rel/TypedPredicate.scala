/*
 * SM8 Core -- TypedPredicate phantom-typed witness (the current implementation, the design contract/where).
 * The phantom-typed wrapper around the existing `Predicate` AST
 * (in `sm8-core/predicate/Predicate.scala`). The phantom type
 * parameter `[D]` carries the dimension identity at the type
 * level -- a typo at the call site (e.g. `Refs.region` vs
 * `Refs.region2`) is a COMPILE error, not a runtime error.
 * Per the design contract §"the current implementation Core types": this trait is the PROTOCOL in
 * core. The witness INSTANCE lives in the consumer's code (`object
 * Refs { ... }` in a plugin or example) -- NOT in method-local scope
 * (which would capture the enclosing scope and break Spark closure-
 * serialization with `NotSerializableException` at executor
 * startup).
 *  * Implementations): the typed builder sits next to the data,
 * behavior lives elsewhere. The phantom `[D]` is purely type-level
 * (zero runtime cost per [[scala-perf-testing-mindset]] SS3: zero
 * per-row allocation; case-class `Impl` allocates once at query-
 * build time, driver-side).
 *  * user's explicit concern): this trait extends `Serializable`
 * (verified by the closure-safety spec). The case-class `Impl`
 * form (vs. the anonymous-class form that broke in the current implementation)
 * preserves all fields through `ObjectOutputStream` round-trip --
 * see `TypedPredicateClosureSafetySpec`.
 *  * pure carrier, no methods beyond derived accessors. SS2 (shape vs
 * validity separate): the case-class constructor is unconditional;
 * the typed builder factory validates at the boundary (per
 * [[scala-jvm-safety-mindset]] SS2).
 */
package io.sm8.core.rel

import io.sm8.core.predicate.{CompareOp, Predicate}

/**
 * Phantom-typed witness for a filter predicate. The phantom `[D]`
 * carries the dimension identity at the type level.
 * Per the design contract current implementation (closure-safety contract): the witness MUST
 * be defined at `object` level (singleton, class-load time) for
 * Spark closure-safety. Method-local definitions capture the
 * enclosing scope (which may include non-Serializable locals --
 * e.g. a `SparkSession`) and break Spark closure serialization at
 * executor startup.
 *  * exhaustive): the case class `Impl` form provides a proper
 * equals/hashCode + Java getters (per the current implementation lesson -- the
 * anonymous-class form returned `null` from `ObjectOutputStream`
 * round-trip because Scala doesn't generate Java getters for `def`
 * without parens).
 */
sealed trait TypedPredicate[D] extends Serializable {

  /** The underlying predicate AST (the engine-portable shape). */
  def predicate: Predicate

  /** The name of this predicate (for observability +
    * QueryBuilder DSL chaining). */
  def name: String
}

object TypedPredicate {

  /** Internal case-class implementation. Per the current implementation lesson: case
    * class (not anonymous-class) so the `predicate` field has a
    * proper Java getter, survives `ObjectOutputStream` round-trip,
    * and Spark closure serialization. */
  private final case class Impl[D](
      theName:      String,
      thePredicate: Predicate
  ) extends TypedPredicate[D] {
    override def name:      String    = theName
    override def predicate: Predicate = thePredicate
  }

  /** Generic factory. The phantom `[D]` is the witness identity. */
  def of[D](
      name:      String,
      predicate: Predicate
  ): TypedPredicate[D] =
    Impl[D](theName = name, thePredicate = predicate)

  // === Specialized factories (per the current implementation pattern) ===

  /** `field = value` -- the most common case. */
  def eq[D](
      field: String,
      value: Any
  ): TypedPredicate[D] =
    of[D](name = s"${field}=${value}", predicate =
      Predicate.Compare(field = field, op = CompareOp.Eq, value = value))

  /** `field != value`. */
  def ne[D](
      field: String,
      value: Any
  ): TypedPredicate[D] =
    of[D](name = s"${field}!=${value}", predicate =
      Predicate.Compare(field = field, op = CompareOp.Ne, value = value))

  /** `field < value`. */
  def lt[D](
      field: String,
      value: Any
  ): TypedPredicate[D] =
    of[D](name = s"${field}<${value}", predicate =
      Predicate.Compare(field = field, op = CompareOp.Lt, value = value))

  /** `field <= value`. */
  def le[D](
      field: String,
      value: Any
  ): TypedPredicate[D] =
    of[D](name = s"${field}<=${value}", predicate =
      Predicate.Compare(field = field, op = CompareOp.Le, value = value))

  /** `field > value`. */
  def gt[D](
      field: String,
      value: Any
  ): TypedPredicate[D] =
    of[D](name = s"${field}>${value}", predicate =
      Predicate.Compare(field = field, op = CompareOp.Gt, value = value))

  /** `field >= value`. */
  def ge[D](
      field: String,
      value: Any
  ): TypedPredicate[D] =
    of[D](name = s"${field}>=${value}", predicate =
      Predicate.Compare(field = field, op = CompareOp.Ge, value = value))

  /** `field IN (v1, v2, ...)` -- the in-list case. */
  def in[D](
      field:  String,
      values: List[Any]
  ): TypedPredicate[D] =
    of[D](name = s"${field} IN (${values.mkString(", ")})", predicate =
      Predicate.In(field = field, values = values, negate = false))

  /** `field IS NULL`. */
  def isNull[D](
      field: String
  ): TypedPredicate[D] =
    of[D](name = s"${field} IS NULL", predicate =
      Predicate.IsNull(field = field, negate = false))

  /** `field IS NOT NULL`. */
  def isNotNull[D](
      field: String
  ): TypedPredicate[D] =
    of[D](name = s"${field} IS NOT NULL", predicate =
      Predicate.IsNull(field = field, negate = true))

  /** Combine multiple predicates via AND. */
  def and[D](
      children: TypedPredicate[D]*
  ): TypedPredicate[D] = {
    val names = children.map(_.name).mkString(" AND ")
    of[D](name = names, predicate =
      Predicate.and(children.map(_.predicate).toList))
  }

  /** Combine multiple predicates via OR. */
  def or[D](
      children: TypedPredicate[D]*
  ): TypedPredicate[D] = {
    val names = children.map(_.name).mkString(" OR ")
    of[D](name = names, predicate =
      Predicate.or(children.map(_.predicate).toList))
  }
}
