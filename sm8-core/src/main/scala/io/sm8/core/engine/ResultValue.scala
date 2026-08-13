/*
 * SM8 Core — ResultValue ADT.
 *
 * Engine-portable typed value representation for `ResultRow.values`.
 * Sealed trait + 8 cases (NullV + 7 case classes). Primitive
 * fields (Long, Double, Boolean, BigDecimal, String,
 * java.time.Instant, java.time.LocalDate) preserve wire fidelity
 * across engines.
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct core +
 * match existing style + Scala 2.13 idiom): sealed trait + case
 * objects + final case classes. NOT Scala 3 `enum`. Case-class
 * fields preserve the legacy types (Long for IntV — per design
 * §4.5.4 "engines normalize integer types to 64-bit").
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait
 * dispatch, pure data, derived value on the companion): the
 * `isNull` companion helper is a 1-line pattern match — a
 * derived value (per mantra step 1: "cheap, total, pure, purely
 * a function of the fields already there") that belongs on the
 * companion, not as a method on each case.
 *
 * Per [[scala-jvm-safety-mindset]]: all fields are non-null
 * primitives or `Serializable` Java types (BigDecimal, Instant,
 * LocalDate, String). Per [[scala-spark-batch-bugs-mindset]]:
 * `Product with Serializable` enables safe Spark closure capture
 * — the case-class auto-derives `equals`/`hashCode`/`toString`
 * (Product) + Java-serialization round-trip (Serializable).
 *
 * Per [[scala-impact-analysis-mindset]]: ADDITIVE to sm8-core.
 * No SDK type changes. PR-C2 (sm8-platform's `encodePortableCell`
 * + `toJavaValue` restructure) and later engine-portable path
 * PRs (PR-C5+) consume this.
 */
package io.sm8.core.engine

/** Engine-portable result-value ADT — Phase 2 contract. Mirrors
  * the design doc §4.5.4 "ResultValue" (the type-safe value
  * representation for `ResultRow.values`).
  *
  * ==Why a sealed ADT (not `Any`)==
  *
  * The previous `ResultRow.values: List[Any]` (per the
  * v0.3.0 design review's CRITIQUE: "values: List[Any] violates
  * the \u00a71.3 transitively-serializable invariant") was
  * rejected. Per the design: "null is JVM null and rejected in
  * non-null fields; decimals preserve declared precision and
  * scale; timestamps normalize to UTC `Instant`; dates are
  * `LocalDate`; arrays are recursive `Vector[Any]`; structs
  * are nested `ResultRow`; maps are ordered `Vector[(Any,Any)]`".
  *
  * The sealed ADT forces every consumer to handle the closed
  * set of value shapes. Adding a new case is a compile-time
  * change at every consumer site \u2014 no silent `Any` cast.
  *
  * ==Why `extends Product with Serializable`==
  *
  * Per the design §1.3: portable values are transitively
  * Serializable. `ResultValue` flows through cache, audit, MCP.
  * The case-class auto-derives `equals`/`hashCode`/`toString`
  * (Product) + Java-serialization round-trip (Serializable).
  *
  * ==Why each case has a unique 1-letter suffix (`V`)==
  *
  * `Null` is a Scala keyword (or at least a soft-reserved word
  * \u2014 used as `NullValue` in `LiteralValue` and we use `NullV`
  * here for consistency with the `Bool`/`Int`/`Double`/etc.
  * naming in `LiteralValue` minus the `Value` suffix). */
sealed trait ResultValue extends Product with Serializable

object ResultValue {

  /** SQL NULL. Maps to the JDBC null value at the engine
    * boundary. NOT the same as JVM `null` \u2014 a `ResultValue.NullV`
    * is a real value in the row, while JVM `null` would mean
    * "no value at all". Per the design §4.5.4: the consumer
    * pattern-matches on `case NullV => ...` to handle the
    * "value is SQL NULL" case distinctly from "value is
    * absent". */
  case object NullV extends ResultValue

  /** Boolean. */
  final case class BoolV(v: Boolean) extends ResultValue

  /** 64-bit signed integer. Matches the Spark / Trino /
    * DuckDB convention of widening all integer types to
    * 64-bit (per the design's "engines normalize integer types
    * to 64-bit"). */
  final case class IntV(v: Long) extends ResultValue

  /** 64-bit IEEE 754 double. */
  final case class DoubleV(v: Double) extends ResultValue

  /** Arbitrary-precision decimal. Preserves the engine-declared
    * `precision` and `scale` (per the design \u00a74.5.4). */
  final case class DecimalV(v: BigDecimal) extends ResultValue

  /** Variable-length string. The default fallback for
    * un-typed / unknown columns. */
  final case class StringV(v: String) extends ResultValue

  /** Point-in-time instant (UTC). All engines normalize to
    * `java.time.Instant` at the engine boundary (per the
    * design \u00a74.5.4: "timestamps normalize to UTC `Instant`").
    * The JVM default timezone is NEVER used. */
  final case class TimestampV(v: java.time.Instant) extends ResultValue

  /** Date (no time-of-day). */
  final case class DateV(v: java.time.LocalDate) extends ResultValue

  /** Null check. Returns `true` if the value is `NullV`, `false`
    * otherwise. Convenience for consumers. */
  def isNull(rv: ResultValue): Boolean = rv match {
    case NullV => true
    case _     => false
  }
}