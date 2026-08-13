package io.sm8.core.expr

import io.sm8.core.schema.SealedDataType

/** Engine-portable literal-value ADT — Phase 2 contract. Mirrors
  * the design doc §4.5.2 "LiteralValue" (16 cases total: 7 numeric
  * + 1 text + 1 boolean + 1 binary + 2 temporal + 3 nested + 1 special).
  *
  * A `LiteralValue` is the runtime value of a literal expression. It
  * is **engine-portable** in the sense that it carries the value in
  * a portable shape (the `LiteralValue` case), but the actual transport
  * (Spark `Literal`, Trino parameter binding, Databricks value) is
  * engine-specific. Per the data-driven mantra, the value SHAPE is
  * in core; the engine-specific transport is in the engine adapter.
  *
  * ==Why a separate type from `SealedDataType`==
  *
  * `SealedDataType` (the type) and `LiteralValue` (the value) are
  * distinct concepts. The type is engine-portable; the value
  * representation must match the engine's transport. For example,
  * `Int` in the type is "32-bit signed integer" — but the value
  * representation depends on whether the engine uses Java's
  * `Integer`, Trino's `INTEGER`, or a custom protocol.
  *
  * The design's risk #9: "Decimal scale/overflow differs by engine" —
  * engines must report `EngineError.DecimalOverflow` when a result
  * exceeds the declared precision. `DecimalValue(v: BigDecimal)`
  * is the portable value; the engine reports overflow at the
  * boundary.
  *
  * ==Why 16 cases (not fewer, not more)==
  *
  * The set covers the values that a portable expression can carry:
  *   - **Numeric** (7): Int, Byte, Short, Long, Float, Double, Decimal (BigDecimal)
  *   - **Text** (1): String
  *   - **Boolean** (1)
  *   - **Binary** (1): Vector[Byte]
  *   - **Temporal** (2): Instant, LocalDate
  *   - **Nested** (3): Array, Map, Struct
  *   - **Special** (1): NullValue (the absence of a value)
  *
  * `Float` is included because some engines (Trino) use 32-bit floats
  * for analytical workloads (vs. 64-bit `Double`). The design has
  * `FloatValue(v: Float)` as a separate case from `DoubleValue(v: Double)`.
  *
  * ==Why core (engine-portable)==
  *
  * The value SHAPE is universal (numeric, text, boolean, etc.). The
  * engine adapter translates to its native transport. Per
  * scala-data-driven-refactor, the shape lives in core; the
  * transport lives in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + case classes / case objects (no behavior)
  * - Equality auto-derived (case classes)
  * - `Product with Serializable` for Java-serialization round-trip
  * - Each case carries only the data needed to identify the value
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/expr/LiteralValue.scala`
  */
sealed trait LiteralValue extends Product with Serializable

object LiteralValue {

  // -- Numeric (5) --

  /** 32-bit signed integer. Maps to Spark's `IntegerType` value,
    * Java's `Integer`, Trino's `INTEGER` parameter. */
  final case class IntValue(v: Int) extends LiteralValue

  /** 8-bit signed integer. Maps to Spark's `ByteType` value,
    * Java's `Byte`, Trino's `TINYINT` parameter. */
  final case class ByteValue(v: Byte) extends LiteralValue

  /** 16-bit signed integer. Maps to Spark's `ShortType` value,
    * Java's `Short`, Trino's `SMALLINT` parameter. */
  final case class ShortValue(v: Short) extends LiteralValue

  /** 64-bit signed integer. Maps to Spark's `LongType` value,
    * Java's `Long`, Trino's `BIGINT` parameter. The portable default
    * for integer values (per the design's recommendation to use
    * BigInt for new fields). */
  final case class LongValue(v: Long) extends LiteralValue

  /** 32-bit floating-point. Maps to Spark's `FloatType` value,
    * Java's `Float`, Trino's `REAL` parameter. Some engines use
    * 32-bit floats for analytical workloads (faster, less memory). */
  final case class FloatValue(v: Float) extends LiteralValue

  /** 64-bit floating-point. Maps to Spark's `DoubleType` value,
    * Java's `Double`, Trino's `DOUBLE` parameter. */
  final case class DoubleValue(v: Double) extends LiteralValue

  /** Fixed-precision decimal. The portable type is `BigDecimal`
    * (Java's `BigDecimal`, which carries precision and scale). Maps to
    * Spark's `Decimal(p, s)` value, Trino's `DECIMAL(p, s)`. Per the
    * design's risk #9: "Decimal scale/overflow differs by engine" —
    * engines must report `EngineError.DecimalOverflow` when the
    * result exceeds the declared precision. */
  final case class DecimalValue(v: BigDecimal) extends LiteralValue

  // -- Text (1) --

  /** Variable-length string. Maps to Spark's `StringType` value,
    * Java's `String`, Trino's `VARCHAR` parameter. */
  final case class StringValue(v: String) extends LiteralValue

  // -- Boolean (1) --

  /** Boolean. Maps to Spark's `BooleanType` value, Java's `Boolean`,
    * Trino's `BOOLEAN` parameter. */
  final case class BoolValue(v: Boolean) extends LiteralValue

  // -- Binary (1) --

  /** Variable-length binary (e.g. a hash, a serialized message).
    * Maps to Spark's `BinaryType` value, Java's `byte[]` (in Spark's
    * internal representation), Trino's `VARBINARY` parameter. */
  final case class BinaryValue(v: Vector[Byte]) extends LiteralValue

  // -- Temporal (2) --

  /** Instant (point in time, UTC). Maps to Spark's `TimestampType`
    * value, Trino's `TIMESTAMP WITH TIME ZONE` parameter. Per the
    * design's risk #10: "Timestamp uses JVM default timezone" —
    * portable models use UTC `Instant`; engines normalize to their
    * declared IANA zone. */
  final case class TimestampValue(v: java.time.Instant) extends LiteralValue

  /** Date (no time component). Maps to Spark's `DateType` value,
    * Trino's `DATE` parameter. */
  final case class DateValue(v: java.time.LocalDate) extends LiteralValue

  // -- Nested (3) --

  /** Array of literal values. Maps to Spark's `ArrayType` value
    * (which holds elements of a single `DataType`), Trino's `ARRAY`
    * parameter. */
  final case class ArrayValue(values: List[LiteralValue]) extends LiteralValue

  /** Map of literal keys to literal values. Maps to Spark's `MapType`
    * value, Trino's `MAP` parameter. */
  final case class MapValue(values: List[(LiteralValue, LiteralValue)])
      extends LiteralValue

  /** Struct of (name, value) pairs. Maps to Spark's `StructType` value,
    * Trino's `ROW` parameter. */
  final case class StructValue(fields: List[(String, LiteralValue)])
      extends LiteralValue

  // -- Special (1) --

  /** The absence of a value (SQL `NULL`). Per the design's
    * "Empty string is normalized to null" rule: this is the
    * literal value `NULL`, distinct from the type-level
    * "this field accepts null" (`Field.nullable: Boolean`).
    *
    * Maps to Spark's `NullType` (which is its own value type),
    * Trino's `null` parameter. */
  case object NullValue extends LiteralValue
}