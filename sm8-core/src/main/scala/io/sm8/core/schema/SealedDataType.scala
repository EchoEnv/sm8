/*
 * SM8 Core — SealedDataType ADT.
 * Engine-portable sealed ADT for primitive + nested types.
 * Closed set per [[scala-data-driven-refactor-mindset]] (sealed
 * trait + case objects / case classes; Scala 2.13 idiom; NOT
 * Scala 3 `enum`).
 * * match existing style): 13 cases covering SQL primitives +
 * temporal + decimal + nested + JSON + binary. Mirrors the legacy
 * the engine-portable types spec
 * contract preserved verbatim — package rename only, no behavior
 * change). A `Binary` case was added (review
 * pass #2, finding #2) to support end-to-end `ResultValue.BinaryV`
 * round-trip through the cache journal.
 * * No SDK type changes (Plugin, Connector, PreHook, PostHook,
 * Transformer, Context, Engine, ConnectorRegistry, HookManager,
 * TransformerRegistry all untouched). The sm8-platform
 * bootstrap + `sealedTypeTag` restructure) and later
 * engine-portable adapters consume this.
 */
package io.sm8.core.schema

/** Engine-portable sealed ADT for primitive + nested types —
 * the engine-portable contract. Mirrors the design doc §4.5.2 "Portable types".
 * Every type that flows through the portable model, the MCP wire
 * format, or any engine adapter is one of the cases here. Engine
 * adapters map each case to their native type (Spark `DataType`,
 * Trino `Type`, Databricks `DataType`, etc.).
 * ==Why a sealed ADT (not a String)==
 * Per the design doc §0 correction 2: "Capabilities describe what
 * an engine supports; policies describe what this query asks the
 * engine to do." Similarly, types describe the data shape. A free-
 * form `type: String` field would let engines invent new types
 * that the model validator, type checker, and engine resolvers
 * couldn't classify. A closed ADT forces every component to handle
 * the closed set of types.
 * ==Why these specific cases==
 * The set covers the common SQL primitive + nested types:
 * - **Primitives** (5): BigInt, Int, Double, Varchar, Boolean
 * - **Temporal** (2): Timestamp, Date
 * - **Decimal** (1): Decimal(precision, scale)
 * - **Nested** (3): Array(elementType), Map(keyType, valueType),
 *  Row(fields)
 * - **Special** (1): Json (JSON string)
 * The design's "v0.3.0 phantom-ADT finding" removed a few
 * speculative cases (Interval, Uuid, etc.) — those land via the
 * ExtensionValue mechanism instead.
 * ==Why core (engine-portable)==
 * Types are universal across query engines. The engine adapter
 * maps each case to its native type. Per scala-data-driven-refactor,
 * data (types) lives in core; behavior (engine-specific mapping)
 * lives in the engine adapter layer.
 * ==Data-driven mantra compliance==
 * - Pure data: sealed trait + case classes / case objects (no behavior)
 * - Equality auto-derived (case classes)
 * - `Product with Serializable` for Java-serialization round-trip
 * - Each case carries only the data needed to identify the type
 * ==Boundary contract==
 * Zero Spark imports. Verifiable by:
 * `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/schema/SealedDataType.scala`
 * ==Reserved future cases (NOT in v0.3.0)==
 * Per design §11, the following are deliberately deferred:
 * - `Decimal(precision, scale)` `Infinity` / `NaN` sub-cases
 *  (Trino decimal-overflow path; not portable yet)
 * - `Uuid` (represented as `Varchar` for portability)
 * - `Interval(startType, endType)` (removed per v0.2.x
 *  phantom-ADT finding; not portable across engines)
 * `Null` is NOT a type — it's a value-level concept handled by
 * `Field.nullable: Boolean` and the `ResultValue.Null` ADT case
 * (added in v0.3.0). The design's rule "empty string is not null;
 * no engine may rewrite it" is enforced at the lowerer level, not
 * here.
 *  not a type-level one
 */
sealed trait SealedDataType extends Product with Serializable

object SealedDataType {

 // -- Primitives --

 /** 64-bit signed integer. Maps to Spark's `LongType`, Trino's
 * `BIGINT`, Databricks' `BIGINT`. */
 case object BigInt extends SealedDataType

 /** 32-bit signed integer. Maps to Spark's `IntegerType`, Trino's
 * `INTEGER`, Databricks' `INT`. Note: portable models should
 * default to `BigInt` for new fields; `Int` is for legacy data. */
 case object Int extends SealedDataType

 /** 64-bit floating-point. Maps to Spark's `DoubleType`, Trino's
 * `DOUBLE`, Databricks' `DOUBLE`. */
 case object Double extends SealedDataType

 /** Variable-length string. Maps to Spark's `StringType`, Trino's
 * `VARCHAR`, Databricks' `STRING`. */
 case object Varchar extends SealedDataType

 /** Boolean. Maps to Spark's `BooleanType`, Trino's `BOOLEAN`,
 * Databricks' `BOOLEAN`. */
 case object Boolean extends SealedDataType

 // -- Temporal --

 /** Timestamp with optional timezone. Maps to Spark's
 * `TimestampType`, Trino's `TIMESTAMP WITH TIME ZONE`, Databricks'
 * `TIMESTAMP`. Per the design's "Timestamp uses JVM default
 * timezone" risk: portable models use UTC `Instant`; engines
 * normalize to their declared IANA zone. */
 case object Timestamp extends SealedDataType

 /** Date (no time component). Maps to Spark's `DateType`, Trino's
 * `DATE`, Databricks' `DATE`. */
 case object Date extends SealedDataType

 // -- Decimal --

 /** Fixed-precision decimal. The precision is the total number of
 * digits; scale is the number after the decimal point. Maps to
 * Spark's `DecimalType(p, s)`, Trino's `DECIMAL(p, s)`, Databricks'
 * `DECIMAL(p, s)`. The design's risk #9: "Decimal scale/overflow
 * differs by engine" — engines must report `EngineError.DecimalOverflow`
 * when the result exceeds the declared precision. */
 final case class Decimal(precision: Int, scale: Int) extends SealedDataType

 // -- Nested --

 /** Array of elements of the given type. The element type is
 * "version-gated" per the design's request policy matrix: not
 * all engines support all element types. Maps to Spark's
 * `ArrayType(elementType)`, Trino's `ARRAY(elementType)`, etc. */
 final case class Array(elementType: SealedDataType) extends SealedDataType

 /** Map from key type to value type. Maps to Spark's `MapType`,
 * Trino's `MAP(keyType, valueType)`, etc. */
 final case class Map(keyType: SealedDataType, valueType: SealedDataType)
  extends SealedDataType

 /** Struct / row type with named fields. Maps to Spark's
 * `StructType`, Trino's `ROW(field1 type1,...)`, etc. */
 final case class Row(fields: Seq[Field]) extends SealedDataType

 // -- Special --

 /** JSON string (untyped / free-form). Maps to Spark's `StringType`
 * (with a JSON convention) or a dedicated JSON type if the engine
 * supports it. */
 case object Json extends SealedDataType

 /** Variable-length binary blob. Maps to Spark's `BinaryType`, Trino's
 * `VARBINARY`, Databricks' `BINARY`. The engine-portable wire
 * format (`RestateCachedRow.T_BINARY`) encodes the bytes as
 * Base64; `ResultValue.BinaryV` carries the raw `Array[Byte]`.
 * Added for binary widening support (review pass #2 finding #2) to support
 * end-to-end binary columns through the cache journal. */
 case object Binary extends SealedDataType
}