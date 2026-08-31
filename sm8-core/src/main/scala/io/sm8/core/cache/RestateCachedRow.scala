/*
 * SM8 Platform — Engine-portable cached-row wire format (Scala 2.13).
 *
 * Replaces the legacy Java record in
 * `/tmp/semanticdf/semanticdf-platform/src/main/java/io/semanticdf/platform/query/RestateCachedRow.java`
 * with a Scala `case class` for the `sm8-platform` module
 * (Scala-first per the restructure plan; no Java carried into
 * our reactor).
 *
 * Scala 2.13 idiom + match existing style + smart constructor
 * for validity-at-boundary): `final case class` + companion with
 * `val` T_* tag constants. `require(.)` for null/size validation
 * matches the Java compact constructor's behavior. NOT Scala 3
 * `enum`. NOT a Java record.
 *
 * behavior): the 9 T_* tag constants are pure string constants —
 * no methods, no state.
 *
 * - Same field names (fieldNames, fieldTypes, rows)
 * - Same field types (List[String], List[Array[String]])
 * - Same T_* tag values ("null", "string", "long", "double",
 *  "decimal", "boolean", "timestamp", "date", "binary")
 * - Same validation semantics (non-null fields; matching
 *  fieldNames/fieldTypes size; matching per-row cell count)
 * - Same Serializable behavior (`Product with Serializable`)
 * - Same Jackson wire format (Jackson reads Scala case-class
 *  constructor params by reflection — the JSON shape is
 *  identical to the Java record)
 *
 * - `require(. ne null,.)` rejects null at the boundary
 * - Caller bugs surface as IllegalArgumentException at the
 *  smart constructor, never as a downstream NPE
 *
 * for programmer errors): null/size violations are programmer
 * errors (caller violated the contract) → `require` / `throw IAE`,
 * not `Either`. Expected failures (cache miss, encoding error)
 * use EngineError in higher layers.
 */
package io.sm8.core.cache

/**
 * Purely-data record mirror of an engine-portable cached result
 * for use across a Restate journal boundary.
 *
 * Why a typed `List[Array[String]]` (not `Array[org.apache.spark.sql.Row>`):
 * the Restate SDK's default Jackson serializer can WRITE Spark Rows
 * via `GenericRowWithSchema` but cannot READ them back on journal
 * replay — `Row` is an abstract Spark class with no default
 * constructor. End-to-end tests showed the deserialization throws
 * `InvalidDefinitionException: Cannot construct instance of
 * org.apache.spark.sql.Row`. The fix: each cell is a String-encoded
 * form of the original value; the corresponding `fieldTypes` tag
 * drives `decodeCell(tag, encoded)` back to the typed value on
 * replay. Every Spark cell type round-trips with full type
 * fidelity (no Long→Integer overflow, no BigDecimal→Double
 * precision loss, no Timestamp→epoch Long unit confusion).
 *
 * ==Wire format (must match the legacy Java record)==
 *
 * {
 *  "fieldNames": ["carrier", "rows"],
 *  "fieldTypes": ["string", "long"],
 *  "rows": [["AA", "1234"], ["BB", "5678"]]
 * }
 *
 * @param fieldNames Column names. Must be non-null; size must
 *     equal `fieldTypes.size`.
 * @param fieldTypes Per-column type tags (one of the 9 `T_*`
 *     constants). Must be non-null; size must
 *     equal `fieldNames.size`.
 * @param rows  Encoded cell values, one `Array[String]` per
 *     row. Each cell-array must have exactly
 *     `fieldNames.size` cells (rows may be null
 *     for missing data; rows themselves must not be
 *     null).
 *
 * ==Serialization==
 *
 * The type extends `Product with Serializable`, so Spark's default
 * Java serializer (`spark.serializer =
 * org.apache.spark.serializer.JavaSerializer`) handles it without
 * any registration — closures that capture a `RestateCachedRow`
 * distribute cleanly to executors.
 *
 * If a downstream consumer enables Kryo serialization
 * (`spark.serializer = org.apache.spark.serializer.KryoSerializer`),
 * the sm8-platform + sm8-core schema types below must be added
 * to `spark.kryo.classesToRegister` (or registered via
 * `Kryo.register`):
 *
 * {{{
 * io.sm8.core.schema.SealedDataType
 * io.sm8.core.schema.SealedDataType$BigInt$
 * io.sm8.core.schema.SealedDataType$Int$
 * io.sm8.core.schema.SealedDataType$Double$
 * io.sm8.core.schema.SealedDataType$Varchar$
 * io.sm8.core.schema.SealedDataType$Boolean$
 * io.sm8.core.schema.SealedDataType$Timestamp$
 * io.sm8.core.schema.SealedDataType$Date$
 * io.sm8.core.schema.SealedDataType$Decimal
 * io.sm8.core.schema.SealedDataType$Array
 * io.sm8.core.schema.SealedDataType$Map
 * io.sm8.core.schema.SealedDataType$Row
 * io.sm8.core.schema.SealedDataType$Json$
 * io.sm8.core.schema.Field
 * io.sm8.platform.query.cache.RestateCachedRow
 * }}}
 *
 * The Jackson wire format used by Restate SDK journals is identical
 * to the legacy Java record — verified by
 * `RestateCachedRowSerializationSpec.jacksonWireShapeMatchesLegacyRecord`.
 */
final case class RestateCachedRow(
 fieldNames: List[String],
 fieldTypes: List[String],
 rows:  List[Array[String]],
 /** True when the source `PortableQueryResult` was capped by the
 * engine (ADR-009-e). Preserved through the cache journal so a
 * cache HIT serves the truncated flag verbatim — the "no silent
 * drop" invariant holds on the cache path. Defaulted `false`
 * keeps existing constructor sites compiling unchanged. */
 truncated: Boolean = false
) extends Product with Serializable {

 require(fieldNames ne null, "fieldNames must be non-null")
 require(fieldTypes ne null, "fieldTypes must be non-null")
 require(rows ne null, "rows must be non-null")
 require(
 fieldNames.size == fieldTypes.size,
 s"fieldNames.size (${fieldNames.size}) != fieldTypes.size (${fieldTypes.size})"
 )
 // Row-length validation lives at the encoder (CachedRowDecoder.toRestateCachedRowFromPortable)
 // which is the only non-test constructor caller. This separation keeps the case-class
 // invariant a programmer-error check (null + size) and the runtime-error check
 // (row-length) at the journal boundary as typed-Left per [[scala-error-handling-mindset]].
}

object RestateCachedRow {

 /**
 * Allowed cell-type tags.
 *
 * Deliberately `final val String` (NOT a sealed trait + case
 * objects): these tags appear verbatim in the JSON wire format
 * and must be 1:1 compatible with the legacy Java record's
 * `public static final String` constants. A sealed-trait + case-
 * objects approach would force Jackson `@JsonValue` plumbing
 * and break the wire contract — at zero runtime benefit since
 * the strings are inherently wire-stable, not domain types.
 *
 * Wire vocabulary: 9 tags (T_NULL through T_BINARY). The
 * `EngineTypeTags.of(SealedDataType)` companion helper maps
 * the closed `SealedDataType` ADT to these wire tags with
 * compiler-enforced exhaustiveness on the in-memory side.
 */
 final val T_NULL  = "null"
 final val T_STRING = "string"
 final val T_LONG  = "long"
 final val T_DOUBLE = "double"
 final val T_DECIMAL = "decimal"
 final val T_BOOLEAN = "boolean"
 final val T_TIMESTAMP = "timestamp"
 final val T_DATE  = "date"
 final val T_BINARY = "binary"
}