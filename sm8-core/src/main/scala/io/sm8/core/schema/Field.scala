/*
 * SM8 Core — Field case class.
 *
 * Engine-portable field (name + SealedDataType + nullable).
 * Pure data per [[scala-data-driven-refactor-mindset]] (no
 * behavior; equality auto-derived; `Product with Serializable`
 * for Java-serialization round-trip).
 *
 * Scala 2.13 idiom): `final case class` + companion with
 * `nonNull`/`nullable` sugar factories. NOT Scala 3 `enum`.
 *
 * No SDK type changes. PR-C1 (sm8-platform) and PR-C3
 * (`toQueryResultFromPortable`) consume this.
 */
package io.sm8.core.schema

/** Engine-portable field —
 * Phase 2 contract. Mirrors the design doc §12 "Field".
 *
 * A `Field` is the engine-portable name of a column with its
 * portable data type and nullability. It is the contract that flows
 * through the portable model after source resolution, through the
 * MCP wire format (`describe_model.data.fields`), and through every
 * engine adapter's type-mapping.
 *
 * ==Why a case class (not a tuple)==
 *
 * The three fields (name, dataType, nullable) have specific
 * semantics that benefit from a named type. A `(String, SealedDataType,
 * Boolean)` tuple is correct but loses the name labels.
 *
 * ==Why core (engine-portable)==
 *
 * The field SHAPE is universal across engines (name + type +
 * nullability). The engine-specific implementation (Spark's
 * `StructField`, Trino's `Type`, Databricks' `DataType`) is the
 * data the engine resolver USES to produce this shape. Per
 * scala-data-driven-refactor, the shape lives in core; the
 * engine-specific data lives in the engine adapter.
 *
 * ==Data-driven mantra compliance==
 *
 * - Pure data: `final case class` (no behavior)
 * - Equality auto-derived (case class)
 * - `Product with Serializable` for Java-serialization round-trip
 * - Field names are wire-stable (per the design's "wire-stable
 * string contract"): renaming a field in a model is a breaking
 * change to consumers
 *
 * ==Boundary contract==
 *
 * Zero Spark imports. Verifiable by:
 * `grep -r 'org.apache.spark' sm8-core/src/main/scala/io/sm8/core/schema/Field.scala`
 *
 * ==Naming convention==
 *
 * Field names follow SQL convention (lowercase, underscores). The
 * design doesn't enforce this (case-sensitive matching); engine
 * adapters lowercase on their side if needed. Field names flow
 * through the MCP wire format unchanged — case is significant
 * unless the engine says otherwise.
 */
final case class Field(
 name:  String,
 dataType: SealedDataType,
 nullable: Boolean,
) extends Product with Serializable

object Field {

 /** A non-nullable field. Sugar for `Field(name, dataType, nullable = false)`. */
 def nonNull(name: String, dataType: SealedDataType): Field =
 Field(name, dataType, nullable = false)

 /** A nullable field. Sugar for `Field(name, dataType, nullable = true)`. */
 def nullable(name: String, dataType: SealedDataType): Field =
 Field(name, dataType, nullable = true)
}