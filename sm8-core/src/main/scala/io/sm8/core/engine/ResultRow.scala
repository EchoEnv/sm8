package io.sm8.core.engine

/** Engine-portable result-row ADT \u2014 Phase 2 contract. Mirrors
 * the design doc \u00a74.5.4 "ResultRow" (a single row of query
 * output, with a typed `values` list and a back-reference to
 * the schema).
 *
 * ==Why `values: List[ResultValue]` (not `List[Any`)==
 *
 * Per the v0.3.0 design review's CRITIQUE: `values: List[Any]`
 * violates the \u00a71.3 transitively-serializable invariant.
 * `ResultValue` is the sealed ADT that captures every
 * portable value type (per the design's "null / bool / int /
 * decimal / string / timestamp / date / array / struct /
 * map" list).
 *
 * ==Why the back-reference to `schema`==
 *
 * Conformance tests compare rows with `==`. The `schema` field
 * is part of the equality check (two rows are equal only if
 * BOTH their `values` AND their `schema` match). This catches
 * the "row1 with the right values but wrong schema" bug
 * (per the design's \u00a74.5.4 conformance property).
 *
 * ==Why `extends Product with Serializable`==
 *
 * `ResultRow` flows through cache, audit, MCP. The case
 * class auto-derives `equals`/`hashCode`/`toString` (Product)
 * + Java-serialization round-trip (Serializable). */
final case class ResultRow(
 values: List[ResultValue],
 schema: ResultSchema,
) extends Product with Serializable {

 /** Number of values in the row. Convenience for conformance
 * tests asserting the row length matches the schema field
 * count. */
 def size: Int = values.size

 /** True iff `values.size == schema.fields.size`. Per the
 * design's conformance property: a row is well-formed only
 * if its value count matches its schema's field count. */
 def isWellFormed: Boolean = values.size == schema.fields.size

 /** Look up a value by field name. Returns `None` if the
 * field name doesn't exist in the schema. */
 def get(name: String): Option[ResultValue] = {
 schema.field(name).flatMap { field =>
  val idx = schema.fields.indexOf(field)
  if (idx < 0) None
  else values.lift(idx)
 }
 }
}