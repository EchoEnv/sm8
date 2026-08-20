package io.sm8.core.engine

/** Engine-portable query-result ADT \u2014 Phase 2 contract. Mirrors
 * the design doc \u00a74.5.4 "PortableQueryResult" (the engine-
 * neutral result shape used by MCP, cache, audit).
 *
 * ==Why `rows: Vector[ResultRow]` (not `Iterator[ResultRow]`)==
 *
 * Per the v0.3.0 design review's CRITIQUE 1.3: "Iterator is one-
 * shot runtime state and breaks the \u00a71.3 transitive-serializable
 * invariant". An `Iterator` is mutable runtime state \u2014 it can't
 * be cached, audited, or shipped to a worker. `Vector` is the
 * engine-portable, serializable, cacheable shape.
 *
 * ==Why `extends Product with Serializable`==
 *
 * `PortableQueryResult` is the wire-safe shape. It flows
 * through cache, audit, MCP, REST. The case class auto-derives
 * `equals`/`hashCode`/`toString` (Product) + Java-serialization
 * round-trip (Serializable).
 *
 * ==Why `metadata: Map[String, String]`==
 *
 * Per the design \u00a74.5.4: portable result carries engine-
 * specific metadata (query plan digest, engine version, etc.)
 * as string key-value pairs. The schema-stripped consumer
 * reads only `schema` and `rows`; the engine-aware consumer
 * reads `metadata` for diagnostics. */
final case class PortableQueryResult(
 schema: ResultSchema,
 rows:  Vector[ResultRow],
 metadata: Map[String, String] = Map.empty,
) extends Product with Serializable {

 /** Number of rows. Convenience for MCP / cache / audit. */
 def rowCount: Int = rows.size

 /** True iff no rows. */
 def isEmpty: Boolean = rows.isEmpty

 /** True iff `rows.forall(_.isWellFormed)`. Per the design's
 * conformance property: a result is well-formed only if
 * EVERY row is well-formed. */
 def isWellFormed: Boolean = rows.forall(_.isWellFormed)
}

object PortableQueryResult {

 /** Empty result \u2014 the canonical "zero rows" answer. */
 val empty: PortableQueryResult = PortableQueryResult(
 schema = ResultSchema(Nil),
 rows  = Vector.empty,
 metadata = Map.empty,
 )
}