/*
 * SM8 Platform — QueryRequest wire DTO.
 *
 * Engine-portable wire DTO for the platform's REST entry point.
 * Replaces the Java `QueryRequest` record in
 * `semanticdf-platform/.../QueryService.java` (lines 1063-1072)
 * with a Scala 2.13 case class.
 *
 * Per karpathy-guidelines-mindset (Scala 2.13 idiom + match
 * existing style): `final case class`. NOT Scala 3 `enum` or Java
 * `record`. The wire shape is preserved: same 5 fields, same
 * types, same JSON serialization (Jackson maps case-class
 * constructor params identically to Java record components).
 *
 * Per scala-data-driven-refactor-mindset (pure data): no
 * methods beyond the auto-derived ones (equals, hashCode, copy,
 * toString, productElement, productArity). `Product with Serializable`
 * for transport + cache round-trip.
 *
 * Per scala-impact-analysis-mindset: 0 callers in our reactor
 * (the legacy `QueryRequest` record in `QueryService.java` lives
 * in `/tmp/semanticdf`, untouched). This case class is the
 * canonical form going forward; PR-C5+ will route the legacy
 * callers through it.
 *
 * Per scala-jvm-safety-mindset: all fields are `String` or
 * `List[String]` — no null at the field level; `Option` is
 * applied at the BUILD boundary (PR-C5a's `buildMCPRequest`).
 */
package io.sm8.platform.query

/**
 * Engine-portable wire DTO for the platform's query entry point.
 *
 * Consumers (sm8-core's `QueryRequest` builder, the Restate
 * handler layer) adapt to this shape. The `engine` field is
 * optional (empty string = use registry default).
 *
 * ==Wire format (must match the legacy Java record)==
 *
 *   {
 *     "modelName":   "flights",
 *     "measures":    ["rows"],
 *     "dimensions":  ["carrier"],
 *     "where":       "carrier = 'AA'",
 *     "engine":      ""           // or "spark", "trino", etc.
 *   }
 */
final case class QueryRequest(
    modelName:  String,
    measures:   List[String],
    dimensions: List[String],
    where:      String,
    engine:     String
) extends Product with Serializable