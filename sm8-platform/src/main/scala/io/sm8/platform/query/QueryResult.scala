/*
 * SM8 Platform — QueryResult case class.
 *
 * Engine-portable response wrapper for the platform's MCP wire
 * shape. Replaces the Java `QueryResult` record in
 * `semanticdf-platform/.../QueryService.java` (lines 1075-1080)
 * with a Scala 2.13 case class.
 *
 * Per scala-data-driven-refactor-mindset (pure data + Product
 * with Serializable): pure data, no methods. The `Product`
 * provides case-class `equals`/`hashCode`/`toString`/`copy` +
 * pattern-match destructuring. `Serializable` enables Spark
 * closure capture + Restate SDK journal serialization.
 *
 * Per karpathy-guidelines-mindset (Scala 2.13 idiom + match
 * existing style): `final case class`. NOT Scala 3 `enum` or
 * Java `record`.
 *
 * Per scala-impact-analysis-mindset (wire-contract preservation):
 *   - Same field names (model, measures, rows, truncated, rowCount)
 *   - Same field types (String, List<String>, List<List<Object>>,
 *     boolean, long)
 *   - Same access semantics (no defaults, no extra fields)
 *   - Same JSON wire shape (Jackson serializes Scala case-class
 *     constructor params identically to the Java record)
 *
 * Per scala-jvm-safety-mindset: all fields are non-null
 * `String`/`List`/`Object`/`Boolean`/`Long`. The `rows: List[List[Object]]`
 * may contain null entries (per the wire contract — null row
 * entries are allowed in `RestateCachedRow`; see PR-C4b).
 */
package io.sm8.platform.query

/**
 * Engine-portable response wrapper for the platform's MCP wire
 * shape.
 *
 * Constructs one at the end of the engine-portable path; the
 * outer `Restate SDK` (or MCP server) serializes this to JSON
 * for the MCP client.
 *
 * ==Field semantics==
 *
 *   - `model` — the model name, or `"unknown"` if the caller
 *     passed `null` (per the legacy Java convention)
 *   - `measures` — the column names from the cache row
 *     (same as `RestateCachedRow.fieldNames`)
 *   - `rows` — the decoded cells, one `List[Object]` per row
 *     (null row entries become `List.empty[Object]`, per the
 *     PR-C4b contract)
 *   - `truncated` — `true` if `rows.size >= maxRows` (the env-
 *     var-aware cap from the legacy Java `CacheBridge.effectiveMaxRows`,
 *     defaulting to `100_000` per `CacheBridge.DefaultMaxRows`)
 *   - `rowCount` — `rows.size.toLong` (matches the legacy
 *     `rowCount` field's long type)
 */
final case class QueryResult(
    model:      String,
    measures:   List[String],
    rows:       List[List[Object]],
    truncated:  Boolean,
    rowCount:   Long
) extends Product with Serializable

object QueryResult {

  /** Convert a journaled `RestateCachedRow` back to a wire
    * `QueryResult`.
    *
    * Relocated from `CachedRowDecoder.toQueryResultFromJournaled`
    * during cache-rehome Phase 1: the decoder is engine-portable
    * (io.sm8.core.cache) and must not know the platform wire DTO;
    * this conversion is platform-side by definition (it produces
    * THIS type).
    *
    * @param modelName the model name (or `null` for "unknown")
    * @param journaled the cached row to decode
    * @param maxRows   the truncation cap (default 100_000, matching
    *                 the legacy `CacheBridge.DefaultMaxRows`)
    */
  def fromJournaled(
      modelName: String,
      journaled: io.sm8.core.cache.RestateCachedRow,
      maxRows:   Int = 100_000
  ): QueryResult = {
    val rows = io.sm8.core.cache.CachedRowDecoder.fromRestateCachedRow(journaled)
    QueryResult(
      model     = if (modelName == null) "unknown" else modelName,
      measures  = journaled.fieldNames,
      rows      = rows,
      // ADR-009-e follow-up (P3): OR the engine-set `journaled.truncated`
      // (set when the engine capped the result on the driver collect path)
      // with the local display-cap heuristic. The OR is conservative —
      // either signal flips truncated=true. Fixes the silent-truncation
      // class this ADR exists to eliminate on the journal→wire boundary.
      truncated = journaled.truncated || rows.size >= maxRows,
      rowCount  = rows.size.toLong
    )
  }
}