/*
 * SM8 Platform — CacheBridge (cache-key derivation).
 *
 * Replaces the legacy Java `io.semanticdf.cache.CacheBridge`
 * (semanticdf-platform) with a Scala 2.13 object. The
 * `platformCacheKey(...)` method produces a deterministic SHA-256
 * hash over the full request shape, so two equivalent queries
 * produce the same key (matches the legacy semantics).
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct core +
 * Scala 2.13 idiom + match existing style): `object` (not class)
 * with `def`s. No state. No deps beyond JDK's `MessageDigest`.
 * Matches the pattern set by `MCPEngineRegistry` and
 * `MCPEngineProvider` (PR-C0c).
 *
 * Per [[scala-jvm-safety-mindset]]: deterministic across JVM
 * restarts (SHA-256, not `hashCode` which is JVM-instance-specific).
 *
 * Per [[scala-impact-analysis-mindset]]: pure additive. 0 callers
 * in our reactor today (PR-C5b-ext-β will wire `EngineService.runQuery`
 * to use it). Legacy stays in `/tmp/semanticdf`.
 *
 * Per [[scala-jar-packaging-mindset]]: no new Maven deps. JDK's
 * `java.security.MessageDigest` is part of the standard library.
 *
 * ==Length-prefix encoding (collision-safe)==
 *
 * The legacy used a delimiter-only encoding (`|`, `,`) which is
 * collision-prone: `List("a,b")` and `List("a", "b")` both encode
 * as `"a,b"`. The senior data engineer's review of PR-C5b-ext-β
 * flagged this as silent wrong-data. Per
 * [[scala-data-driven-refactor-mindset]] "data is data", the
 * canonical form is a length-prefixed encoding — each field is
 * tagged with its byte length, so any delimiter ambiguity is
 * resolved.
 */
package io.sm8.platform.query.cache

import java.security.MessageDigest

/**
 * Cache-key derivation for the engine-portable path.
 *
 * The cache key is a SHA-256 hex digest of the full request
 * shape (engine identity, model, model-version, measures,
 * dimensions, where). SHA-256 ensures cross-JVM determinism
 * (unlike `hashCode`, which is per-JVM-instance). Two equivalent
 * queries produce the same key, so cache hits work across
 * restarts and replicas.
 *
 * ==Engine identity (cross-engine cache isolation)==
 *
 * Per the legacy v0.3.0 DE finding 11: "Spark request and Trino
 * request for the same model share a cache entry" was a bug.
 * The engine name is part of the key so a Trino-evaluated
 * result cannot satisfy a Spark-evaluated request. PR-C5b-ext-β
 * passes the engine from `request.engine` (or the registry's
 * default when blank).
 */
object CacheBridge {

  /**
   * Compute the platform cache key for a given request shape.
   *
   * Mirrors the legacy `CacheBridge.platformCacheKey(modelName,
   * version, measures, dimensions, where)` (semanticdf-platform)
   * plus the engine name (added per the senior data engineer's
   * review of PR-C5b-ext-β to fix the cross-engine cache
   * isolation bug).
   *
   * SHA-256 over a length-prefixed concatenation of the fields.
   * Per [[scala-data-driven-refactor-mindset]] "data is data":
   * the canonical form must be a bijection between request-shape
   * and key (no two distinct request shapes may share a key). A
   * delimiter-only encoding admits `List("a,b") == List("a","b")`
   * collisions; length-prefixing resolves this.
   *
   * Empty fields: `null` modelName → `"null"` segment (per the
   * legacy's behavior); `null` where → empty segment.
   *
   * @param engine     the engine identity (e.g. "spark", "trino")
   * @param modelName  the model name (e.g. "flights")
   * @param version    a model-version surrogate (the legacy uses
   *                   `model.hashCode()`; for v0.3.1 we don't have
   *                   a real version field)
   * @param measures   the query's measure columns
   * @param dimensions the query's dimension columns
   * @param where      the raw-SQL `where` filter (None for no filter)
   * @return           a SHA-256 hex string (64 chars lowercase)
   */
  def platformCacheKey(
      engine: String,
      modelName: String,
      version: Int,
      measures: List[String],
      dimensions: List[String],
      where: Option[String]
  ): String = {
    // Length-prefix every field, concatenate, then length-prefix
    // the whole blob. The outer length-prefix disambiguates
    // concatenations: `("a","b","c")` and `("ab","c")` produce
    // different outer lengths.
    val payload: String = lengthPrefixed(
      lengthPrefixed(engine) +
      lengthPrefixed(modelName) +
      lengthPrefixed(version.toString) +
      lengthPrefixedSeq(measures) +
      lengthPrefixedSeq(dimensions) +
      lengthPrefixed(where.getOrElse(""))
    )
    val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    digest.digest(payload.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  /**
   * Length-prefix a field: `len:value`. The len is the byte length
   * of the value (UTF-8). Two distinct values never produce the
   * same prefix-encoded string.
   */
  private def lengthPrefixed(value: String): String = {
    val bytes = value.getBytes("UTF-8")
    bytes.length.toString + ":" + value
  }

  /**
   * Length-prefix a list: `len(item1_len:item1,item2_len:item2,...)`.
   * Distinguishes `List("a", "b")` from `List("a,b")` and from
   * `List("ab")` (each list element is itself length-prefixed).
   */
  private def lengthPrefixedSeq(xs: List[String]): String = {
    val items = xs.map(lengthPrefixed)
    items.length.toString + ":" + items.mkString(",")
  }
}