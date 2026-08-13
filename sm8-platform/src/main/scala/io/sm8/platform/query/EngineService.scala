/*
 * SM8 Platform — EngineService (engine-portable path).
 *
 * Scala 2.13 restructure of the Java `QueryService.runQueryViaEngineRegistry`
 * (semanticdf-platform lines 420-579). PR-C5a ships the BUILD +
 * SELECT segments (lines 425-493): the conversion from the wire
 * DTO to `MCPQueryRequest`, and the engine selection.
 *
 * ==JVM-safety bug fix (the headline change)==
 *
 * The legacy Java code used `MCPEngineProvider[] providerHolder =
 * new MCPEngineProvider[1]` as a mutable cell to escape the
 * `Either[EngineError, MCPEngineProvider]` out of the
 * `engineRegistry.select(...)` call:
 *
 *   MCPEngineProvider[] providerHolder = new MCPEngineProvider[1];
 *   Either<...> selectResult = engineRegistry.select(name);
 *   if (selectResult.isRight()) {
 *     providerHolder[0] = selectResult.right().get();  // ← escape hatch
 *   }
 *   if (providerHolder[0] == null) { throw new IAE(...); }
 *
 * Per [[scala-jvm-safety-mindset]]: this is a primitive-array-as-
 * mutable-cell pattern — a Java idiom for "I don't have a
 * monad-handling context, so I'll use a 1-element array to escape
 * the value." The Scala 2.13 equivalent is a direct `for`-
 * comprehension or `match` on the `Either`:
 *
 *   val selectResult: Either[EngineError, MCPEngineProvider] =
 *     registry.select(name)
 *
 * No array, no index, no null-check. The `Either` value flows
 * through the rest of the code as a typed value.
 *
 * ==Error-handling refactor==
 *
 * The legacy Java code threw `IllegalArgumentException` at the
 * boundary when the `Either` was `Left(...)`, losing the typed
 * `EngineError` info. The Scala version returns the `Either`
 * unchanged — the caller (PR-C5b's wrapper) handles the error
 * path. Per [[scala-error-handling-mindset]] "errors are data".
 *
 * Per [[scala-data-driven-refactor-mindset]] (sealed-trait
 * dispatch + MatchError-free): the `buildMCPRequest` helper
 * pattern-matches on `Option` (Scala native) — no Map-based
 * dispatch.
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers in our reactor
 * (the legacy `QueryService.runQueryViaEngineRegistry` stays in
 * `/tmp/semanticdf` for later migration PRs). PR-C5a ships the
 * engine-selection + MCP-request build; the cache + execute
 * segments land in PR-C5b.
 */
package io.sm8.platform.query

import io.sm8.core.engine.{
  EngineError,
  MCPEngineProvider,
  MCPEngineRegistry,
  MCPQueryRequest
}
import io.sm8.core.model.{FilterSpec, Model}

/**
 * Engine-portable path entry point. PR-C5a ships the engine
 * selection + MCPQueryRequest build. The cache + execute segments
 * land in PR-C5b (reuses `CachedRowDecoder` + `PortableCellCodec`
 * from previous PRs).
 */
object EngineService {

  /**
   * Match Java 11+ `String.isBlank()` semantics.
   *
   * The legacy Java code uses `request.where().isBlank()` (and
   * `request.engine().isBlank()` indirectly). Scala 2.13's
   * `String.isBlank` (via `StringLike.isBlank = forall(Character.isWhitespace)`)
   * is NOT equivalent to Java's `String.isBlank()` — Java's method
   * also accepts NBSP (U+00A0), FIGURE SPACE (U+2007), and
   * NARROW NO-BREAK SPACE (U+202F), which
   * `Character.isWhitespace` excludes.
   *
   * Replicates Java's `isBlank()` semantics by listing those three
   * codepoints explicitly. The legacy wire contract is preserved
   * (verified by the senior data engineer review for PR-C5a).
   */
  private val BlankPattern = java.util.regex.Pattern.compile(
    """^[\s\u00A0\u202F\u202F]*$""",
    java.util.regex.Pattern.UNICODE_CHARACTER_CLASS
  )

  /** Match Java 11+ `String.isBlank()` semantics. The legacy code uses
    * `request.where().isBlank()`; Scala 2.13`s `s.isBlank` does NOT match
    * (NBSP / FIGURE SPACE / NARROW NO-BREAK SPACE excluded). */
  private def isBlankLikeJava(s: String): Boolean = {
    BlankPattern.matcher(s).matches()
  }

  /**
   * Build an `MCPQueryRequest` from the platform's wire
   * `QueryRequest` (the Scala case class added in this PR).
   *
   * Handles:
   *   - `null` dimensions/measures → empty `Seq`
   *   - `null` or blank `where` → `None`
   *   - typed `filters` — empty list (the platform's wire DTO has
   *     raw SQL `where`, not typed AST filters; converting to
   *     typed FilterSpecs is deferred to follow-up work per
   *     the design doc)
   *
   * @param request the wire DTO from the platform's REST entry
   * @return        the engine-portable request shape
   */
  def buildMCPRequest(request: QueryRequest): MCPQueryRequest = {
    // The Scala `QueryRequest` (defined in this PR) has Scala
    // `List[String]` fields — no Java→Scala conversion needed.
    // (The legacy Java record's `List<String>` fields required
    // `JavaConverters.asScalaBuffer(...).toList()`; that path
    // is gone.)
    val dimensions: Seq[String] =
      Option(request.dimensions).map(_.toSeq).getOrElse(Seq.empty)
    val measures: Seq[String] =
      Option(request.measures).map(_.toSeq).getOrElse(Seq.empty)
    val where: Option[String] =
      Option(request.where).filter(s => !isBlankLikeJava(s))
    val filters: List[FilterSpec] = Nil
    MCPQueryRequest(
      model     = request.modelName,
      dimensions = dimensions,
      measures   = measures,
      limit      = None,
      timeGrain  = None,
      timeRange  = None,
      where      = where,
      filters    = filters
    )
  }

  /**
   * Select the engine provider for a query.
   *
   * Replaces the legacy Java `providerHolder[0]` array trick
   * (JVM-safety bug fix) with a direct `Either` return:
   *
   *   - `request.engine` non-blank → select by name
   *   - `request.engine` null/blank → use registry's default
   *
   * Returns `Left(EngineError.EngineUnavailable)` if the engine
   * is not registered OR is registered but unavailable. The
   * caller (PR-C5b's wrapper) handles the error path.
   *
   * @param model     the engine-portable model (used for the
   *                  cache key + future validation; not used in
   *                  PR-C5a)
   * @param request   the wire DTO
   * @param registry  the engine-portable registry
   * @return          `Right(provider)` on success; `Left(error)` on
   *                  unavailable
   */
  def selectEngine(
      model: Model,
      request: QueryRequest,
      registry: MCPEngineRegistry
  ): Either[EngineError, MCPEngineProvider] = {
    // TODO(C5b): use `model` for `provider.query(model, mcpReq, ctx)`.
    // PR-C5a doesn't consume the model — selection is purely
    // registry-driven. The parameter is reserved for the cache +
    // execute path that lands in PR-C5b.
    val engineName: String =
      Option(request.engine)
        .filter(s => !isBlankLikeJava(s))
        .getOrElse(registry.defaultEngine)
    registry.select(engineName)
  }
}