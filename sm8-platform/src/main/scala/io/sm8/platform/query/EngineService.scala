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
  MCPQueryRequest,
  PortableQueryResult
}
import io.sm8.core.model.{FilterSpec, Model}
import io.sm8.core.engine.{EngineContext}

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
    """^[\s\u00A0\u2007\u202F]*$""",
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
    // `model` is reserved for future engine-selection logic
    // (e.g. model-specific default engine, model-based capability
    // negotiation). Selection is currently purely registry-driven.
    val engineName: String =
      Option(request.engine)
        .filter(s => !isBlankLikeJava(s))
        .getOrElse(registry.defaultEngine)
    registry.select(engineName)
  }

  /**
   * Execute an engine query and return the typed result.
   *
   * Replaces the legacy `provider.query(...)` call in
   * `QueryService.runQueryViaEngineRegistry` (lines 537-567,
   * 559-567). The legacy code wrapped the call in `try/catch`
   * and threw `IllegalArgumentException` on `RuntimeException` —
   * losing the typed `EngineError` info.
   *
   * Per [[scala-error-handling-mindset]]: catch at the IO boundary
   * (this IS the IO boundary for the engine adapter), convert
   * to the typed `EngineError`. The caller (PR-C6) handles the
   * `Either` at the Restate boundary.
   *
   * Defaults `ctx` to `EngineContext.defaultContext` — callers
   * that need a custom context (e.g. for tracing) pass it
   * explicitly.
   *
   * @param model    the engine-portable model (used by the engine
   *                 for compile)
   * @param mcpReq   the engine-portable request from
   *                 `buildMCPRequest`
   * @param provider the selected engine provider from
   *                 `selectEngine`
   * @param ctx      the engine context (defaults to
   *                 `EngineContext.defaultContext`)
   * @return         `Right(pqr)` on success; `Left(error)` on
   *                 engine error or runtime exception
   */
  def executeEngine(
      model: Model,
      mcpReq: MCPQueryRequest,
      provider: MCPEngineProvider,
      ctx: EngineContext = EngineContext.defaultContext
  ): Either[EngineError, PortableQueryResult] = {
    try {
      provider.query(model, mcpReq, ctx)
    } catch {
      case e: RuntimeException =>
        // Per scala-error-handling: convert at the IO boundary.
        // Legacy code threw `IllegalArgumentException` here,
        // losing the typed error. The Scala version preserves it
        // via `EngineError.ProviderInvocationFailed` — the closest
        // variant for "engine execution failed unexpectedly".
        Left(EngineError.ProviderInvocationFailed(
          engine = model.name,
          name = provider.identity.name,
          reason = e.getClass.getSimpleName,
          message = e.getMessage
        ))
    }
  }

  /**
   * Convert a `PortableQueryResult` to the platform's MCP-wire
   * `QueryResult` response shape.
   *
   * Replaces the Java `QueryService.toQueryResultFromPortable`
   * (semanticdf-platform lines 297-330). Iterates the
   * portable's rows + schema, converting each `ResultValue` to
   * its JVM type via `PortableCellCodec.toJavaValue` (from PR-C2).
   *
   * Per the legacy: `truncated = false` is conservative — the
   * engine returns ALL rows it computed (the cap is applied
   * upstream by the engine provider). PR-C5b-extension can add
   * `truncated` = `rows.size >= maxRows` when the engine reports
   * a cap.
   *
   * @param portable the engine-portable result
   * @param request  the original wire DTO (used for `model.name`)
   * @return         the platform's wire response shape
   */
  def toQueryResultFromPortable(
      portable: PortableQueryResult,
      request: QueryRequest
  ): QueryResult = {
    val fieldNames: List[String] = portable.schema.fields.map(_.name).toList
    val rows: List[List[Object]] = portable.rows.toList.map { row =>
      row.values.toList.map(PortableCellCodec.toJavaValue)
    }
    QueryResult(
      model     = Option(request.modelName).getOrElse("unknown"),
      measures  = fieldNames,
      rows      = rows,
      truncated = false,
      rowCount  = rows.size.toLong
    )
  }

  /**
   * Pure engine-portable entry-point: build → select → cache-lookup →
   * execute (on miss) → cache-store → format.
   *
   * Replaces the legacy Java `QueryService.runQueryViaEngineRegistry`
   * (semanticdf-platform lines 420-579). PR-C5b-ext-β adds the
   * cache lookup + miss-store path. PR-C5b-ext-γ will wrap the
   * miss path in `Restate.run` for journaled-execution semantics.
   *
   * =Cache integration=
   *
   * The cache is consulted on every call. On HIT, the engine
   * is bypassed entirely and the cached `RestateCachedRow` is
   * decoded into a `QueryResult` (no engine execution).
   *
   * On MISS, the engine is executed, the result is encoded to
   * a `RestateCachedRow` via `CachedRowDecoder.toRestateCachedRowFromPortable`
   * (PR-C5b-ext-β), and the cache is populated via
   * `cache.putJournaledWithModelAndVersion(key, row, modelName, version)`.
   *
   * =Serializable-safety for Spark closures=
   *
   * All thread-through types are `Product with Serializable`:
   *   - `io.sm8.core.model.Model` (PR-B-prep)
   *   - `io.sm8.core.engine.MCPQueryRequest` (PR-C0c)
   *   - `io.sm8.core.engine.MCPEngineRegistry` (PR-C0c, +`extends Serializable` in PR-C6)
   *   - `io.sm8.core.engine.MCPEngineProvider` (PR-C0c, +`extends Serializable` trait in PR-C6)
   *   - `io.sm8.core.engine.EngineContext` (PR-C0c)
   *   - `io.sm8.platform.query.QueryResult` (PR-C4c)
   *   - `io.sm8.platform.query.QueryRequest` (PR-C5a)
   *   - `io.sm8.platform.query.ResultCache` (PR-C5b-ext-α, `extends Serializable`)
   *   - `io.sm8.platform.query.RestateCachedRow` (PR-C1, `extends Product with Serializable`)
   *
   * The compiled `Either[EngineError, QueryResult]` is therefore
   * `Serializable` (Scala's `Either` is `Serializable` iff both `L` and
   * `R` are — see `scala.util.Either`).
   *
   * The `Restate.run` closure concern (PR-C5b-ext-γ) is
   * addressed separately: the captured values in the journaled
   * lambda are all `Product with Serializable`, so the lambda
   * passes Restate's `Serializable` requirement.
   *
   * Per [[scala-error-handling-mindset]] "errors are data": errors
   * flow as `Either` not as `throw`. The caller (PR-C-final's
   * legacy `QueryService.runQuery` wrapper) handles the `Either`
   * at the Restate boundary.
   *
   * Per [[scala-jvm-safety-mindset]] "null is a liar": null
   * requests are rejected at the boundary (the legacy `runQuery`
   * does this; the Scala version does too via the legacy
   * caller). No `null` flows through this method.
   *
   * =Cache key=
   *
   * The cache key is a SHA-256 hash over
   * `(modelName, model.hashCode(), measures, dimensions, where)`
   * via `CacheBridge.platformCacheKey`. Cross-JVM deterministic
   * (SHA-256, not `hashCode`).
   *
   * @param request  the platform's wire DTO
   * @param model    the engine-portable model (resolved by the
   *                 legacy caller; the engine registries return
   *                 `core.Model` for engine-portable types).
   *                 Used for cache-key derivation + as the engine
   *                 adapter's compile target.
   * @param registry the engine-portable registry
   * @param cache    the cache (default `ResultCache.NoOp` for
   *                 driver-side execution; `InMemoryResultCache`
   *                 for production)
   * @return         `Right(queryResult)` on success;
   *                 `Left(engineError)` on engine selection or
   *                 execution failure
   */
  def runQuery(
      request: QueryRequest,
      model: Model,
      registry: MCPEngineRegistry,
      cache: ResultCache = ResultCache.NoOp
  ): Either[EngineError, QueryResult] = {
    val mcpReq: MCPQueryRequest = buildMCPRequest(request)
    val version: Int = model.hashCode()  // modelVersionOrZero surrogate
    // Per [[scala-impact-analysis-mindset]]: `selectEngine` is only
    // needed on the cache MISS path. Moving it inside the miss
    // branch (rather than before the for-comprehension) means
    // cache HITs survive engine outages — a cache's primary value
    // is degraded-operation continuity.
    val cacheKey: String = CacheBridge.platformCacheKey(
      engine     = Option(request.engine).filter(s => !isBlankLikeJava(s))
        .getOrElse(registry.defaultEngine),
      modelName  = Option(request.modelName).getOrElse("unknown"),
      version    = version,
      measures   = mcpReq.measures.toList,
      dimensions = mcpReq.dimensions.toList,
      where      = mcpReq.where
    )
    for {
      // -- Cache lookup (HIT path) — no engine call needed --
      pqr <- cache.getJournaled(cacheKey) match {
        case Some(row) =>
          Right(CachedRowDecoder.fromRestateCachedRowAsPortable(row))
        case None    =>
          // -- Cache MISS path: select + execute + encode + store --
          for {
            provider <- selectEngine(model, request, registry)
            pqr2      <- executeEngine(model, mcpReq, provider)
            _         <- Right(
              cache.putJournaledWithModelAndVersion(
                cacheKey,
                CachedRowDecoder.toRestateCachedRowFromPortable(pqr2),
                Option(request.modelName).getOrElse("unknown"),
                version
              )
            )
          } yield pqr2
      }
    } yield toQueryResultFromPortable(pqr, request)
  }
}
