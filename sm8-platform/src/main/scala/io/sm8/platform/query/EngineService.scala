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

import io.sm8.core.cache._
import io.sm8.platform.query.cache.CacheBridge
import io.sm8.core.cache._
import io.sm8.platform.query.cache.CacheBridge
import io.sm8.core.engine.{
  EngineError,
  MCPEngineProvider,
  MCPEngineRegistry,
  MCPQueryRequest,
  PortableQueryResult
}
import io.sm8.core.model.{FilterSpec, Model}
import io.sm8.core.engine.EngineContext
import io.sm8.core.engine.{ EngineHookRequest, EngineHookResult }
import io.sm8.platform.query.hooks.EngineHookDispatcher
 import io.sm8.sdk.{Context, PipelineStage}
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
        //
        // Per review pass #2 (DE-reviewer MAJOR #7): the `engine`
        // field in `EngineError` is the engine identity (e.g.
        // "spark", "trino"), not the model name. `EngineError`'s
        // `toErrorDetail` formats `"<engine>"` as the error
        // context, and downstream consumers filter errors by
        // engine identity. Setting it to `model.name` broke
        // error reporting — corrected to `provider.identity.name`.
        Left(EngineError.ProviderInvocationFailed(
          engine = provider.identity.name,
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
   * Hook-aware engine-portable entry-point.
   *
   * Same flow as `runQueryWithHooks` but with a Plugin-Context dispatch step
   * around the engine call. This makes RFC §13's
   * own plugin" DoD testable for the first time: any PreExecute hook
   * registered via `engine.hooks.registerPreHook(HookStage.PreExecute,
   * ...)` fires on every cache-MISS; any PostExecute hook fires after
   * engine execution; `context.stop = true` short-circuits the engine
   * call while still firing observers on the post side.
   *
   * ==Efficient by construction (sm8-implementation-rules rule 2)==
   *
   * - Allocates exactly ONE Context case-class snapshot on entry
   *   (the initial Context carrying the typed `EngineHookRequest`).
   *   Every subsequent state is a `case class .copy(...)` returning a
   *   fresh immutable value — no `var`, no `mutable.*`.
   * - The engine-call thunk is a `Function1[Context, Either[EngineError, Context]]`
   *   captured once per cache-MISS, invoked once.
   * - Priority sort happens once per call (cached inside HookManagerImpl).
   *
   * ==Type-class + data-driven (sm8-implementation-rules rule 1)==
   *
   * - All dispatch via sealed-trait match on `HookStage` — no Map
   *   tables, no `Any` casts.
   * - The typed `EngineHookRequest` carries the `Model` + `MCPQueryRequest`
   *   inside `context.request`; plugins with `engine.hooks.registerPreHook`
   *   can cast `context.request.asInstanceOf[EngineHookRequest]` to read
   *   typed values (cast at the registered hook boundary — the
   *   dispatcher's public API is typed; the implementation file's
   *   inline `executor` is the only place the cast happens inside
   *   the platform layer).
   *
   * ==RFC alignment==
   *
   * - RFC §6 pipeline: this is the EXECUTE stage with Pre+Post
   *   hooks firing. Parse/Resolve/Format stages stay as no-ops in
   *   the platform layer; the platform delegates the Execute
   *   dispatch to this dispatcher, threading the typed
   *   `EngineHookRequest` through.
   * - RFC §8 priority ranges: pre-hooks at priority 50 (core range)
   *   run first; first-party at 100+ next; community at 900+
   *   last. The dispatcher makes NO opinion on the choice — it
   *   surfaces the SDK's priority order unchanged.
   * - RFC §9 fail-fast: hook throws propagate. The dispatcher's
   *   `run` does NOT catch them — the boundary catch is in
   *   `executeEngine`, which the executor thunk already uses.
   *
   * @param request    the platform's wire DTO
   * @param model      the engine-portable model
   * @param registry   the engine-portable registry
   * @param cache      the result cache
   * @param dispatcher the hook dispatcher (typically built once
   *                   from `engine.hooks` after all plugins have
   *                   registered). `EngineHookDispatcher.NoOp` for
   *                   backward-compat (no hooks fire).
   * @return           `Right(QueryResult)` on success;
   *                   `Left(EngineError)` on engine selection,
   *                   execution, or hook-dispatch failure.
   */
  def runQueryWithHooks(
      request:    QueryRequest,
      model:      Model,
      registry:   MCPEngineRegistry,
      cache:      ResultCache,
      dispatcher: EngineHookDispatcher
  ): Either[EngineError, QueryResult] = {
    val mcpReq: MCPQueryRequest = buildMCPRequest(request)
    val version: Int           = model.version
    val cacheKey: String       = CacheBridge.platformCacheKey(
      engine     = Option(request.engine).filter(s => !isBlankLikeJava(s))
        .getOrElse(registry.defaultEngine),
      modelName  = Option(request.modelName).getOrElse("unknown"),
      version    = version,
      measures   = mcpReq.measures.toList,
      dimensions = mcpReq.dimensions.toList,
      where      = mcpReq.where
    )
    // Build the initial Context once. All subsequent state is
    // `ctx.copy(...)` — immutable, no `var`, no shared mutable state.
    val hookRequest = EngineHookRequest(model, mcpReq, cacheKey)
    val initialCtx: Context = Context(
      stage   = PipelineStage.Execute,
      request = hookRequest,
      result  = None,
      meta    = Map.empty,
      stop    = false
    )
    // -- Executor: pure engine call; cache handled by CachePlugin hook --
    // The cache lookup + populate is no longer inline in the
    // executor; the new io.sm8.plugins.cache.CachePlugin
    // (registered via QueryService.definition's plugins: Seq[Plugin])
    // owns the read-through (PreExecute) and write-through
    // (PostExecute) hooks. On HIT, the PreExecute hook sets
    // context.stop = true and the dispatcher skips this executor;
    // on MISS, the executor runs and writes back via PostExecute.
    val engineExecutor: Context => Either[EngineError, Context] = { ctx =>
      val hookReq = ctx.request match {
        case hookReq: EngineHookRequest => hookReq
        case other =>
          return Left(EngineError.ProviderInvocationFailed(
            engine = "<dispatcher>",
            name   = "EngineHookDispatcher",
            reason = "UnexpectedRequestType",
            message =
              s"sm8: Context.request must be EngineHookRequest, got ${other.getClass.getName}"
          ))
      }
      for {
        provider <- selectEngine(model, request, registry)
        pqr      <- executeEngine(model, hookReq.mcpRequest, provider)
      } yield ctx.copy(result = Some(EngineHookResult(pqr)))
    }
  
    dispatcher
      .run(initialCtx, engineExecutor)
      .flatMap { finalCtx =>
        finalCtx.result match {
          case Some(EngineHookResult(pqr)) =>
            Right(toQueryResultFromPortable(pqr, request))
          case Some(other) =>
            // Per scala-error-handling-mindset: programmer error
            // (the dispatcher's contract is "executor populates
            // result on success"). Surface as a typed EngineError.
            Left(EngineError.ProviderInvocationFailed(
              engine = "<dispatcher>",
              name   = "EngineHookDispatcher",
              reason = "UnexpectedResultType",
              message = s"sm8: dispatcher returned unexpected result type ${other.getClass.getName}"
            ))
          case None =>
            // No result set — also a programmer error (dispatcher
            // contract: pre+post hooks passed but executor didn't
            // populate). Surface as typed EngineError.
            Left(EngineError.ProviderInvocationFailed(
              engine = "<dispatcher>",
              name   = "EngineHookDispatcher",
              reason = "NoResult",
              message = "sm8: dispatcher pipeline completed without executor populating Context.result"
            ))
        }
      }
  }
}
