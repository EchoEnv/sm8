/*
 * SM8 Platform — EngineService (engine-portable path).
 *
 * Scala 2.13 restructure of the Java `QueryService.runQueryViaEngineRegistry`
 * (semanticdf-platform lines 420-579). PR-C5a ships the BUILD +
 * SELECT segments (lines 425-493): the conversion from the wire
 * DTO to `QueryRequest`, and the engine selection.
 *
 * ==JVM-safety bug fix (the headline change)==
 *
 * The legacy Java code used `EngineProvider[] providerHolder =
 * new EngineProvider[1]` as a mutable cell to escape the
 * `Either[EngineError, EngineProvider]` out of the
 * `engineRegistry.select(...)` call:
 *
 *   EngineProvider[] providerHolder = new EngineProvider[1];
 *   Either<...> selectResult = engineRegistry.select(name);
 *   if (selectResult.isRight()) {
 *     providerHolder[0] = selectResult.right().get();  // ← escape hatch
 *   }
 *   if (providerHolder[0] == null) { throw new IAE(...); }
 *
 * Per scala-jvm-safety-mindset: this is a primitive-array-as-
 * mutable-cell pattern — a Java idiom for "I don't have a
 * monad-handling context, so I'll use a 1-element array to escape
 * the value." The Scala 2.13 equivalent is a direct `for`-
 * comprehension or `match` on the `Either`:
 *
 *   val selectResult: Either[EngineError, EngineProvider] =
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
 * path. Per scala-error-handling-mindset "errors are data".
 *
 * Per scala-data-driven-refactor-mindset (sealed-trait
 * dispatch + MatchError-free): the `buildMCPRequest` helper
 * pattern-matches on `Option` (Scala native) — no Map-based
 * dispatch.
 *
 * Per scala-impact-analysis-mindset: 0 callers in our reactor
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
  EngineProvider,
  EngineRegistry,
  PortableQueryResult
}
import io.sm8.core.engine.{ QueryRequest => CoreQueryRequest }
import io.sm8.core.model.{FilterSpec, Model}
import io.sm8.core.engine.EngineContext
import io.sm8.core.engine.{ EngineHookRequest, EngineHookResult }
import io.sm8.platform.query.hooks.EngineHookDispatcher
import io.sm8.sdk.{Context, PipelineStage}

import scala.util.control.NonFatal
/**
 * Engine-portable path entry point. PR-C5a ships the engine
 * selection + QueryRequest build. The cache + execute segments
 * land in PR-C5b (reuses `CachedRowDecoder` + `PortableCellCodec`
 * from previous PRs).
 */
object EngineService {

  /** ADR-009-e: server-side materialization cap (deployment policy,
   * RFC §3), in rows. The engine (connector) enforces this as the
   * default cap on the driver `collect()` when a query passes no
   * `request.limit`, flagging the result `truncated`. This value is
   * deployment-side: callers cannot trip it off — a query's
   * `request.limit` may only NARROW it (min), never widen it.
   *
   * Per ADR-009-e follow-up (P2 — review): this is the SINGLE
   * policy constant. The connector (`SparkEngineProvider.DefaultResultCapRows`)
   * mirrors it as its enforcement default. Threading from platform
   * into connector construction is explicitly deferred (no
   * `EngineContext.maxRowsPolicy`); the drift-guard test
   * (`CapConstantEqualitySpec`) fails the build if the two diverge. */
  val DefaultResultCapRows: Long = 1_000_000L
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
   * Build a `QueryRequest` from the platform's wire
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
  def buildMCPRequest(request: io.sm8.platform.query.QueryRequest): CoreQueryRequest = {
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
    CoreQueryRequest(
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
      registry: EngineRegistry
  ): Either[EngineError, EngineProvider] = {
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
   * Per scala-error-handling-mindset: catch at the IO boundary
   * (this IS the IO boundary for the engine adapter), convert
   * to the typed `EngineError`. The caller (PR-C6) handles the
   * `Either` at the Restate boundary.
   *
   * Defaults `ctx` to `EngineContext.defaultContext` — callers
   * that need a custom context (e.g. for tracing) pass it
   * explicitly.
   *
   * @param ctx          the engine context (defaults to
   *                 `EngineContext.defaultContext`)
   * @return         `Right(pqr)` on success; `Left(error)` on
   *                 engine error or runtime exception
   */
  def executeEngine(
      model: Model,
      mcpReq: CoreQueryRequest,
      provider: EngineProvider,
      ctx: EngineContext = EngineContext.defaultContext
  ): Either[EngineError, PortableQueryResult] = {
    try {
      provider.query(model, mcpReq, ctx)
    } catch {
      // Per scala-error-handling: convert at the IO boundary.
      // Legacy code threw `IllegalArgumentException` here,
      // losing the typed error. The Scala version preserves it
      // via `EngineError.ProviderInvocationFailed` — the closest
      // variant for "engine execution failed unexpectedly".
      //
      // Only `NonFatal` is converted: an `Error` (OOM, ...) must
      // propagate so a fatally-broken JVM fails loud, and an
      // `InterruptedException` re-sets the thread's interrupt flag
      // first so the cancellation is never lost (P1-S2).
      //
      // Per review pass #2 (DE-reviewer MAJOR #7): the `engine`
      // field in `EngineError` is the engine identity (e.g.
      // "spark", "trino"), not the model name. `EngineError`'s
      // `toErrorDetail` formats `"<engine>"` as the error
      // context, and downstream consumers filter errors by
      // engine identity.
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        Left(EngineError.ProviderInvocationFailed(
          engine = provider.identity.name,
          name = provider.identity.name,
          reason = e.getClass.getSimpleName,
          message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
        ))
      case NonFatal(e) =>
        Left(EngineError.ProviderInvocationFailed(
          engine = provider.identity.name,
          name = provider.identity.name,
          reason = e.getClass.getSimpleName,
          message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)
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
   * ADR-009-e: `truncated` is forwarded VERBATIM from the engine-
   * portable result. The engine (connector) applies the server-side
   * cap and flags the result; the platform no longer hardcodes
   * `false` (the old "engine applies the cap upstream" deferral).
   * The CLI renders `(TRUNCATED)` from this platform JSON — the
   * consumer already exists; the producer now forwards.
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
      truncated = portable.truncated,
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
   * - The typed `EngineHookRequest` carries the `Model` + `QueryRequest`
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
      request:    io.sm8.platform.query.QueryRequest,
      model:      Model,
      registry:   EngineRegistry,
      cache:      ResultCache,
      dispatcher: EngineHookDispatcher
  ): Either[EngineError, QueryResult] = {
    val mcpReq: CoreQueryRequest = buildMCPRequest(request)
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
      // P1-S3: previously `case other => return Left(...)` used a
      // non-local return inside the closure — it only worked because
      // the dispatcher runs this thunk synchronously on the same
      // thread; it would silently misbehave on any other thread.
      // Compose via a typed Either + flatMap instead.
      val hookReqE: Either[EngineError, EngineHookRequest] = ctx.request match {
        case hookReq: EngineHookRequest => Right(hookReq)
        case other =>
          Left(EngineError.ProviderInvocationFailed(
            engine = "<dispatcher>",
            name   = "EngineHookDispatcher",
            reason = "UnexpectedRequestType",
            message =
              s"sm8: Context.request must be EngineHookRequest, got ${other.getClass.getName}"
          ))
      }
      hookReqE.flatMap { hookReq =>
        // Per-query decision oracle: the post-PreExecute Context.meta
        // carries the broadcast + skew arm decisions (and the
        // broadcast byte-gate threshold) from any registered plugin's
        // hook. We fold them into a typed DecisionHints and pass as
        // the 4th arg to executeEngine so the engine's seed helpers
        // see eCtx.decisionHints instead of reading context.meta
        // strings. None on each field means "no oracle; the adapter
        // uses its inline fallback". The fold is naturally gated by
        // "we only build decisionCtx when the executor fires" (a
        // throwing oracle short-circuits the dispatcher before this
        // thunk runs).
        val decisionCtx: io.sm8.core.engine.EngineContext =
          io.sm8.core.engine.EngineContext.defaultContext.copy(
            decisionHints = Some(io.sm8.core.engine.DecisionHints(
              broadcastArmed          = ctx.meta.get("sm8.broadcast.arm").collect { case b: Boolean => b },
              skewArmed               = ctx.meta.get("sm8.skew.arm").collect { case b: Boolean => b },
              broadcastThresholdBytes = ctx.meta.get("sm8.broadcast.thresholdBytes").collect { case l: Long => l }
            ))
          )
        for {
          provider <- selectEngine(model, request, registry)
          pqr      <- executeEngine(model, hookReq.mcpRequest, provider, decisionCtx)
        } yield ctx.copy(result = Some(EngineHookResult(pqr)))
      }
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
