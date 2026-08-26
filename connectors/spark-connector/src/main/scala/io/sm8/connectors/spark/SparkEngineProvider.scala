/*
 * SM8 Spark Engine Provider - real runtime (Layer C of Step 8 follow-up + PR #41 Model.compile port).
 *
 * Per  mantra #1 ("closures
 * captured by Spark UDFs / lambdas in Dataset.map must avoid
 * non-serializable refs"): this provider captures a SparkSession
 * (which IS Serializable in Spark 3.5 and 4.1 - verified by the
 * PR #36 closure-safety gate at runtime via PluginSerializationSpec).
 * The DataFrame handle captured per query is transient (lives only
 * inside query()); the SparkTypeBridge + PortableExprCompiler are
 * pure object refs.
 *
 * Per  mantra #3 (long-lived state): the
 * captured compiler is created per query (no static / ThreadLocal
 * state). The SparkSession ref is constructor-frozen.
 *
 * Per  mantra #5 (driver vs executor
 * asymmetry): the `compile(model, ctx)` and `collect()` calls
 * both run in the driver process. No driver-side resources leak
 * to executors. ResultRow construction happens in the driver.
 *
 * Per  mantra #2 (isolate the hot path):
 * the compile path (PortableQueryCompiler) and the per-row decode
 * path (decodeRow/decodeCell) are separated so future profiling
 * can attribute cost cleanly.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{
 EngineContext,
 EngineError,
 EngineHookRequest,
 EngineHookResult,
 EngineProvider,
 QueryRequest,
 PortableQueryResult,
 ResultRow,
 ResultSchema,
 ResultValue
}
import io.sm8.core.model.Model
import io.sm8.core.query.QueryBuilder
import io.sm8.core.rel.RelOpPlanPrinter
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.sdk.{Context, HookRunner, PipelineStage}

import org.apache.spark.sql.SparkSession

final class SparkEngineProvider(
 val spark:   SparkSession,
 val bridge:   SparkTypeBridge.type,
 val sparkEngineName: String = "spark-3.5",
 // PR-3b (ADR-008-P §C1): optional hook runner wrapping the execute stage.
 // When `None` (bare-deploy shape), the compile runs directly. When `Some`,
 // PreExecute hooks fire before compile (may set ctx.stop), PostExecute
 // after. Per ADR §C1: uses the existing `HookRunner` SDK Protocol;
 // no new payload types.
 val hookRunner:  Option[HookRunner] = None,
 // ADR-009-e: server-side default row cap (deployment policy,
 // RFC §3). Constructor-frozen — NOT a per-query EngineContext
 // field, NOT caller-trip-able: the caller's request.limit may
 // only NARROW the cap (min), never widen it. Defaults to the
 // connector-level deployment constant; the platform
 // (EngineService) declares its own DefaultResultCapRows as the
 // deployment-side policy value.
 val resultCapRows: Long = SparkEngineProvider.DefaultResultCapRows
) extends EngineProvider {
 // ADR-009-e follow-up (P3): bound the cap below Spark's Int range so the
 // cap+1 probe at the `.limit()` call (which takes Int) never wraps to
 // a negative value. The -1 keeps room for the +1 probe row. A cap of
 // 2_000_000_000 is well above any realistic deployment value (the
 // default is 1_000_000) — exceeding this is a misconfiguration that
 // deserves a loud failure at construction, not a silent negative
 // limit() that confuses the planner later.
 require(
  resultCapRows > 0L && resultCapRows <= Int.MaxValue.toLong - 1L,
  s"SparkEngineProvider.resultCapRows=$resultCapRows is out of Spark limit range (must be 1..${Int.MaxValue.toLong - 1L})"
 )
 // Per-query session design: TEST-ONLY seam to expose the per-query
 // SparkSession from `query()` for falsifiable tests.
 // `private[spark]` = package-private to the connector;
 // not a production API. Set in `query()` before the
 // compileSteps call; cleared at method exit. Tests read
 // `withQuerySessionTL` to assert per-query conf.
 @transient private[spark] val querySessionTL: ThreadLocal[org.apache.spark.sql.SparkSession] =
   new ThreadLocal[org.apache.spark.sql.SparkSession]()
 def withQuerySessionTL(): org.apache.spark.sql.SparkSession = {
   val qs = querySessionTL.get
   assert(qs != null, "querySessionTL not populated; did query() run?")
   qs
 }
 private[spark] def clearQuerySessionTL(): Unit = querySessionTL.remove()
 // Per-query session design: post-query reference for tests that
 // verify the per-query conf AFTER `query()` returns (the
 // `querySessionTL` seam is cleared at exit; this one is not).
 // Set in query()'s production branch; tests read
 // `withLastQuerySession`. Production code never reads it.
 @transient private[spark] val lastQuerySessionTL: ThreadLocal[org.apache.spark.sql.SparkSession] =
   new ThreadLocal[org.apache.spark.sql.SparkSession]()
 def withLastQuerySession(): org.apache.spark.sql.SparkSession = {
   val qs = lastQuerySessionTL.get
   assert(qs != null, "lastQuerySessionTL not populated; did query() run?")
   qs
 }
 /**
 * Typed URL realization (PR-B per RFC `adapters.md` Rule 4).
 *
 * Builds a real `SparkEngineProvider` connected to the given
 * URL via `SparkSession.builder().master(url).getOrCreate()`.
 * The URL accepts any Spark master URL:
 * - classic cluster: `spark://host:7077`
 * - local mode:  `local[*]` (driver-side only)
 * - Spark Connect: `spark-connect://host:port` (Spark 3.4+)
 *
 * Per RFC §3: the connector is the ONLY piece that knows about
 * `SparkSession`. The platform and the deployment module hold
 * only the string.
 *
 * @return `Some(realizedProvider)` on success; `None` if the
 *   URL is blank (per-connector grammar: non-blank
 *   Spark master URL required)
 */
 override def realize(url: String): Option[EngineProvider] =
 if (url == null || url.trim.isEmpty) None
 else Some(new SparkEngineProvider(
  spark   = org.apache.spark.sql.SparkSession.builder().master(url).getOrCreate(),
  bridge   = SparkTypeBridge,
  sparkEngineName = sparkEngineName,
  hookRunner  = None))

 override lazy val identity: io.sm8.core.engine.EngineIdentity =
 io.sm8.core.engine.EngineIdentity(
  name     = sparkEngineName,
  nativeVersion  = if (spark != null) spark.version else "<uninitialized>",
  engineAdapterVersion = "0.1.0"
 )

 /** PR-O4g (ADR-008-O): the null-sentinel no-arg ctor is gone.
 * ServiceLoader discovery now goes through
 * SparkEngineProviderDescriptor. The `available` flag stays
 * `spark != null`-aware as a defensive measure against
 * direct null-injection (the constructor still allows any
 * reference; null just disables the provider).
 */
 override val available: Boolean = spark != null

 /** PR-O4a (ADR-008-O): lifecycle hook — stop the
 * constructor-frozen SparkSession on JVM exit. Idempotent
 * (SparkSession.stop is a no-op after the first call).
 */
 // PR-O4e (ADR-008-O): track every DataFrame we persist() so we
 // can unpersist() them at JVM exit (per the cache-plugin
 // long-lived-model intent). ConcurrentHashMap for thread-safe
 // put/remove with no per-call allocation.
@transient
 private val persistedFrames: java.util.concurrent.ConcurrentHashMap[java.lang.Long, org.apache.spark.sql.Dataset[_]] =
   new java.util.concurrent.ConcurrentHashMap()
 private val persistedSeq: java.util.concurrent.atomic.AtomicLong =
 new java.util.concurrent.atomic.AtomicLong(0L)

 /** PR-O4e: register a persisted DataFrame for paired
 * unpersist-on-shutdown. Returns the unregister-token.
 */
 private[spark] def trackPersist(df: org.apache.spark.sql.Dataset[_]): Long = {
 val tok = persistedSeq.incrementAndGet()
 persistedFrames.put(tok, df)
 tok
 }

 /** PR-O4a + PR-O4e: lifecycle hook -- unpersist every tracked
 * DataFrame, then stop the constructor-frozen SparkSession.
 * Idempotent (SparkSession.stop is a no-op after the first call).
 */
 override def close(): Unit = try {
 import scala.collection.JavaConverters._
 persistedFrames.asScala.foreach { case (_, df) =>
  try df.unpersist()
  catch { case _: Throwable => () }
 }
 persistedFrames.clear()
 if (spark != null) spark.stop()
 } catch {
 case _: Throwable => ()
 }

 /** P1-SM-02: re-initialize `@transient` fields after Java
 * deserialization. `querySessionTL`, `lastQuerySessionTL`, and
 * `persistedFrames` are deliberately `@transient` (a SparkSession
 * must not be held in a ThreadLocal across journal replays), but
 * serialization then leaves them `null`, so a provider restored
 * from bytes NPEs on the first `query()` (`querySessionTL.get`)
 * or `close()` (`persistedFrames.asScala`).
 *
 * Returning a fresh instance carrying the deserialized
 * non-transient constructor state (spark, bridge,
 * sparkEngineName, hookRunner) gives the replacement fully
 * initialized ThreadLocals + persist-map. All per-call state is
 * dropped, which is correct: the per-query session design never
 * shares sessions across calls.
 *
 * @throws java.io.ObjectStreamException if the replacement cannot
 *         be constructed
 * @return a fully-initialized provider sharing the restored
 *         constructor state
 */
@throws[java.io.ObjectStreamException]
private def readResolve(): Object =
 new SparkEngineProvider(spark, bridge, sparkEngineName, hookRunner, resultCapRows)

 // PR-M4 (GAP 5 — the most critical): the IR-extension path
 // (PR-H/I/J/K/L) was inert in production — `query` called
 // `PortableQueryCompiler.compile(model, ctx)` directly, bypassing
 // `QueryBuilder.build` entirely. PR-M4 wires the IR path:
 //
 // 1. Resolve the model's primary source via `SparkSourceResolver`
 //  (PR-M3) -- brings the live `df.schema` into scope.
 // 2. Run `ModelValidator.validateAgainstSchema` (PR-M2) on the
 //  model + resolved schema. Fail-loud typed `SchemaValidation`
 //  on unknown-field references.
 // 3. Run `QueryBuilder.build` (PR-L) to lower Model -> RelOp.
 //  Cycle detection runs here (built-in, no extra wiring).
 // 4. Apply the existing pipeline: compile the RelOp via
 //  `PortableQueryCompiler.compileRelOp` (new in PR-M4), then
 //  request-level `where` + `limit` + `collect` + `decode`.
 //
 // surfaces as a typed `EngineError` -- no silent defaults.
 //
 // PR-M4 (GAP 6): hook dispatch. The dispatcher is OPTIONAL --
 // `None` means no plugin hooks fire (the default for the
 // bare-deploy shape). Production deployments inject a real
 // `EngineHookDispatcher` via the new constructor parameter.
 override def query(
  model: Model,
  request: QueryRequest,
  ctx:  EngineContext): Either[EngineError, PortableQueryResult] = {
 if (spark == null) {
  return Left(EngineError.ConnectionFailed(
  engine = sparkEngineName,
  reason = "SparkSession is null",
  message = "SparkEngineProvider.query called with null SparkSession"))
 }
 // Per-query session design: per-query session. spark.newSession() shares the
 // base SparkContext and forks a fresh SessionState with its own
 // SQLConf. The clone is never .stop()ed (that would tear down the
 // shared SparkContext). The reference goes out of scope and the
 // SessionState is GC-reclaimable.
 val createdQuerySessionHere: Boolean = querySessionTL.get == null
 val querySession: org.apache.spark.sql.SparkSession =
   if (!createdQuerySessionHere) {
     // Test seam: if a test pre-populated `querySessionTL`, reuse it
     // (lets the test register temp views on a session the provider
     // will then query). The TL is NOT cleared on exit in this branch
     // — the test owns the lifecycle (it calls clearQuerySessionTL
     // in its finally).
     querySessionTL.get
   } else {
     val qs = spark.newSession()
     // Per-query session design: copy the parent's temp views to the per-query
     // session so a query that references `createOrReplaceTempView`
     // on the base session (the standard Spark test pattern) still
     // resolves. `spark.newSession()` shares SharedState (persistent
     // tables) but creates a fresh SessionState (temp views). This
     // re-registers the parent's temp views on the clone so the
     // compile resolves ByName refs to the test's views. Driver-side
     // only; never captures executor state.
     SparkEngineProvider.copyTempViews(spark, qs)
     querySessionTL.set(qs)
     // Post-query reference for tests (not cleared at method exit).
     lastQuerySessionTL.set(qs)
     qs
   }
 // 5. PR-M4 (GAP 6): wrap the build+compile step in the bound
 //  dispatcher (deferred -- O3+1).
 // 6. Apply request-level where + limit + collect + decode.
 //
 // The compile steps are factored into a thunk; the for-comp
 // returns the final DataFrame; the dispatching code wraps that
 // thunk.
 val resolver = new SparkSourceResolver(querySession, SparkSourceResolver.SessionCatalogModelRegistry)
 val compileSteps: io.sm8.core.engine.EngineContext => Either[EngineError, org.apache.spark.sql.DataFrame] = { eCtx =>
  // Seed the broadcast byte-threshold from the
  // model's join estimates. When the caller set no explicit
  // `broadcastRightBelowBytes`, a model that declares ANY join
  // `estimatedRows` arms the adapter's broadcast byte gate with the
  // default budget (10 MiB). The estimate is an ARM (presence), not
  // a numeric value: the runtime `sizeInBytes` check in the lowerer
  // remains authoritative, so a large side is never physically
  // broadcast (OOM-safe). Caller/hook-set hints (explicit
  // `broadcastRightBelowBytes`) always win over the seed; the
  // `preferredStrategy` axis is untouched (the (Cross, Some)
  // rejection guard is never triggered). Read from the final eCtx
  // (post-hooks) so a PreExecute hook may still override.
 val effectiveCtx: io.sm8.core.engine.EngineContext =
    SparkEngineProvider.seedBroadcastThreshold(querySession, eCtx, model)
  // Per-query skew factor design: per-query skew factor seed (per-query, single
  // conditional; honors JoinHints.skewFactor when Some). The fresh-session
  // conf has no operator-pre-set value, so a Some(f) sets the per-query
  // AQE factor race-free. None leaves the fresh session at the
  // inherited-from-SparkConf value (or static 5.0 default).
  val skewCtx: io.sm8.core.engine.EngineContext =
    SparkEngineProvider.seedSkewFactor(querySession, effectiveCtx, model)
  // PR-33 (ADR-008-R SSfilterPushdown typed-DSL wire-up): the
  // full pipeline now uses `resolveWithPushdown` (per PR-28) +
  // the canonical `compileRelOp` overload (per PR-32) + the
  // new `TypedQueryCompiler.apply` overload that suppresses
  // the in-memory `whereFiltersOp` when the pre-filtered DF
  // is supplied. This closes the data-engineer SHOULD finding
  // from PR-31 (the duplicate-filter path).
  //
  // change for empty filters): when `request.whereFilters` is
  // Nil, `resolveWithPushdown` falls back to `resolve` +
  // `readSourceDF` (the existing path), and the PR-33
  // TypedQueryCompiler overload's `preFilteredDf` is unused
  // (the whereFilters check is off -- the in-memory filter
  // is a no-op anyway). Net effect: zero behavior change for
  // callers that don't use whereFilters.
  for {
  pushdownResult <- resolver.resolveWithPushdown(
       model.source, request.whereFilters, identity)
  (resolved, preFilteredDf) = pushdownResult
  scan  <- resolved match {
      case s: io.sm8.core.engine.ResolvedSource.Scan =>
      Right[EngineError, io.sm8.core.engine.ResolvedSource.Scan](s)
      case _ => Left(EngineError.UnsupportedCapability(
        engine = sparkEngineName,
        capability = "SourceResolver.resolveWithPushdown",
        message = s"non-Scan resolution for source $model.source: ${resolved.getClass.getSimpleName}"))
     }
  relOp <- QueryBuilder.build(model, resolver, identity)
  // PR-32: canonical `compileRelOp(model, relOp, ctx, scan, preFilteredDf)`
  // overload validates the model against the resolved source's
  // schema BEFORE lowering.
 df0  <- new PortableQueryCompiler(querySession).compileRelOp(
      model, relOp, skewCtx, scan, Some(preFilteredDf))
  // PR-33: the new `TypedQueryCompiler.apply(df, request, ctx,
  // preFilteredDf)` overload SUPPRESSES the in-memory
  // `whereFiltersOp` when the pre-filtered DF is supplied
  // (the filter was already pushed at the source).
 df  <- TypedQueryCompiler(querySession).apply(
      df0, request, skewCtx, Some(preFilteredDf))
  } yield df
 }
 // PR-3b (ADR-008-P §C1): wrap the compileSteps thunk in the optional
 // hook runner. When `hookRunner` is `None` (bare-deploy shape), the
 // compile runs directly (no Pre/Post hooks fire -- preserving the
 // existing behavior). When `Some(runner)`, the runner fires
 // PreExecute hooks before compile (any may set `ctx.stop = true` to
 // short-circuit) and PostExecute hooks after (observability).
 //
 // end-to-end): this single line of wiring is what makes every
 // registered Pre/Post hook actually fire on every spark-connector
 // query. Default `None` preserves the bare-deploy shape; production
 // deployments inject a real `EngineHookDispatcher` (which extends
 // the SDK `HookRunner` Protocol).
 //
 // the runner is stateless; its lifecycle is the caller's (passed
 // in via constructor; closed when this provider is closed).
 //
 // `compileSteps` closure captures only `engine = sparkEngineName`
 // (a String -- Serializable) and `resolver` (a `SparkSourceResolver`
 // created per call -- holds no executor-side state). No
 // SparkSession / DataFrame / HookManager refs cross the closure
 // boundary into the runner.
 //
 // Per ADR-008-P §C1 (cacheKey computation): `CachePlugin` (the only
 // consumer of `cacheKey`) computes its own cacheKey from
 // `EngineHookRequest.model` + `EngineHookRequest.mcpRequest` at
 // hook entry. The spark-connector passes a deterministic default
 // (`model.name | <mcpRequest>`) so the smoke test in
 // `SparkEngineProviderReplaySafetySpec` can verify cache-hit
 // behavior end-to-end. PR-3a will replace this default with
 // `EngineHookRequest.cacheKey = CachePlugin.computeKey(.)`.
 val cacheKey: String = s"${model.name}|${request}"
 // PR-9: schema metadata shared by the HIT (returned via cached
 // PQR) and MISS (built by `applyPostCompilePipeline`) paths.
 // The cached PQR carries its own metadata; the MISS path adds
 // these. 
 // style": same keys as the pre-PR-9 inline `Right(.)` block
 // — engine identity, spark version, IR-path provenance.
 val schemaMetadata: Map[String, String] = Map(
  "engine.id"  -> sparkEngineName,
  "engine.version" -> (if (spark != null) spark.version else "<uninitialized>"),
  "ir.path"  -> "pr-m4")
 // The runner's `execute` callback receives a `Context` (per the
 // HookRunner Protocol). The actual `EngineContext` for the executor
 // lives in `Context.meta("engineContext")` (per RFC §7 scratch space
 // convention); the compiled `DataFrame` flows back via
 // `Context.meta("compiledDf")` on the cache-MISS path. This keeps
 // the Protocol types-only (no Spark types in the HookRunner SDK
 // surface) while preserving the engine-portable execution contract
 // and avoiding the 2x `compileSteps` re-execution that the pre-PR-9
 // code paid on every cache-MISS query.
 //
 // DataFrame is a Spark *driver-side* handle. It is NOT shipped to
 // executors. Stashing it in `Context.meta` is safe because the
 // HookRunner Protocol runs entirely on the driver (the runner does
 // not marshal Context across the wire). For the Restate journal
 // path (PR-C5b-ext-γ), the cache-write PostHook does NOT capture
 // the DataFrame (it stores a `RestateCachedRow` per
 // 
 val initialCtx: Context = Context(
  request = EngineHookRequest(model, request, cacheKey),
  stage = PipelineStage.Execute,
  meta = Map("engineContext" -> ctx))
 // compiled: Either[EngineError, DataFrame]
 // - HIT path (c.stop = true after runner): no compile; use the
 //  runner's `ctx.result` (an EngineHookResult containing the
 //  cached `PortableQueryResult`) — converted to a 1-row "echo"
 //  DataFrame via the EngineService `toQueryResultFromPortable`
 //  boundary, but the compile branch is skipped entirely.
 // - MISS path: compileSteps runs ONCE inside the runner callback;
 //  the compiled DataFrame is stashed in `runCtx.meta("compiledDf")`
 //  and extracted by the outer code below. NO second `compileSteps`
 //  invocation — this is the perf cliff PR-9 closes.
 // The query result is `Either[EngineError, PortableQueryResult]`.
 // Three sub-paths:
 // 1. HIT (runner wired + PreExecute cache-read set c.stop=true +
 //  c.result = Some(cachedPQR)): return the cached PQR directly.
 //  NO compile, NO collect, NO decode. This is the e2e HookRunner
 //  C1 closure (verified by `SparkEngineProviderHookRunnerSpec`).
 // 2. MISS (runner wired): compileSteps runs ONCE inside the
 //  runner callback; the DataFrame is stashed in
 //  `runCtx.meta("compiledDf")`; the outer code extracts it and
 //  runs the where/limit/collect/decode pipeline ONCE.
 //  PR-9 T2-3: this fixes the 2x compileSteps perf cliff.
 // 3. Bare-deploy (no runner): compileSteps runs once directly;
 //  pipeline runs as before. (Pre-PR-9 behavior minus the
 //  unused runner wiring.)
val compiled: Either[EngineError, PortableQueryResult] =
 try {
  hookRunner match {
  case Some(runner) =>
   runner.run(initialCtx, { runCtx =>
   runCtx.meta.get("engineContext") match {
    case Some(eCtx: EngineContext) =>
    compileSteps(eCtx).map { df =>
     runCtx.copy(meta = runCtx.meta + ("compiledDf" -> df))
    }
    case _ =>
    Left(EngineError.UnsupportedCapability(
     engine  = sparkEngineName,
     capability = "SparkEngineProvider.query",
     message = "Context.meta missing 'engineContext' (sm8-internal invariant violated)"))
   }
   }).flatMap { finalCtx =>
   finalCtx.result match {
    case Some(EngineHookResult(cachedPqr)) =>
    Right[EngineError, PortableQueryResult](cachedPqr)
    case _ =>
    finalCtx.meta.get("compiledDf") match {
     case Some(df) if df.isInstanceOf[org.apache.spark.sql.DataFrame] =>
     applyPostCompilePipeline(
      df.asInstanceOf[org.apache.spark.sql.DataFrame],
      request, schemaMetadata,
      cap = resultCapRows)
     case Some(other) =>
     Left(EngineError.UnsupportedCapability(
      engine  = sparkEngineName,
      capability = "SparkEngineProvider.query",
      message = s"Context.meta('compiledDf') has unexpected type ${other.getClass.getName} (sm8-internal invariant violated)"))
     case None =>
     Left(EngineError.UnsupportedCapability(
      engine  = sparkEngineName,
      capability = "SparkEngineProvider.query",
      message = "Context.meta missing 'compiledDf' (sm8-internal invariant violated)"))
    }
   }
   }
  case None =>
   compileSteps(ctx) match {
   case Right(df: org.apache.spark.sql.DataFrame) =>
    applyPostCompilePipeline(df, request, schemaMetadata,
      cap = resultCapRows)
   case Left(err: EngineError) =>
    Left[EngineError, PortableQueryResult](err)
   }
  }
 } finally {
  // Per-query session design (v0.5-r1 invariant): clear the TL
  // seam on EVERY exit — success, Left, or raw Throwable — so a
  // throwing lambda inside `compiled`'s construction does NOT
  // leak the per-query SparkSession on the worker thread.
  // `createdQuerySessionHere` was set at method entry; we only
  // clear what we created (a test pre-populated TL is the test's
  // lifecycle).
  if (createdQuerySessionHere) clearQuerySessionTL()
 }
 compiled
}

 /**
 * PR-9: extract the where/limit/collect/decode pipeline into a
 * helper method so the HIT-path and MISS-path branches in `query`
 * can share it. 
 * style": this is the same logic the pre-PR-9 inline block had,
 * just lifted into a method (no semantic change).
 *
 * `.filter` + `.limit` + `collect` are driver-side; no
 * executor-side closure capture. The DataFrame itself is NOT
 * shipped to executors in this method.
 *
 * Per ADR-008-P §A3 (PR-1): paired persist/unpersist lifecycle
 * with typed errors. The persist() itself was already applied
 * upstream by `applyAggregations` when materialize==Persist; the
 * passed-in `df` carries that storageLevel. `wasPersisted` is
 * derived from the PASSED-IN df BEFORE `.filter()`/`.limit()`
 * (which build fresh uncached plans and reset storageLevel to
 * NONE — the reviewers' P2 wasPersisted bug). We unpersist the
 * persisted upstream frame in `finally`, surface typed errors on
 * collect/unpersist failure (no Throwable swallow).
 *
 * ADR-009-e: applies the server-side materialization cap via a
 * cap+1 probe (`limit(min(cap, request.limit) + 1)` → `collect` →
 * `truncated = collected.length > effectiveCap`, dropping the
 * probe row). `collected.length` is O(1) on the materialized
 * array — NO df.count() on the hot path.
 */
 private[spark] def applyPostCompilePipeline(
  df:    org.apache.spark.sql.DataFrame,
  request:   QueryRequest,
  schemaMetadata: Map[String, String],
  // ADR-009-e: server-side materialization cap (deployment policy,
  // RFC §3). The caller's `request.limit` may only NARROW it
  // (min), never widen it — callers cannot trip the guard off.
  cap:   Long = SparkEngineProvider.DefaultResultCapRows
 ): Either[EngineError, PortableQueryResult] = {
  // ADR-009-e fix (reviewers' P2): `wasPersisted` MUST be derived
  // from the PASSED-IN `df` BEFORE applying filter/limit. Spark's
  // `.filter()` / `.limit()` build NEW uncached logical plans, so
  // `withLimit.storageLevel` is always StorageLevel.NONE even when
  // the upstream aggregate frame was persisted (ADR-008-P paired
  // persist). Deriving from withLimit made the finally's unpersist
  // a silent no-op on exactly the capped path this ADR makes the
  // default — the persisted frame leaked until provider.close().
  val wasPersisted: Boolean =
    !df.storageLevel.equals(org.apache.spark.storage.StorageLevel.NONE)
  val filtered: org.apache.spark.sql.DataFrame =
    request.where.filter(_.nonEmpty) match {
      case Some(w) => df.filter(w)
      case None => df
    }
  // Effective cap = min(policy cap, caller limit). The cap+1 probe
  // pulls ONE row beyond the effective cap so `truncated` is
  // truthful: a source returning EXACTLY effectiveCap rows reports
  // truncated=false (no off-by-one). `collected.length` is O(1) on
  // the materialized array — NO df.count(), NO extra Spark action
  // (perf-mandated by the ADR).
  val effectiveCap: Long = math.min(cap, request.limit.getOrElse(cap))
 val withLimit: org.apache.spark.sql.DataFrame =
  // ADR-009-e follow-up (P3): clamp the +1 probe row into Int range.
  // `cap` is bounded above by `Int.MaxValue - 1L` (ctor require), and
  // `request.limit` is policy-reachable as a Long, so the addition can
  // still hit Int.MaxValue. math.min keeps the +1 probe row in range
  // for any effectiveCap ≤ Int.MaxValue - 1L — the same ceiling the
  // ctor enforces on the policy.
  filtered.limit((math.min(effectiveCap, Int.MaxValue.toLong - 1L) + 1L).toInt)
  val schema: ResultSchema = ResultSchema(
    withLimit.schema.fields.map { f =>
      Field(
        name  = f.name,
        dataType = bridge.sparkTypeToSealedDataType(f.dataType),
        nullable = f.nullable
      )
    }.toList
  )
  // Per ADR-009-e error-handling guardrail: do NOT wrap collect()
  // in a catch. Real Spark failures (SparkException, executor OOM)
  // propagate per the PR-176 NonFatal discipline; the dispatcher
  // wraps only NonFatal. `truncated` is a VALUE, not an exception
  // path.
  val collected: Array[org.apache.spark.sql.Row] =
    try {
      withLimit.collect()
    } finally {
      if (wasPersisted) {
        // Unpersist the ORIGINAL persisted upstream frame (df),
        // never withLimit — which is a fresh uncached plan and was
        // never persisted (see wasPersisted derivation above).
        try df.unpersist()
        catch {
          case e: Throwable =>
            // Per scala-error-handling-mindset §4: do NOT swallow.
            // unpersist failures indicate a real Spark executor
            // state problem (NotSerializableException, OOM,
            // SparkException from storage). We log to stderr (the
            // canonical SM8 stderr channel per RFC §9). Real typed
            // error on collect-failure is handled upstream.
            System.err.println(s"sm8: SparkEngineProvider unpersist failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
        }
      }
    }
  val pulledCount: Int = collected.length
  // Truthful truncation: only when the probe found MORE than the
  // effective cap. Drop the single probe row.
  val truncated: Boolean = pulledCount.toLong > effectiveCap
  val cappedRows: Array[org.apache.spark.sql.Row] =
    if (truncated) collected.dropRight(1) else collected
  val rows: Vector[ResultRow] = cappedRows.iterator.map { row =>
    ResultRow(values = decodeRow(row, schema), schema = schema)
  }.toVector
  Right(PortableQueryResult(
    schema = schema,
    rows  = rows,
    metadata = schemaMetadata,
    truncated = truncated))
  }
 /** PR-N1: walk the produced `RelOp` tree via the engine-portable
 * `QueryBuilder` + the core `RelOpPlanPrinter`. Output is a
 * multi-line indented plan: 1 header line (model name + engine
 * identity) + 1 line per RelOp node. Per RFC §3 ownership the
 * IR building + plan serialisation are core; this method only
 * glues them together with the Spark-specific resolver + identity.
 *
 * `QueryBuilder.build` (cycle in calculatedMeasures, unresolved
 * source, etc.) is rendered as the plan prefix plus a typed error
 * line -- never a thrown exception.
 *
 * Per-query session design: explain() acquires its own per-query session
 * (null-safe per §4b; the null-spark provider path must not
 * throw at lazy-init). The per-query session threads the per-query
 * skew factor + broadcast seeds into `compileModelToDataFrame`.
 *
 * @param model the model whose IR is built + compiled + printed
 * @param request the request (used for `where` / `limit` in the
 *                  Spark physical plan render)
 * @param ctx the engine context (carries `joinHints` etc.)
 * @return Right with the SM8 IR plan + (when a live Spark
 *         session is available) the extended Spark physical
 *         plan; Left is never produced here (errors are
 *         surfaced as plan footers, per PR-27 contract)
 */
 override def explain(
  model: Model,
  request: QueryRequest,
  ctx:  EngineContext): Either[EngineError, String] = {
 val header =
  s"=== SM8 Plan: ${model.name} | engine=${sparkEngineName} version=${identity.nativeVersion} ==="
 QueryBuilder.build(model, resolver, identity) match {
  case Right(relOp) =>
  // SM8 semantic plan (always printed when the IR builds).
  val sm8Section = header + "\n" + RelOpPlanPrinter.print(relOp)
  // Spark physical plan (extended) -- printed only when a live
  // SparkSession is available AND the smoke-compile succeeds.
  // pure driver-side Catalyst operation (no executor closures).
  spark match {
   case null =>
   // No Spark session -- SM8 semantic only.
   Right(sm8Section)
   case _ =>
   // Per PR-27: the smoke-compile uses the SAME
   // `compileModelToDataFrame` helper as `query()` --
   // including `ModelValidator.validateAgainstSchema`. This
   // fixes the UNRESOLVED_COLUMN bug discovered in the
   // diagnostic spec (the relOp's columns were not
   // validated against the resolved source's actual schema).
   // Per-query session design: a fresh per-query session mirrors
   // `query()` (each call gets a fresh SessionState + conf;
   // never .stop()ed; reference is method-local).
   val querySession: org.apache.spark.sql.SparkSession = {
     val qs = spark.newSession()
     // Per-query session design: copy temp views from the base session so the
     // smoke-compile resolves SourceRef.ByName refs to the test's
     // temp views (same rationale as `query()`).
     SparkEngineProvider.copyTempViews(spark, qs)
     qs
   }
   compileModelToDataFrame(model, request, ctx, querySession) match {
    case Right(df) =>
    val sparkPlan = df.queryExecution.explainString(
     org.apache.spark.sql.execution.ExplainMode.fromString("extended"))
    Right(sm8Section + "\n== Spark Physical Plan (via df.explain(true)) ==\n" + sparkPlan)
    case Left(err) =>
    // surface as a footer (never a silent drop).
    Right(sm8Section + "\n<<smoke compile failed: " + err.getClass.getSimpleName + ": " + err.toString + ">>")
   }
  }
  case Left(err) =>
  Right(header + "\n" + s"<<build failed: ${err.getClass.getSimpleName}: ${err.toString}>>")
 }
 }

 /** PR-27 (ADR-008-R SSexplain): the shared compile-pipeline helper
 * used by BOTH `query()` (existing) and `explain()` (PR-27).
 *
 * Implementations) + DRY: one source of truth for the compile
 * pipeline at the connector layer.
 *
 * the smoke-compile MUST call `ModelValidator.validateAgainstSchema`
 * BEFORE `compileRelOp` -- otherwise the relOp's columns are
 * not validated against the resolved source's actual schema
 * (UNRESOLVED_COLUMN errors at compile time, not at model load
 * time). This is the same pipeline that `query()` uses.
 *
 * silent): every step surfaces as `Left(EngineError.*)`.
 */
private[spark] def compileModelToDataFrame(
 model: Model,
 request: QueryRequest,
 ctx:  EngineContext,
 // Per-query session design decision: this helper now takes the
 // per-query `SparkSession` explicitly. explain() acquires a
 // per-query session and threads it through; query() does the
 // same. The null-spark provider path (the supported `null` config)
 // is preserved by short-circuiting with a typed Left when the
 // session is null.
 querySession: org.apache.spark.sql.SparkSession): Either[EngineError, org.apache.spark.sql.DataFrame] = {
 val resolver = if (querySession != null) new SparkSourceResolver(querySession, SparkSourceResolver.SessionCatalogModelRegistry)
  else new SparkSourceResolver(null, SparkSourceResolver.SessionCatalogModelRegistry)
 // PR-31 (ADR-008-R SSfilterPushdown wire-up, deferred from PR-28):
 // use `resolveWithPushdown` to push the typed whereFilters down to
 // the source.
 // (closure-safety): the source-level filter is built driver-side
 // via `predicateToColumn`; no executor-side closure capture.
 //
 // change for empty filters): when `request.whereFilters` is Nil,
 // `resolveWithPushdown` falls back to `resolve` + `readSourceDF`
 // (the existing path -- unchanged for 19 callers).
 //
 // The pre-filtered DF flows through `compileRelOp` via the new
 // overload added in PR-31, which forwards it to the Scan lower.
 // The `ResolvedSource` is preserved as the wire DTO for the
 // ModelValidator step (which needs the schema, not the DF).
 for {
  // PR-31: use resolveWithPushdown to get (ResolvedSource,
  // pre-filtered DataFrame). The pre-filtered DF is used in
  // compileRelOp; the ResolvedSource is used by the canonical
  // compileRelOp overload (PR-32 -- validates against the
  // schema then lowers).
  pushdownResult <- resolver.resolveWithPushdown(model.source, request.whereFilters, identity)
  (resolved, preFilteredDf) = pushdownResult
  scan  <- resolved match {
  case s: io.sm8.core.engine.ResolvedSource.Scan =>
   Right[EngineError, io.sm8.core.engine.ResolvedSource.Scan](s)
   case _ => Left(EngineError.UnsupportedCapability(
     engine = sparkEngineName,
     capability = "SourceResolver.resolveWithPushdown",
     // Per PR-32 data-engineer NIT fix: the message
     // no longer references the stale PR-M4 framing.
     // The current behaviour is fully PR-M4 / PR-M5
     // (full RelOp -> DataFrame lowering via
     // MinimalRelOpLowerer); a non-Scan resolution
     // surfaces as a typed `UnsupportedCapability`.
     message = s"non-Scan resolution for source $model.source: ${resolved.getClass.getSimpleName}"))
  }
  relOp <- QueryBuilder.build(model, resolver, identity)
  // The canonical `compileRelOp(model, relOp, ctx, scan, preFilteredDf)`
  // overload validates the model against the resolved source's
  // schema BEFORE lowering (the validator is baked into the
  // canonical entry point).
  // ADR-009-a broadcast seed on the per-query (or null) session.
  seedCtx = SparkEngineProvider.seedBroadcastThreshold(querySession, ctx, model)
  // Per-query skew factor design: per-query skew factor seed (per-query, single
  // conditional; honors JoinHints.skewFactor when Some).
  skewCtx = SparkEngineProvider.seedSkewFactor(querySession, seedCtx, model)
  // null-spark short-circuit: PQC + TQC require a live session.
  // The null-spark provider path is exercised by
  // SparkEngineProviderSpec:100,119 and
  // SparkEngineProviderExplainSpec:85,102,112 (the explain path
  // uses its own null-safe per-call resolver, so it does not
  // call this path with a null session).
  df0 <-
   if (querySession != null) new PortableQueryCompiler(querySession).compileRelOp(model, relOp, skewCtx, scan, Some(preFilteredDf))
   else {
     // Per PR-32 contract: a null-session smoke returns a typed
     // UnsupportedCapability rather than throwing.
     return Left(EngineError.UnsupportedCapability(
      engine = sparkEngineName,
      capability = "PortableQueryCompiler.compileRelOp (null session)",
      message = "Cannot compile: SparkSession is null (SM8-only semantic path)"))
   }
  df <- if (querySession != null) TypedQueryCompiler(querySession).apply(df0, request, skewCtx)
   else return Left(EngineError.UnsupportedCapability(
    engine = sparkEngineName,
    capability = "TypedQueryCompiler.apply (null session)",
    message = "Cannot compile: SparkSession is null (SM8-only semantic path)"))
 } yield df
 }

 // PR-N1: the resolver used by `explain` to produce the IR tree.
 // the same model registry / spark session as the compiler's path.
 private lazy val resolver: io.sm8.core.engine.SourceResolver = {
 val registry = sparkSourceRegistry.getOrElse(io.sm8.connectors.spark.ModelRegistry.NoopModelRegistry)
 new SparkSourceResolver(spark, registry)
 }
 private lazy val sparkSourceRegistry: Option[io.sm8.connectors.spark.ModelRegistry] = None

 /** Decode a Spark `Row` to a `List[ResultValue]` aligned with `schema.fields`.
 *
 * Per  mantra #3 (count allocations):
 * the row is a flat seq; no nested walker. The `while` loop
 * preallocates a single Array of size n and converts to List
 * once at the end.
 *
 * Per scala-error-handling-mindset: a null Spark cell becomes
 * `ResultValue.NullV` (never throws NPE on the boundary).
 */
 private def decodeRow(
  row: org.apache.spark.sql.Row,
  schema: ResultSchema): List[ResultValue] = {
 val n: Int = schema.fields.size
 val values = new Array[ResultValue](n)
 var i = 0
 while (i < n) {
  val cell: AnyRef = row.get(i).asInstanceOf[AnyRef]
  val fieldType = schema.fields(i).dataType
  values(i) = decodeCell(cell, fieldType)
  i += 1
 }
 values.toList
 }

 private def decodeCell(
  cell:  AnyRef,
  dataType: SealedDataType): ResultValue = {
 if (cell == null) return ResultValue.NullV
 dataType match {
  case SealedDataType.Boolean =>
  cell match {
   case b: java.lang.Boolean => ResultValue.BoolV(b)
   case _     => ResultValue.NullV
  }
  case SealedDataType.Int | SealedDataType.BigInt =>
  cell match {
   case n: Number => ResultValue.IntV(n.longValue)
   case _   => ResultValue.NullV
  }
  case SealedDataType.Double =>
  cell match {
   case n: Number => ResultValue.DoubleV(n.doubleValue)
   case _   => ResultValue.NullV
  }
  case SealedDataType.Decimal(_, _) =>
  cell match {
   case d: java.math.BigDecimal => ResultValue.DecimalV(d)
   case s: String    => ResultValue.DecimalV(BigDecimal(s))
   case _      => ResultValue.NullV
  }
  case SealedDataType.Varchar =>
  cell match {
   case s: String => ResultValue.StringV(s)
   case _   => ResultValue.StringV(cell.toString)
  }
  case SealedDataType.Timestamp =>
  cell match {
   case ts: java.sql.Timestamp =>
   ResultValue.TimestampV(ts.toInstant)
   case _ => ResultValue.NullV
  }
  case SealedDataType.Date =>
  cell match {
   case d: java.sql.Date =>
   ResultValue.DateV(d.toLocalDate)
   case ld: java.time.LocalDate =>
   ResultValue.DateV(ld)
   case _ => ResultValue.NullV
  }
  case _ =>
  ResultValue.StringV(cell.toString)
 }
 }
}


/**
 * Spark Engine Provider companion.
 *
 * Holds the broadcast-seed default constant: the byte budget used
 * to arm the adapter's broadcast gate when a model declares a join
 * estimate. Mirrors Spark's own `autoBroadcastJoinThreshold` default
 * (10 MiB) -- an explicit  `JoinHints.broadcastRightBelowBytes` from
 * the caller always overrides this seed; absent either, Spark's own
 * heuristic governs.
 */
object SparkEngineProvider {
  /** ADR-009-e: server-side default materialization cap (rows) for
   * the driver `collect()`. Closes the silent driver-OOM (SM-07)
   * when a caller passes no `request.limit`: the engine materializes
   * at most this many rows and flags the result `truncated`. The
   * value is deployment policy (RFC §3) — the platform's
   * `EngineService.DefaultResultCapRows` mirrors it; callers can
   * only NARROW it via request.limit, never widen or disable it. */
  val DefaultResultCapRows: Long = 1_000_000L

  val BroadcastSeedDefaultBytes: Long = 10L * 1024L * 1024L

  /**
   * Arms the adapter's broadcast byte-gate when the model declares
   * any join `estimatedRows` (inline presence rule) OR when a
   * plugin's PreExecute hook armed the broadcast oracle (typed
   * transport via EngineContext.decisionHints per ADR-009-d v0.3).
   *
   * The estimate is an ARM (presence), not a value: the runtime
   * `sizeInBytes` check stays authoritative, so a large side is
   * never physically broadcast. Caller/hook-set hints always win
   * over the seed; `preferredStrategy` is untouched (the Cross
   * + strategy rejection guard is never triggered).
   *
   * Oracle precedence: when the broadcast oracle is present
   * (Some(b)), the oracle's arm Boolean wins over the inline
   * presence rule; when present, the oracle's threshold bytes
   * win over the session autoBroadcastJoinThreshold default. None
   * on either field preserves today's behavior (inline arm +
   * session default).
   *
   * @param spark the connected Spark session whose configured
   *              auto-broadcast threshold seeds the gate when no
   *              oracle threshold is provided
   * @param eCtx the possibly-hint-bearing engine context
   * @param model the query model (join estimates consulted by the
   *              inline presence rule)
   * @return the context with a seeded broadcast threshold if armed
   */
  def seedBroadcastThreshold(
    spark: org.apache.spark.sql.SparkSession,
    eCtx:  io.sm8.core.engine.EngineContext,
    model: io.sm8.core.model.Model
  ): io.sm8.core.engine.EngineContext = {
    // Per-query decision oracle: prefer the plugin's arm + threshold
    // bytes when present; fall back to the inline presence rule +
    // the session default threshold. The two regimes differ on
    // identical models with est > BroadcastThresholdRows (plugin
    // disarms; inline arms) — observable divergence proves the
    // wiring.
    val broadcastOracle = eCtx.decisionHints
    val oracleArm: Option[Boolean]    = broadcastOracle.flatMap(_.broadcastArmed)
    val oracleThreshold: Option[Long] = broadcastOracle.flatMap(_.broadcastThresholdBytes)
    val sessionThreshold: Long = oracleThreshold.getOrElse(
      try {
        // Spark renders the threshold with a 'b' suffix (e.g.
        // "10485760b") — strip it before parsing. Any parse failure
        // falls back to the default rather than breaking the query.
        val raw = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
        val v = raw.stripSuffix("b").stripSuffix("B").toLong
        // -1 is Spark's "disable the auto-broadcast heuristic" sentinel;
        // treat it as "no session threshold" (fall back to default)
        // rather than arming a zero/negative budget.
        if (v > 0L) v else BroadcastSeedDefaultBytes
      } catch {
        case _: NoSuchElementException => BroadcastSeedDefaultBytes
        case _: NumberFormatException  => BroadcastSeedDefaultBytes
      })
    val armed: Boolean = oracleArm.getOrElse(
      model.joins.exists(_.estimatedRows.isDefined)
    )
    val seeded: Option[Long] = eCtx.joinHints.broadcastRightBelowBytes.orElse(
      if (armed) Some(sessionThreshold) else None
    )
    if (seeded == eCtx.joinHints.broadcastRightBelowBytes) eCtx
    else eCtx.copy(joinHints = eCtx.joinHints.copy(broadcastRightBelowBytes = seeded))
  }

  /**
   * Per-query skew factor seed. Writes
   * `spark.sql.adaptive.skewJoin.skewedPartitionFactor` on the
   * per-query `SparkSession` (or null in the null-spark path)
   * when (a) the model declares a join `estimatedRows` AND
   * `JoinHints.skewFactor` is `Some(f)`, OR (b) a plugin's
   * PreExecute hook armed the skew oracle (typed transport via
   * EngineContext.decisionHints per ADR-009-d v0.3). Single
   * conditional; the fresh-session conf has no operator-pre-set
   * value (the honest-inheritance property), so this set is
   * race-free and authoritative for the originating query.
   * `None` leaves the fresh session at the shared-SparkConf
   * value (or static 5.0 default). Null-safe: the null-spark
   * path short-circuits.
   *
   * Oracle precedence: when the skew oracle is present
   * (Some(b)), the oracle's arm Boolean wins over the inline
   * presence rule. None on the oracle field preserves today's
   * behavior (inline arm + JoinHints.skewFactor precondition).
   *
   * @param querySession the per-query `SparkSession` (or null)
   * @param eCtx the engine context (carries `joinHints.skewFactor`
   *              and optionally `decisionHints.skewArmed` from a
   *              plugin)
   * @param model the query model (join estimates consulted by the
   *              inline presence rule)
   * @return the engine context (unchanged on the no-
   *         estimatedRows / null-spark paths; the querySession
   *         has had the AQE factor set on the Some(f) path)
   */
  def seedSkewFactor(
    querySession: org.apache.spark.sql.SparkSession,
    eCtx:  io.sm8.core.engine.EngineContext,
    model: io.sm8.core.model.Model
  ): io.sm8.core.engine.EngineContext = {
    if (querySession == null) return eCtx
    // Per-query decision oracle: prefer the plugin's skewArmed
    // when present; fall back to the inline presence rule (which
    // itself requires JoinHints.skewFactor = Some(f) to actually
    // write). The two regimes differ on identical models —
    // observable divergence proves the wiring. A model with no
    // join estimates is NOT seeded even if the oracle (or a
    // Some(f)) was supplied — the caller asked for a factor they
    // did not pair with a large-row declaration.
    val hasEstimatedJoin: Boolean =
      model.joins.exists(_.estimatedRows.isDefined)
    val skewOracleArm: Option[Boolean] = eCtx.decisionHints.flatMap(_.skewArmed)
    // Per the docstring above: no join estimates = no seed, even if
    // the oracle armed. The AND keeps the no-join contract intact
    // (caller asked for a factor they did not pair with a large-row
    // declaration). The oracle still disarms (Some(false) wins) and
    // still arms inline (Some(true) overrides no-estimate when at
    // least one join declares an estimate).
    val shouldArm: Boolean = hasEstimatedJoin && skewOracleArm.getOrElse(hasEstimatedJoin)
    if (shouldArm) {
      eCtx.joinHints.skewFactor match {
        case Some(f) =>
          querySession.conf.set(
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
            f.toLong)
          eCtx
        case None => eCtx
      }
    } else eCtx
  }

  /**
   * Per-query session design: copy the base session's temp views to a
   * per-query session. `spark.newSession()` shares the
   * `SharedState` (persistent tables) but creates a fresh
   * `SessionState` whose `SessionCatalog` is empty of temp views.
   * Standard Spark tests register temp views on the base session
   * (`createOrReplaceTempView`); without this copy, a per-query
   * compile referencing such a view (`SourceRef.ByName.resolve`)
   * would fail with "table not found".
   *
   * Driver-side only: never touches executor state.
   *
   * @param parent the base session (where tests register temp views)
   * @param clone  the per-query session (fresh, empty catalog)
   */
  def copyTempViews(
    parent: org.apache.spark.sql.SparkSession,
    clone:  org.apache.spark.sql.SparkSession
  ): Unit = {
    val parentCatalog = parent.sessionState.catalog
    val cloneCatalog  = clone.sessionState.catalog
    parentCatalog.getTempViewNames().foreach[Unit] { name =>
      parentCatalog.getTempView(name).foreach {
        view: org.apache.spark.sql.catalyst.plans.logical.View =>
        cloneCatalog.createTempView(
          name,
          org.apache.spark.sql.catalyst.catalog.TemporaryViewRelation(
            tableMeta = view.desc,
            plan = Option(view.child)),
          overrideIfExists = true)
      }
    }
  }
}
