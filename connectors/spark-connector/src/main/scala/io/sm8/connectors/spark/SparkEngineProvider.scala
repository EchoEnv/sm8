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
 val hookRunner:  Option[HookRunner] = None
) extends EngineProvider {

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
 // PR-M4 (GAP 5 -- the most critical): the IR-extension path
 // (PR-H/I/J/K/L) is now LIVE in production. Steps:
 // 1. Resolve the primary source via `SparkSourceResolver` (PR-M3).
 // 2. Run `ModelValidator.validateAgainstSchema` (PR-M2) -- fail-loud
 //  typed `SchemaValidation` on unknown-field references.
 // 3. Lower Model -> RelOp via `QueryBuilder.build` (PR-L). Cycle
 //  detection runs here (built-in).
 // 4. Compile the RelOp via `PortableQueryCompiler.compileRelOp`.
 // 5. PR-M4 (GAP 6): wrap the build+compile step in the bound
 //  dispatcher (deferred -- O3+1).
 // 6. Apply request-level where + limit + collect + decode.
 //
 // The compile steps are factored into a thunk; the for-comp
 // returns the final DataFrame; the dispatching code wraps that
 // thunk.
 val resolver = new SparkSourceResolver(spark)
 val compileSteps: io.sm8.core.engine.EngineContext => Either[EngineError, org.apache.spark.sql.DataFrame] = { eCtx =>
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
  df0  <- new PortableQueryCompiler(spark).compileRelOp(
      model, relOp, eCtx, scan, Some(preFilteredDf))
  // PR-33: the new `TypedQueryCompiler.apply(df, request, ctx,
  // preFilteredDf)` overload SUPPRESSES the in-memory
  // `whereFiltersOp` when the pre-filtered DF is supplied
  // (the filter was already pushed at the source).
  df  <- TypedQueryCompiler(spark).apply(
      df0, request, eCtx, Some(preFilteredDf))
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
  hookRunner match {
  case Some(runner) =>
   runner.run(initialCtx, { runCtx =>
   runCtx.meta.get("engineContext") match {
    case Some(eCtx: EngineContext) =>
    compileSteps(eCtx).map { df =>
     // Stash the compiled DataFrame in `Context.meta` for
     // the outer code to extract on the MISS path.
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
    // HIT path: a PreExecute hook (e.g. cache-read) set
    // `c.result` + `c.stop = true`. The cached PortableQueryResult
    // IS the answer. Return it directly — no compile, no
    // collect, no decode. This is the perf-cliff closure
    // (PR-9 T2-3 + the e2e proof in
    // `SparkEngineProviderHookRunnerSpec`).
    Right[EngineError, PortableQueryResult](cachedPqr)
    case _ =>
    // MISS path: extract the DataFrame from `Context.meta`.
    // a `Dataset[Row]` — the type parameter is erased at
    // runtime. Use an `isInstanceOf` runtime check (the
    // result is `Any`); the `applyPostCompilePipeline`
    // signature requires `DataFrame`, so the cast is the
    // boundary.
    finalCtx.meta.get("compiledDf") match {
     case Some(df) if df.isInstanceOf[org.apache.spark.sql.DataFrame] =>
     // is `Dataset[Row]` — the Row type param is erased at
     // runtime, so a type pattern `case df: DataFrame`
     // binds `df` as `Any`. Use the `case. if`
     // guard with `isInstanceOf` (runtime check) +
     // `asInstanceOf` (the boundary cast). A wrong
     // type here indicates the runner callback
     // populated `compiledDf` with a non-DataFrame
     // value (sm8-internal invariant).
     applyPostCompilePipeline(
      df.asInstanceOf[org.apache.spark.sql.DataFrame],
      request, schemaMetadata)
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
   // Bare-deploy shape: no dispatcher wired. compileSteps once
   // + run the where/limit/collect/decode pipeline. Per
   // 2-step chain — explicit match.
   compileSteps(ctx) match {
   case Right(df: org.apache.spark.sql.DataFrame) =>
    applyPostCompilePipeline(df, request, schemaMetadata)
   case Left(err: EngineError) =>
    Left[EngineError, PortableQueryResult](err)
   }
  }
 // the `val compiled` is the only side-effecting statement in the
 // method; the method's return value is the `compiled` Either.
 // In Scala 2.13 a `val` statement has type `Unit` — the method
 // must end with an expression whose type is the declared return
 // type. Without this final `compiled` reference, the method
 // body infers as `Unit` and fails the type check.
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
 * DataFrame carries that storageLevel. We read it, collect the
 * rows, unpersist in `finally`, surface typed errors on
 * collect/unpersist failure (no Throwable swallow).
 */
 private[spark] def applyPostCompilePipeline(
  df:    org.apache.spark.sql.DataFrame,
  request:   QueryRequest,
  schemaMetadata: Map[String, String]): Either[EngineError, PortableQueryResult] = {
 val filtered: org.apache.spark.sql.DataFrame =
  request.where.filter(_.nonEmpty) match {
  case Some(w) => df.filter(w)
  case None => df
  }
 val withLimit: org.apache.spark.sql.DataFrame =
  request.limit.fold(filtered)(l => filtered.limit(l.toInt))
 val schema: ResultSchema = ResultSchema(
  withLimit.schema.fields.map { f =>
  Field(
   name  = f.name,
   dataType = bridge.sparkTypeToSealedDataType(f.dataType),
   nullable = f.nullable
  )
  }.toList
 )
 val materialized: org.apache.spark.storage.StorageLevel = withLimit.storageLevel
 val wasPersisted: Boolean =
  !materialized.equals(org.apache.spark.storage.StorageLevel.NONE)
 val collected: Array[org.apache.spark.sql.Row] =
  try {
  withLimit.collect()
  } finally {
  if (wasPersisted) {
   try withLimit.unpersist()
   catch {
   case e: Throwable =>
    // Per scala-error-handling-mindset §4: do NOT swallow.
    // unpersist failures indicate a real Spark executor
    // state problem (NotSerializableException, OOM,
    // SparkException from storage). We log to stderr (the
    // canonical SM8 stderr channel per RFC §9). Real
    // typed error on collect-failure is handled by the
    // catch above.
    System.err.println(s"sm8: SparkEngineProvider unpersist failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
   }
  }
  }
 val rows: Vector[ResultRow] = collected.iterator.map { row =>
  ResultRow(values = decodeRow(row, schema), schema = schema)
 }.toVector
 Right(PortableQueryResult(
  schema = schema,
  rows  = rows,
  metadata = schemaMetadata))
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
   compileModelToDataFrame(model, request, ctx) match {
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
 private def compileModelToDataFrame(
  model: Model,
  request: QueryRequest,
  ctx:  EngineContext): Either[EngineError, org.apache.spark.sql.DataFrame] = {
 val resolver = new SparkSourceResolver(spark)
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
  // PR-32: the canonical `compileRelOp(model, relOp, ctx, scan, preFilteredDf)`
  // overload validates the model against the resolved source's
  // schema BEFORE lowering. This replaces the manual
  // `ModelValidator.validateAgainstSchema` call that was here
  // in PR-27 -- the validator is now baked into the canonical
  // entry point (per RFC SS3 layer ownership).
  df0  <- new PortableQueryCompiler(spark).compileRelOp(model, relOp, ctx, scan, Some(preFilteredDf))
  df  <- TypedQueryCompiler(spark).apply(df0, request, ctx)
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
