package com.example.caching

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import io.sm8.core.engine.{
  EngineContext, EngineError, EngineProvider, QueryRequest, ResultValue
}
import io.sm8.core.model.{CachePolicy, Dimension, Measure, Model, ModelBuilder, ModelStatus, SourceRef}
import io.sm8.core.model.ModelValidator
import io.sm8.core.expr.ExprSugar._
import io.sm8.sdk.{Context, HookRunner, HookStage, PipelineStage}

/** sm8 caching-rollup-warmpath example — the "my dashboard runs the same
  * aggregate every 5 seconds" story.
  *
  * Real-world shape: an ops dashboard re-issues the same aggregate query
  * hundreds of times per hour. Without a cache, each refresh pays the
  * full engine cost; with a read-through cache, only the first call
  * after a model-version bump does.
  *
  * The example walks three phases:
  *
  *  1. COLD PATH — N identical queries WITHOUT a cache: every call runs
  *     the full engine (compile + Spark job + decode). The provider has
  *     no `hookRunner`, so no hooks fire and nothing short-circuits.
  *  2. WARM PATH — the SAME N queries WITH the cache wired:
  *     `InMemoryResultCache` + `CachePlugin` registered through a
  *     minimal consumer-side `HookRunner` (this example ships its own
  *     ~30-line runner; `HookRunnerOrchestration` is the platform's
  *     heavier equivalent). Query 1 MISSES and runs; queries 2..N HIT
  *     (`ctx.stop = true` from the PreExecute hook short-circuits the
  *     engine; PostExecute still fires so the write-through is a cheap
  *     dedup no-op).
  *  3. INVALIDATION — bump `Model.version`; the cache key domain
  *     changes (`model|version|request`), so the next query MISSES
  *     again (the old version's entries stay resident but unreachable
  *     for the new model version). This is the model-version-driven
  *     invalidation contract (`putJournaledWithModelAndVersion`).
  *
  * The plugin's own hit/miss counters (`CachePlugin.hits`/`misses`)
  * print as a summary at end of run (a real deployment would forward
  * these to a `MetricsSink` via `MetricsRegistry.register`).
  *
  * Layer discipline: consumer imports sm8-core + spark-
  * connector + cache-plugin (a plugins/ artifact, the documented
  * consumer-facing plugin use case). NO sm8-platform import — the
  * example ships its own minimal HookRunner implementation.
  */
object Main {

  private val Logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  /** How many times the dashboard re-issues the same query. */
  private val Refreshes = 20

  /** Build the sales `Model`. `version` participates in the cache key
    * domain, so bumping it invalidates prior entries (the
    * invalidation demo in main).
    */
  private def buildSalesModel(version: Int): Model =
    ModelBuilder()
      .withName("sales")
      .withVersion(version)
      .withSource(SourceRef.ByName(table = "sales_spark"))
      .withStatus(ModelStatus.Draft)
      .withDimensions(List(
        Dimension.field("region", "region")))
      .withMeasures(List(
        Measure.aggregate("total_amount", "amount".asField.sum)))
      .withPolicies(io.sm8.core.model.ModelPolicyDefaults(
        materialize = io.sm8.core.model.MaterializePolicy.None,
        cache       = CachePolicy.WriteThrough("dashboard"),
        audit       = io.sm8.core.model.AuditPolicy.NoAudit))
      .build match {
      case Right(m) => m
      case Left(err) =>
        throw new IllegalStateException(s"sm8: failed to build sales Model: $err")
      }

  /** Minimal consumer-side `HookRunner`: fires the registered
    * PreExecute hooks in priority order, runs the engine thunk unless a
    * pre-hook set `ctx.stop`, then fires PostExecute hooks.
    *
    * This is the ~30-line essence of what `HookRunnerOrchestration` (in
    * sm8-platform) does for the full 4-stage pipeline. A bare consumer
    * that only needs Pre/Post around Execute can ship this instead of
    * importing the platform module.
    *
    * Hook-throw contract matches the platform dispatcher: a NonFatal
    * throw from any hook is caught and surfaced as
    * `EngineError.HookFailed` (a fatal Error propagates).
    */
  private final class MinimalHookRunner(hooks: io.sm8.core.HookManagerImpl)
      extends HookRunner {

    /** Fire PreExecute hooks in priority order, run the engine thunk
      * unless a pre-hook short-circuited, then fire PostExecute hooks.
      *
      * @param initial the starting Context (must carry `request` =
      *     EngineHookRequest and `meta("sm8.cache.policy")` set by the
      *     caller — the consumer-side fold the platform's
      *     EngineService.runQueryWithHooks would otherwise perform)
      * @param execute the engine thunk. Returns Right(ctx with result
      *     populated) on success; Left(EngineError) on failure. Skipped
      *     when a pre-hook set `stop`.
      * @return the final Context (post-hooks mutated) on success; the
      *     typed error on failure.
      */
    override def run(
        initial: Context,
        execute: Context => Either[EngineError, Context]
    ): Either[EngineError, Context] = {
      // --- PreExecute hooks, priority order (already sorted by the
      // manager; see HookManagerImpl.hooksForStage) ---
      var ctx = initial
      var stopped = false
      val preHooks = hooks.preHooksFor(HookStage.PreExecute)
      val it = preHooks.iterator
      while (it.hasNext && !stopped) {
        val (hook, priority) = it.next()
        try {
          ctx = hook.run(ctx)
          if (ctx.stop) stopped = true
        } catch {
          case e: InterruptedException =>
            Thread.currentThread().interrupt()
            return Left(EngineError.HookFailed(
              engine = "example", name = hook.name, priority = priority,
              stage = "PreExecute",
              message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
          case scala.util.control.NonFatal(e) =>
            return Left(EngineError.HookFailed(
              engine = "example", name = hook.name, priority = priority,
              stage = "PreExecute", message = e.getMessage))
        }
      }
      // --- Execute thunk (skipped on short-circuit) ---
      if (!stopped) {
        execute(ctx) match {
          case Right(next) => ctx = next
          case Left(err)   => return Left(err)
        }
      }
      // --- PostExecute hooks, priority order. A post-hook whose
      // runsOnStop = false is SKIPPED when a pre-hook set stop (the
      // cache HIT path: CacheWritePostHook is a mutator and must not
      // re-journal an entry the read hook just served). This gate is
      // what makes writes=2 (one per MISS) instead of writes=21.
      val postHooks = hooks.postHooksFor(HookStage.PostExecute)
      val pit = postHooks.iterator
      while (pit.hasNext) {
        val (hook, priority) = pit.next()
        if (stopped && !hook.runsOnStop) {
          // short-circuit mutator; still count as fired for parity
        } else {
          try ctx = hook.run(ctx)
          catch {
            case e: InterruptedException =>
              Thread.currentThread().interrupt()
              return Left(EngineError.HookFailed(
                engine = "example", name = hook.name, priority = priority,
                stage = "PostExecute",
                message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName)))
            case scala.util.control.NonFatal(e) =>
              return Left(EngineError.HookFailed(
                engine = "example", name = hook.name, priority = priority,
                stage = "PostExecute", message = e.getMessage))
          }
        }
      }
      Right(ctx)
    }
  }

  /** Run one query through the provider and log the row count. */
  private def runQuery(
      label: String,
      provider: EngineProvider,
      model: Model,
      request: QueryRequest): Int = {
    provider.query(model, request, EngineContext.defaultContext) match {
      case Right(pqr) =>
        Logger.info(s"$label: ${pqr.rows.size} rows")
        pqr.rows.size
      case Left(err) =>
        throw new IllegalStateException(
          s"sm8: $label query FAILED: ${err.getClass.getSimpleName}: $err")
    }
  }

  /** Render a PortableQueryResult as a normalized, sorted row multiset
    * (see the multi-engine example for why: order-independence).
    */
  private def normalizedRows(
      pqr: io.sm8.core.engine.PortableQueryResult): Vector[String] =
    pqr.rows.map { row =>
      row.values.map {
        case ResultValue.StringV(s)      => s
        case ResultValue.IntV(n)         => n.toString
        case ResultValue.DoubleV(d)      => d.toString
        case ResultValue.DecimalV(d)     => d.toString
        case ResultValue.NullV           => "null"
        case ResultValue.BoolV(b)        => b.toString
        case ResultValue.TimestampV(t)   => t.toString
        case ResultValue.DateV(d)        => d.toString
        case ResultValue.BinaryV(bytes)  => java.util.Base64.getEncoder.encodeToString(bytes)
      }.mkString(",")
    }.sorted

  /** Lifecycle entry point: cold path (no cache) -> warm path
    * (WriteThrough cache through a consumer-side HookRunner) ->
    * invalidation via model-version bump -> counters summary.
    * Exits 0 on success; throws on any typed engine error.
    *
    * @param args unused (mirrors the other examples' shape)
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sm8-caching-rollup-warmpath")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    try {
      Logger.info("=" * 70)
      Logger.info("sm8 caching-rollup-warmpath example — read-through cache lifecycle")
      Logger.info("=" * 70)

      // Ingest the CSV and declare the model.
      Logger.info("STEP 1: INGEST events.csv -> Spark temp view; DECLARE the sales Model (WriteThrough)")
      val csvPath = "data/events.csv"
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)
      df.createOrReplaceTempView("sales_spark")
      Logger.info(s"  spark view 'sales_spark' rows: ${df.count()}")

      val modelV1 = buildSalesModel(version = 1)
      ModelValidator.validate(modelV1) match {
        case Right(_) => Logger.info("  model v1 validation: ok")
        case Left(errs) =>
          throw new IllegalStateException(s"sm8: model validation failed: $errs")
      }

      val provider = new io.sm8.connectors.spark.SparkEngineProviderDescriptor()
        .realize("local[*]") match {
        case Some(p) => p
        case None => throw new IllegalStateException(
          "sm8: SparkEngineProviderDescriptor.realize(local[*]) returned None")
      }

      val request = QueryRequest(
        model = modelV1.name,
        dimensions = Seq("region"),
        measures = Seq("total_amount"))

      // Cold path: every query pays full engine cost.
      Logger.info(s"STEP 2: COLD PATH — $Refreshes identical queries, NO cache")
      Logger.info("  (every call runs the full engine: compile + Spark job + decode)")
      val t0 = System.nanoTime()
      var coldRows = 0
      var i = 0
      while (i < Refreshes) {
        coldRows = runQuery(s"  cold[$i]", provider, modelV1, request)
        i += 1
      }
      // Capture one cold-path result for the value-parity check below.
      val coldPqr = provider.query(modelV1, request, EngineContext.defaultContext) match {
        case Right(pqr) => pqr
        case Left(err) =>
          throw new IllegalStateException(
            s"sm8: cold parity query FAILED: ${"$"}{err.getClass.getSimpleName}: ${"$"}{err.toString}")
      }
      val coldNorm = normalizedRows(coldPqr)
      val coldMs = (System.nanoTime() - t0) / 1000000
      Logger.info(s"  cold path: $Refreshes queries in ${coldMs}ms (~${coldMs / Refreshes}ms/query)")

      // Warm path: the cache is wired; only the first query pays.
      Logger.info(s"STEP 3: WARM PATH — the SAME $Refreshes queries WITH the read-through cache")
      Logger.info("  (query 1 MISSES + runs; queries 2..N HIT and short-circuit the engine)")

      val cache    = new io.sm8.plugins.cache.InMemoryResultCache(maxEntries = 256)
      val plugin   = new io.sm8.plugins.cache.CachePlugin(cache)
      val engine   = io.sm8.core.EngineFactory.create(Seq(plugin))
      // The hooks registry now carries CacheReadPreHook (priority 50) +
      // CacheWritePostHook (priority 60) — both gated on
      // ctx.meta("sm8.cache.policy") which EngineContext.defaultContext
      // does NOT set. The consumer-side runner therefore seeds the meta
      // with the model's own CachePolicy before running the hooks (the
      // platform's EngineService does the same fold in
      // runQueryWithHooks; here the example performs the fold itself).
      val runner   = new MinimalHookRunner(
        engine.hooks.asInstanceOf[io.sm8.core.HookManagerImpl])

      val warmProvider = new io.sm8.connectors.spark.SparkEngineProviderDescriptor()
        .realize("local[*]") match {
        case Some(p) => p
        case None => throw new IllegalStateException(
          "sm8: warm SparkEngineProviderDescriptor.realize returned None")
      }

      val t1 = System.nanoTime()
      var warmRows = 0
      var warmNorm: Vector[String] = Vector.empty
      i = 0
      while (i < Refreshes) {
        // Seed the policy fold + run through the hooks.
        val initial = Context(
          stage = PipelineStage.Execute,
          request = io.sm8.core.engine.EngineHookRequest(
            model = modelV1,
            mcpRequest = request,
            cacheKey = s"sales|v${modelV1.version}|region=product"),
          meta = Map("sm8.cache.policy" -> modelV1.defaultPolicies.cache)
        )
        val out = runner.run(initial, { ctx =>
          // MISS path: run the real engine (the PreHook did not stop).
          warmProvider.query(modelV1, request, EngineContext.defaultContext) match {
            case Right(pqr) =>
              // Hand the result back through the Context so the
              // PostExecute write-through hook can journal it.
              Right(ctx.copy(result = Some(io.sm8.core.engine.EngineHookResult(pqr))))
            case Left(err) => Left(err)
          }
        })
        out match {
          case Right(ctx) =>
            ctx.result match {
              case Some(r: io.sm8.core.engine.EngineHookResult) =>
                warmRows = r.pqr.rows.size
                warmNorm = normalizedRows(r.pqr)
                Logger.info(s"  warm[$i]: ${r.pqr.rows.size} rows")
              case Some(other) =>
                throw new IllegalStateException(
                  s"sm8: warm query returned unexpected result type: ${other.getClass.getSimpleName}")
              case None =>
                throw new IllegalStateException("sm8: warm query returned no result")
            }
          case Left(err) =>
            throw new IllegalStateException(
              s"sm8: warm query FAILED: ${err.getClass.getSimpleName}: $err")
        }
        i += 1
      }
      val warmMs = (System.nanoTime() - t1) / 1000000
      Logger.info(s"  warm path: $Refreshes queries in ${warmMs}ms (~${warmMs / Refreshes}ms/query)")

      if (coldRows != warmRows) {
        throw new IllegalStateException(
          s"sm8: cold path returned $coldRows rows but warm path returned $warmRows")
      }
      if (coldNorm != warmNorm) {
        throw new IllegalStateException(
          "sm8: cold and warm results differ in VALUE (normalized row multisets differ)")
      }
      Logger.info(s"  value parity: cold and warm normalized row multisets are identical (${coldNorm.size} rows)")

      // Invalidation: bump the model version, watch the next miss.
      Logger.info("STEP 4: INVALIDATION — bump Model.version 1 -> 2; the next query MISSES")
      val modelV2 = buildSalesModel(version = 2)
      val requestV2 = request.copy(model = modelV2.name)
      val initialV2 = Context(
        stage = PipelineStage.Execute,
        request = io.sm8.core.engine.EngineHookRequest(
          model = modelV2,
          mcpRequest = requestV2,
          cacheKey = s"sales|v${modelV2.version}|region=product"),
        meta = Map("sm8.cache.policy" -> modelV2.defaultPolicies.cache)
      )
      val outV2 = runner.run(initialV2, { ctx =>
        warmProvider.query(modelV2, requestV2, EngineContext.defaultContext) match {
          case Right(pqr) => Right(ctx.copy(result = Some(io.sm8.core.engine.EngineHookResult(pqr))))
          case Left(err)  => Left(err)
        }
      })
      outV2 match {
        case Right(ctx) =>
          val n = ctx.result.collect {
            case r: io.sm8.core.engine.EngineHookResult => r.pqr.rows.size
          }.getOrElse(0)
          Logger.info(s"  v2 query after bump: $n rows (MISSED — new key domain)")
        case Left(err) =>
          throw new IllegalStateException(
            s"sm8: v2 query FAILED: ${err.getClass.getSimpleName}: $err")
      }

      // Counters summary.
      Logger.info("STEP 5: METRICS SUMMARY")
      Logger.info("=" * 70)
      Logger.info("Cache + plugin counters (the same counters a real deployment")
      Logger.info("exposes on `sm8-server --metrics-port`):")
      Logger.info(s"  CachePlugin.reads  = ${plugin.readFires.get}")
      Logger.info(s"  CachePlugin.writes = ${plugin.writeFires.get}")
      Logger.info(s"  CachePlugin.hits   = ${plugin.hits.get}")
      Logger.info(s"  CachePlugin.misses = ${plugin.misses.get}")
      val hitRatio = {
        val total = plugin.hits.get + plugin.misses.get
        if (total == 0) 0.0 else plugin.hits.get.toDouble / total
      }
      Logger.info(f"  hit ratio          = $hitRatio%.2f")
      Logger.info("=" * 70)
      Logger.info("Caching lifecycle complete: cold -> warm -> invalidate.")
    } catch {
      case t: Throwable =>
        Logger.error(s"sm8 caching-rollup-warmpath example FAILED: ${t.getClass.getSimpleName}: ${t.getMessage}")
        throw t
    } finally {
      spark.stop()
    }
  }
}
