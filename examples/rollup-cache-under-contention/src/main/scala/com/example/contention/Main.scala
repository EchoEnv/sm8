package com.example.contention

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import io.sm8.core.cache.{CachedRowDecoder, MetricsRegistry}
import io.sm8.core.engine.{
  EngineContext, EngineError, EngineProvider, QueryRequest, ResultValue
}
import io.sm8.core.model.{CachePolicy, Model, ModelBuilder, ModelStatus, SourceRef}
import io.sm8.core.model.ModelValidator
import io.sm8.core.expr.ExprSugar._
import io.sm8.plugins.cache.{CachePlugin, InMemoryResultCache}
import io.sm8.sdk.{Context, HookRunner, HookStage, PipelineStage}

import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

/** sm8 rollup-cache-under-contention example — N concurrent dashboard
  * clients hit the SAME model with the SAME query; the read-through
  * cache's single-flight property collapses the thundering herd to ONE
  * engine execution per (model, version, request) key.
  *
  * Real-world shape: an ops dashboard auto-refreshes from 4 workers;
  * all 4 fire the same aggregate at the same instant; without
  * single-flight all 4 run the full engine concurrently. With it, ONE
  * runs and the other 3 block on the leader's future (the platform's
  * `getOrComputeJournaled` contract — `putIfAbsent` on the inflight
  * map, never `computeIfAbsent`, so the CHM bin lock is never held
  * during compute).
  *
  * Three phases:
  *
  *  1. THUNDERING HERD — 4 workers × 5 rounds = 20 concurrent queries
  *     against a cold cache. Exactly ONE engine execution is expected
  *     (per single-flight); the other 19 ride the leader's future.
  *  2. STEADY STATE — the same 20 queries against a warm cache. All
  *     HIT (no engine execution).
  *  3. VERSION ROLL mid-storm — bump `Model.version` to 2; the storm
  *     continues; the first v2 query MISSES and runs, the rest HIT on
  *     the new key domain.
  *
  * The example verifies the single-flight contract by observing the
  * engine-execution counter (incremented only inside the compute
  * thunk): the thundering-herd phase must show exactly 1 engine
  * execution (not 4), and the version-roll phase exactly 1 more.
  *
  * Layer discipline: consumer imports sm8-core + spark-connector +
  * cache-plugin (io.sm8.plugins). No sm8-platform import — the example
  * ships the same MinimalHookRunner shape proven in the
  * caching-rollup-warmpath example.
  */
object Main {

  private val Logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  /** Client threads hitting the model concurrently. */
  private val Workers = 4

  /** Rounds each worker issues (Workers × Rounds = total queries). */
  private val Rounds = 5

  /** Build the sales `Model` at a given version. Version participates
    * in the cache key domain — bumping it invalidates prior entries.
    */
  private def buildSalesModel(version: Int): Model =
    ModelBuilder()
      .withName("sales")
      .withVersion(version)
      .withSource(SourceRef.ByName(table = "sales_spark"))
      .withStatus(ModelStatus.Draft)
      .withDimensions(List(
        io.sm8.core.model.Dimension.field("region", "region")))
      .withMeasures(List(
        io.sm8.core.model.Measure.aggregate("total_amount", "amount".asField.sum)))
      .withPolicies(io.sm8.core.model.ModelPolicyDefaults(
        materialize = io.sm8.core.model.MaterializePolicy.None,
        cache       = CachePolicy.WriteThrough("contention"),
        audit       = io.sm8.core.model.AuditPolicy.NoAudit))
      .build match {
      case Right(m) => m
      case Left(err) =>
        throw new IllegalStateException(s"sm8: failed to build sales Model: $err")
      }

  /** Minimal consumer-side `HookRunner` (the single-stage sibling of
    * the platform's `HookRunnerOrchestration`; proven in the
    * caching-rollup-warmpath example). Honors `PostHook.runsOnStop`.
    */
  private final class MinimalHookRunner(hooks: io.sm8.core.HookManagerImpl)
      extends HookRunner {

    /** Fire PreExecute hooks in priority order, run the engine thunk
      * unless a pre-hook short-circuited, then fire PostExecute hooks.
      *
      * @param initial the starting Context (must carry `request` =
      *     EngineHookRequest and `meta("sm8.cache.policy")` — the
      *     consumer-side fold the platform's EngineService normally
      *     performs)
      * @param execute the engine thunk; skipped when a pre-hook sets
      *     `stop`
      * @return the final Context on success; the typed error on failure
      */
    override def run(
        initial: Context,
        execute: Context => Either[EngineError, Context]
    ): Either[EngineError, Context] = {
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
      if (!stopped) {
        execute(ctx) match {
          case Right(next) => ctx = next
          case Left(err)   => return Left(err)
        }
      }
      val postHooks = hooks.postHooksFor(HookStage.PostExecute)
      val pit = postHooks.iterator
      while (pit.hasNext) {
        val (hook, priority) = pit.next()
        if (stopped && !hook.runsOnStop) {
          // Mutator short-circuit: don't re-journal on a HIT.
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

  /** Normalize a PortableQueryResult as a sorted row multiset
    * (order-independent value comparison across engines and paths).
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

  /** One client "request": runs the full hooked path (PreExecute cache
    * lookup -> engine thunk on miss -> PostExecute write-through) for
    * one (model, request) pair, and returns the normalized row multiset.
    *
    * Engine executions are counted in `engineExecs` — this counter
    * lives INSIDE the compute thunk, so it only increments on a cache
    * MISS (the single-flight leader). HITs ride the leader's future.
    */
  private def clientQuery(
      runner: HookRunner,
      provider: EngineProvider,
      model: Model,
      request: QueryRequest,
      cache: InMemoryResultCache,
      engineExecs: AtomicInteger): Vector[String] = {
    val cacheKey = s"${model.name}|v${model.version}|region-aggregate"
    val initial = Context(
      stage = PipelineStage.Execute,
      request = io.sm8.core.engine.EngineHookRequest(
        model = model,
        mcpRequest = request,
        cacheKey = cacheKey),
      meta = Map("sm8.cache.policy" -> model.defaultPolicies.cache)
    )
    val out = runner.run(initial, { ctx =>
      // MISS path: use the single-flight read-through so concurrent
      // identical calls coalesce into ONE engine execution.
      val row = cache.getOrComputeJournaled(
        cacheKey,
        new java.util.function.Supplier[io.sm8.core.cache.RestateCachedRow] {
          /** The engine-compute thunk: runs the provider query and encodes
            * the result to the journaled form. Invoked exactly once per
            * cache key per single-flight window.
            *
            * @return the journaled form of the engine result
            */
          override def get(): io.sm8.core.cache.RestateCachedRow = {
            val n = engineExecs.incrementAndGet()
            Logger.info(s"  [engine-exec #$n] ${model.name} v${model.version} MISS -> running engine")
            provider.query(model, request, EngineContext.defaultContext) match {
              case Right(pqr) =>
                CachedRowDecoder.toRestateCachedRowFromPortable(pqr) match {
                  case Right(r) => r
                  case Left(err) =>
                    throw new IllegalStateException(
                      s"sm8: encode failed: ${err.getClass.getSimpleName}: $err")
                }
              case Left(err) =>
                throw new IllegalStateException(
                  s"sm8: engine FAILED: ${err.getClass.getSimpleName}: $err")
            }
          }
        })
      // Decode the journaled form back to a portable result so the
      // caller gets the same wire type either way (HIT or MISS).
      val pqr = CachedRowDecoder.fromRestateCachedRowAsPortable(row)
      Right(ctx.copy(result =
        Some(io.sm8.core.engine.EngineHookResult(pqr))))
    })
    out match {
      case Right(ctx) =>
        ctx.result.collect { case r: io.sm8.core.engine.EngineHookResult =>
          normalizedRows(r.pqr)
        }.getOrElse(throw new IllegalStateException("sm8: no result"))
      case Left(err) =>
        throw new IllegalStateException(
          s"sm8: query FAILED: ${err.getClass.getSimpleName}: $err")
    }
  }

  /** Lifecycle entry point: thundering herd (cold cache) -> steady
    * state (warm) -> version roll (new key domain) -> counters summary.
    * Exits 0 on success; throws on any typed engine error or on a
    * violated single-flight / worker-agreement invariant.
    *
    * @param args unused (mirrors the other examples' shape)
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sm8-rollup-cache-contention")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    try {
      Logger.info("=" * 70)
      Logger.info("sm8 rollup-cache-under-contention — single-flight under a query storm")
      Logger.info("=" * 70)

      // Ingest the CSV and declare the model.
      Logger.info("STEP 1: INGEST orders.csv; DECLARE the sales Model (WriteThrough)")
      val df = spark.read.option("header", "true").option("inferSchema", "true")
        .csv("data/orders.csv")
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

      val cache  = new InMemoryResultCache(maxEntries = 256)
      val plugin = new CachePlugin(cache)
      val engine = io.sm8.core.EngineFactory.create(Seq(plugin))
      val runner = new MinimalHookRunner(
        engine.hooks.asInstanceOf[io.sm8.core.HookManagerImpl])

      val request = QueryRequest(
        model = modelV1.name,
        dimensions = Seq("region"),
        measures = Seq("total_amount"))

      // Cold cache, 4 concurrent workers x 5 rounds.
      Logger.info(s"STEP 2: THUNDERING HERD — $Workers workers x $Rounds rounds = ${Workers * Rounds} concurrent queries against a COLD cache")
      val engineExecs = new AtomicInteger(0)
      val pool: ExecutorService = Executors.newFixedThreadPool(Workers)
      val expected: Vector[String] = Vector.empty
      try {
        val futures = scala.collection.mutable.ListBuffer[Future[Vector[String]]]()
        for (_ <- 1 to Rounds) {
          for (_ <- 1 to Workers) {
            futures += pool.submit(new Callable[Vector[String]] {
              /** One storm query against the cold cache. Called once per worker
                * per round (Workers × Rounds = 20 total). The single-flight
                * semantic of `InMemoryResultCache.getOrComputeJournaled` is
                * the only mechanism that collapses the 20 concurrent calls to
                * ONE engine execution.
                *
                * @return the normalized row multiset (compared across all
                *         workers via `distinct.size == 1`).
                */
              override def call(): Vector[String] =
                clientQuery(runner, provider, modelV1, request, cache, engineExecs)
            })
          }
          // Small stagger so the workers interleave; the herd still hits
          // the same key at roughly the same moment each round.
          Thread.sleep(50)
        }
        var allResults: List[Vector[String]] = Nil
        futures.foreach { f =>
          allResults = f.get(60, TimeUnit.SECONDS) :: allResults
        }
        pool.shutdown()
        pool.awaitTermination(30, TimeUnit.SECONDS)

        val execs = engineExecs.get
        Logger.info(s"  total queries: ${Workers * Rounds}")
        Logger.info(s"  engine executions: $execs (single-flight expected: 1)")
        Logger.info(s"  distinct result shapes: ${allResults.distinct.size} (all workers must agree)")

        if (execs != 1) {
          throw new IllegalStateException(
            s"sm8: single-flight violated — expected 1 engine execution, saw $execs")
        }
        if (allResults.distinct.size != 1) {
          throw new IllegalStateException(
            s"sm8: workers disagreed on the result (${allResults.distinct.size} distinct shapes)")
        }
        Logger.info("  single-flight contract holds: 1 execution, all workers agree")
      } finally {
        pool.shutdownNow()
      }

      // Warm cache, same storm.
      Logger.info(s"STEP 3: STEADY STATE — the same ${Workers * Rounds}-query storm against a WARM cache")
      val engineExecsWarm = new AtomicInteger(0)
      val warmPool: ExecutorService = Executors.newFixedThreadPool(Workers)
      try {
        val warmFutures = scala.collection.mutable.ListBuffer[Future[Vector[String]]]()
        for (_ <- 1 to Rounds) {
          for (_ <- 1 to Workers) {
            warmFutures += warmPool.submit(new Callable[Vector[String]] {
              /** Steady-state storm query — all HITs, no engine executions.
                *
                * @return the normalized row multiset (identical to the herd phase's result).
                */
              override def call(): Vector[String] =
                clientQuery(runner, provider, modelV1, request, cache, engineExecsWarm)
            })
          }
          Thread.sleep(50)
        }
        var warmAll: List[Vector[String]] = Nil
        warmFutures.foreach { f =>
          warmAll = f.get(60, TimeUnit.SECONDS) :: warmAll
        }
        warmPool.shutdown()
        warmPool.awaitTermination(30, TimeUnit.SECONDS)

        Logger.info(s"  engine executions: ${engineExecsWarm.get} (expected 0 — all HIT)")
        if (engineExecsWarm.get != 0) {
          throw new IllegalStateException(
            s"sm8: warm phase should be all HIT, saw ${engineExecsWarm.get} engine executions")
        }
        if (warmAll.distinct.size != 1) {
          throw new IllegalStateException(
            s"sm8: workers disagreed on the warm result (${warmAll.distinct.size} distinct shapes)")
        }
        Logger.info("  steady state holds: 0 executions, all workers agree")
      } finally {
        warmPool.shutdownNow()
      }

      // Mid-storm model version bump; new key domain.
      Logger.info("STEP 4: VERSION ROLL — bump Model.version 1 -> 2; storm continues")
      val modelV2 = buildSalesModel(version = 2)
      val requestV2 = request.copy(model = modelV2.name)
      val engineExecsV2 = new AtomicInteger(0)
      val v2Pool: ExecutorService = Executors.newFixedThreadPool(Workers)
      try {
        val v2Futures = scala.collection.mutable.ListBuffer[Future[Vector[String]]]()
        for (_ <- 1 to Rounds) {
          for (_ <- 1 to Workers) {
            v2Futures += v2Pool.submit(new Callable[Vector[String]] {
              /** Version-roll storm query — v1 cache miss + new key domain; ONE
                * execution expected, all workers HIT afterward.
                *
                * @return the normalized row multiset (identical to the herd
                *         phase's result if the data did not change).
                */
              override def call(): Vector[String] =
                clientQuery(runner, provider, modelV2, requestV2, cache, engineExecsV2)
            })
          }
          Thread.sleep(50)
        }
        var v2All: List[Vector[String]] = Nil
        v2Futures.foreach { f =>
          v2All = f.get(60, TimeUnit.SECONDS) :: v2All
        }
        v2Pool.shutdown()
        v2Pool.awaitTermination(30, TimeUnit.SECONDS)

        Logger.info(s"  engine executions: ${engineExecsV2.get} (single-flight expected: 1 — new key domain)")
        if (engineExecsV2.get != 1) {
          throw new IllegalStateException(
            s"sm8: v2 single-flight violated — expected 1 engine execution, saw ${engineExecsV2.get}")
        }
        if (v2All.distinct.size != 1) {
          throw new IllegalStateException(
            s"sm8: v2 workers disagreed on the result (${v2All.distinct.size} distinct shapes)")
        }
        Logger.info("  version roll holds: 1 execution on the new key domain, all workers agree")
      } finally {
        v2Pool.shutdownNow()
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
      val total = plugin.hits.get + plugin.misses.get
      val ratio = if (total == 0) 0.0 else plugin.hits.get.toDouble / total
      Logger.info(f"  hit ratio          = $ratio%.2f")
      Logger.info("=" * 70)
      Logger.info("Contention lifecycle complete: herd -> steady -> version roll.")
    } catch {
      case t: Throwable =>
        Logger.error(s"sm8 rollup-cache-contention example FAILED: ${t.getClass.getSimpleName}: ${t.getMessage}")
        throw t
    } finally {
      spark.stop()
    }
  }
}
