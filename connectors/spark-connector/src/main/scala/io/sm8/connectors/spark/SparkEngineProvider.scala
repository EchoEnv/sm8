/*
 * SM8 Spark Engine Provider - real runtime (Layer C of Step 8 follow-up + PR #41 Model.compile port).
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 ("closures
 * captured by Spark UDFs / lambdas in Dataset.map must avoid
 * non-serializable refs"): this provider captures a SparkSession
 * (which IS Serializable in Spark 3.5 and 4.1 - verified by the
 * PR #36 closure-safety gate at runtime via PluginSerializationSpec).
 * The DataFrame handle captured per query is transient (lives only
 * inside query()); the SparkTypeBridge + PortableExprCompiler are
 * pure object refs.
 *
 * Per scala-jvm-safety-mindset mantra #3 (long-lived state): the
 * captured compiler is created per query (no static / ThreadLocal
 * state). The SparkSession ref is constructor-frozen.
 *
 * Per scala-spark-batch-bugs-mindset mantra #5 (driver vs executor
 * asymmetry): the `compile(model, ctx)` and `collect()` calls
 * both run in the driver process. No driver-side resources leak
 * to executors. ResultRow construction happens in the driver.
 *
 * Per scala-perf-testing-mindset mantra #2 (isolate the hot path):
 * the compile path (PortableQueryCompiler) and the per-row decode
 * path (decodeRow/decodeCell) are separated so future profiling
 * can attribute cost cleanly.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{
  EngineContext,
  EngineError,
  MCPEngineProvider,
  MCPQueryRequest,
  PortableQueryResult,
  ResultRow,
  ResultSchema,
  ResultValue
}
import io.sm8.core.model.Model
import io.sm8.core.query.QueryBuilder
import io.sm8.core.rel.RelOpPlanPrinter
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession

final class SparkEngineProvider(
    val spark:           SparkSession,
    val bridge:          SparkTypeBridge.type,
    val sparkEngineName: String = "spark-3.5",
    val hookDispatcher: Option[io.sm8.core.engine.HookRunner] = None
) extends MCPEngineProvider {



  /**
    * Real-runtime constructor (Phase 4 — Driver-side Spark Connect).
    *
    * Builds a SparkSession via
    * `SparkSession.builder().master(masterUrl).getOrCreate()`. The url
    * is a plain string — it can be:
    *   - a classic Spark cluster URL: `spark://host:7077`
    *   - a local-mode URL: `local[*]` (driver-side only)
    *   - a Spark Connect URL: `spark-connect://host:port` (Spark 3.4+)
    *
    * Per RFC §3: the connector is the ONLY piece that imports
    * `org.apache.spark.*`. The platform holds only a string.
    *
    * Used by Main's reflection: platform finds the discovered stub
    * (created via the no-arg ctor) and replaces it with the real one
    * via this ctor.
    */
  def this(masterUrl: String) =
    this(
      SparkSession.builder().master(masterUrl).getOrCreate(),
      SparkTypeBridge,
      "spark-3.5"
    )

  /**
    * Typed URL realization (PR-B per RFC `adapters.md` Rule 4).
    *
    * Builds a real `SparkEngineProvider` connected to the given
    * URL via `SparkSession.builder().master(url).getOrCreate()`.
    * The URL accepts any Spark master URL:
    *   - classic cluster: `spark://host:7077`
    *   - local mode:     `local[*]` (driver-side only)
    *   - Spark Connect:  `spark-connect://host:port` (Spark 3.4+)
    *
    * Per RFC §3: the connector is the ONLY piece that knows about
    * `SparkSession`. The platform and the deployment module hold
    * only the string.
    *
    * @return `Some(realizedProvider)` on success; `None` if the
    *         URL is blank (per-connector grammar: non-blank
    *         Spark master URL required)
    */
  override def realize(url: String): Option[MCPEngineProvider] =
    if (url == null || url.trim.isEmpty) None
    else Some(new SparkEngineProvider(url))


  override lazy val identity: io.sm8.core.engine.EngineIdentity =
    io.sm8.core.engine.EngineIdentity(
      name                 = sparkEngineName,
      nativeVersion        = if (spark != null) spark.version else "<uninitialized>",
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
  //   1. Resolve the model's primary source via `SparkSourceResolver`
  //      (PR-M3) -- brings the live `df.schema` into scope.
  //   2. Run `ModelValidator.validateAgainstSchema` (PR-M2) on the
  //      model + resolved schema. Fail-loud typed `SchemaValidation`
  //      on unknown-field references.
  //   3. Run `QueryBuilder.build` (PR-L) to lower Model -> RelOp.
  //      Cycle detection runs here (built-in, no extra wiring).
  //   4. Apply the existing pipeline: compile the RelOp via
  //      `PortableQueryCompiler.compileRelOp` (new in PR-M4), then
  //      request-level `where` + `limit` + `collect` + `decode`.
  //
  // Per [[scala-error-handling-mindset]]: every step's failure
  // surfaces as a typed `EngineError` -- no silent defaults.
  //
  // PR-M4 (GAP 6): hook dispatch. The dispatcher is OPTIONAL --
  // `None` means no plugin hooks fire (the default for the
  // bare-deploy shape). Production deployments inject a real
  // `EngineHookDispatcher` via the new constructor parameter.
  override def query(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    if (spark == null) {
      return Left(EngineError.ConnectionFailed(
        engine  = sparkEngineName,
        reason  = "SparkSession is null",
        message = "SparkEngineProvider.query called with null SparkSession",
      ))
    }
    // PR-M4 (GAP 5 -- the most critical): the IR-extension path
    // (PR-H/I/J/K/L) is now LIVE in production. Steps:
    //   1. Resolve the primary source via `SparkSourceResolver` (PR-M3).
    //   2. Run `ModelValidator.validateAgainstSchema` (PR-M2) -- fail-loud
    //      typed `SchemaValidation` on unknown-field references.
    //   3. Lower Model -> RelOp via `QueryBuilder.build` (PR-L). Cycle
    //      detection runs here (built-in).
    //   4. Compile the RelOp via `PortableQueryCompiler.compileRelOp`.
    //   5. PR-M4 (GAP 6): wrap the build+compile step in the bound
    //      `HookRunner` if one is configured. None = no hooks fire.
    //   6. Apply request-level where + limit + collect + decode.
    //
    // The compile steps are factored into a thunk; the for-comp
    // returns the final DataFrame; the dispatching code wraps that
    // thunk with the optional HookRunner.
    val resolver = new SparkSourceResolver(spark)
    val compileSteps: io.sm8.core.engine.EngineContext => Either[EngineError, org.apache.spark.sql.DataFrame] = { eCtx =>
      for {
        resolved <- resolver.resolve(model.source, identity)
        scan     <- resolved match {
                     case s: io.sm8.core.engine.ResolvedSource.Scan => Right[EngineError, io.sm8.core.engine.ResolvedSource.Scan](s)
                     case _ => Left(EngineError.UnsupportedCapability(
                              engine = sparkEngineName,
                              capability = "SourceResolver.resolve",
                              message = s"non-Scan resolution (deferred to PR-M4 full RelOp path)"))
                   }
        _        <- io.sm8.core.model.ModelValidator.validateAgainstSchema(model, scan)
                    .left.map(e => EngineError.UnsupportedCapability(
                      engine = sparkEngineName,
                      capability = "ModelValidator.validateAgainstSchema",
                      message = e.message))
        relOp    <- QueryBuilder.build(model, resolver, identity)
        df       <- new PortableQueryCompiler(spark).compileRelOp(relOp, eCtx)
      } yield df
    }
    val compiled: Either[EngineError, org.apache.spark.sql.DataFrame] = hookDispatcher match {
      case Some(hr) =>
        try hr.run[org.apache.spark.sql.DataFrame](ctx, compileSteps)
        catch {
          case e: RuntimeException => Left(EngineError.CancellationFailed(
            engine = sparkEngineName,
            reason  = "hook-throw",
            message = s"Hook dispatcher threw: ${e.getMessage}",
          ))
        }
      case None => compileSteps(ctx)
    }

    // PR-M4 (GAP 7 -- already wired in PortableQueryCompiler):
    // `applyGroupByAgg` now applies `calculatedMeasures` via
    // `withColumn` AFTER the agg. The pipeline below applies the
    // request-level where + limit + collect + decode. Per
    // [[scala-spark-batch-bugs-mindset]] mantras #1 + #5: the
    // `.filter` + `.limit` + `collect` are driver-side; no
    // executor-side closure capture.
    compiled.flatMap { limited =>
      val filtered = request.where.filter(_.nonEmpty) match {
        case Some(w) => limited.filter(w)
        case None    => limited
      }
      val withLimit = request.limit.fold(filtered)(l => filtered.limit(l.toInt))
      val schema = ResultSchema(
        withLimit.schema.fields.map { f =>
          Field(
            name     = f.name,
            dataType = bridge.sparkTypeToSealedDataType(f.dataType),
            nullable = f.nullable
          )
        }.toList
      )
      // PR-O4e (ADR-008-O): MaterializePolicy.Persist -> paired
      // unpersist at query boundary. After .collect() the result rows
      // are already in-memory in `collected`; the persisted Spark
      // form is then unpersisted to free executor cache. The
      // cache-plugin InMemoryResultCache keeps the per-query
      // answer for its own TTL. The Spark-level persist() then
      // unpersist() is opt-in; without MaterializePolicy.Persist
      // the DF was never persisted so this is a no-op.
      val wasPersisted = !withLimit.storageLevel.equals(
        org.apache.spark.storage.StorageLevel.NONE
      )
      if (wasPersisted) try withLimit.persist() catch { case _: Throwable => () }
      val collected: Array[org.apache.spark.sql.Row] = withLimit.collect()
      if (wasPersisted) try withLimit.unpersist() catch { case _: Throwable => () }
      val rows: Vector[ResultRow] = collected.iterator.map { row =>
        ResultRow(values = decodeRow(row, schema), schema = schema)
      }.toVector
      Right(PortableQueryResult(
        schema   = schema,
        rows     = rows,
        metadata = Map(
          "engine.id"      -> sparkEngineName,
          "engine.version" -> (if (spark != null) spark.version else "<uninitialized>"),
          "ir.path"        -> "pr-m4",
        ),
      ))
    }
  }
  /** PR-N1: walk the produced `RelOp` tree via the engine-portable
    * `QueryBuilder` + the core `RelOpPlanPrinter`. Output is a
    * multi-line indented plan: 1 header line (model name + engine
    * identity) + 1 line per RelOp node. Per RFC §3 ownership the
    * IR building + plan serialisation are core; this method only
    * glues them together with the Spark-specific resolver + identity.
    *
    * Per [[scala-error-handling-mindset]]: a `Left` from
    * `QueryBuilder.build` (cycle in calculatedMeasures, unresolved
    * source, etc.) is rendered as the plan prefix plus a typed error
    * line -- never a thrown exception.
    */
  override def explain(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String] = {
    val header =
      s"=== SM8 Plan: ${model.name} | engine=${sparkEngineName} version=${identity.nativeVersion} ==="
    QueryBuilder.build(model, resolver, identity) match {
      case Right(relOp) =>
        Right(header + "\n" + RelOpPlanPrinter.print(relOp))
      case Left(err) =>
        Right(header + "\n" + s"<<build failed: ${err.getClass.getSimpleName}: ${err.toString}>>")
    }
  }

  // PR-N1: the resolver used by `explain` to produce the IR tree.
  // Per [[karpathy-guidelines-mindset]] "smallest correct change":
  // the same model registry / spark session as the compiler's path.
  private lazy val resolver: io.sm8.core.engine.SourceResolver = {
    val registry = sparkSourceRegistry.getOrElse(io.sm8.connectors.spark.ModelRegistry.NoopModelRegistry)
    new SparkSourceResolver(spark, registry)
  }
  private lazy val sparkSourceRegistry: Option[io.sm8.connectors.spark.ModelRegistry] = None

  /** Decode a Spark `Row` to a `List[ResultValue]` aligned with `schema.fields`.
    *
    * Per scala-perf-testing-mindset mantra #3 (count allocations):
    * the row is a flat seq; no nested walker. The `while` loop
    * preallocates a single Array of size n and converts to List
    * once at the end.
    *
    * Per scala-error-handling-mindset: a null Spark cell becomes
    * `ResultValue.NullV` (never throws NPE on the boundary).
    */
  private def decodeRow(
      row:    org.apache.spark.sql.Row,
      schema: ResultSchema,
  ): List[ResultValue] = {
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
      cell:     AnyRef,
      dataType: SealedDataType,
  ): ResultValue = {
    if (cell == null) return ResultValue.NullV
    dataType match {
      case SealedDataType.Boolean =>
        cell match {
          case b: java.lang.Boolean => ResultValue.BoolV(b)
          case _                    => ResultValue.NullV
        }
      case SealedDataType.Int | SealedDataType.BigInt =>
        cell match {
          case n: Number => ResultValue.IntV(n.longValue)
          case _         => ResultValue.NullV
        }
      case SealedDataType.Double =>
        cell match {
          case n: Number => ResultValue.DoubleV(n.doubleValue)
          case _         => ResultValue.NullV
        }
      case SealedDataType.Decimal(_, _) =>
        cell match {
          case d: java.math.BigDecimal => ResultValue.DecimalV(d)
          case s: String               => ResultValue.DecimalV(BigDecimal(s))
          case _                       => ResultValue.NullV
        }
      case SealedDataType.Varchar =>
        cell match {
          case s: String => ResultValue.StringV(s)
          case _         => ResultValue.StringV(cell.toString)
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
