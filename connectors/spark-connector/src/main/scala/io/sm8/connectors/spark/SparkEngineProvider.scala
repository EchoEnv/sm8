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
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession

final class SparkEngineProvider(
    val spark:           SparkSession,
    val bridge:          SparkTypeBridge.type,
    val sparkEngineName: String = "spark-3.5"
) extends MCPEngineProvider {

  /** No-arg constructor for Java ServiceLoader discovery.
    *
    * Produces the contract-gap stub (`spark = null`, `available = false`)
    * so the class is loaded by ServiceLoader without a SparkSession. The
    * production wiring (Main) constructs the real provider with a live
    * SparkSession and replaces the stub. Per RFC §3 the engine is the
    * only piece that knows about SparkSession; the descriptor here is
    * a pure-data presence marker.
    */
  def this() = this(null, SparkTypeBridge, "spark-3.5")

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

  override val available: Boolean = spark != null

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
    // Per scala-spark-batch-bugs-mindset mantra #5 (driver vs
    // executor asymmetry): every step in this for-comprehension
    // runs in the driver process. compile() builds the typed
    // DataFrame (lazy); filter/limit are typed transforms; collect()
    // materializes rows to the driver; decodeRow/decodeCell build
    // portable ResultValue carriers. No driver-side resource leaks
    // to executors; no executor-side closures.
    val pipeline: Either[EngineError, PortableQueryResult] = for {
      // 1. compile the model: source + filters + dimension projection.
      //    Per scala-spark-batch-bugs-mindset mantra #3 (schema-drift
      //    verify at the boundary): the schema comes from the actual
      //    compiled DataFrame.schema, not caller-supplied dimensions.
      compiled <- new PortableQueryCompiler(spark).compile(model, ctx)
      // 2. apply the request-level where clause (raw SQL path
      //    from MCPQueryRequest) on top of the compiled model.
      //    Per scala-spark-batch-bugs-mindset mantra #5: the
      //    .filter(w) here runs in the driver (it builds a Column
      //    expression, not a UDF); collect() below materializes.
      filtered = request.where.filter(_.nonEmpty) match {
        case Some(w) => compiled.filter(w)
        case None    => compiled
      }
      // 3. apply the request-level limit
      limited  = request.limit.fold(filtered)(l => filtered.limit(l.toInt))
      // 4. materialize: schema + collect() + per-row decode.
      //    Per scala-spark-batch-bugs-mindset mantra #5: collect()
      //    runs in the driver. ResultRow construction happens here.
      schema = ResultSchema(
        limited.schema.fields.map { f =>
          Field(
            name     = f.name,
            dataType = bridge.sparkTypeToSealedDataType(f.dataType),
            nullable = f.nullable
          )
        }.toList
      )
      collected: Array[org.apache.spark.sql.Row] = limited.collect()
      rows: Vector[ResultRow] = collected.iterator.map { row =>
        ResultRow(values = decodeRow(row, schema), schema = schema)
      }.toVector
    } yield PortableQueryResult(
      schema   = schema,
      rows     = rows,
      metadata = Map("engine.id" -> sparkEngineName, "engine.version" -> spark.version),
    )
    try {
      pipeline
    } catch {
      case e: Exception =>
        e match {
          case _: org.apache.spark.sql.AnalysisException =>
            Left(EngineError.ProviderInvocationFailed(
              engine = sparkEngineName,
              name   = "SparkEngineProvider",
              reason = "SparkAnalysisException",
              message = s"${e.getClass.getSimpleName}: ${e.getMessage}",
            ))
          case _ =>
            Left(EngineError.ConnectionFailed(
              engine  = sparkEngineName,
              reason  = s"${e.getClass.getSimpleName}: ${e.getMessage}",
              message = s"non-AnalysisException runtime failure: ${e.getMessage}",
            ))
        }
    }
  }

  override def explain(
      model:   Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String] =
    Right(s"spark.explain(${model.name}): engine=${sparkEngineName} version=${if (spark != null) spark.version else "<uninitialized>"}")

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
