/*
 * SM8 Spark Connector — ValidationProbes.
 *
 * Deployment-side closures that feed the QueryValidationService D2
 * (schema-drift detection) + D3 (compiled SQL preview) parameters.
 *
 * Layer placement: these closures live in the CONNECTOR (not the
 * platform) because only the connector knows how to talk to a
 * Spark catalog or drive a DataFrame compile. The platform sees
 * only the engine-portable shapes (`List[Field]`,
 * `Either[EngineError, String]`); sm8-server bridges the two via
 * reflective lookup (same pattern as the RollupRefresher bridge in
 * Main.scala — zero compile-time connector deps on the server).
 */
package io.sm8.connectors.spark

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField

import io.sm8.core.engine.EngineError
import io.sm8.core.model.{Model, SourceRef}
import io.sm8.core.schema.Field

/** D2 live-warehouse-schema probe.
  *
  * Answers "what columns does the connector see for the model's
  * source table right now?" via `spark.table(...).schema` — the
  * analyzer-resolved StructType, which carries real `DataType`s
  * (bridged through SparkTypeBridge) rather than the catalog's
  * lossy type strings. The platform freezes the returned list on
  * the validate service at boot; the service compares it against
  * the model's declared dimensions/measures to detect drift.
  */
object DeclaredSchemaProbe {

  /** Probe the live schema of the model's primary source table.
    *
    * @param spark the active SparkSession (catalog/table IO only;
    *              no data is read)
    * @param model the deployed model (only `source` is consulted)
    * @return      `Right` one Field per resolved column with the
    *              SparkTypeBridge mapping applied; `Left` a typed
    *              EngineError when the source isn't a ByName table
    *              or the table can't be resolved in the catalog.
    *              Never throws for expected failure modes.
    */
  def listFields(
      spark: SparkSession,
      model: Model
  ): Either[EngineError, List[Field]] =
    model.source match {
      case SourceRef.ByName(_, _, table) =>
        val attempted: Try[List[Field]] =
          Try(spark.table(table).schema.fields.toList.map { f: StructField =>
            Field(
              name     = f.name,
              dataType = SparkTypeBridge.sparkTypeToSealedDataType(f.dataType),
              nullable = f.nullable
            )
          })
        attempted.toEither.left.map { case NonFatal(e) =>
          EngineError.UnsupportedCapability(
            engine     = "spark",
            capability = "declared-schema-probe",
            message    = s"catalog probe failed for table '$table': " +
              s"${e.getClass.getSimpleName}: ${e.getMessage}"
          )
        }
      case other =>
        Left(EngineError.UnsupportedCapability(
          engine     = "spark",
          capability = "declared-schema-probe",
          message    = s"source shape '${other.getClass.getSimpleName}' has no " +
            "catalog table to probe"
        ))
    }
}

/** D3 compiled-SQL-preview probe.
  *
  * Compiles the model + validate-time request into the plan the
  * execute WOULD run and renders it as a string. **This is a Spark
  * logical/physical plan string, NOT SQL text** — Spark has no
  * first-class DataFrame-to-SQL emitter. The platform surfaces
  * this honestly (the field Scaladoc says "engine plan or SQL");
  * consumers should label it a preview, not runnable SQL.
  *
  * The compile reuses the provider's own compile path so the
  * preview reflects exactly what an execute would do (same
  * resolver, same rollup routing, same pushdown).
  */
object CompiledSqlProbe {

  /** Compile the model+request to a plan string.
    *
    * Uses a per-query session (same discipline as
    * `SparkEngineProvider.query`) so the probe neither leaks temp
    * views into the parent session nor sees stale ones; the clone
    * is never stopped (that would tear down the shared
    * SparkContext) — it goes out of scope and is GC-reclaimable.
    *
    * @param provider the boot-realized Spark provider (its captured
    *                 session + config drive the compile)
    * @param model    the deployed model
    * @param request  the validate-time request (dims/measures/where
    *                 the user actually asked about)
    * @param ctx      engine context (timeouts etc.)
    * @return         `Right` the formatted plan string; `Left` the
    *                 typed compile error (the platform maps this to
    *                 `compiledSql = None` — a preview gap is not a
    *                 validation failure).
    */
  def compile(
      provider: SparkEngineProvider,
      model: Model,
      request: io.sm8.core.engine.QueryRequest,
      ctx: io.sm8.core.engine.EngineContext
  ): Either[EngineError, String] = {
    val spark = provider.spark
    if (spark == null) {
      Left(EngineError.UnsupportedCapability(
        engine     = "spark",
        capability = "compiled-sql-preview",
        message    = "null-spark provider configuration has no session to compile against"
      ))
    } else {
      // Mirror query()'s per-query session pattern: fresh clone,
      // parent temp views copied, never stopped.
      val qs = spark.newSession()
      SparkEngineProvider.copyTempViews(spark, qs)
      provider.compileModelToDataFrame(model, request, ctx, qs) match {
        case Right(df) =>
          Try(df.queryExecution.explainString(
            org.apache.spark.sql.execution.ExplainMode.fromString("formatted")))
            .toEither.left.map { case NonFatal(e) =>
              EngineError.UnsupportedCapability(
                engine     = "spark",
                capability = "compiled-sql-preview",
                message    = s"plan render failed: ${e.getClass.getSimpleName}: ${e.getMessage}"
              )
            }
        case Left(err) => Left(err)
      }
    }
  }
}
