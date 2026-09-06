/*
 * SM8 Spark Connector — RollupRefresher (Ticket 6 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md).
 *
 * The "one command rebuilds a model's rollups" surface: resolves a
 * model by name, then eagerly re-materializes EVERY rollup the
 * model declares (saveAsTable — the job runs; `Right` = durable).
 *
 * ==Why eager (not the v1 temp-view path)==
 *
 * A refresh command's whole point is durability: cron invokes the
 * CLI, the CLI calls the server trigger, the server calls this.
 * The Ticket 5 lazy temp view is session-scoped and dies with the
 * SparkSession — useless for cron. `persistCatalog` (Ticket 5)
 * runs the aggregation job before returning and writes a real
 * catalog table; failures surface as typed EngineErrors.
 *
 * ==Per-rollup isolation==
 *
 * One failed rollup does NOT abort the others: results are
 * collected per-rollup (name -> Either[error, table]) so a cron
 * run reports exactly which rollups refreshed and which failed —
 * fail-loud per rollup, never a silent partial refresh.
 *
 * ==Closure safety==
 *
 * Same contract as the materializer: driver-side built-in Column
 * aggregations only; nothing user-code ships to executors.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.EngineError
import io.sm8.core.model.Model

import org.apache.spark.sql.SparkSession

object RollupRefresher {

  /** The outcome of refreshing ONE rollup. */
  sealed trait RollupRefreshResult extends Product with Serializable
  object RollupRefreshResult {
    final case class Refreshed(rollup: String, table: String) extends RollupRefreshResult
    final case class Failed(rollup: String, error: EngineError) extends RollupRefreshResult
  }

  /** Refresh every rollup declared on the named model.
    *
    * @param spark       the session (table IO only — no closure
    *                    capture; see file header)
    * @param modelName   registered model name to look up
    * @param modelOf     resolver from name -> Model (the caller's
    *                    model source; the CLI/server wires its own)
    * @return per-rollup results in declaration order
    */
  def refreshModel(
      spark: SparkSession,
      modelName: String,
      modelOf: String => Option[Model]
  ): Either[EngineError, List[RollupRefreshResult]] =
    modelOf(modelName) match {
      case None =>
        Left(EngineError.UnsupportedCapability(
          engine = "spark-connector",
          capability = "RollupRefresher.model",
          message = s"model '$modelName' not found — cannot refresh rollups"))
      case Some(model) =>
        val results = model.rollups.map { spec =>
          RollupMaterializer.materialize(spark, model, spec, eager = true) match {
            case Right(table) => RollupRefreshResult.Refreshed(spec.name, table)
            case Left(e)      => RollupRefreshResult.Failed(spec.name, e)
          }
        }
        Right(results)
    }
}
