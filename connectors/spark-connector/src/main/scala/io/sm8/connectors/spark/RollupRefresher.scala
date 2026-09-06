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

  /** JDK-typed refresh adapter for REFLECTIVE callers (the
    * sm8-server trigger closure has no compile dependency on this
    * module, so it cannot unwrap Scala `Either`/`List` types —
    * 2.13 projection APIs made that archaeology fragile). Returns
    * a plain JDK map:
    *
    *   - "ok":     java.lang.Boolean (true iff every rollup refreshed)
    *   - "error":  String or null (model-level failure, e.g. not found)
    *   - "results": java.util.List[java.util.Map[String,String]] with
    *     keys rollup / table / error (error empty = refreshed)
    *
    * Per-rollup isolation: one failure never blocks the others.
    *
    * @param spark   the session (table IO only)
    * @param modelName the model name to refresh
    * @param modelOf scala Function1 name -> Model (server supplies
    *                its boot-model resolver)
    * @return the JDK result map described above
    */
  def refreshModelJ(
      spark: SparkSession,
      modelName: String,
      modelOf: scala.Function1[String, Option[Model]]
  ): java.util.Map[String, Object] = {
    val out = new java.util.HashMap[String, Object]()
    refreshModel(spark, modelName, name => modelOf(name)) match {
      case Left(e) =>
        out.put("ok", java.lang.Boolean.FALSE)
        out.put("error", e.message)
      case Right(results) =>
        val allOk = results.forall {
          case _: RollupRefreshResult.Refreshed => true
          case _                                => false
        }
        out.put("ok", java.lang.Boolean.valueOf(allOk))
        val list = new java.util.ArrayList[java.util.Map[String, String]]()
        results.foreach {
          case RollupRefreshResult.Refreshed(r, t) =>
            val m = new java.util.HashMap[String, String]()
            m.put("rollup", r)
            m.put("table", t)
            m.put("error", "")
            list.add(m)
          case RollupRefreshResult.Failed(r, e) =>
            val m = new java.util.HashMap[String, String]()
            m.put("rollup", r)
            m.put("table", "")
            m.put("error", e.message)
            list.add(m)
        }
        out.put("results", list)
    }
    out
  }
}
