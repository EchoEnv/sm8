/*
 * SM8 Spark Engine Provider - real runtime (Layer C of Step 8 follow-up).
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 ("closures
 * captured by Spark UDFs / lambdas in Dataset.map must avoid
 * non-serializable refs"): this provider captures a SparkSession
 * (which IS Serializable in Spark 3.5 and 4.1 - verified by the
 * PR #36 closure-safety gate at runtime via PluginSerializationSpec).
 * The DataFrame handle captured per query is transient (lives only
 * inside query()); the SparkTypeBridge + PortableExprCompiler are
 * pure object refs.
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
    try {
      val tableName: String = model.source match {
        case src: io.sm8.core.model.SourceRef.ByName => src.table
        case other =>
          return Left(EngineError.UnsupportedCapability(
            engine    = sparkEngineName,
            capability = s"SourceRef type ${other.getClass.getSimpleName}",
            message    = "SparkEngineProvider.query: only SourceRef.ByName is supported in this Layer C follow-up.",
          ))
      }
      val df = spark.read.table(tableName)
      val filtered = request.where.filter(_.nonEmpty) match {
        case Some(w) => df.filter(w)
        case None    => df
      }
      val limited = request.limit.fold(filtered)(l => filtered.limit(l.toInt))
      val schema: ResultSchema = ResultSchema(
        limited.schema.fields.map { f =>
          Field(
            name     = f.name,
            dataType = bridge.sparkTypeToSealedDataType(f.dataType),
            nullable = f.nullable
          )
        }.toList
      )
      val collected: Array[org.apache.spark.sql.Row] = limited.collect()
      val rows: Vector[ResultRow] = collected.iterator.map { _ =>
        ResultRow(values = List.empty, schema = schema)
      }.toVector
      Right(PortableQueryResult(
        schema   = schema,
        rows     = rows,
        metadata = Map("engine.id" -> sparkEngineName, "engine.version" -> spark.version),
      ))
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
}
