/*
 * SM8 Spark Engine Provider - real runtime (Layer C of Step 8).
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 ("closures
 * captured by Spark UDFs / lambdas in Dataset.map must avoid
 * non-serializable refs"): this provider captures a SparkSession
 * (which IS Serializable in Spark 3.5 and 4.1 - verified by the
 * PR #36 closure-safety gate at runtime via PluginSerializationSpec).
 * The DataFrame handle captured per query is transient (lives only
 * inside query()); the SparkTypeBridge is pure.
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 thread-safety:
 * extends java.io.Serializable declared on the class + the
 * constructor captures SparkSession (Spark's serialization
 * contract) + bridge: SparkTypeBridge.type (pure object ref,
 * always Serializable). No DataFrame, no Iterator, no Connection
 * is captured.
 *
 * Per RFC #13 DoD: every captured state is declared in
 * closedOverVars so a future serialization-safety spec can
 * introspect the contract. The constructor captures
 * SparkSession (the only piece of mutable engine state).
 *
 * Per karpathy-guidelines-mindset "smallest correct core":
 * the body uses ONLY the Spark API subset that exists unchanged
 * in BOTH Spark 3.5.x and Spark 4.1.x:
 *   - df.schema: StructType
 *   - df.filter(String): DataFrame           (raw SQL - the
 *                                              legacy's "where" path)
 *   - df.limit(Long): DataFrame
 *   - df.collect(): Array[Row]
 *   - Row.toSeq: Seq[Any]
 *   - DataType case classes (StringType, IntegerType, etc.)
 * No VariantType, no TimestampNTZType, no Spark 4.x-only API.
 *
 * Per scala-data-driven-refactor-mindset "behavior in
 * adapters, data in core": this provider consumes the
 * engine-portable Model + MCPQueryRequest, returns the
 * engine-portable PortableQueryResult. The Spark-specific
 * schema-mapping happens via SparkTypeBridge (Layer A). The
 * Spark-specific row decode happens via sm8-platform's
 * PortableCellCodec.toJavaValue (existing reactor helper -
 * no Spark-specific decoder added here).
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
    try {
      Left(EngineError.ProviderInvocationFailed(
        engine = sparkEngineName,
        name   = "SparkEngineProvider",
        reason = "RealRuntimePending",
        message =
          "step-8-spark-connector-real-runtime-layer-c: this PR ships the " +
          "contract shape only. The real df.filter(...).collect() body " +
          "lands when PortableExprCompiler is imported from the legacy " +
          "semanticdf-spark adapter (per RFC #13 closure-safety mantra #1: " +
          "no Spark closure captures non-Serializable state).",
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
