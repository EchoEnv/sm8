/*
 * SM8 Spark Engine Provider -- ServiceLoader descriptor (PR-O4g, ADR-008-O).
 *
 * Replaces the legacy `SparkEngineProvider` no-arg ctor's null-SparkSession
 * sentinel (architect review P1-3: risk for Restate journal-capture; null
 * SparkSession is a code smell for any Serializable class).
 *
 * ServiceLoader discovers THIS descriptor (no spark import = safe for
 * deserialization). Its `available = false` (no SparkSession to query);
 * its `realize(url)` constructs the real `SparkEngineProvider(url)` with
 * a live SparkSession via `SparkSession.builder().master(url).getOrCreate()`.
 *
 * Per [[karpathy-guidelines-mindset]]: "refuse needless abstractions" --
 * the descriptor only carries identity + realize(); the heavy provider
 * stays in SparkEngineProvider(spark).
 *
 * Per [[scala-jvm-safety-mindset]]: zero `null` sentinel anywhere;
 * Serializable contract is upheld trivially (no fields).
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineIdentity, EngineProvider}

final class SparkEngineProviderDescriptor
    extends EngineProvider {

  override lazy val identity: EngineIdentity =
    EngineIdentity(
      name                 = "spark-3.5",
      nativeVersion        = "<uninitialized>",
      engineAdapterVersion = "0.1.0",
    )

  override val available: Boolean = false

  /** PR-O4g: validate the URL before realizing. Per the connector
    * grammar (per-connector string contract): the URL must be a
    * non-blank Spark-compatible string. Blank input = None
    * (preserves the realize-blank contract).
    */
  override def realize(url: String): Option[EngineProvider] =
    if (url == null || url.trim.isEmpty) None
    else Some(new SparkEngineProvider(
      spark           = org.apache.spark.sql.SparkSession.builder().master(url).getOrCreate(),
      bridge          = SparkTypeBridge,
      sparkEngineName = "spark-3.5",
      hookRunner      = None,
    ))

  override def query(
      model:   io.sm8.core.model.Model,
      request: io.sm8.core.engine.QueryRequest,
      ctx:     io.sm8.core.engine.EngineContext,
  ): Either[io.sm8.core.engine.EngineError, io.sm8.core.engine.PortableQueryResult] =
    Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
      engine     = identity.name,
      capability = "SparkEngineProviderDescriptor.query",
      message    = "Descriptor carries no SparkSession; call realize(url) first.",
    ))

  override def explain(
      model:   io.sm8.core.model.Model,
      request: io.sm8.core.engine.QueryRequest,
      ctx:     io.sm8.core.engine.EngineContext,
  ): Either[io.sm8.core.engine.EngineError, String] =
    Right(s"spark.explain(${model.name}): no SparkSession (descriptor)")
}

object SparkEngineProviderDescriptor {
  def identity: EngineIdentity =
    EngineIdentity(name = "spark-3.5", nativeVersion = "unknown", engineAdapterVersion = "0.1.0")
}
