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
 * the descriptor only carries identity + realize(); the heavy provider
 * stays in SparkEngineProvider(spark).
 *
 * Serializable contract is upheld trivially (no fields).
 *
 * PR-15 (ADR-008-Q §C2): the descriptor now extends `TypedRealizationProvider`
 * (subtrait of `EngineProvider`) to provide typed-error realization.
 * The `realizeTyped` override wraps `SparkSession.builder().getOrCreate()`
 * exceptions into typed `ConnectionFailed` errors instead of silent `None`.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineUrl, TypedRealizationProvider}

final class SparkEngineProviderDescriptor
 extends TypedRealizationProvider {

 override lazy val identity: EngineIdentity =
 EngineIdentity(
  name     = "spark-3.5",
  nativeVersion  = "<uninitialized>",
  engineAdapterVersion = "0.1.0")

 override val available: Boolean = false

 /** PR-O4g (legacy): validate the URL before realizing. Returns
 * `None` on blank OR on `SparkSession.builder().getOrCreate()`
 * failure (silent). Use `realizeTyped` for typed-error realization.
 */
 override def realize(url: String): Option[EngineProvider] =
 if (url == null || url.trim.isEmpty) None
 else try Some(new SparkEngineProvider(
  spark   = org.apache.spark.sql.SparkSession.builder().master(url).getOrCreate(),
  bridge   = SparkTypeBridge,
  sparkEngineName = "spark-3.5",
  hookRunner  = None)) catch {
  case _: Throwable => None
 }

 /** PR-15 (ADR-008-Q §C2): typed realization. Wraps
 * `SparkSession.builder().getOrCreate()` exceptions into typed
 * `ConnectionFailed` errors instead of silent `None`.
 */
 override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
 parsedUrl match {
  case spark: EngineUrl.Spark =>
  try Right(new SparkEngineProvider(
   spark   = org.apache.spark.sql.SparkSession.builder().master(spark.master).getOrCreate(),
   bridge   = SparkTypeBridge,
   sparkEngineName = "spark-3.5",
   hookRunner  = None)) catch {
   case e: Throwable =>
   Left(EngineError.ConnectionFailed(
    engine = "spark",
    reason = "SparkSession.builder().getOrCreate() failed",
    message = s"sm8: Spark connector: ${e.getClass.getSimpleName}: ${e.getMessage}"
   ))
  }
  case other =>
  Left(EngineError.ConnectionFailed(
   engine = "spark",
   reason = "unexpected EngineUrl case for spark descriptor",
   message = s"sm8: Spark descriptor received non-Spark EngineUrl: ${other.getClass.getSimpleName}"
  ))
 }

 override def query(
  model: io.sm8.core.model.Model,
  request: io.sm8.core.engine.QueryRequest,
  ctx:  io.sm8.core.engine.EngineContext): Either[io.sm8.core.engine.EngineError, io.sm8.core.engine.PortableQueryResult] =
 Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "SparkEngineProviderDescriptor.query",
  message = "Descriptor carries no SparkSession; call realize(url) first."))

 override def explain(
  model: io.sm8.core.model.Model,
  request: io.sm8.core.engine.QueryRequest,
  ctx:  io.sm8.core.engine.EngineContext): Either[io.sm8.core.engine.EngineError, String] =
 Right(s"spark.explain(${model.name}): no SparkSession (descriptor)")
}

object SparkEngineProviderDescriptor {
 def identity: EngineIdentity =
 EngineIdentity(name = "spark-3.5", nativeVersion = "unknown", engineAdapterVersion = "0.1.0")
}
