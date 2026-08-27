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
 *
 * P2 cluster (PR-176 NonFatal discipline by topic): both `realize` and
 * `realizeTyped` narrow their catches to `NonFatal` so JVM `Error`
 * subclasses (OOM, StackOverflow) propagate uncaught — matching the
 * discipline established at `EngineImpl.scala:50` + `EngineService.scala:258-264`.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineUrl, TypedRealizationProvider}

import scala.util.control.NonFatal

// Not `final`: the P2 cluster regression tests (sites 1 + 2) subclass
// this descriptor to override the `newSparkSession` seam and inject
// controlled `AnalysisException` / `OutOfMemoryError` failures,
// verifying the PR-176 NonFatal discipline narrowing. The descriptor
// remains an SPI entry point (ServiceLoader discovers it by class name).
class SparkEngineProviderDescriptor
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
 *
 * P2 cluster: catch is narrowed to `NonFatal` per PR-176 NonFatal
 * discipline (cite by topic). JVM `Error` subclasses (OOM, StackOverflow)
 * propagate uncaught — matching `EngineImpl.scala:50`. The legacy
 * silent-`None` semantics on `NonFatal` are preserved for backward
 * compatibility with PR-O4g callers; new code should use `realizeTyped`.
 */
 override def realize(url: String): Option[EngineProvider] =
 if (url == null || url.trim.isEmpty) None
 else try Some(new SparkEngineProvider(
  spark   = newSparkSession(url),
  bridge   = SparkTypeBridge,
  sparkEngineName = "spark-3.5",
  hookRunner  = None)) catch {
  case NonFatal(_) => None
 }

 /** P2 cluster (PR-176 NonFatal discipline by topic): test seam.
 * Exposed `protected[spark]` so regression tests in this package
 * can inject controlled exceptions (`AnalysisException`,
 * `OutOfMemoryError`) to verify the narrowing discipline without
 * depending on Spark's runtime master-resolution behavior.
 * Production behavior is unchanged: `SparkSession.builder().master(master).getOrCreate()`. */
 protected[spark] def newSparkSession(master: String): org.apache.spark.sql.SparkSession =
 org.apache.spark.sql.SparkSession.builder().master(master).getOrCreate()

 /** PR-15 (ADR-008-Q §C2): typed realization. Wraps
 * `SparkSession.builder().getOrCreate()` exceptions into typed
 * `ConnectionFailed` errors instead of silent `None`.
 *
 * P2 cluster (PR-176 NonFatal discipline by topic): the catch is
 * narrowed to `NonFatal(e)` so JVM `Error` subclasses (OOM,
 * StackOverflow) propagate to the caller — matching the
 * discipline established at `EngineService.executeEngine:258-264`
 * (which would otherwise convert any escaped NonFatal into
 * `ProviderInvocationFailed`). For the common case (an
 * `AnalysisException` from a bad master URL), the branch
 * preserves the cause class in the `message` string so operators
 * can diagnose root cause; this mirrors the narrowing pattern at
 * `MinimalRelOpLowerer.scala:194-200` (`NoSuchTableException` +
 * `AnalysisException` are the two specific Spark exceptions
 * surfaced as typed-Left).
 */
 override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
 parsedUrl match {
  case spark: EngineUrl.Spark =>
  try Right(new SparkEngineProvider(
   spark   = newSparkSession(spark.master),
   bridge   = SparkTypeBridge,
   sparkEngineName = "spark-3.5",
   hookRunner  = None)) catch {
   case NonFatal(e) =>
   val message = e match {
    case _: org.apache.spark.sql.AnalysisException =>
    s"sm8: Spark connector: AnalysisException from SparkSession.builder: ${e.getMessage}"
    case other =>
    s"sm8: Spark connector: ${other.getClass.getSimpleName}: ${other.getMessage}"
   }
   Left(EngineError.ConnectionFailed(
    engine = "spark",
    reason = "SparkSession.builder().getOrCreate() failed",
    message = message
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
