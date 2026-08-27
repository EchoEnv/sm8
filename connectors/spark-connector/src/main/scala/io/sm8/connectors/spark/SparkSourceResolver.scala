/*
 * SM8 Spark Connector -- SparkSourceResolver (PR-M3, closes ADR-008-L GAP 3).
 *
 * The concrete `SourceResolver` implementation for the spark-connector.
 * Bridges portable `SourceRef` -> live `ResolvedSource.Scan` (with the
 * actual `df.schema` mapped via `SparkTypeBridge`). Per RFC SS3:
 * this is adapter behavior -- the resolver knows about spark catalogs,
 * session-scoped views, and file paths; the core IR stays pure.
 *
 * - #1 (closure-safety): constructor-injected SparkSession; no
 *  companion state; `extends java.io.Serializable`.
 * - #3 (schema-drift verify at the boundary): the schema is the
 *  ACTUAL `df.schema` (resolves every Table/Path lazily), not a
 *  caller-supplied "expected schema". The downstream
 *  `ModelValidator.validateAgainstSchema` then verifies every
 *  Dimension/Measure/Filter references a real field.
 * - #5 (driver-vs-executor): `spark.table(.)` + `spark.read.`
 *  are driver-side catalog calls; no executor-side closure.
 *
 * Incompatible, UnsupportedCapability) surface the 4 failure modes
 * without throwing.
 *
 * one source-resolver + one registry, two reference impls.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity, ResolvedSource, SourceResolver}
import io.sm8.core.model.SourceRef
import io.sm8.core.schema.{Field, SealedDataType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import io.sm8.core.rel.TypedPredicate
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
// Not `final`: the P2 cluster regression tests (site 3) subclass
// this resolver to override the `loadTableByName` + `loadPathByPath`
// seams and inject controlled failures (e.g. `InterruptedException`),
// verifying the PR-176 NonFatal discipline narrowing. The resolver
// remains a `SourceResolver` trait implementation (interface-only).
class SparkSourceResolver(
 val spark: SparkSession,
 val registry: ModelRegistry = ModelRegistry.NoopModelRegistry) extends SourceResolver {

 import SparkSourceResolver._

 override def resolve(
  source: SourceRef,
  identity: EngineIdentity): Either[EngineError, ResolvedSource] = source match {
 case src: SourceRef.ByName =>
  resolveByName(src, identity)
 case src: SourceRef.ByPath =>
  resolveByPath(src, identity)
 case src: SourceRef.ByProvider =>
  Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "SourceRef.ByProvider",
  message = "SourceRef.ByProvider requires a registered ProviderRef closure (deferred to PR-M4)."))
 }

 override def resolveModel(
  name:  String,
  identity: EngineIdentity): Either[EngineError, SourceRef] =
 registry.resolveModel(name).left.map {
  case e: EngineError.UnsupportedCapability =>
  // Re-tag with the engine identity for diagnostics.
  e.copy(message = s"${e.message} (engine='${identity.name}')")
  case other => other
 }

 // -- private helpers --


private def resolveByName(
 src:  SourceRef.ByName,
 identity: EngineIdentity): Either[EngineError, ResolvedSource] = {
val tableName = src.table
// P2 cluster (PR-176 NonFatal discipline by topic): narrow the catch
// to `AnalysisException` (the specific Spark exception raised when a
// table is not in the active catalog). The narrow discipline mirrors
// `MinimalRelOpLowerer.scala:213-216` (the canonical narrowing example
// for spark-connector IO boundaries). Other `NonFatal` failures
// (e.g. Spark RPC faults, executor OOM-on-init) propagate to
// `EngineService.executeEngine:258-264`'s `NonFatal -> ProviderInvocationFailed`
// typed conversion; `Error` subclasses propagate to the caller per
// the PR-176 discipline.
val dfOpt: Option[org.apache.spark.sql.DataFrame] =
 try Some(loadTableByName(tableName)) catch {
 case _: org.apache.spark.sql.AnalysisException => None
 }
dfOpt match {
 case Some(df) =>
 try {
  val schema = scanSchema(df.schema.fields.toList)
  Right(ResolvedSource.Scan(source = src, schema = schema))
 } catch {
  case e: Exception =>
  Left(EngineError.UnsupportedCapability(
   engine  = identity.name,
   capability = "SourceRef.ByName.schema",
   message = s"Schema read for table '$tableName' failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))
 }
 case None =>
 Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "SourceRef.ByName.resolve",
  message = s"Spark table '$tableName' not found in the active catalog."))
}
}

private def resolveByPath(
 src:  SourceRef.ByPath,
 identity: EngineIdentity): Either[EngineError, ResolvedSource] = {
// P2 cluster (PR-176 NonFatal discipline by topic): same narrow as
// `resolveByName` above — catch only `AnalysisException` (the specific
// Spark exception raised by an unreadable / malformed path). The
// canonical example is `MinimalRelOpLowerer.scala:213-216`.
val dfOpt: Option[org.apache.spark.sql.DataFrame] =
 try Some(loadPathByPath(src.format, src.options, src.path)) catch {
 case _: org.apache.spark.sql.AnalysisException => None
 }
dfOpt match {
 case Some(df) =>
 try {
  val schema = scanSchema(df.schema.fields.toList)
  Right(ResolvedSource.Scan(source = src, schema = schema))
 } catch {
  case e: Exception =>
  Left(EngineError.UnsupportedCapability(
   engine  = identity.name,
   capability = "SourceRef.ByPath.schema",
   message = s"Schema read for path '${src.path}' failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))
 }
 case None =>
 Left(EngineError.UnsupportedCapability(
  engine  = identity.name,
  capability = "SourceRef.ByPath.resolve",
  message = s"Spark path read failed: ${src.format} @ ${src.path}"))
}
}
 /** Map a Spark StructField list to the portable `Field` (name +
 * SealedDataType + nullable). 
 * mantra #3 (schema-drift verify at the boundary): the types
 * match Spark's analysis-time types, not caller-supplied. */
 private def scanSchema(
  fields: List[StructField]): List[Field] = fields.map { f =>
 Field(
  name  = f.name,
  dataType = sparkTypeToSealedDataType(f.dataType),
  nullable = f.nullable)
}

// P2 cluster (PR-176 NonFatal discipline by topic): test seams.
// Exposed `protected[spark]` so the regression tests in this package
// can inject controlled failures (e.g. `InterruptedException`) without
// depending on Spark's runtime catalog / filesystem behavior.
// Production behavior is unchanged: `spark.table(name)` /
// `spark.read.format(fmt).options(opts).load(path)`.
protected[spark] def loadTableByName(name: String): org.apache.spark.sql.DataFrame =
 spark.table(name)
protected[spark] def loadPathByPath(
 format: String,
 options: Map[String, String],
 path: String): org.apache.spark.sql.DataFrame = {
 val reader = spark.read.format(format)
 options.foldLeft(reader)((acc, kv) => acc.option(kv._1, kv._2)).load(path)
}

 private val sparkTypeBridge = SparkTypeBridge
 private def sparkTypeToSealedDataType(
  t: org.apache.spark.sql.types.DataType
 ): SealedDataType = sparkTypeBridge.sparkTypeToSealedDataType(t)

 // PR-28 (ADR-008-R SSfilterPushdown): Catalyst-level filter pushdown.
 //
 // Implementations) + RFC SS3 (layer ownership): filter pushdown
 // is at the CONNECTOR layer -- the core IR stays pure (no spark
 // types in the typed predicate shape). This method is the
 // implementation of the protocol; the protocol shape is the
 // existing TypedPredicate (sm8-core/rel/).
 //
 // the user's explicit priority): the captured state in the
 // returned DataFrame is the DataFrame + the predicate columns
 // (both Serializable). The DataFrame itself is NOT captured
 // into any UDF closure (the pushdown happens at scan time,
 // NOT in any executor-side function).
 //
 // the predicate is built once via predicateToColumn (the
 // existing helper from PR-21). The typed Either returns the
 // actual DataFrame on success, or a typed EngineError on
 // failure (per SS1: typed
 // errors, never silent).
 //
 // for ByPath Parquet, the predicate is pushed into the
 // Parquet row-group filter at scan time. For ByName (catalog
 // tables / temp views), the predicate is pushed as a
 // df.filter(predicate) AFTER the table/view is resolved. For
 // ByProvider, no pushdown is attempted (deferred to a
 // follow-up).
 def resolveWithPushdown(
  source: SourceRef,
  filters: Seq[TypedPredicate[_]],
  identity: EngineIdentity): Either[EngineError, (ResolvedSource, org.apache.spark.sql.DataFrame)] = {
 if (filters.isEmpty) {
  // No filters -- no pushdown; the existing path is
  // backward-compatible (
  // SS3: zero behavior change for 19 callers).
  resolve(source, identity).map(s => (s, readSourceDF(source, identity)))
 } else {
  // Build a combined AND-of-all-predicates and apply it AT
  // THE SOURCE (per the user's "Catalyst pushdown" approval).
  val combinedColumnE: Either[EngineError, Column] =
  filters.foldLeft[Either[EngineError, Column]](Right(lit(true))) {
   (accE, pred) => for {
   acc <- accE
   col <- io.sm8.connectors.spark.PortableExprCompiler.predicateToColumn(pred.predicate)
   } yield acc && col
  }
  combinedColumnE.flatMap { combinedColumn =>
  // Resolve the source first (preserves the existing resolve
  // contract -- the schema is the actual df.schema).
  resolve(source, identity).map { resolved =>
   // Read the source DataFrame + apply the combined filter.
   // For Parquet: the predicate is pushed into the
   // Parquet row-group filter at scan time.
   val df0 = readSourceDF(source, identity)
   val df1 = df0.filter(combinedColumn)
   (resolved, df1)
  }
  }
 }
 }

 // PR-28: shared read helper (used by both resolve and
 // resolveWithPushdown).
 private def readSourceDF(
  source: SourceRef,
  identity: EngineIdentity): org.apache.spark.sql.DataFrame = source match {
 case src: SourceRef.ByName =>
  spark.table(src.table)
 case src: SourceRef.ByPath =>
  val reader = spark.read.format(src.format)
  src.options.foldLeft(reader)((acc, kv) => acc.option(kv._1, kv._2)).load(src.path)
 case _: SourceRef.ByProvider =>
  // Per RFC SS3: ByProvider requires a registered ProviderRef
  // closure. The SourceFilterPushdown for ByProvider is
  // deferred.
  spark.emptyDataFrame
 }
 }
object SparkSourceResolver {

 /** P2 cluster (PR-176 NonFatal discipline by topic): a
 * `SourceResolver` that returns a typed `Left(UnsupportedCapability)`
 * for every call. Used when the underlying `SparkSession` is null
 * (the supported null-spark provider configuration, exercised by
 * `SparkEngineProvider.lazy val resolver` and
 * `compileModelToDataFrame`'s null-querySession path).
 *
 * Pre-P2 the call sites constructed `new SparkSourceResolver(null, ...)`
 * and relied on the broad `case _: Exception => None` swallow to turn
 * the resulting NPE into a typed `UnsupportedCapability` — silently
 * absorbing any NonFatal. Post-P2 the narrow catch
 * (`case _: AnalysisException => None`) lets the NPE escape. This
 * factory keeps the null-spark contract observable (typed Left) while
 * preserving the PR-176 NonFatal discipline at every IO boundary.
 */
 def nullSparkResolver(engineName: String): io.sm8.core.engine.SourceResolver =
 new io.sm8.core.engine.SourceResolver {
  override def resolve(
   source: SourceRef,
   identity: EngineIdentity): Either[EngineError, ResolvedSource] =
   Left(EngineError.UnsupportedCapability(
    engine  = engineName,
    capability = "SparkSourceResolver.nullSparkResolver",
    message = "No SparkSession available (null-spark provider configuration)."))
  override def resolveModel(
   name:  String,
   identity: EngineIdentity): Either[EngineError, SourceRef] =
   Left(EngineError.UnsupportedCapability(
    engine  = engineName,
    capability = "SparkSourceResolver.nullSparkResolver.resolveModel",
    message = "No SparkSession available (null-spark provider configuration)."))
 }

 /** Engine-portable registry that maps a model name to a
 * `SourceRef.ByName` pointing at the active Spark catalog /
 * session-scoped view. The natural v1 mapping for single-cluster
 * deployments -- every model is a table or temp view. */
 object SessionCatalogModelRegistry extends ModelRegistry {
 override def resolveModel(name: String): Either[EngineError, SourceRef] =
  Right(SourceRef.ByName(catalog = Some("default"), namespace = None, table = name))
 }
}
