/*
 * SM8 Spark connector — shared adapter conformance suite.
 *
 * Extends the unified `AdapterConformanceSpec` from sm8-core's
 * test-jar. Spark's master URI is free-form (`local[*]`, `spark://`,
 * `spark-connect://`, `yarn`, …) — realize rejects only blank/null
 * input, so the grammar-invalid branch is vacuous here (empty
 * `invalidUrls`); blank/null rejection still applies.
 *
 * The determinism check runs against a LIVE `local[*]` SparkSession
 * with a registered temp view: it exercises the full compile→execute
 * path on a real session. Typed-column closure-safety at the decode
 * boundary is covered separately by SparkEngineProviderReplaySafetySpec
 * and the big-data scale spec; this suite pins the determinism and
 * typed-error contract, not the decode path.
 *
 * Session-sharing note: this spec's `lazy val spark` shares the
 * underlying SparkContext with the other spark-connector specs in
 * one JVM, but is itself a distinct SparkSession. The temp view it
 * registers is visible to the descriptor's realized provider through
 * the per-query `copyTempViews` handoff. If another suite registered
 * the same view name first, `createOrReplaceTempView` makes this
 * spec's registration authoritative.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineUrl, QueryRequest, TypedRealizationProvider}
import io.sm8.core.model.{Model, ModelStatus, SourceRef}
import io.sm8.sdk.contract.AdapterConformanceSpec

import org.apache.spark.sql.{RowFactory, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class SparkAdapterConformanceSpec extends AdapterConformanceSpec {

  // Use a dedicated SparkSession for the conformance determinism check.
  private[connectors] lazy val spark: SparkSession =
    SparkSession.builder().master("local[*]").appName("tConformance").getOrCreate()

  /** The ServiceLoader descriptor under test (URL-grammar + live-session realize).
    *
    * @return the spark descriptor
    */
  override def descriptor: TypedRealizationProvider = new SparkEngineProviderDescriptor()

  /** Wire-stable name matching [[SparkEngineConstants]].
    *
    * @return "spark"
    */
  override def wireName: String = "spark"

  /** The local master — always realizable in a test JVM.
    *
    * @return "local[*]"
    */
  override def validUrl: String = "local[*]"

  /** Spark master URIs are free-form (`local[*]`, `spark://`,
    * `spark-connect://`, `yarn`, …); `SparkSession.builder().master(_)`
    * accepts non-Spark strings too, so the grammar-invalid branch is
    * empty. Realize only rejects blank/null, and `None` on a
    * `NonFatal` session-creation failure (fatal `Error`s propagate
    * to the caller — a narrowing catch).
    *
    * @return empty — no rejectable grammar
    */
  override def invalidUrls: Seq[String] = Seq.empty

  /** Trino URL is foreign to this engine.
    *
    * @return a Trino EngineUrl the spark descriptor must reject
    */
  override def foreignEngineUrl: EngineUrl = EngineUrl.Trino("jdbc:trino://localhost:8080")

  /** A (model, request) pair referencing a temp view registered in
    * this spec's `beforeAll`-style setup. The view is registered
    * lazily so it exists exactly once per test run. */
  override def wellFormedQuery = {
    // Register a tiny temp view the model can resolve.
    val schema = new StructType(Array(
      StructField("id",   IntegerType, nullable = false),
      StructField("name", StringType,  nullable = true),
    ))
    val rows = Seq(
      RowFactory.create(1: java.lang.Integer, "alice"),
      RowFactory.create(2: java.lang.Integer, "bob"),
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.createOrReplaceTempView("conformance_people")

    val model = Model.of(
      name    = "conformance-model",
      version = 1,
      source  = SourceRef.ByName(table = "conformance_people"),
      status  = ModelStatus.Draft,
      dimensions = Nil,
      measures   = Nil
    ).toOption.get

    (model, QueryRequest(model = model.name))
  }
}
