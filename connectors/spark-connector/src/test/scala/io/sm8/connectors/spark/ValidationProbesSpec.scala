/*
 * SM8 Spark Connector — ValidationProbes spec.
 *
 * D2+D3 probe acceptance against a local Spark session: the
 * declared-schema probe must reflect the REAL resolved schema of a
 * registered temp view (StructType → Field via SparkTypeBridge),
 * and the compiled-SQL probe must render a non-empty plan string
 * through the same compile path an execute uses.
 *
 * Session discipline: one shared test SparkSession (local[*]); the
 * temp views registered here are test-owned and torn down in
 * afterAll.
 */
package io.sm8.connectors.spark

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.engine.{EngineContext, EngineError, QueryRequest}
import io.sm8.core.model.{AuditPolicy, CachePolicy, MaterializePolicy, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.SealedDataType

class ValidationProbesSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().master("local[*]").appName("validation-probes-spec").getOrCreate()
    val implicits = spark.implicits
    import implicits._
    Seq((1, "eu", java.sql.Date.valueOf("2026-01-01")))
      .toDF("amount", "region", "event_date")
      .createOrReplaceTempView("events_probe")
  }

  override def afterAll(): Unit = {
    try spark.catalog.dropTempView("events_probe")
    finally { spark.stop(); super.afterAll() }
  }

  private def model(table: String): Model =
    Model.of(
      name = "probe_events",
      version = 1,
      dimensions = List(
        io.sm8.core.model.Dimension.field("region", "region"),
        io.sm8.core.model.Dimension.field("event_date", "event_date", SealedDataType.Date)
      ),
      measures = List(
        io.sm8.core.model.Measure.aggregate(
          "total",
          io.sm8.core.rel.AggregateFn.Sum,
          io.sm8.core.expr.Expr.FieldRef("amount"))
      ),
      source = SourceRef.ByName(table = table)
    ).right.get

  // -- D2: DeclaredSchemaProbe --

  test("listFields: byName source → live fields with bridged types") {
    val fields = DeclaredSchemaProbe.listFields(spark, model("events_probe"))
    fields.isRight shouldBe true
    val Right(fs) = fields
    fs.map(_.name).toSet shouldBe Set("amount", "region", "event_date")
    fs.find(_.name == "event_date").get.dataType shouldBe SealedDataType.Date
    fs.find(_.name == "amount").get.dataType shouldBe SealedDataType.Int
  }

  test("listFields: unknown table → typed Left (never throws)") {
    val res = DeclaredSchemaProbe.listFields(spark, model("no_such_table_probe"))
    res.isLeft shouldBe true
    val Left(err) = res
    err.message should include ("no_such_table_probe")
  }

  test("listFields: non-byName source → typed Left (no catalog table to probe)") {
    val byPath = model("events_probe").copy(
      source = SourceRef.ByPath(format = "parquet", path = "/tmp/nowhere"))
    DeclaredSchemaProbe.listFields(spark, byPath).isLeft shouldBe true
  }

  // -- D3: CompiledSqlProbe --

  test("compile: well-formed model+request → non-empty plan string") {
    val provider = new SparkEngineProvider(spark, SparkTypeBridge)
    val req = QueryRequest(
      model = "probe_events",
      dimensions = Seq("region"),
      measures = Seq("total"))
    val res = CompiledSqlProbe.compile(provider, model("events_probe"), req, EngineContext.defaultContext)
    res.isRight shouldBe true
    val Right(plan) = res
    plan.nonEmpty shouldBe true
  }

  test("compile: null-spark provider → typed Left (UnsupportedCapability)") {
    val provider = new SparkEngineProvider(null, SparkTypeBridge)
    val req = QueryRequest(model = "probe_events")
    val res = CompiledSqlProbe.compile(provider, model("events_probe"), req, EngineContext.defaultContext)
    res.isLeft shouldBe true
    val Left(err) = res
    err.message should include ("null-spark")
  }
}
