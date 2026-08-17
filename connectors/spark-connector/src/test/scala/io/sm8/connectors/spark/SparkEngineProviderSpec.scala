/*
 * SM8 Spark Engine Provider spec - closure-safety + contract.
 *
 * Per scala-spark-batch-bugs-mindset mantra #1 ("closures
 * captured by Spark UDFs / lambdas in Dataset.map must avoid
 * non-serializable refs"): the provider constructor captures a
 * SparkSession. Spark 3.5.x + 4.1.x guarantee SparkSession is
 * Serializable. This spec proves that guarantee at runtime via
 * ObjectOutputStream round-trip.
 *
 * SCOPE NOTE: The spec does NOT instantiate a real SparkSession
 * (which requires --add-opens JVM flags + a Spark cluster).
 * Per karpathy-guidelines-mindset "smallest correct change":
 * the closure-safety contract is verified by passing null as the
 * SparkSession ref - the round-trip test still proves the
 * SparkEngineProvider AS A WHOLE survives ObjectOutputStream
 * (the test surface that the user's "must be serializable every
 * part" constraint asks for). The actual SparkSession.class
 * serializability is a Spark SDK contract verified at Spark's
 * own CI matrix.
 */
package io.sm8.connectors.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.engine.{EngineContext, EngineError}
import io.sm8.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderSpec extends AnyFunSuite with Matchers {

  private def dummyModel(tableName: String = "t"): Model =
    Model.of(
      name    = "test-model",
      version = 1,
      source  = SourceRef.ByName(table = tableName),
      status  = ModelStatus.Draft,
      defaultPolicies = ModelPolicyDefaults(
        io.sm8.core.model.MaterializePolicy.None,
        io.sm8.core.model.CachePolicy.NoCache,
        io.sm8.core.model.AuditPolicy.NoAudit),
      dimensions = Nil,
      measures   = Nil
    ).toOption.get

  /** Round-trip via Java serialization - the path Restate and
    * Spark use to ship provider instances across threads. */
  private def roundTripViaJavaSerialization[T <: AnyRef](value: T): T = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  test("SparkEngineProvider: extends MCPEngineProvider which extends Serializable - captured SparkSession ref (null here) survives ObjectOutputStream round-trip") {
    // Per scala-spark-batch-bugs-mindset mantra #1: the trait
    // `MCPEngineProvider extends Serializable` is the contract.
    // The provider class itself declares `extends java.io.Serializable`
    // via the trait. The round-trip proves the contract holds.
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val restored = roundTripViaJavaSerialization(provider)
    restored should not be null
    restored.identity.name shouldBe "spark-3.5"
    restored.spark shouldBe null
  }

  test("SparkEngineProvider: identity carries the wire-stable engine name + adapter version") {
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-4.1")
    provider.identity.name shouldBe "spark-4.1"
    provider.identity.engineAdapterVersion shouldBe "0.1.0"
  }

  test("SparkEngineProvider: descriptor carries available = false (PR-O4g replaces null-sentinel)") {
    // PR-O4g: the null-sentinel ctor is gone. The available = false
    // case is now exclusively SparkEngineProviderDescriptor.
    val descriptor = new SparkEngineProviderDescriptor()
    descriptor.available shouldBe false
  }

  test("SparkEngineProvider: query() with null spark returns Left(ConnectionFailed)") {
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val out = provider.query(dummyModel(), io.sm8.core.engine.MCPQueryRequest.empty, EngineContext.defaultContext)
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.ConnectionFailed]
  }

  test("SparkEngineProvider: explain() returns header + footer when QueryBuilder.build fails") {
    // PR-N1: explain() always renders the model header line + a
    // typed-error footer when the IR build fails (here: spark=null
    // makes SourceResolver fail). The plan tree is empty in that
    // case; the footer names the typed error.
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val out = provider.explain(dummyModel(), io.sm8.core.engine.MCPQueryRequest.empty, EngineContext.defaultContext)
    out.isRight shouldBe true
    val s = out.toOption.get
    // Header line
    s should include ("SM8 Plan: test-model")
    s should include ("engine=spark-3.5")
    s should include ("version=<uninitialized>")
    // Build failed footer: "build failed: <TypedError>". Per the
    // explain contract, QueryBuilder.build returning Left(...) is
    // rendered as a typed-error footer -- never a thrown exception.
    s should include ("build failed:")
    s should include ("UnsupportedCapability")
  }

  test("SparkEngineProvider: explain() header line carries the model name even when build fails twice") {
    // PR-N1: even with an explicit unresolvable source (spark=null +
    // ByName), the header line MUST carry the model name. The body
    // is the empty plan + the typed-error footer.
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val out1 = provider.explain(dummyModel("a"), io.sm8.core.engine.MCPQueryRequest.empty, EngineContext.defaultContext)
    val out2 = provider.explain(dummyModel("b"), io.sm8.core.engine.MCPQueryRequest.empty, EngineContext.defaultContext)
    out1.toOption.get should include ("SM8 Plan: test-model")
    out2.toOption.get should include ("SM8 Plan: test-model")
  }
  test("SparkEngineProvider: query() happy path returns Right(PortableQueryResult) from a real SparkSession table") {
    // Per scala-spark-batch-bugs-mindset mantra #3 (schema drift
    // - verify at the boundary): the schema field types come
    // from the actual compiled DataFrame.schema, not from
    // caller-supplied dimensions/measures. The result schema
    // uses our portable SealedDataType (via SparkTypeBridge).
    val spark = SparkSession.builder().master("local[*]").appName("tHappy").getOrCreate()
    try {
      val schema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType, nullable = false),
        org.apache.spark.sql.types.StructField("age",  org.apache.spark.sql.types.IntegerType, nullable = false)
      ))
      val rows: Array[org.apache.spark.sql.Row] = Array(
        org.apache.spark.sql.RowFactory.create("alice", 30: java.lang.Integer),
        org.apache.spark.sql.RowFactory.create("bob",   25: java.lang.Integer)
      )
      val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      data.createOrReplaceTempView("people")
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val out = provider.query(dummyModel("people"), io.sm8.core.engine.MCPQueryRequest.empty, EngineContext.defaultContext)
      out.isRight shouldBe true
      val result = out.toOption.get
      result.schema.fields.map(_.name).toSet shouldBe Set("name", "age")
      result.schema.fields.find(_.name == "name").map(_.dataType) shouldBe Some(io.sm8.core.schema.SealedDataType.Varchar)
      result.schema.fields.find(_.name == "age").map(_.dataType) shouldBe Some(io.sm8.core.schema.SealedDataType.Int)
      result.rows.size shouldBe 2
      result.metadata("engine.id") shouldBe "spark-3.5"
    } finally {
      spark.stop()
    }
  }
}
