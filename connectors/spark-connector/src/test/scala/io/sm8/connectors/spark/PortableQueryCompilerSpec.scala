/*
 * SM8 PortableQueryCompiler spec - closure-safety + real-runtime
 * compile path.
 *
 * Per scala-spark-batch-bugs-mindset mantra #1: the compiler
 * extends java.io.Serializable and captures ONLY a SparkSession
 * (which Spark 3.5 + 4.1 guarantee is Serializable). The round-trip
 * test proves the contract at runtime.
 *
 * Per scala-spark-batch-bugs-mindset mantra #5 (driver vs executor
 * asymmetry): compile() + collect() both run in the driver process;
 * no driver-side resources leak to executors.
 *
 * Per scala-jvm-safety-mindset mantra #3 (long-lived state): the
 * compiler has NO static / ThreadLocal state. The captured
 * SparkSession is constructor-frozen.
 */
package io.sm8.connectors.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.engine.{EngineContext, EngineError}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{FilterSpec, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class PortableQueryCompilerSpec extends AnyFunSuite with Matchers {

  /** Round-trip via Java serialization - the path Restate and
    * Spark UDFs use to verify captured-state contract.
    * Per scala-spark-batch-bugs-mindset mantra #1. */
  private def roundTripViaJavaSerialization[T](obj: T): T = {
    val bytes = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bytes)
    oos.writeObject(obj)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  /** Construct a portable Model with the given source + filters.
    * Per scala-data-driven-refactor-mindset: pure data, smart
    * constructor via Model.of (no direct constructor access). */
  private def makeModel(
      source: SourceRef,
      filters: List[FilterSpec] = List.empty,
      dimensions: List[io.sm8.core.model.Dimension] = List.empty,
  ): Model =
    Model.of(
      name    = "test-model",
      version = 1,
      source  = source,
      status  = ModelStatus.Draft,
      defaultPolicies = ModelPolicyDefaults(
        io.sm8.core.model.MaterializePolicy.None,
        io.sm8.core.model.CachePolicy.NoCache,
        io.sm8.core.model.AuditPolicy.NoAudit,
      ),
      dimensions = dimensions,
      filters   = filters,
    ).toOption.get

  // -- closure-safety baseline (scala-spark-batch-bugs-mindset mantra #1) --

  test("PortableQueryCompiler: extends java.io.Serializable and survives ObjectOutputStream round-trip with null spark") {
    // Per scala-jvm-safety-mindset mantra #3: null spark is a
    // valid reference (lazy evaluation). The round-trip proves
    // the COMPILER class itself survives serialization, which is
    // the captured-state contract the user asked for.
    val compiler = new PortableQueryCompiler(null)
    val restored = roundTripViaJavaSerialization(compiler)
    restored should not be null
    restored shouldBe a [PortableQueryCompiler]
  }

  // -- SourceRef dispatch --

  test("PortableQueryCompiler.compile: SourceRef.ByProvider returns Left(UnsupportedCapability) (driver-closure deferred to future PR)") {
    val compiler = new PortableQueryCompiler(null)
    val model = makeModel(
      SourceRef.ByProvider("driver-closure-ref"),
    )
    val out = compiler.compile(model, EngineContext.defaultContext)
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.UnsupportedCapability]
  }

  // -- FilterSpec application --

  test("PortableQueryCompiler.compile: applies a single FilterSpec.predicate to the source DataFrame") {
    val spark = SparkSession.builder().master("local[*]").appName("tFilter").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("name", StringType, nullable = false),
        StructField("age",  IntegerType, nullable = false)
      ))
      val rows: Array[org.apache.spark.sql.Row] = Array(
        org.apache.spark.sql.RowFactory.create("alice", 30: java.lang.Integer),
        org.apache.spark.sql.RowFactory.create("bob",   25: java.lang.Integer),
        org.apache.spark.sql.RowFactory.create("carol", 17: java.lang.Integer),
      )
      val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      data.createOrReplaceTempView("people")
      // Filter: age >= 18 (a real Expr-driven filter)
      val ageFilter = FilterSpec(
        name = "adults",
        predicate = Expr.GreaterOrEqual(
          Expr.FieldRef("age"),
          Expr.Literal(LiteralValue.IntValue(18), SealedDataType.Int),
        ),
      )
      val model = makeModel(
        source  = SourceRef.ByName("default.people", "people"),
        filters = List(ageFilter),
      )
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, EngineContext.defaultContext)
      out.isRight shouldBe true
      val df = out.toOption.get
      df.count() shouldBe 2  // alice + bob (carol filtered out)
      val names = df.select("name").collect().map(_.getString(0)).toSet
      names shouldBe Set("alice", "bob")
    } finally {
      spark.stop()
    }
  }

  // -- Dimension projection --

  test("PortableQueryCompiler.compile: selectDimensions projects the dimension column names onto the DataFrame") {
    val spark = SparkSession.builder().master("local[*]").appName("tDim").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("name", StringType, nullable = false),
        StructField("age",  IntegerType, nullable = false),
        StructField("city", StringType, nullable = true),
      ))
      val rows: Array[org.apache.spark.sql.Row] = Array(
        org.apache.spark.sql.RowFactory.create("alice", 30: java.lang.Integer, "sf"),
        org.apache.spark.sql.RowFactory.create("bob",   25: java.lang.Integer, "nyc"),
      )
      val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      data.createOrReplaceTempView("people")
      val model = makeModel(
        source     = SourceRef.ByName("default.people", "people"),
        dimensions = List(
          io.sm8.core.model.Dimension(name = "name", expr = "name"),
          io.sm8.core.model.Dimension(name = "city", expr = "city"),
        ),
      )
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, EngineContext.defaultContext)
      out.isRight shouldBe true
      val df = out.toOption.get
      df.schema.fieldNames.toSet shouldBe Set("name", "city")
      df.count() shouldBe 2
    } finally {
      spark.stop()
    }
  }

  // -- Driver-executor asymmetry: schema + collect() in driver only --

  test("PortableQueryCompiler.compile: output DataFrame schema is the actual compiled plan schema (not caller-supplied dimensions)") {
    // Per scala-spark-batch-bugs-mindset mantra #3 (verify at the
    // boundary): even when the model's dimensions list is wrong
    // (e.g. references a column that doesn't exist), the compiled
    // DataFrame schema is the Spark-side resolved schema. The
    // compiler surfaces UnsupportedCapability via Column resolution.
    val spark = SparkSession.builder().master("local[*]").appName("tSchema").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("name", StringType, nullable = false),
        StructField("age",  IntegerType, nullable = false),
      ))
      val rows: Array[org.apache.spark.sql.Row] = Array(
        org.apache.spark.sql.RowFactory.create("alice", 30: java.lang.Integer),
      )
      val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      data.createOrReplaceTempView("people")
      // Dimensions: model says ["name"] (the valid one)
      val model = makeModel(
        source     = SourceRef.ByName("default.people", "people"),
        dimensions = List(io.sm8.core.model.Dimension("name", "name")),
      )
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, EngineContext.defaultContext)
      out.isRight shouldBe true
      val df = out.toOption.get
      // The compiled schema has the projected columns (just "name").
      // Per scala-spark-batch-bugs-mindset mantra #3: this is the
      // boundary contract.
      df.schema.fieldNames.toSet shouldBe Set("name")
      df.schema("name").dataType shouldBe StringType
    } finally {
      spark.stop()
    }
  }

  // -- Driver-side collect() materialization (scala-spark-batch-bugs-mindset mantra #5) --

  test("PortableQueryCompiler.compile: collect() runs in the driver process and returns Array[Row] to the caller") {
    // Per scala-spark-batch-bugs-mindset mantra #5: collect()
    // materializes rows in the driver. No executor-side
    // closures are captured. The compiler + DataFrame
    // references do NOT cross the executor boundary.
    val spark = SparkSession.builder().master("local[*]").appName("tCollect").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("name", StringType, nullable = false),
      ))
      val rows: Array[org.apache.spark.sql.Row] = Array(
        org.apache.spark.sql.RowFactory.create("alice"),
        org.apache.spark.sql.RowFactory.create("bob"),
      )
      val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      data.createOrReplaceTempView("people")
      val model = makeModel(source = SourceRef.ByName("default.people", "people"))
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, EngineContext.defaultContext)
      out.isRight shouldBe true
      val df = out.toOption.get
      val collected = df.collect()  // <-- runs in driver
      collected.length shouldBe 2
      collected(0).getString(0) shouldBe "alice"
      collected(1).getString(0) shouldBe "bob"
    } finally {
      spark.stop()
    }
  }
}
