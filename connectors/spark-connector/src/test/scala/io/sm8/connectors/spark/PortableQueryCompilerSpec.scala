/*
 * SM8 PortableQueryCompiler spec - closure-safety + real-runtime
 * compile path.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: the compiler
 * extends java.io.Serializable and captures ONLY a SparkSession
 * (which Spark 3.5 + 4.1 guarantee is Serializable). The round-trip
 * test proves the contract at runtime.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #5 (driver vs executor
 * asymmetry): compile() + collect() both run in the driver process;
 * no driver-side resources leak to executors.
 *
 * Per [[scala-jvm-safety-mindset]] mantra #3 (long-lived state): the
 * compiler has NO static / ThreadLocal state. The captured
 * SparkSession is constructor-frozen.
 */
package io.sm8.connectors.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.engine.{EngineContext, EngineError}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{FilterSpec, JoinSpec, MaterializePolicy, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.rel.JoinKind
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class PortableQueryCompilerSpec extends AnyFunSuite with Matchers {

  /** Round-trip via Java serialization - the path Restate and
    * Spark UDFs use to verify captured-state contract.
    * Per [[scala-spark-batch-bugs-mindset]] mantra #1. */
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
    * Per [[scala-data-driven-refactor-mindset]]: pure data, smart
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
    // Per [[scala-jvm-safety-mindset]] mantra #3: null spark is a
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
        source  = SourceRef.ByName(table = "people"),
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
        source     = SourceRef.ByName(table = "people"),
        dimensions = List(
          io.sm8.core.model.Dimension.field(name = "name", "name"),
          io.sm8.core.model.Dimension.field(name = "city", "city"),
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
    // Per [[scala-spark-batch-bugs-mindset]] mantra #3 (verify at the
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
        source     = SourceRef.ByName(table = "people"),
        dimensions = List(io.sm8.core.model.Dimension.field("name", "name")),
      )
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, EngineContext.defaultContext)
      out.isRight shouldBe true
      val df = out.toOption.get
      // The compiled schema has the projected columns (just "name").
      // Per [[scala-spark-batch-bugs-mindset]] mantra #3: this is the
      // boundary contract.
      df.schema.fieldNames.toSet shouldBe Set("name")
      df.schema("name").dataType shouldBe StringType
    } finally {
      spark.stop()
    }
  }

  // -- Driver-side collect() materialization (scala-spark-batch-bugs-mindset mantra #5) --

  test("PortableQueryCompiler.compile: collect() runs in the driver process and returns Array[Row] to the caller") {
    // Per [[scala-spark-batch-bugs-mindset]] mantra #5: collect()
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
      val model = makeModel(source = SourceRef.ByName(table = "people"))
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

  // ===== PR-N5: MaterializePolicy.Persist dispatch =====

  test("PortableQueryCompiler.applyAggregations: MaterializePolicy.Persist(MEMORY_ONLY) calls df.persist(MEMORY_ONLY)") {
    // PR-N5: when a model's materialize policy is Persist("MEMORY_ONLY"),
    // applyAggregations calls df.persist(StorageLevel.MEMORY_ONLY) on
    // the resulting DataFrame. The DataFrame's storageLevel reflects
    // the change. None / Cache paths are a no-op (no persist call).
    val spark = SparkSession.builder().master("local[1]").appName("tPersistOK").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("val", IntegerType, nullable = false),
      ))
      val rows = Array(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer))
      val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      // Build a Model with a Sum measure and the Persist policy.
      val model = Model.of(
        name    = "persist-model",
        version = 1,
        source  = SourceRef.ByName(table = "t"),
        measures = List(
          io.sm8.core.model.Measure(
            name = "total",
            expr = io.sm8.core.rel.AggregateCall(
              fn = io.sm8.core.rel.AggregateFn.Sum,
              input = Some(Expr.FieldRef("val")),
              alias = "total",
              distinct = false ,
              arguments = Nil,
            ),
          ),
        ),
        defaultPolicies = ModelPolicyDefaults(
          materialize = MaterializePolicy.Persist("MEMORY_ONLY"),
          cache       = io.sm8.core.model.CachePolicy.NoCache,
          audit       = io.sm8.core.model.AuditPolicy.NoAudit,
        ),
        status = ModelStatus.Published,
      ).toOption.get
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.applyAggregations(df, model)
      out.isRight shouldBe true
      val persisted = out.toOption.get
      persisted.storageLevel.useMemory shouldBe true
    } finally {
      spark.stop()
    }
  }

  test("PortableQueryCompiler.applyAggregations: MaterializePolicy.Persist(NOT_A_REAL_LEVEL) returns Left(UnsupportedCapability)") {
    // PR-N5: an unknown StorageLevel name (typo, wrong case) is a
    // typed error, not a thrown IllegalArgumentException.
    val spark = SparkSession.builder().master("local[1]").appName("tPersistBad").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("val", IntegerType, nullable = false),
      ))
      val rows = Array(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer))
      val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      val model = Model.of(
        name    = "bogus-persist-model",
        version = 1,
        source  = SourceRef.ByName(table = "t"),
        measures = List(
          io.sm8.core.model.Measure(
            name = "total",
            expr = io.sm8.core.rel.AggregateCall(
              fn = io.sm8.core.rel.AggregateFn.Sum,
              input = Some(Expr.FieldRef("val")),
              alias = "total",
              distinct = false ,
              arguments = Nil,
            ),
          ),
        ),
        defaultPolicies = ModelPolicyDefaults(
          materialize = MaterializePolicy.Persist("NOT_A_REAL_LEVEL"),
          cache       = io.sm8.core.model.CachePolicy.NoCache,
          audit       = io.sm8.core.model.AuditPolicy.NoAudit,
        ),
        status = ModelStatus.Published,
      ).toOption.get
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.applyAggregations(df, model)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err.asInstanceOf[EngineError.UnsupportedCapability].message should include ("Unknown Spark StorageLevel")
      err.asInstanceOf[EngineError.UnsupportedCapability].message should include ("NOT_A_REAL_LEVEL")
    } finally {
      spark.stop()
    }
  }

  test("PortableQueryCompiler.applyAggregations: MaterializePolicy.None is a no-op (no persist call)") {
    // PR-N5 contract: the None / Cache paths MUST NOT call persist --
    // only the Persist(level) case calls it. The Cache case is
    // owned by the cache-plugin (out of scope for the connector).
    val spark = SparkSession.builder().master("local[1]").appName("tPersistNone").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("val", IntegerType, nullable = false),
      ))
      val rows = Array(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer))
      val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      val model = Model.of(
        name    = "none-persist-model",
        version = 1,
        source  = SourceRef.ByName(table = "t"),
        measures = List(
          io.sm8.core.model.Measure(
            name = "total",
            expr = io.sm8.core.rel.AggregateCall(
              fn = io.sm8.core.rel.AggregateFn.Sum,
              input = Some(Expr.FieldRef("val")),
              alias = "total",
              distinct = false ,
              arguments = Nil,
            ),
          ),
        ),
        defaultPolicies = ModelPolicyDefaults(
          materialize = MaterializePolicy.None,
          cache       = io.sm8.core.model.CachePolicy.NoCache,
          audit       = io.sm8.core.model.AuditPolicy.NoAudit,
        ),
        status = ModelStatus.Published,
      ).toOption.get
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.applyAggregations(df, model)
      out.isRight shouldBe true
      out.toOption.get.storageLevel.useMemory shouldBe false  // NONE
    } finally {
      spark.stop()
    }
  }

  test("PortableQueryCompiler.applyAggregations: MaterializePolicy.Cache returns typed Left(UnsupportedCapability) — actionable message (ADR-009-f Fix 3)") {
    // ADR-009-f v3.2 Fix 3: the materialize path's `case _ =>
    // Right(result)` is GONE for `Cache` — it is now a typed Left
    // with an actionable message telling the contributor how to get
    // actual caching (CachePolicy.ReadThrough via the cache-plugin)
    // versus actual materialization (MaterializePolicy.Persist). This
    // is the falsifiable assertion: a model with materialize = Cache
    // MUST surface a typed error, NOT silently behave like None.
    val spark = SparkSession.builder().master("local[1]").appName("tPersistCache").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("val", IntegerType, nullable = false),
      ))
      val rows = Array(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer))
      val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      val model = Model.of(
        name    = "cache-materialize-model",
        version = 1,
        source  = SourceRef.ByName(table = "t"),
        measures = List(
          io.sm8.core.model.Measure(
            name = "total",
            expr = io.sm8.core.rel.AggregateCall(
              fn = io.sm8.core.rel.AggregateFn.Sum,
              input = Some(Expr.FieldRef("val")),
              alias = "total",
              distinct = false,
              arguments = Nil,
            ),
          ),
        ),
        defaultPolicies = ModelPolicyDefaults(
          materialize = MaterializePolicy.Cache,
          cache       = io.sm8.core.model.CachePolicy.NoCache,
          audit       = io.sm8.core.model.AuditPolicy.NoAudit,
        ),
        status = ModelStatus.Published,
      ).toOption.get
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.applyAggregations(df, model)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err.engine shouldBe "spark-3.5"
      // The capability name is the actionable identifier.
      val cap = err.asInstanceOf[EngineError.UnsupportedCapability]
      cap.capability shouldBe "MaterializePolicy.Cache"
      // The message MUST name the right routing path: result caching
      // is via CachePolicy.ReadThrough (NOT a typo like
      // "MaterializePolicy.ReadThrough"). The PR-178 discipline says
      // silent no-ops are contract violations — the message must be
      // genuinely actionable.
      cap.message should include ("For connector-side materialization")
      cap.message should include ("CachePolicy.ReadThrough")
      cap.message should not include "MaterializePolicy.ReadThrough"
    } finally {
      spark.stop()
    }
  }

  // ===== PR-O2 (ADR-008-O, P0-4): size-based broadcast-join hint =====

  test("PortableQueryCompiler.applyJoins: unset broadcastRightBelowBytes trusts Spark default") {
    // When the hint is unset (None), the connector does NOT wrap
    // with broadcast -- it lets Spark's `autoBroadcastJoinThreshold`
    // (typically 10MB) apply naturally. This is the SAFE default.
    val spark = SparkSession.builder().master("local[1]").appName("tO2NoHint").getOrCreate()
    try {
      val schema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
      ))
      spark.createDataFrame(
        java.util.Arrays.asList(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer)),
        schema,
      ).createOrReplaceTempView("O2NHKLeft")
      spark.createDataFrame(
        java.util.Arrays.asList(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer)),
        schema,
      ).createOrReplaceTempView("O2NHKRight")
      val ctxNoHint = io.sm8.core.engine.EngineContext.defaultContext  // joinHints = JoinHints() = None
      val model = Model.of(
        name   = "o2-nohint",
        version = 1,
        source = io.sm8.core.model.SourceRef.ByName(table = "O2NHKLeft"),
        joins  = List(JoinSpec(
          name       = "j1",
          rightModel = "O2NHKRight",
          kind       = JoinKind.Inner,
          keys       = List(("id", "id")),
        )),
      ).toOption.get
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, ctxNoHint)
      out.isRight shouldBe true
      out.toOption.get.count() shouldBe 1
    } finally {
      spark.stop()
    }
  }

  test("PortableQueryCompiler.applyJoins: broadcastRightBelowBytes set above right-side runs join cleanly") {
    // Per [[scala-spark-batch-bugs-mindset]] mantra #7 (broadcast joins):
    // when the hint is set AND rightDf.sizeInBytes <= threshold, the
    // join uses `broadcast(rightDf)` instead of shuffle-hash.
    val spark = SparkSession.builder().master("local[1]").appName("tO2Hint").getOrCreate()
    try {
      val schema = new org.apache.spark.sql.types.StructType(Array(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
        org.apache.spark.sql.types.StructField("val", org.apache.spark.sql.types.IntegerType),
      ))
      spark.createDataFrame(
        java.util.Arrays.asList(
          org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, 10: java.lang.Integer),
        ),
        schema,
      ).createOrReplaceTempView("O2HintLeft")
      spark.createDataFrame(
        java.util.Arrays.asList(
          org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, 100: java.lang.Integer),
        ),
        schema,
      ).createOrReplaceTempView("O2HintRight")
      val joinHints = io.sm8.core.engine.JoinHints(
        broadcastRightBelowBytes = Some(1048576L),  // 1MB
        skewFactor               = None,
        preferredStrategy        = None,
      )
      val ctxWithHint = io.sm8.core.engine.EngineContext.defaultContext.copy(joinHints = joinHints)
      val model = Model.of(
        name   = "o2-hint",
        version = 1,
        source = io.sm8.core.model.SourceRef.ByName(table = "O2HintLeft"),
        joins  = List(JoinSpec(
          name       = "j1",
          rightModel = "O2HintRight",
          kind       = JoinKind.Inner,
          keys       = List(("id", "id")),
        )),
      ).toOption.get
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.compile(model, ctxWithHint)
      out.isRight shouldBe true
      out.toOption.get.count() shouldBe 1
    } finally {
      spark.stop()
    }
  }

  // ===== SM-08 (ADR-009-e): unpartitioned Window gate =====

  /** Model with a Sum measure + a calculated measure referencing
    * Expr.All — the shape that reaches the window path. */
  private def allCalcModel(
      dims: List[io.sm8.core.model.Dimension]): Model =
    Model.of(
      name    = "sm08-model",
      version = 1,
      source  = SourceRef.ByName(table = "t"),
      dimensions = dims,
      measures = List(
        io.sm8.core.model.Measure(
          name = "total",
          expr = io.sm8.core.rel.AggregateCall(
            fn = io.sm8.core.rel.AggregateFn.Sum,
            input = Some(Expr.FieldRef("val")),
            alias = "total",
            distinct = false,
            arguments = Nil,
          ),
        ),
      ),
      calculatedMeasures = List(
        io.sm8.core.model.CalculatedMeasure(
          name = "pct",
          expr = Expr.Divide(Expr.FieldRef("val"), Expr.All("total")),
        ),
      ),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache       = io.sm8.core.model.CachePolicy.NoCache,
        audit       = io.sm8.core.model.AuditPolicy.NoAudit,
      ),
      status = ModelStatus.Published,
    ).toOption.get

  test("SM-08: zero-dimension + Expr.All calculated measure → Left(UnsupportedCapability(Window.UnpartitionedPercentOfTotal)); NO Spark job runs") {
    // Acceptance #6: a calc measure referencing Expr.All with zero
    // dimensions must be rejected at compile time — a single-window
    // whole-scan the driver-side cap cannot bound. The plan is never
    // built (left returned before any withColumn/collect).
    val spark = SparkSession.builder().master("local[1]").appName("tSM08").getOrCreate()
    try {
      val schema = new StructType(Array(StructField("val", IntegerType, nullable = false)))
      val rows   = Array(org.apache.spark.sql.RowFactory.create(1: java.lang.Integer))
      val df     = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      val model  = allCalcModel(dims = List.empty)
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.applyAggregations(df, model)
      out.isLeft shouldBe true
      val err = out.left.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err match {
        case e: EngineError.UnsupportedCapability =>
          e.capability shouldBe "Window.UnpartitionedPercentOfTotal"
          e.engine shouldBe "spark-3.5"
        case o => fail(s"expected UnsupportedCapability, got $o")
      }
    } finally {
      spark.stop()
    }
  }

  test("SM-08: the same model WITH a dimension still compiles (gate fires only on the empty-dims shape)") {
    // The gate must not reject legitimate partitioned windows: a
    // single dimension prevents the single-window whole-scan, so the
    // model compiles to a Right (the window plan is built).
    val spark = SparkSession.builder().master("local[1]").appName("tSM08dim").getOrCreate()
    try {
      val schema = new StructType(Array(
        StructField("val", IntegerType, nullable = false),
        StructField("grp", IntegerType, nullable = false),
      ))
      val rows = Array(
        org.apache.spark.sql.RowFactory.create(1: java.lang.Integer, 1: java.lang.Integer),
        org.apache.spark.sql.RowFactory.create(2: java.lang.Integer, 1: java.lang.Integer),
      )
      val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      val model = allCalcModel(dims = List(io.sm8.core.model.Dimension.field("grp", "grp")))
      val compiler = new PortableQueryCompiler(spark)
      val out = compiler.applyAggregations(df, model).map(_.count())
      out.isRight shouldBe true
    } finally {
      spark.stop()
    }
  }
}
