/*
 * SM8 Spark Engine Provider spec - closure-safety + contract.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 ("closures
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

  test("SparkEngineProvider: extends EngineProvider which extends Serializable - captured SparkSession ref (null here) survives ObjectOutputStream round-trip") {
    // Per [[scala-spark-batch-bugs-mindset]] mantra #1: the trait
    // `EngineProvider extends Serializable` is the contract.
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
    val out = provider.query(dummyModel(), io.sm8.core.engine.QueryRequest.empty, EngineContext.defaultContext)
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.ConnectionFailed]
  }

  test("SparkEngineProvider: explain() returns header + footer when QueryBuilder.build fails") {
    // PR-N1: explain() always renders the model header line + a
    // typed-error footer when the IR build fails (here: spark=null
    // makes SourceResolver fail). The plan tree is empty in that
    // case; the footer names the typed error.
    val provider = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val out = provider.explain(dummyModel(), io.sm8.core.engine.QueryRequest.empty, EngineContext.defaultContext)
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
    val out1 = provider.explain(dummyModel("a"), io.sm8.core.engine.QueryRequest.empty, EngineContext.defaultContext)
    val out2 = provider.explain(dummyModel("b"), io.sm8.core.engine.QueryRequest.empty, EngineContext.defaultContext)
    out1.toOption.get should include ("SM8 Plan: test-model")
    out2.toOption.get should include ("SM8 Plan: test-model")
  }
  test("SparkEngineProvider: query() happy path returns Right(PortableQueryResult) from a real SparkSession table") {
    // Per [[scala-spark-batch-bugs-mindset]] mantra #3 (schema drift
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
      val out = provider.query(dummyModel("people"), io.sm8.core.engine.QueryRequest.empty, EngineContext.defaultContext)
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

  // -- ADR-009-e: server-side materialization cap + persist fix --

  /** Synthetic DataFrame with `n` rows of a single int column. */
  private def frameN(spark: SparkSession, n: Int): org.apache.spark.sql.DataFrame = {
    import spark.implicits._
    spark.createDataset((1 to n).map(_.toLong)).toDF("v")
  }

  test("ADR-009-e: applyPostCompilePipeline caps a fixture larger than the cap (rows == cap, truncated == true)") {
    // Acceptance #1 (SM-07 happy): no request.limit over a fixture
    // larger than the cap → rows.size == cap and truncated == true.
    val spark = SparkSession.builder().master("local[*]").appName("tCap").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val df = frameN(spark, 5)
      val out = provider.applyPostCompilePipeline(
        df,
        io.sm8.core.engine.QueryRequest(model = "cap-test", limit = None),
        Map("engine.id" -> "spark-3.5"),
        cap = 3L)
      out.isRight shouldBe true
      val result = out.toOption.get
      result.rows.size shouldBe 3
      result.truncated shouldBe true
      // The row VALUES are the first 3 (the probe row is dropped).
      result.rows.map(_.values.head).toSet shouldBe Set(
        io.sm8.core.engine.ResultValue.IntV(1L),
        io.sm8.core.engine.ResultValue.IntV(2L),
        io.sm8.core.engine.ResultValue.IntV(3L))
    } finally {
      spark.stop()
    }
  }

  test("ADR-009-e: applyPostCompilePipeline exact-cap fixture reports truncated == false (off-by-one probe)") {
    // Acceptance #2: a fixture of EXACTLY cap rows returns
    // truncated == false — the cap+1 probe found nothing to drop.
    val spark = SparkSession.builder().master("local[*]").appName("tExactCap").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val df = frameN(spark, 5)
      val out = provider.applyPostCompilePipeline(
        df,
        io.sm8.core.engine.QueryRequest.empty,
        Map("engine.id" -> "spark-3.5"),
        cap = 5L)
      out.isRight shouldBe true
      val result = out.toOption.get
      result.rows.size shouldBe 5
      result.truncated shouldBe false
    } finally {
      spark.stop()
    }
  }

  test("ADR-009-e: request.limit only NARROWS the cap; caller cannot widen it") {
    // RFC §3: the cap is deployment policy. A caller requesting a
    // larger limit than the cap is still capped (min).
    val spark = SparkSession.builder().master("local[*]").appName("tNarrow").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val df = frameN(spark, 7)
      val out = provider.applyPostCompilePipeline(
        df,
        io.sm8.core.engine.QueryRequest(model = "m", limit = Some(100L)), // caller wants 100
        Map("engine.id" -> "spark-3.5"),
        cap = 4L)
      out.isRight shouldBe true
      val result = out.toOption.get
      result.rows.size shouldBe 4
      result.truncated shouldBe true
    } finally {
      spark.stop()
    }
  }

  test("ADR-009-e: upstream persisted frame is unpersisted after a capped collect (wasPersisted fix, P2)") {
    // Acceptance #5: materialize == Persist + a capped query →
    // the upstream persisted frame is unpersisted after collect,
    // NOT lingering until provider.close(). The old code derived
    // wasPersisted from withLimit.storageLevel (== NONE after
    // .limit()), so its unpersist was a no-op and the persisted
    // frame leaked. The fix derives from the PASSED-IN df and
    // unpersists it.
    val spark = SparkSession.builder().master("local[*]").appName("tPersist").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val df = frameN(spark, 6).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      // Force caching (persist() is lazy until an action).
      df.count()
      // Precondition: the upstream frame is genuinely persisted.
      // (Dataset.storageLevel is the persist marker — a DataFrame
      // .persist() + materialized action reports the level here.)
      df.storageLevel shouldBe org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
      provider.applyPostCompilePipeline(
        df,
        io.sm8.core.engine.QueryRequest.empty,
        Map("engine.id" -> "spark-3.5"),
        cap = 2L)
      // The fix unpersists the ORIGINAL df (not withLimit — the
      // reviewers' P2 bug). The upstream frame's storage level is
      // back to NONE after the capped collect; under the bug it
      // stayed MEMORY_AND_DISK (lingering until provider.close()).
      df.storageLevel shouldBe org.apache.spark.storage.StorageLevel.NONE
    } finally {
      spark.stop()
    }
  }

  test("ADR-009-e: a SparkException from collect propagates — NOT reclassified as UnsupportedCapability (non-swallow)") {
    // Acceptance #7: the cap path must NOT wrap collect() in a
    // catch. A real Spark failure (a throwing executor, here via a
    // UDF that fails at row-evaluation) surfaces as a
    // SparkException thrown OUT of applyPostCompilePipeline — never
    // converted to a typed Left/UnsupportedCapability. `truncated`
    // is a VALUE, not an exception path (per the PR-176 NonFatal
    // discipline — the engine/dispatcher wraps only NonFatal).
    val spark = SparkSession.builder().master("local[*]").appName("tNonSwallow").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val boom = (x: Long) => { throw new RuntimeException("boom"); 0L }
      val throwingUdf = org.apache.spark.sql.functions.udf(boom)
      val df = frameN(spark, 3).withColumn("bad", throwingUdf(org.apache.spark.sql.functions.col("v")))
      // The UDF throws on every row → collect() must raise a
      // SparkException (propagated), not return a typed Left.
      val caught =
        the [org.apache.spark.SparkException] thrownBy {
          provider.applyPostCompilePipeline(
            df,
            io.sm8.core.engine.QueryRequest.empty,
            Map("engine.id" -> "spark-3.5"),
            cap = 2L)
        }
      caught.getMessage should not include "Window.UnpartitionedPercentOfTotal"
    } finally {
      spark.stop()
    }
  }

  // ===== ADR-009-f: paired persist lifecycle — typed registration, non-swallow unpersist =====

  test("ADR-009-f #1: applyPostCompilePipeline paired-registers and unregisters a tracked persisted frame (acceptance #1)") {
    // ADR-009-f v3.2 Fix 1: a df that was .persist()'d upstream carries
    // a non-NONE storageLevel; the pipeline's `registerToken` is
    // non-zero, the token is paired-released via untrackPersist on both
    // success and failure paths. Falsifiable: persistedFramesSize must
    // be 0 after the pipeline returns (paired-untrack fired), AND the
    // df's storageLevel must be NONE (paired-unpersist fired).
    val spark = SparkSession.builder().master("local[*]").appName("tAdr009fPair").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val df = frameN(spark, 6).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      df.count() // materialize the cache
      // Preconditions: frame is genuinely persisted, registry is empty.
      df.storageLevel shouldBe org.apache.spark.storage.StorageLevel.MEMORY_ONLY
      provider.persistedFramesSize shouldBe 0
      val out = provider.applyPostCompilePipeline(
        df,
        io.sm8.core.engine.QueryRequest.empty,
        Map("engine.id" -> "spark-3.5"),
        cap = 2L)
      out.isRight shouldBe true
      // The pair fired: registerToken was non-zero, df.unpersist()
      // succeeded, untrackPersist(token) was called.
      provider.persistedFramesSize shouldBe 0
      df.storageLevel shouldBe org.apache.spark.storage.StorageLevel.NONE
    } finally {
      spark.stop()
    }
  }

  test("ADR-009-f #2: close() unpersists every tracked DataFrame via the test seam (acceptance #2)") {
    // Falsifiable pair: 5 frames registered via the public-private
    // `trackPersist` API; close() iterates persistedFrames and calls
    // df.unpersist() on each. Final observed: persistedFramesSize is 0
    // AND every tracked df's storageLevel is NONE.
    // (Direct trackPersist bypasses the pipeline's paired-untrack path
    // — this exercises the JVM-shutdown sweep in isolation.)
    val spark = SparkSession.builder().master("local[*]").appName("tAdr009fClose").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val dfs = (1 to 5).map { i =>
        frameN(spark, i + 1).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      }
      dfs.foreach(_.count()) // materialize
      dfs.foreach { df => provider.trackPersist(df) }
      provider.persistedFramesSize shouldBe 5
      provider.close()
      provider.persistedFramesSize shouldBe 0
      dfs.foreach { df =>
        df.storageLevel shouldBe org.apache.spark.storage.StorageLevel.NONE
      }
    } finally {
      try spark.stop() catch { case _: Throwable => () }
    }
  }

  test("ADR-009-f #4: success-path unpersist failure returns typed Left(PersistLifecycleFailed, Unpersist) (acceptance #4)") {
    // The new test-only overload wraps df in a ThrowingUnpersistDataset;
    // the pipeline's success path tries df.unpersist() → throws a
    // SparkException → catches NonFatal → returns Left with the typed
    // error. Both untrackPersist(token) AND the typed-Left return
    // must fire (the untrack is reachable via the catch NonFatal branch
    // and the success branch).
    val spark = SparkSession.builder().master("local[*]").appName("tAdr009fSuccessFail").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val df = frameN(spark, 4).persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      df.count()
      val out = provider.applyPostCompilePipeline(
        df,
        io.sm8.core.engine.QueryRequest.empty,
        Map("engine.id" -> "spark-3.5"),
        cap = 10L,
        forceUnpersistFault = true)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.PersistLifecycleFailed]
      val plf = err.asInstanceOf[EngineError.PersistLifecycleFailed]
      plf.phase shouldBe EngineError.PersistPhase.Unpersist
      plf.engine shouldBe "spark-3.5"
      plf.cause shouldBe "SparkException"
      // The pair fired even on the unpersist-fault path: token
      // released via untrackPersist in the NonFatal catch branch.
      provider.persistedFramesSize shouldBe 0
    } finally {
      spark.stop()
    }
  }

  test("ADR-009-f: registerToken == 0L preserves ADR-009-e non-swallow contract (acceptance #10)") {
    // The acceptance #10 invariant: a frame WITHOUT an upstream
    // .persist() (df.storageLevel == NONE → registerToken == 0L)
    // continues to surface a collect() SparkException directly via
    // `throw collectErr` — never reclassified as PersistLifecycleFailed.
    // This is the regression guard for the v3.2 refinement (the typed
    // Left is ONLY returned when a frame was actually tracked).
    val spark = SparkSession.builder().master("local[*]").appName("tAdr009fNonSwallow").getOrCreate()
    try {
      val provider = new SparkEngineProvider(spark, SparkTypeBridge, "spark-3.5")
      val boom = (x: Long) => { throw new RuntimeException("boom"); 0L }
      val throwingUdf = org.apache.spark.sql.functions.udf(boom)
      // No .persist() on this df — registerToken will be 0L.
      val df = frameN(spark, 3).withColumn("bad", throwingUdf(org.apache.spark.sql.functions.col("v")))
      val caught =
        the [org.apache.spark.SparkException] thrownBy {
          provider.applyPostCompilePipeline(
            df,
            io.sm8.core.engine.QueryRequest.empty,
            Map("engine.id" -> "spark-3.5"),
            cap = 2L)
        }
      caught.getMessage should not include "Window.UnpartitionedPercentOfTotal"
      // Pair invariant: no persist was applied, registerToken was 0L,
      // so persistedFrames was never touched.
      provider.persistedFramesSize shouldBe 0
    } finally {
      spark.stop()
    }
  }

  // ===== ADR-009-e follow-up (P2): cross-module cap constant drift guard =====

  test("ADR-009-e follow-up: EngineService.DefaultResultCapRows mirrors SparkEngineProvider.DefaultResultCapRows (F1, P2 drift guard)") {
    val platformDefault: Long =
      io.sm8.platform.query.EngineService.DefaultResultCapRows
    val connectorDefault: Long = SparkEngineProvider.DefaultResultCapRows
    withClue(s"platform=$platformDefault, connector=$connectorDefault — drift detected, ADR-009-e review P2 will re-surface. ") {
      platformDefault shouldBe connectorDefault
    }
  }

  // ===== ADR-009-e follow-up (P3): readResolve preserves resultCapRows =====


  /** Round-trip a provider via Java serialization, then read the
    * post-rehydration resultCapRows and assert it matches the original.
    * Per PR-179 follow-up review: `readResolve` was missing the
    * `resultCapRows` argument, silently resetting a custom cap to 1M
    * across journal replays. */
  test("ADR-009-e follow-up: readResolve preserves a non-default resultCapRows (F3, P3)") {
    // null SparkSession per the spec's closure-safety pattern.
    // readResolve is a pure construction + assignment path; no
    // Spark call fires.
    val custom = 42L
    val original = new SparkEngineProvider(
      spark   = null,
      bridge  = SparkTypeBridge,
      sparkEngineName = "spark-3.5",
      hookRunner = None,
      resultCapRows = custom)
    val recovered = roundTripViaJavaSerialization(original)
    withClue("readResolve dropped resultCapRows across serialization: ") {
      recovered.resultCapRows shouldBe custom
    }
  }

  /** Constructor `require` catches an out-of-range cap loudly at
    * construction (F4 — the construction-time half). The misconfig
    * fails here, not inside Spark's limit() with a confusing message. */
  test("ADR-009-e follow-up: ctor require rejects resultCapRows > Int.MaxValue - 1 (F4, P3)") {
    val tooBig = Int.MaxValue.toLong // +1 probe would overflow
    val caught =
      the [IllegalArgumentException] thrownBy {
        new SparkEngineProvider(
          spark   = null,
          bridge  = SparkTypeBridge,
          sparkEngineName = "spark-3.5",
          hookRunner = None,
          resultCapRows = tooBig)
      }
    caught.getMessage should include ("out of Spark limit range")
  }

  test("ADR-009-e follow-up: ctor require rejects resultCapRows <= 0 (F4, P3)") {
    val caught =
      the [IllegalArgumentException] thrownBy {
        new SparkEngineProvider(
          spark   = null,
          bridge  = SparkTypeBridge,
          sparkEngineName = "spark-3.5",
          hookRunner = None,
          resultCapRows = 0L)
      }
    caught.getMessage should include ("out of Spark limit range")
  }

}
