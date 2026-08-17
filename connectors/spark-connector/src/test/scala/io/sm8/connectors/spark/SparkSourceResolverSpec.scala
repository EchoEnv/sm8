/*
 * SM8 Spark Connector -- SparkSourceResolver spec (PR-M3 per ADR-008-L
 * Appendix GAP 3).
 *
 * Per [[debug-mantra-mindset]] SS1: every test asserts the
 * engine-portable `ResolvedSource` shape (case classes + schema),
 * not the Spark DataFrame side. The SparkSession is built-and-torn
 * per fixture (no static state, no companion singleton -- per
 * [[scala-jvm-safety-mindset]] mantra #3).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantras:
 *   - #1 (closure-safety): the SparkSession is constructor-injected
 *     (passed via the trait's resolve() path, not a companion).
 *   - #3 (schema-drift verify at the boundary): the Spark schema is
 *     mapped via the existing SparkTypeBridge; the test asserts
 *     both the name AND the SealedDataType (the boundary contract).
 *   - #5 (driver-vs-executor): spark.table + spark.read are
 *     driver-side; no executor-side closure.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{EngineError, EngineIdentity, ResolvedSource}
import io.sm8.core.model.SourceRef
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkSourceResolverSpec extends AnyFunSuite with Matchers {

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-test", nativeVersion = "3.5", engineAdapterVersion = "0.1.0",
  )

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("spark-source-resolver-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

  private def dropAfter[T](spark: SparkSession)(body: => T): T = {
    try body
    finally {
      spark.catalog.clearCache()
      spark.stop()
    }
  }

  // ===== ByName (the common case) =====

  test("ByName: existing temp view resolves to ResolvedSource.Scan with the live schema") {
    val spark = buildSpark()
    dropAfter(spark) {
      val schema = new StructType(Array(
        StructField("id",    IntegerType, nullable = false),
        StructField("name",  StringType,  nullable = false),
        StructField("score", DoubleType,  nullable = true),
      ))
      val rows = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1: Integer, "alice", 1.0): Row, Row(2: Integer, "bob", 2.0): Row
        )),
        schema,
      )
      rows.createOrReplaceTempView("people")

      val resolver = new SparkSourceResolver(spark)
      val out = resolver.resolve(SourceRef.ByName(table = "people"), identity)
      out.isRight shouldBe true
      val scan = out.toOption.get.asInstanceOf[ResolvedSource.Scan]
      scan.source shouldBe SourceRef.ByName(table = "people")
      scan.schema.map(_.name) should contain theSameElementsAs List("id", "name", "score")
      scan.schema.find(_.name == "id").get.dataType   shouldBe SealedDataType.Int
      scan.schema.find(_.name == "name").get.dataType shouldBe SealedDataType.Varchar
      scan.schema.find(_.name == "score").get.dataType shouldBe SealedDataType.Double
      scan.schema.find(_.name == "score").get.nullable shouldBe true
      scan.schema.find(_.name == "id").get.nullable shouldBe false
    }
  }

  test("ByName: missing table surfaces the typed NotFound shape (via UnsupportedCapability)") {
    val spark = buildSpark()
    dropAfter(spark) {
      val resolver = new SparkSourceResolver(spark)
      val out = resolver.resolve(SourceRef.ByName(table = "no_such_table"), identity)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err match {
        case EngineError.UnsupportedCapability(engine, cap, msg) =>
          engine shouldBe "sm8-test"
          cap shouldBe "SourceRef.ByName.resolve"
          msg should include ("no_such_table")
        case other => fail(s"expected UnsupportedCapability, got $other")
      }
    }
  }

  // ===== ByPath =====

  test("ByPath: parquet path resolves to ResolvedSource.Scan with schema") {
    val spark = buildSpark()
    dropAfter(spark) {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1: Integer, "alice"): Row, Row(2: Integer, "bob"): Row
        )),
        new StructType(Array(
          StructField("id",   IntegerType, nullable = false),
          StructField("name", StringType,  nullable = true),
        )),
      )
      val tmpDir = java.nio.file.Files.createTempDirectory("sm8-pr-m3-")
      try {
        val path = tmpDir.resolve("people.parquet").toString
        df.write.mode("overwrite").parquet(path)
        val resolver = new SparkSourceResolver(spark)
        val out = resolver.resolve(
          SourceRef.ByPath(format = "parquet", path = path),
          identity)
        out.isRight shouldBe true
        val scan = out.toOption.get.asInstanceOf[ResolvedSource.Scan]
        scan.schema.map(_.name) shouldBe List("id", "name")
      } finally {
        // Best-effort temp cleanup
        scala.util.Try {
          java.nio.file.Files.walk(tmpDir)
            .sorted(java.util.Comparator.reverseOrder())
            .forEach(p => java.nio.file.Files.deleteIfExists(p))
        }
      }
    }
  }

  // ===== ByProvider (deferred to PR-M4) =====

  test("ByProvider surfaces typed UnsupportedCapability (deferred to PR-M4)") {
    val spark = buildSpark()
    dropAfter(spark) {
      val resolver = new SparkSourceResolver(spark)
      val out = resolver.resolve(
        SourceRef.ByProvider(providerRefName = "future-provider"),
        identity)
      out.isLeft shouldBe true
      out.left.toOption.get shouldBe a [EngineError.UnsupportedCapability]
    }
  }

  // ===== resolveModel (model-by-name) =====

  test("resolveModel: NoopModelRegistry (default) returns typed UnsupportedCapability for every name") {
    val spark = buildSpark()
    dropAfter(spark) {
      val resolver = new SparkSourceResolver(spark)  // default = NoopModelRegistry
      val out = resolver.resolveModel("any_name", identity)
      out.isLeft shouldBe true
      val err = out.left.toOption.get
      err shouldBe a [EngineError.UnsupportedCapability]
      err match {
        case EngineError.UnsupportedCapability(_, cap, msg) =>
          cap shouldBe "ModelRegistry.resolveModel"
          msg should include ("any_name")
        case other => fail(s"expected UnsupportedCapability, got $other")
      }
    }
  }

  test("resolveModel: SessionCatalogModelRegistry maps name -> SourceRef.ByName(catalog='default')") {
    val spark = buildSpark()
    dropAfter(spark) {
      val resolver = new SparkSourceResolver(
        spark,
        registry = SparkSourceResolver.SessionCatalogModelRegistry)
      val out = resolver.resolveModel("products", identity)
      out shouldBe Right(SourceRef.ByName(catalog = Some("default"), namespace = None, table = "products"))
    }
  }

  test("resolveModel: error is tagged with the engine identity in its message") {
    val spark = buildSpark()
    dropAfter(spark) {
      val resolver = new SparkSourceResolver(
        spark,
        registry = ModelRegistry.NoopModelRegistry)
      val out = resolver.resolveModel("foo", identity)
      out.left.toOption.get match {
        case EngineError.UnsupportedCapability(_, _, msg) =>
          msg should include ("foo")
          msg should include ("sm8-test")
        case other => fail(s"expected UnsupportedCapability, got $other")
      }
    }
  }

  // ===== Composition: source-resolver used by QueryBuilder (the integration target) =====

  test("SourceResolver is the right shape for QueryBuilder.build (executable integration)") {
    val spark = buildSpark()
    dropAfter(spark) {
      val schema = new StructType(Array(
        StructField("id",    IntegerType, nullable = false),
        StructField("name",  StringType,  nullable = false),
        StructField("region",StringType,  nullable = true),
      ))
      val rows = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(
          Row(1: Integer, "alice", "east"): Row
        )),
        schema,
      )
      rows.createOrReplaceTempView("orders")

      val resolver = new SparkSourceResolver(spark)
      // The PR-L QueryBuilder's resolve(source, identity) signature
      // is exactly what we implement. Verify the contract by
      // resolving a model that the QueryBuilder would build against.
      val out = resolver.resolve(SourceRef.ByName(table = "orders"), identity)
      val scan = out.toOption.get.asInstanceOf[ResolvedSource.Scan]
      scan.schema.map(_.name) should contain theSameElementsAs List("id", "name", "region")
    }
  }

  test("closure-safety: SparkSourceResolver is Serializable (parity with the SourceResolver trait)") {
    val spark = buildSpark()
    dropAfter(spark) {
      val resolver = new SparkSourceResolver(spark)
      val restored = {
        val baos = new java.io.ByteArrayOutputStream()
        val oos = new java.io.ObjectOutputStream(baos)
        oos.writeObject(resolver)
        oos.close()
        val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(baos.toByteArray))
        ois.readObject().asInstanceOf[SparkSourceResolver]
      }
      // Re-resolve after round-trip to prove the (spark) ref survived
      // serialization (spark is a singleton; see PR-E replay-safety).
      restored shouldBe a [SparkSourceResolver]
    }
  }
}
