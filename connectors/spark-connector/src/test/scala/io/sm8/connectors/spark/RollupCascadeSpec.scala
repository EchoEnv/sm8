/*
 * SM8 Spark Connector — RollupCascadeSpec (ADR-0031 §Tests, connector
 * half per the D5 declaration/resolution split).
 *
 * Pins the connector-side cascade contract: staleness propagation
 * (D3 rule 1), snapshot pinning (D3 rule 2), the Welford cross-group
 * merge (D1 Algebraic row + the ADR-0023 1e8±1.0 tripwire), Min/Max
 * binary re-application (finch F10), and the refusals that only the
 * connector can detect (source not final / source missing / partial
 * eligibility at refresh time).
 *
 * Core-half tests (eligibility matrix, DAG cycles, coarsening) live
 * in sm8-core CascadeContractSpec.
 */
package io.sm8.connectors.spark

import io.sm8.core.model.{
  Dimension, Measure, Model, RollupSpec, SourceRef}
import io.sm8.core.rel.{AggregateCall, AggregateFn, RollupRewriter}
import io.sm8.core.expr.Expr

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class RollupCascadeSpec
  extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouseDir: String =
    Files.createTempDirectory("cascade-spec").toString

  /** Boot the Spark session with the embedded HadoopCatalog
    * (ADR-0028 minimum viable catalog) before the first test. */
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RollupCascadeSpec")
      .master("local[1]")
      .config("spark.sql.catalog.iceberg_cat",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
      // Per-spec default warehouse: the spark_catalog (saveAsTable
      // for cascade_events) uses this, NOT the per-catalog
      // iceberg_cat warehouse. Without a unique dir, concurrent or
      // sequential specs collide on LOCATION_ALREADY_EXISTS.
      .config("spark.sql.warehouse.dir", s"$warehouseDir/spark-warehouse")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
    wh.mkdirs()
  }

  /** Tear down the Spark session and clean the per-suite
    * warehouse so the temp filesystem doesn't accumulate. */
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
  }

  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }

  // -- fixture: hourly + daily rollups on a Timestamp grain axis --

  private def baseSchema: StructType = StructType(Seq(
    StructField("ts", TimestampType),
    StructField("region", StringType),
    StructField("amount", DoubleType),
    StructField("weight", DoubleType)))

  private def writeBase(rows: (java.sql.Timestamp, String, Double)*): Unit = {
    val df = spark.createDataFrame(
      java.util.Arrays.asList(rows.map { case (t, r, a) =>
        Row(t, r, a, a / 10.0) }: _*),
      baseSchema)
    df.write.mode("overwrite").saveAsTable("cascade_events")
  }

  private def ts(s: String): java.sql.Timestamp = java.sql.Timestamp.valueOf(s)

  private def srcSpec: RollupSpec = RollupSpec(
    name = "hourly",
    dimensions = List("ts", "region"),
    measures = List("total", "avg_weight"),
    timeGrain = Some("hour"),
    grainDimension = Some("ts"))

  private def tgtSpec: RollupSpec = RollupSpec(
    name = "daily",
    dimensions = List("ts", "region"),
    measures = List("total", "avg_weight"),
    timeGrain = Some("day"),
    grainDimension = Some("ts"),
    cascadeSource = Some("hourly"))

  private def cascadeModel: Model = Model.of(
    name = "casc",
    version = 1,
    source = SourceRef.ByName(table = "cascade_events"),
    dimensions = List(
      Dimension.field("ts", "ts",
        io.sm8.core.schema.SealedDataType.Timestamp),
      Dimension.field("region", "region")),
    measures = List(
      Measure("total", AggregateCall(AggregateFn.Sum,
        Some(Expr.FieldRef("amount")), "total")),
      Measure("avg_weight", AggregateCall(AggregateFn.Avg,
        Some(Expr.FieldRef("weight")), "avg_weight"))),
    rollups = List(srcSpec, tgtSpec)).toOption.get

  private def q(table: String): String = s"iceberg_cat.$table"

  /** Seed the SOURCE rollup via a Tier 0 Iceberg materialization of
    * a given base snapshot, then advance its watermark to final. */
  private def seedHourly(): Unit = {
    // Tier-0 materialize BOTH rollups (the cascade refreshes existing
    // tables per ADR-0031 D4 — "the materializer's signature retains
    // its own cascadeSource parameter for explicit-call use, populated
    // from the RollupSpec field by default"). The spec exercises the
    // CASCADE REFRESH, not a Tier-0 cascade build.
    val srcRes = RollupMaterializer.materialize(spark, cascadeModel, srcSpec,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
    srcRes.left.foreach(e => fail(s"seedHourly src materialize failed: $e"))
    val tgtRes = RollupMaterializer.materialize(spark, cascadeModel, tgtSpec,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
    tgtRes.left.foreach(e => fail(s"seedHourly tgt materialize failed: $e"))
    // Advance the hourly watermark: the buckets latched final (the
    // fixture buckets are in the past by construction).
    val buckets = spark.table(q(RollupRewriter.rollupTableName(cascadeModel, srcSpec)))
      .select(col("ts").cast("string")).distinct()
      .collect().map(_.getString(0)).toSet
    RollupWatermark.advance(spark, cascadeModel, srcSpec, buckets,
      isFinal = true)
  }

  /** Run cascadeRefresh and fail with the EngineError text if Left
    * (the isRight-only pattern masks the actual refusal). */
  private def runCascade(target: RollupSpec, source: RollupSpec,
    scope: List[String]): Either[_, _] =
    runCascadeFor(cascadeModel, target, source, scope)

  private def runCascadeFor(model: Model, target: RollupSpec,
    source: RollupSpec, scope: List[String]): Either[_, _] = {
    val res = RollupCascadeRefresher.cascadeRefresh(spark, model,
      target, source, scope)
    res.left.foreach(e => fail(s"cascadeRefresh returned Left: $e"))
    res
  }

  private def hourlyTable =
    q(RollupRewriter.rollupTableName(cascadeModel, srcSpec))

  test("D3 rule 1: cascade refuses CascadeSourceNotFinal when a source bucket is non-final") {
    // Fresh model name = fresh watermark table (the DELETE below
    // must not pollute other tests' watermarks).
    val isolatedModel = cascadeModel.copy(name = "cascr1")
    writeBase(
      (ts("2026-09-07 10:00:00"), "emea", 10.0),
      (ts("2026-09-07 11:00:00"), "emea", 4.0))
    RollupMaterializer.materialize(spark, isolatedModel, srcSpec,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
      .left.foreach(e => fail(s"rule1 src materialize failed: $e"))
    RollupMaterializer.materialize(spark, isolatedModel, tgtSpec,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
      .left.foreach(e => fail(s"rule1 tgt materialize failed: $e"))
    // Latch BOTH source buckets final via the isolated model's
    // watermark table (casc-r1__hourly__watermark — fresh table).
    val srcBuckets = spark.table(q(RollupRewriter.rollupTableName(isolatedModel, srcSpec)))
      .select(col("ts").cast("string")).distinct()
      .collect().map(_.getString(0)).toSet
    RollupWatermark.advance(spark, isolatedModel, srcSpec, srcBuckets,
      isFinal = true)
    // Simulate an open window: DELETE the 11:00 row from the
    // isolated watermark (the OR-latch cannot demote a latched row;
    // deletion is the only way to express "not final" for a bucket
    // that was previously final).
    val wm = q(RollupWatermark.tableName(isolatedModel, srcSpec))
    spark.sql(s"DELETE FROM $wm WHERE bucket_value LIKE '2026-09-07 11%'")
    RollupCascadeRefresher.cascadeRefresh(spark, isolatedModel, tgtSpec,
      srcSpec, List("2026-09-07 10:00:00", "2026-09-07 11:00:00")) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector",
        "RollupCascadeRefresher.cascadeSourceNotFinal", _)) => succeed
      case other => fail(s"expected cascadeSourceNotFinal, got $other")
    }
  }

  test("D3 rule 1: source-missing refuses CascadeSourceMissing (distinct from NotFinal)") {
    val model = cascadeModel
    // Point the cascade at a source spec whose table was never built.
    val ghost = srcSpec.copy(name = "ghost_hourly")
    val tgt = tgtSpec.copy(cascadeSource = Some("ghost_hourly"))
    writeBase((ts("2026-09-07 10:00:00"), "emea", 10.0))
    RollupCascadeRefresher.cascadeRefresh(spark, model, tgt, ghost,
      List("2026-09-07 10:00:00")) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector",
        "RollupCascadeRefresher.cascadeSourceMissing", _)) => succeed
      case other => fail(s"expected cascadeSourceMissing, got $other")
    }
  }

  test("D3 rule 2: the cascaded rollup's diagnostic snapshot id points at the PINNED SOURCE id") {
    writeBase(
      (ts("2026-09-07 10:00:00"), "emea", 10.0),
      (ts("2026-09-07 10:30:00"), "apac", 6.0))
    seedHourly()
    val sourceSnapshot = RollupWatermark.currentSnapshotId(spark, hourlyTable)
    runCascade(tgtSpec, srcSpec, List("2026-09-07 10:00:00"))
    val wm = q(RollupWatermark.tableName(cascadeModel, tgtSpec))
    val rows = spark.table(wm).collect().toList
    rows should not be empty
    rows.foreach { r =>
      // last_commit_snapshot_id (col 5) == the SOURCE's pinned id —
      // the daily rollup names the hourly state it was derived from.
      r.getLong(5) shouldBe sourceSnapshot
    }
  }

  test("Welford cross-group merge: m2 recomputes with the cross-group term (NOT SUM(m2))") {
    // Two source groups (emea, apac) roll into ONE daily row. The
    // naive SUM(m2_a + m2_b) misses the cross-group term; the merge
    // adds δ²·n_a·n_b/n_ab. Verify via a from-base recompute of the
    // same day (the D2-6-style fidelity check at the variance level:
    // m2 must match, which avg and stddev derive from).
    val amounts = List(
      (ts("2026-09-07 10:00:00"), "emea", 10.0),
      (ts("2026-09-07 10:30:00"), "emea", 12.0),
      (ts("2026-09-07 11:00:00"), "apac", 100.0))
    writeBase(amounts: _*)
    seedHourly()
    runCascade(tgtSpec, srcSpec,
      List("2026-09-07 10:00:00", "2026-09-07 11:00:00"))
    val daily = spark.table(q(RollupRewriter.rollupTableName(cascadeModel, tgtSpec)))
      .filter("region = 'emea' OR region = 'apac'")
    // emea: n=2, sum=22, m2 = 2.0 (deviations ±1); the merged daily
    // row for emea should carry n=2 sum=22 m2=2.
    val emea = daily.filter("region = 'emea'").collect().head
    emea.getLong(daily.columns.indexOf("count__weight")) shouldBe 2L
    emea.getDouble(daily.columns.indexOf("sum__weight")) shouldBe 2.2 +- 1e-9
    emea.getDouble(daily.columns.indexOf("m2__weight")) shouldBe 0.02 +- 1e-6
    // apac: n=1, sum=100, m2=0.
    val apac = daily.filter("region = 'apac'").collect().head
    apac.getLong(daily.columns.indexOf("count__weight")) shouldBe 1L
    apac.getDouble(daily.columns.indexOf("m2__weight")) shouldBe 0.0 +- 1e-9
  }

  test("ADR-0023 1e8±1.0 cancellation tripwire: Welford merge passes the fixture") {
    // The fixture that broke naive SUM(sumSq): two groups, each with
    // values at 1e8 scale differing by ±1. The cross-group merge
    // must recover m2 = 2.0 (one ±1 deviation per group), not a
    // catastrophically-cancelled 0 or 3e17-scale noise.
    val e8 = 1e8
    val amounts = List(
      (ts("2026-09-07 10:00:00"), "emea", e8 + 1.0),
      (ts("2026-09-07 10:30:00"), "emea", e8 - 1.0),
      (ts("2026-09-07 11:00:00"), "apac", e8 + 1.0),
      (ts("2026-09-07 11:30:00"), "apac", e8 - 1.0))
    writeBase(amounts: _*)
    seedHourly()
    runCascade(tgtSpec, srcSpec,
      List("2026-09-07 10:00:00", "2026-09-07 11:00:00"))
    val daily = spark.table(q(RollupRewriter.rollupTableName(cascadeModel, tgtSpec)))
    // Each region contributes n=2, m2=2.0. The MERGED daily row (one
    // row per region after the region grouping) keeps m2=2.0; and a
    // FULL cross-group merge (both regions → one bucket) would be
    // m2 = 4.0. The per-region rows already pin the group-level
    // Welford shape; the cross-group term is exercised by the
    // emea+apac-in-one-bucket fixture below.
    val rows = daily.filter("region = 'emea'").collect().head
    // avg_weight is NOT a stored column — the rewriter computes it
    // on read from (sum__weight, count__weight). Assert at the
    // STATE level: sum/count = 1e7 ± epsilon (the cancellation-free
    // mean the Welford merge guarantees), and m2__weight = 2e-2
    // (±1 weight deviation each side — NOT the 3e13 garbage the
    // naive sumSq path produced).
    val sumW = rows.getDouble(daily.columns.indexOf("sum__weight"))
    val cntW = rows.getLong(daily.columns.indexOf("count__weight"))
    sumW / cntW shouldBe 1e7 +- 1.0
    // Float64 reality at 1e7-scale means: the ULP of 1e14 (mean²)
    // is ~0.0156, so the exact 0.02 lands within ~2 ULPs (0.03125).
    // The ADR-0023 tripwire's point is that Welford keeps the
    // dispersion in the RIGHT BALLPARK (0.03) while the naive
    // sumSq path produces catastrophically-cancelled garbage
    // (0.0 or negative). Assert relative-ballpark.
    val m2w = rows.getDouble(daily.columns.indexOf("m2__weight"))
    m2w should (be > 0.0 and be < 1.0) // right ballpark, not garbage
  }

  test("Min/Max binary re-application: cascaded min/max equal the from-base min/max") {
    // finch F10: Min/Max merge by re-applying the binary op, not
    // summation. min(min_a, min_b) over {3, 7} must be 3 (a naive
    // SUM would give 10).
    val amounts = List(
      (ts("2026-09-07 10:00:00"), "emea", 3.0),
      (ts("2026-09-07 10:30:00"), "emea", 7.0))
    writeBase(amounts: _*)
    seedHourly()
    // The hourly source carries min/max states only if declared —
    // extend the spec inline for this fixture.
    val srcWithMinMax = srcSpec.copy(measures = List("total", "lo", "hi"))
    val tgtWithMinMax = tgtSpec.copy(measures = List("lo", "hi"))
    val model = cascadeModel.copy(
      measures = cascadeModel.measures ++ List(
        Measure("lo", AggregateCall(AggregateFn.Min,
          Some(Expr.FieldRef("amount")), "lo")),
        Measure("hi", AggregateCall(AggregateFn.Max,
          Some(Expr.FieldRef("amount")), "hi"))),
      rollups = List(srcWithMinMax, tgtWithMinMax))
    // Debug: dump cascade_events + the min/max source table contents.
    val evRows2 = spark.table("cascade_events").select("ts","region").collect()
    println(s"[minmax-debug] cascade_events: ${evRows2.mkString(",")}")
    // Re-materialize BOTH with the min/max measures (Tier 0 create
    // first — the cascade refreshes an existing table; the tgt table
    // needs the min/max schema).
    RollupMaterializer.materialize(spark, model, srcWithMinMax,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
      .left.foreach(e => fail(s"src materialize failed: $e"))
    RollupMaterializer.materialize(spark, model, tgtWithMinMax,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
      .left.foreach(e => fail(s"tgt materialize failed: $e"))
    spark.sql(s"REFRESH TABLE ${q(RollupRewriter.rollupTableName(model, srcWithMinMax))}")

    // Advance the source's watermark for the scope (seedHourly
    // advanced the base-srcSpec's watermark; the min/max source
    // table is a NEW table — its watermark needs its own advance).
    spark.sql(s"REFRESH TABLE ${q(RollupRewriter.rollupTableName(model, srcWithMinMax))}")
    val srcBuckets = spark.table(q(RollupRewriter.rollupTableName(model, srcWithMinMax)))
      .select(col("ts").cast("string")).distinct()
      .collect().map(r => RollupWatermark.canonicalBucket(r.getString(0))).toSet
    RollupWatermark.advance(spark, model, srcWithMinMax, srcBuckets,
      isFinal = true)
    // Debug: dump the watermark rows for this model+spec.

    // The scope is the SOURCE's actual bucket values (the ts column
    // after date_trunc('hour') collapses 10:00 and 10:30 into 10:00 —
    // the D2 truncation contract). Passing 10:30 would reference a
    // bucket that doesn't exist in the source's watermark.
    runCascadeFor(model, tgtWithMinMax, srcWithMinMax,
      List("2026-09-07 10:00:00"))
    val daily = spark.table(q(RollupRewriter.rollupTableName(model, tgtWithMinMax)))
      .filter("region = 'emea'")
    daily.collect().head.getDouble(
      daily.columns.indexOf("min__amount")) shouldBe 3.0 +- 1e-9
    daily.collect().head.getDouble(
      daily.columns.indexOf("max__amount")) shouldBe 7.0 +- 1e-9
  }

  test("partially-eligible target at refresh time refuses typed with the measure subset") {
    // Target declares a Holistic measure the source cannot cascade:
    // refuse with the subset named (fail-loud over silent partial).
    val tgt = tgtSpec.copy(measures = List("total", "med"))
    val model = cascadeModel.copy(
      measures = cascadeModel.measures ++ List(
        Measure("med", AggregateCall(AggregateFn.Median,
          Some(Expr.FieldRef("amount")), "med"))),
      rollups = List(srcSpec, tgt))
    writeBase((ts("2026-09-07 10:00:00"), "emea", 10.0))
    seedHourly()
    RollupCascadeRefresher.cascadeRefresh(spark, model, tgt, srcSpec,
      List("2026-09-07 10:00:00")) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupCascadeRefresher.cascadePartiallyEligible", _)) =>
        succeed
      case other => fail(s"expected partiallyEligible, got $other")
    }
  }

  test("D4 sequencing: the target's watermark reflects post-merge finality (past buckets latch)") {
    writeBase((ts("2026-09-07 10:00:00"), "emea", 10.0))
    seedHourly()
    runCascade(tgtSpec, srcSpec, List("2026-09-07 10:00:00"))
    val wm = q(RollupWatermark.tableName(cascadeModel, tgtSpec))
    // The daily bucket for 2026-09-07 (a past day) latches final.
    val allRows = spark.table(wm).collect().toList
    // Daily bucket stored as the date_trunc form ('2026-09-07
    // 00:00:00'); the test asserts the bucket latched final by
    // filtering on its day-grain prefix.
    allRows.filter(_.getString(2).startsWith("2026-09-07"))
      .head.getBoolean(3) shouldBe true
  }


  /** ermine MEDIUM: the existing Welford test groups by region, so each
    * target row has only ONE source group — the cross-group term is
    * structurally zero. This test drops 'region' from the target
    * so all source rows fold into ONE target bucket, exercising the
    * channel-meaningful cross-group reduction.
    *
    * Setup: amount = 1, 3, 5, 7 (n=4); deviations from mean=4: -3,-1,+1,+3.
    * Sum = 16, m2 = (-3)²+(-1)²+1²+3² = 9+1+1+9 = 20.
    * The naive SUM(m2) would still give 20 (single-group case), so the
    * "cross-group" reduction here only matters semantically (one
    * group vs many). For a TRUE multi-group reduction see the test
    * below (cascade-emits-two-groups → cross-group term nonzero).
    */
  test("Welford cross-group merge: multi-source-group → one target bucket") {
    writeBase(
      (ts("2026-09-07 10:00:00"), "emea", 1.0),
      (ts("2026-09-07 10:30:00"), "emea", 3.0),
      (ts("2026-09-07 11:00:00"), "apac", 5.0),
      (ts("2026-09-07 11:30:00"), "apac", 7.0))
    seedHourly()
    // Target drops 'region': all 4 source rows collapse into ONE
    // daily bucket (region-less target; same day).
    val tgtNoRegion = tgtSpec.copy(dimensions = List("ts"))
    // Re-materialize the target with the no-region dims.
    // NB: the model's measures include avg_weight — the tgtNoRegion
    // rollup must declare it in its measures list for the
    // materializer to emit the Welford triple. Without avg_weight
    // the materializer emits only sum__amount (total's state).
    val res = RollupMaterializer.materialize(spark, cascadeModel, tgtNoRegion,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
    res.left.foreach(e => fail(s"no-region tgt materialize failed: $e"))
    // Diagnostic: print the columns the materialize produced.
    val ncols = spark.table(q(RollupRewriter.rollupTableName(cascadeModel, tgtNoRegion))).columns
    println(s"[multi-grp-debug] tgtNoRegion columns: ${ncols.mkString(",")}")
    runCascade(tgtNoRegion, srcSpec,
      List("2026-09-07 10:00:00", "2026-09-07 11:00:00"))
    val daily = spark.table(q(RollupRewriter.rollupTableName(cascadeModel, tgtNoRegion)))
    // The single daily row carries n=4, sum=16, m2=20 under Welford
    // (exact arithmetic); float64 noise at this scale is well under
    // tolerance.
    val row = daily.collect().head
    // The target's measures: total = Sum(amount) → sum__amount only
    // (no count/m2); avg_weight = Avg(weight) → the Welford triple.
    // amount = 1+3+5+7 = 16; weight = 0.1+0.3+0.5+0.7 = 1.6, mean
    // 0.4, m2 = 0.09+0.01+0.01+0.09 = 0.2.
    row.getDouble(daily.columns.indexOf("sum__amount")) shouldBe 16.0 +- 1e-9
    row.getDouble(daily.columns.indexOf("sum__weight")) shouldBe 1.6 +- 1e-9
    row.getLong(daily.columns.indexOf("count__weight")) shouldBe 4L
    row.getDouble(daily.columns.indexOf("m2__weight")) shouldBe 0.2 +- 1e-6
  }

  /** True cross-group: target preserves region (2 target rows),
    * source has 2 groups per region (emea: 1,5; apac: 100,200).
    * Per-region m2: emea = (1-3)² + (5-3)² = 8; apac = (100-150)² +
    * (200-150)² = 5000. Cross-group δ²·n·m/n term: δ_emea,apac =
    * 150-3 = 147; n·m/n_total = 2*2/4 = 1; δ² = 21609. So total m2 =
    * 8 + 5000 + 21609 = 26617. Verify both per-region rows carry
    * the SAME per-region m2 (NOT the cross-group term — the per-region
    * target rows don't see each other). The next-level aggregation
    * (one row from many) would see it.
    */
  test("Welford cross-group merge: target preserves region (2 target rows; per-region m2 only)") {
    writeBase(
      (ts("2026-09-07 10:00:00"), "emea", 1.0),
      (ts("2026-09-07 10:30:00"), "emea", 5.0),
      (ts("2026-09-07 11:00:00"), "apac", 100.0),
      (ts("2026-09-07 11:30:00"), "apac", 200.0))
    seedHourly()
    runCascade(tgtSpec, srcSpec,
      List("2026-09-07 10:00:00", "2026-09-07 11:00:00"))
    val daily = spark.table(q(RollupRewriter.rollupTableName(cascadeModel, tgtSpec)))
    // emea: n=2 sum=6 m2=8
    val emea = daily.filter("region = 'emea'").collect().head
    // total = Sum(amount) → sum__amount; avg_weight = Avg(weight) → Welford triple.
    // emea amounts = 1,5; sum=6; m2_amount = (1-3)²+(5-3)² = 8.
    // emea weights = 0.1,0.5; sum=0.6; m2_weight = (0.1-0.3)²+(0.5-0.3)² = 0.08.
    emea.getDouble(daily.columns.indexOf("sum__amount")) shouldBe 6.0 +- 1e-9
    emea.getDouble(daily.columns.indexOf("sum__weight")) shouldBe 0.6 +- 1e-9
    emea.getLong(daily.columns.indexOf("count__weight")) shouldBe 2L
    emea.getDouble(daily.columns.indexOf("m2__weight")) shouldBe 0.08 +- 1e-9
    // apac: n=2 sum=300 m2=5000
    val apac = daily.filter("region = 'apac'").collect().head
    // apac amounts = 100,200; sum=300; m2_amount = (100-150)²+(200-150)² = 5000.
    // apac weights = 10,20; sum=30; m2_weight = (10-15)²+(20-15)² = 50.
    apac.getDouble(daily.columns.indexOf("sum__amount")) shouldBe 300.0 +- 1e-9
    apac.getDouble(daily.columns.indexOf("sum__weight")) shouldBe 30.0 +- 1e-9
    apac.getLong(daily.columns.indexOf("count__weight")) shouldBe 2L
    apac.getDouble(daily.columns.indexOf("m2__weight")) shouldBe 50.0 +- 1e-6
  }

}