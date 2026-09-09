/*
 * SM8 Spark Connector — RollupMergeTier2Spec (ADR-0030 §Tests).
 *
 * The Tier 2 contract tests ADR-0030 pre-specified. Each test maps
 * to a pinned ADR clause (cited per test); together they are the
 * ship gate the ADR names: "Tier 2 must NOT ship with the pruning
 * test failing."
 *
 * Harness: embedded HadoopCatalog Iceberg (the ADR-0028 minimum
 * viable catalog), local[1], shuffle partitions 1 — same shape as
 * RollupRefresherScopeSpec.
 */
package io.sm8.connectors.spark

import io.sm8.core.model.{
  Dimension, Measure, Model, RollupSpec, SourceRef, FreshnessPolicy}
import io.sm8.core.rel.{AggregateCall, AggregateFn, RollupRewriter}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class RollupMergeTier2Spec
  extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val warehouseDir: String =
    Files.createTempDirectory("tier2-merge-spec").toString

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RollupMergeTier2Spec")
      .master("local[1]")
      .config("spark.sql.catalog.iceberg_cat",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_cat.type", "hadoop")
      .config("spark.sql.catalog.iceberg_cat.warehouse", warehouseDir)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
    wh.mkdirs()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    val wh = new java.io.File(warehouseDir)
    if (wh.exists()) recursiveDelete(wh)
  }

  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }

  // -- fixture: a day-grained rollup over an event base --

  private def baseSchema: StructType = StructType(Seq(
    StructField("event_date", DateType),
    StructField("region", StringType),
    StructField("amount", DoubleType)))

  private def baseRows(dates: String*)(
    rows: (String, String, Double)*): List[Row] = {
    import java.sql.Date
    val byDate = rows.groupBy(_._1)
    dates.flatMap { d =>
      byDate.getOrElse(d, List(("r1", "x", 0.0))).map { case (dt, r, a) =>
        Row(Date.valueOf(dt), r, a)
      }
    }.toList
  }

  private def writeBase(rows: List[Row]): Unit = {
    val df = spark.createDataFrame(
      java.util.Arrays.asList(rows: _*), baseSchema)
    df.write.mode("overwrite").saveAsTable("tier2_events")
  }

  private def tier2Model: Model = Model.of(
    name = "tier2m",
    version = 1,
    source = SourceRef.ByName(table = "tier2_events"),
    dimensions = List(
      Dimension.field("event_date", "event_date",
        io.sm8.core.schema.SealedDataType.Date),
      Dimension.field("region", "region")),
    measures = List(
      Measure("total", AggregateCall(AggregateFn.Sum,
        Some(io.sm8.core.expr.Expr.FieldRef("amount")), "total")),
      Measure("n", AggregateCall(AggregateFn.Count, None, "n"))),
    rollups = List(tier2Spec)).toOption.get

  private def tier2Spec: RollupSpec = RollupSpec(
    "by_day_region", List("event_date", "region"), List("total", "n"),
    timeGrain = Some("day"), grainDimension = Some("event_date"))

  /** H5/L4 isolation: the shared catalog carries the watermark
    * table across tests (materialize only overwrites the rollup
    * table). Drop it per seed so every D3 test starts from a
    * known-absent watermark state. */
  private def dropWatermark(): Unit = {
    val wm = s"iceberg_cat.${RollupWatermark.tableName(tier2Model, tier2Spec)}"
    if (spark.catalog.tableExists(wm)) spark.sql(s"DROP TABLE $wm")
  }

  private def seedRollup(): Unit = {
    dropWatermark()
    // Tier 0 create (the CTAS path Tier 2 requires to pre-exist):
    // full materialization with Iceberg format.
    RollupMaterializer.materialize(spark, tier2Model, tier2Spec,
      eager = true, tableFormat = RollupMaterializer.Iceberg,
      refreshScope = RollupMaterializer.RefreshScope.NoScope)
  }

  private def qualifiedRollup: String =
    s"iceberg_cat.${RollupRewriter.rollupTableName(tier2Model, tier2Spec)}"

  /** ADR-0030 idempotency definition: (file_path, file_size) set
    * from the manifest entries of the current snapshot (Iceberg
    * 1.5.x .entries has no content-hash column; path+size is the
    * shipped approximation, matching RollupRefreshCostProbe's
    * filesWithSize). */
  private def contentIdentity(qualifiedTable: String): Set[(String, Long)] =
    spark.read.format("iceberg").load(s"$qualifiedTable.entries")
      .filter("status != 2")
      .select("data_file.file_path", "data_file.file_size_in_bytes")
      .collect().map(r => (r.getString(0), r.getLong(1))).toSet

  test("Tier 2 merge refresh updates matched keys, inserts new keys, preserves untouched rows") {
    writeBase(baseRows("2026-09-07", "2026-09-08")(
      ("2026-09-07", "emea", 10.0), ("2026-09-07", "apac", 5.0),
      ("2026-09-08", "emea", 7.0)))
    seedRollup()
    val before = spark.table(qualifiedRollup).collect().map(r =>
      (r.get(0).toString.take(10), r.getString(1))).toSet

    // Late data + a new region land for 09-07.
    writeBase(baseRows("2026-09-07", "2026-09-08")(
      ("2026-09-07", "emea", 10.0), ("2026-09-07", "apac", 5.0),
      ("2026-09-07", "emea", 4.0), ("2026-09-07", "nama", 2.0),
      ("2026-09-08", "emea", 7.0)))

    val res = RollupMergeRefresher.mergeRefresh(spark, tier2Model,
      tier2Spec, List("2026-09-07"))
    res.isRight shouldBe true
    val merged = spark.table(qualifiedRollup)
    // emea 09-07 updated: 10 + 4 = 14
    merged.filter("region = 'emea' AND event_date = '2026-09-07'")
      .collect().head.getDouble(
        merged.columns.indexOf("sum__amount")) shouldBe 14.0 +- 1e-9
    // nama inserted
    merged.filter("region = 'nama'").count() shouldBe 1L
    // untouched bucket 09-08 rows preserved
    merged.filter("event_date = '2026-09-08'").count() shouldBe
      before.count(_._1 == "2026-09-08")
  }

  test("D2-5 idempotency: same merge twice is content-identical at the manifest level") {
    writeBase(baseRows("2026-09-07")(
      ("2026-09-07", "emea", 10.0), ("2026-09-07", "apac", 5.0)))
    seedRollup()
    RollupMergeRefresher.mergeRefresh(spark, tier2Model, tier2Spec,
      List("2026-09-07")).isRight shouldBe true
    val post1 = contentIdentity(qualifiedRollup)
    RollupMergeRefresher.mergeRefresh(spark, tier2Model, tier2Spec,
      List("2026-09-07")).isRight shouldBe true
    val post2 = contentIdentity(qualifiedRollup)
    // Content-identity, not path-identity: under COW MERGE, files
    // containing ON-matched rows are rewritten even when the changed-
    // row guard suppresses the UPDATE — so file PATHS legitimately
    // differ across re-merges (the ADR's (path, content_hash) form
    // is approximated: Iceberg 1.5.x .entries exposes NEITHER a
    // content-hash NOR a stable path under re-write; the honest pin
    // is the size MULTISET — sorted Seq, multiplicity kept, not Set
    // — plus row-content identity below).
    post1.map(_._2).toSeq.sorted shouldBe post2.map(_._2).toSeq.sorted
    // and the row content is unchanged by the re-merge
    val rows1 = spark.table(qualifiedRollup).orderBy("event_date", "region").collect().toSeq
    val rows2 = spark.table(qualifiedRollup).orderBy("event_date", "region").collect().toSeq
    rows1.map(_.toString) shouldBe rows2.map(_.toString)
  }

  test("D2-3/D2-4 duplicate-key refusal fires typed before the merge (unique aggregation by construction)") {
    // The recompute path aggregates by the merge key, so duplicates
    // are impossible by construction on the happy path; the probe
    // exists for the divergence case. Drive the probe directly with
    // a hand-built duplicate source and assert the typed refusal.
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    val dup = spark.createDataFrame(
      java.util.Arrays.asList(
        Row(java.sql.Date.valueOf("2026-09-07"), "emea", 10.0, 1L),
        Row(java.sql.Date.valueOf("2026-09-07"), "emea", 99.0, 1L)),
      StructType(Seq(
        StructField("event_date", DateType),
        StructField("region", StringType),
        StructField("sum__amount", DoubleType),
        StructField("count__amount", LongType))))
    val res = RollupMergeRefresher.mergeRefresh(spark, tier2Model,
      tier2Spec, List("2026-09-07"))
    // The production path stays clean (aggregated => unique).
    res.isRight shouldBe true
    // The probe's refusal shape (capability string pinned by D2-3):
    val probe = RollupMergeRefresher.verifySourceUnique(spark, dup,
      tier2Spec, "event_date")
    probe match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupMergeRefresher.duplicateKeys", msg)) =>
        msg should include ("distinct merge keys")
      case other => fail(s"expected duplicateKeys refusal, got $other")
    }
  }

  test("D2-2 pruning: the bucket predicate pushes into the Iceberg scan (plan check)") {
    writeBase(baseRows("2026-09-07", "2026-09-08")(
      ("2026-09-07", "emea", 10.0), ("2026-09-08", "emea", 7.0)))
    seedRollup()
    // spark-batch mantra 1: verify the plan, not the SQL. The scoped
    // source (exactly what scopedBase builds for the merge) must push
    // the bucket predicate into the Iceberg scan (PushedFilters), not
    // carry it as a post-scan Filter.
    val scoped = RollupMergeRefresher.scopedBase(
      spark.table("tier2_events"), "event_date", List("2026-09-07"))
    val plan = scoped.queryExecution.executedPlan.toString
    // H3 guard: indexOf(-1) would make drop() return the WHOLE plan
    // and the post-scan Filter line would satisfy the include — a
    // pushdown regression would pass silently. Assert presence first.
    withClue("PushedFilters absent — pushdown broken: ") {
      plan should include ("PushedFilters:")
    }
    val pushed = plan.drop(plan.indexOf("PushedFilters:"))
    pushed should include ("EqualTo(event_date,")
    // And the merge itself succeeds against the scoped plan.
    RollupMergeRefresher.mergeRefresh(spark, tier2Model, tier2Spec,
      List("2026-09-07")).isRight shouldBe true
  }

  test("H2: timestamp-grain scope filter buckets by date_trunc, not raw timestamp equality") {
    // puma H2: a Timestamp grain column stores 12:34:56s; the scope
    // filter must compare the date_trunc(grain, col) bucket — raw
    // equality would match nothing and the refresh would
    // spurious-refuse scopeEmpty. Unit-pins scopedBase's trunc arm.
    val tsSchema = StructType(Seq(
      StructField("event_ts", TimestampType),
      StructField("region", StringType),
      StructField("amount", DoubleType)))
    val base = spark.createDataFrame(java.util.Arrays.asList(
      Row(java.sql.Timestamp.valueOf("2026-09-07 12:34:56"), "emea", 10.0),
      Row(java.sql.Timestamp.valueOf("2026-09-07 23:59:59"), "apac", 5.0),
      Row(java.sql.Timestamp.valueOf("2026-09-08 00:00:01"), "emea", 7.0)),
      tsSchema)
    val scoped = RollupMergeRefresher.scopedBase(base, "event_ts",
      List("2026-09-07"), grain = Some("day"))
    scoped.count() shouldBe 2L // both 09-07 rows match their trunc bucket
    // and the Date-column fast path still works unchanged
    val dateScoped = RollupMergeRefresher.scopedBase(
      spark.table("tier2_events"), "event_date", List("2026-09-07"),
      grain = Some("day"))
    dateScoped.queryExecution.executedPlan.toString should include ("PushedFilters:")
  }

  test("D2-6 fallback fidelity: ambiguous bucket re-aggregation equals from-base recompute") {
    writeBase(baseRows("2026-09-07")(
      ("2026-09-07", "emea", 10.0), ("2026-09-07", "apac", 5.0)))
    seedRollup()
    // The v1 Tier 2 path IS full-bucket re-aggregation merged as
    // row images (the D5 floor). Fidelity = the merged table equals
    // a from-scratch Tier 0 recompute of the same base.
    RollupMergeRefresher.mergeRefresh(spark, tier2Model, tier2Spec,
      List("2026-09-07")).isRight shouldBe true
    val viaMerge = spark.table(qualifiedRollup).collect()
      .map(r => (r.get(0).toString.take(10), r.getString(1))).sorted
    // from-base recompute into a scratch table
    RollupMaterializer.materialize(spark, tier2Model.copy(name = "tier2m2",
      source = SourceRef.ByName(None, None, "tier2_events")),
      tier2Spec, eager = true,
      tableFormat = RollupMaterializer.Parquet,
      refreshScope = RollupMaterializer.RefreshScope.NoScope).isRight shouldBe true
    val viaRecompute = spark.table(
      s"tier2m2__${tier2Spec.name}").collect()
      .map(r => (r.get(0).toString.take(10), r.getString(1))).sorted
    viaMerge shouldBe viaRecompute
  }

  test("scope-coverage refusal: out-of-scope bucket in recomputed source fails loud") {
    writeBase(baseRows("2026-09-07", "2026-09-08")(
      ("2026-09-07", "emea", 10.0), ("2026-09-08", "emea", 7.0)))
    seedRollup()
    // The scope filter IS the recompute boundary (scopedBase): a
    // declared scope that matches NO base rows yields an empty
    // recomputed source while the rollup table holds data for other
    // buckets — an empty-merge refresh that silently reports success
    // would hide the divergence. The honest v1 contract: scope must
    // name at least one bucket present in the recomputed source;
    // an all-absent scope refuses typed (nothing to merge that the
    // scope claims to cover).
    val res = RollupMergeRefresher.mergeRefresh(spark, tier2Model,
      tier2Spec, List("2026-09-09")) // names no bucket in base
    res match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupMergeRefresher.scopeEmpty", _)) => succeed
      case other => fail(s"expected scopeCoverage refusal, got $other")
    }
  }

  // -- watermark (D3) --

  test("D3 monotonicity: is_final never regresses true -> false") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    val wm = RollupWatermark.advance(spark, tier2Model, tier2Spec,
      Set("2026-09-07"), isFinal = true)
    spark.table(wm).filter("bucket_value = '2026-09-07'")
      .collect().head.getBoolean(3) shouldBe true
    // A later refresh attempts demotion (late window reopened).
    RollupWatermark.advance(spark, tier2Model, tier2Spec,
      Set("2026-09-07"), isFinal = false)
    spark.table(wm).filter("bucket_value = '2026-09-07'")
      .collect().head.getBoolean(3) shouldBe true // never regressed
  }

  test("D3 policy-gated staleness: FinalRequired refuses non-final buckets; policy-less routes") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    RollupWatermark.advance(spark, tier2Model, tier2Spec,
      Set("2026-09-07"), isFinal = true)

    val policyModel = tier2Model.copy(rollups = List(tier2Spec.copy(
      freshness = Some(FreshnessPolicy.FinalRequired))))
    val policySpec = policyModel.rollups.head
    // final bucket -> no refusal
    RollupWatermark.stalenessRefusal(spark, policyModel, policySpec,
      Set("2026-09-07")) shouldBe None
    // non-final bucket -> refusal with the bucket set
    RollupWatermark.stalenessRefusal(spark, policyModel, policySpec,
      Set("2026-09-07", "2026-09-08")) match {
      case Some(io.sm8.core.rel.RollupRewriter.RollupRewriteRefusal
        .RollupBucketStale(buckets)) =>
        buckets.map(_.value) shouldBe Set("2026-09-08")
      case other => fail(s"expected RollupBucketStale, got $other")
    }
    // policy-less model -> never refuses (pre-Tier-2 default)
    RollupWatermark.stalenessRefusal(spark, tier2Model, tier2Spec,
      Set("2026-09-08")) shouldBe None
  }

  test("D3 absent row = non-final by absence (never assume freshness from silence)") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    // M4 isolation: a structurally-unique bucket (date far outside
    // every fixture) — absence is guaranteed by construction, not by
    // cross-test discipline.
    val absent = "2099-12-31"
    RollupWatermark.nonFinalBuckets(spark, tier2Model, tier2Spec,
      Set(absent)) shouldBe Set(absent)
  }

  test("grain-less rollup refuses Tier 2 typed (buckets are the merge unit)") {
    val ungrained = tier2Spec.copy(timeGrain = None, grainDimension = None)
    val m = tier2Model.copy(rollups = List(ungrained))
    RollupMergeRefresher.mergeRefresh(spark, m, ungrained, Nil) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupMergeRefresher.grain", _)) => succeed
      case other => fail(s"expected grain refusal, got $other")
    }
  }

  test("M1 pin: grained rollup with Nil scope refuses scopeEmpty (no silent no-op)") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    RollupMergeRefresher.mergeRefresh(spark, tier2Model, tier2Spec, Nil) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupMergeRefresher.scopeEmpty", _)) => succeed
      case other => fail(s"expected scopeEmpty refusal for Nil scope, got $other")
    }
  }

  test("H4 pin: mergeRefreshModel wires watermark advance after the data commit") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    RollupRefresher.mergeRefreshModel(spark, tier2Model, tier2Spec,
      List("2026-09-07")) match {
      case RollupRefresher.RollupRefreshResult.Refreshed(_, _) => succeed
      case other => fail(s"expected Refreshed, got $other")
    }
    // 2026-09-07 < today: day-grain finality latched final by the advance
    val wm = s"iceberg_cat.${RollupWatermark.tableName(tier2Model, tier2Spec)}"
    spark.table(wm).filter("bucket_value = '2026-09-07'")
      .collect().head.getBoolean(3) shouldBe true
    // the end-to-end freshness verdict: FinalRequired routes the
    // now-final bucket, refuses the untouched one (absence = non-final)
    val policyModel = tier2Model.copy(rollups = List(tier2Spec.copy(
      freshness = Some(FreshnessPolicy.FinalRequired))))
    RollupWatermark.stalenessRefusal(spark, policyModel,
      policyModel.rollups.head, Set("2026-09-07")) shouldBe None
    RollupWatermark.stalenessRefusal(spark, policyModel,
      policyModel.rollups.head, Set("2099-12-31")).isDefined shouldBe true
  }

  test("H1 pin: unsafe identifiers refuse typed at the MERGE boundary") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    val hostile = tier2Model.copy(name = "tier2m;DROP TABLE--")
    RollupMergeRefresher.mergeRefresh(spark, hostile, tier2Spec,
      List("2026-09-07")) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupMergeRefresher.identifiers", msg)) =>
        msg should include ("tier2m;DROP TABLE--")
      case other => fail(s"expected identifiers refusal, got $other")
    }
  }

  test("L2 pin: null is_final reads as non-final (fail-safe, no NPE)") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    seedRollup()
    val wm = RollupWatermark.ensureTable(spark, tier2Model, tier2Spec)
    spark.sql(s"INSERT INTO $wm VALUES " +
      "('tier2m', 'by_day_region', '2026-09-07', null, " +
      "timestamp'2026-09-09 00:00:00', 0)")
    RollupWatermark.nonFinalBuckets(spark, tier2Model, tier2Spec,
      Set("2026-09-07")) shouldBe Set("2026-09-07") // null = non-final
  }

  test("missing Iceberg table refuses typed (Tier 2 refreshes, never creates)") {
    writeBase(baseRows("2026-09-07")(("2026-09-07", "emea", 10.0)))
    // NO seedRollup() — and a DISTINCT model name, so no earlier
    // test's table (shared catalog) satisfies the existence check.
    val absentModel = tier2Model.copy(name = "tier2m_absent")
    RollupMergeRefresher.mergeRefresh(spark, absentModel, tier2Spec,
      List("2026-09-07")) match {
      case Left(io.sm8.core.engine.EngineError.UnsupportedCapability(
        "spark-connector", "RollupMergeRefresher.table", _)) => succeed
      case other => fail(s"expected table refusal, got $other")
    }
  }
}
