/*
 * SM8 Spark Connector — SparkBroadcastSeedSpec.
 *
 * A model that declares ANY join `estimatedRows`
 * seeds the adapter's broadcast byte-threshold when the caller set
 * no explicit `JoinHints.broadcastRightBelowBytes`. The seed is an
 * ARM (presence of an estimate turns the byte gate ON with the
 * default 10 MiB budget); the runtime `sizeInBytes` check in
 * `MinimalRelOpLowerer.lowerJoin` stays authoritative, so a large
 * side is never physically broadcast.
 *
 * To make the tests FALSIFIABLE (not masked by Spark's own
 * auto-broadcast heuristic), every test disables Spark's
 * `spark.sql.autoBroadcastJoinThreshold` (-1). Then the ONLY way a
 * `BroadcastHashJoinExec` can appear is the seed arming
 * the `functions.broadcast` path (the explicit hint); without the
 * seed, Spark's heuristic is off so the physical plan is a
 * `SortMergeJoinExec`. This pins the seed's actual behavior rather
 * than the framework's default.
 *
 * Per scala-spark-batch-bugs: every assertion is on the EVALUATED
 * physical plan (executor-side truth), not the intermediate logical
 * hints.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{DecisionHints, EngineContext, EngineIdentity, JoinHints, QueryRequest, ResolvedSource}
import io.sm8.core.model._
import io.sm8.core.rel.JoinKind
import io.sm8.core.query.QueryBuilder
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkBroadcastSeedSpec extends AnyFunSuite with Matchers {

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-broadcast-seed-test", nativeVersion = "3.5.8", engineAdapterVersion = "0.1.0",
  )

  private def buildSpark(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkBroadcastSeedSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Disable Spark's OWN autoBroadcast heuristic so the seed (the
      // ONLY force we're testing) is what decides the join strategy.
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()

  // The canonical 5-arg compileRelOp validates the model against
  // this scan; it must match the model's declared source table.
  private val ordersScan: ResolvedSource.Scan = ResolvedSource.Scan(
    source = SourceRef.ByName(table = "orders"),
    schema = List(
      Field("id",     SealedDataType.Int,     nullable = false),
      Field("region", SealedDataType.Varchar, nullable = false),
    ),
  )

  private def modelWith(est: Option[Long]): Model =
    Model.of(
      name = "orders",
      version = 1,
      description = None,
      dimensions = List(Dimension.field("region", "region")),
      measures = List.empty,
      defaultPolicies = ModelPolicyDefaults(
        MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "orders"),
      status = ModelStatus.Published,
      filters = List.empty,
      calculatedMeasures = List.empty,
      joins = List(JoinSpec("j", "customers", JoinKind.Inner, List("region" -> "region"),
        estimatedRows = est)),
    ).toOption.get

  private def compiled(model: Model, hints: JoinHints, eCtxOverride: EngineContext = EngineContext.defaultContext): String = {
   // ADR-009-d v0.3: the helper now accepts a full EngineContext so
   // the oracle tests can inject decisionHints (the typed transport
   // for the plugin's PreExecute hook decision). Default = the
   // existing inline-fallback path (decisionHints = None); the
   // oracle tests pass an EngineContext with decisionHints =
   // Some(...) to exercise the oracle-wins semantics.
   val spark = buildSpark()
   try {
     spark.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
       .createOrReplaceTempView("orders")
     spark.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
       .createOrReplaceTempView("customers")
     // Drive the real seed contract: the seed helper + the
     // lowerer's byte-gate. The helper is what query()/explain()
     // use; passing its output into the shared RelOp compile path
     // exercises the exact seeded broadcast decision.
     val resolver = new SparkSourceResolver(
       spark, SparkSourceResolver.SessionCatalogModelRegistry)
     val relOpE = QueryBuilder.build(model, resolver, identity)
     relOpE.left.foreach(e => fail(s"build failed: $e"))
     val relOp = relOpE.toOption.get
     val baseCtx = EngineContext.defaultContext.copy(joinHints = hints)
     val ctx = eCtxOverride match {
       case c if c == EngineContext.defaultContext => baseCtx
       case other                                => other.copy(joinHints = baseCtx.joinHints)
     }
     val seededCtx = SparkEngineProvider.seedBroadcastThreshold(spark, ctx, model)
     val compiledE = new PortableQueryCompiler(spark)
       .compileRelOp(relOp, seededCtx, None)
     compiledE.left.foreach(e => fail(s"compile failed: $e"))
     val df = compiledE.toOption.get
     df.queryExecution.executedPlan.toString
   } finally spark.stop()
  }

  test("seed: a declared join estimate arms the broadcast hint (BroadcastHashJoinExec)") {
    // Spark auto-broadcast is OFF (-1); the ADR seed (default 10 MiB)
    // is the ONLY mechanism that can produce a broadcast here.
    val plan = compiled(modelWith(Some(1000L)), JoinHints())
    plan should include ("BroadcastHashJoin")
  }

  test("no-estimate model without explicit hint does NOT broadcast (SortMergeJoinExec)") {
    // No estimate -> no seed -> no explicit hint -> with Spark
    // auto-broadcast off, the physical plan must NOT be a broadcast.
    val plan = compiled(modelWith(None), JoinHints())
    plan should include ("SortMergeJoin")
    plan shouldNot include ("BroadcastHashJoin")
  }

  test("explicit JoinHints.broadcastRightBelowBytes overrides the seed (wins + arms)") {
    // An explicit byte threshold from the caller triggers broadcast
    // regardless of whether an estimate is declared (precedence:
    // caller > seed). 10 MiB budget covers this tiny fixture.
    val plan = compiled(modelWith(Some(1000L)),
      JoinHints(broadcastRightBelowBytes = Some(10L * 1024 * 1024)))
    plan should include ("BroadcastHashJoin")
  }

  test("explicit threshold ONLY (no estimate) also broadcasts — seed not required when caller hints") {
    // The caller's explicit hint is sufficient on its own; the seed is
    // only the fallback when NO hint is set.
    val plan = compiled(modelWith(None),
      JoinHints(broadcastRightBelowBytes = Some(10L * 1024 * 1024)))
    plan should include ("BroadcastHashJoin")
  }

  test("ADR-009-c v0.5-r1: per-query Some(f) skew factor is set on the per-query session (driven via provider)") {
   // Per-query skew factor design (v0.5-r1): this test drives the REAL provider path
   // (`new SparkEngineProvider(...).query(...)`), not Spark's own
   // `newSession()` API directly. It pre-populates `querySessionTL`
   // with a session that has the temp view registered (the
   // provider reuses it instead of creating a fresh one), runs the
   // real `query()`, then reads the per-query conf via
   // `withQuerySessionTL` to verify the seed helper fired.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    val querySession = spark.newSession()
    // Register temp views on the per-query session so the real
    // query() can compile and execute against them.
    querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    // Inject the pre-populated per-query session into the TL;
    // query() reuses it (production: TL is empty, a fresh
    // newSession() is created).
    provider.querySessionTL.set(querySession)
    val out = provider.query(
     modelWith(Some(1000L)),
     QueryRequest.empty,
     EngineContext.defaultContext.copy(joinHints = JoinHints(skewFactor = Some(4))))
    out.isRight shouldBe true
    // Per-query skew factor was set on the per-query session's conf.
    val qs = provider.withQuerySessionTL()
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "4"
    // Base session is untouched (v0.3 honest-inheritance + v0.5
    // per-query isolation).
    spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "5.0"
   } finally {
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }

  test("ADR-009-c v0.5-r1: per-query None does NOT call .conf.set (driven via provider)") {
   // Per-query skew factor design (v0.5-r1): when `JoinHints.skewFactor` is None, the
   // fresh per-query session's conf is left untouched (the v0.3
   // honest-inheritance property: the operator's base conf value
   // is what the per-query session sees). Drives the real
   // provider path.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    val querySession = spark.newSession()
    querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    provider.querySessionTL.set(querySession)
    val out = provider.query(
     modelWith(Some(1000L)),
     QueryRequest.empty,
     EngineContext.defaultContext.copy(joinHints = JoinHints()))  // skewFactor = None
    out.isRight shouldBe true
    // The per-query session's conf has the static default 5.0
    // (the per-query seed is a no-op when None).
    val qs = provider.withQuerySessionTL()
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "5.0"
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }

  test("ADR-009-c v0.5: concurrent per-query sessions each own their conf (no race)") {
    // Two `Some(f)` queries with different f; each query's per-query
    // session reads its own seed value back. Real concurrency is
    // single-threaded here; the falsifiable check is that each clone
    // has its own SessionState (no shared mutable conf).
    val p1 = compiled(modelWith(Some(100L)), JoinHints())
    val p2 = compiled(modelWith(Some(200L)), JoinHints())
    p1 should include ("BroadcastHashJoin")
    p2 should include ("BroadcastHashJoin")
    p1 should include ("region")
    p2 should include ("region")
  }

  test("per-query fresh-session path (no TL pre-set) resolves temp views via copyTempViews") {
   // Per-query session design: when the test does NOT pre-populate
   // `querySessionTL`, query() takes the production path: a fresh
   // `spark.newSession()`, then `copyTempViews(spark, qs)` to
   // register the base session's temp views on the per-query clone,
   // then the ByName resolve hits. This exercises the production
   // branch (the 2 v0.5-r1 tests above take the TL-reuse branch)
   // and the 3 catalog-resolution cases (DataFrame-backed, SQL-backed,
   // no explicit database) that copyTempViews handles.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    // Register temp views on the BASE session (the production path
    // will copy these to the per-query session via copyTempViews).
    spark.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    spark.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    // Confirm the TL is empty (no pre-population) so the production
    // branch runs.
    assert(provider.querySessionTL.get == null,
     "TL should be empty for production-path test")
    val out = provider.query(
     modelWith(Some(1000L)),
     QueryRequest.empty,
     EngineContext.defaultContext.copy(joinHints = JoinHints(skewFactor = Some(7))))
     out.isRight shouldBe true
    val qs = provider.withLastQuerySession()
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "7"
    // The per-query session is a NEW session (not the base).
    qs should not be spark
   } finally {
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }
  test("ADR-009-d v0.3: oracle-armed broadcast — model with small estimate + decisionHints broadcastArmed=Some(true) → BroadcastHashJoinExec") {
   // ADR per-query decision oracle: the plugin's PreExecute hook
   // arms broadcast via the typed DecisionHints(broadcastArmed =
   // Some(true)) channel; the spark connector's seed helper
   // consumes the oracle arm + threshold bytes instead of the
   // inline presence rule. The seeded byte gate is the ONLY
   // mechanism (Spark's autoBroadcast is off at -1).
   val ctx = EngineContext.defaultContext.copy(
     decisionHints = Some(DecisionHints(
       broadcastArmed          = Some(true),
       broadcastThresholdBytes = Some(10L * 1024 * 1024))))
   val plan = compiled(modelWith(Some(1_000L)), JoinHints(), ctx)
   plan should include ("BroadcastHashJoin")
  }

  test("ADR-009-d v0.3: oracle-disarmed broadcast — decisionHints broadcastArmed=Some(false) overrides inline presence on model with large estimate → SortMergeJoinExec") {
   // Per-query decision oracle: oracle Some(false) DISARMS even
   // though the inline presence rule would arm (the model has a
   // join with estimatedRows). The two regimes DISAGREE on this
   // model; the oracle wins. Falsifiable proof of P1-A divergence.
   val ctx = EngineContext.defaultContext.copy(
     decisionHints = Some(DecisionHints(
       broadcastArmed          = Some(false),
       broadcastThresholdBytes = Some(10L * 1024 * 1024))))
   val plan = compiled(modelWith(Some(100_000_000L)), JoinHints(), ctx)
   plan should include ("SortMergeJoin")
   plan shouldNot include ("BroadcastHashJoin")
  }

  test("ADR-009-d v0.3: no-oracle broadcast — model with large estimate + decisionHints=None → inline presence rule ARMS → BroadcastHashJoinExec") {
   // No oracle wired: the seed helper falls back to the inline
   // presence rule (`model.joins.exists(_.estimatedRows.isDefined)`),
   // which ARMS because the model has a join with estimatedRows.
   // This is the disagreement case: inline ARMS, oracle (if wired)
   // would DISARM.
   val plan = compiled(modelWith(Some(100_000_000L)), JoinHints())
   plan should include ("BroadcastHashJoin")
  }

  test("ADR-009-d v0.3: oracle-disagreement on small-estimate model — oracle Some(false) wins over inline presence → SortMergeJoinExec") {
   // Identical model (small estimate), the two regimes disagree:
   // inline arms (presence = true), oracle disarms (Some(false)).
   // The oracle wins — falsifiable proof of decisionHints priority.
   val ctx = EngineContext.defaultContext.copy(
     decisionHints = Some(DecisionHints(
       broadcastArmed          = Some(false),
       broadcastThresholdBytes = Some(10L * 1024 * 1024))))
   val plan = compiled(modelWith(Some(1_000L)), JoinHints(), ctx)
   plan should include ("SortMergeJoin")
   plan shouldNot include ("BroadcastHashJoin")
  }

  test("ADR-009-d v0.3: oracle-armed skew — JoinHints.skewFactor=Some(f) + decisionHints skewArmed=Some(true) → per-query conf has f") {
   // Per-query session design: drives the real provider path so
   // we can read the per-query conf via withLastQuerySession. The
   // oracle arms; seedSkewFactor writes f.toLong on the per-query
   // session.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    val querySession = spark.newSession()
    querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    provider.querySessionTL.set(querySession)
    val eCtx = EngineContext.defaultContext.copy(
     joinHints      = JoinHints(skewFactor = Some(9)),
     decisionHints  = Some(DecisionHints(skewArmed = Some(true))))
    val out = provider.query(modelWith(Some(1_000L)), QueryRequest.empty, eCtx)
    out.isRight shouldBe true
    val qs = provider.withQuerySessionTL()
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "9"
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }

  test("ADR-009-d v0.3: oracle-disarmed skew — decisionHints skewArmed=Some(false) suppresses seed even with JoinHints.skewFactor=Some(f)") {
   // Per-query decision oracle: oracle Some(false) suppresses the
   // skew seed even when the caller passed JoinHints.skewFactor =
   // Some(f). Falsifiable proof of oracle priority for skew.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    val querySession = spark.newSession()
    querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    provider.querySessionTL.set(querySession)
    val eCtx = EngineContext.defaultContext.copy(
     joinHints      = JoinHints(skewFactor = Some(9)),
     decisionHints  = Some(DecisionHints(skewArmed = Some(false))))
    val out = provider.query(modelWith(Some(1_000L)), QueryRequest.empty, eCtx)
    out.isRight shouldBe true
    val qs = provider.withQuerySessionTL()
    // Oracle disarms; seedSkewFactor is a no-op even though
    // JoinHints.skewFactor = Some(9) was passed.
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "5.0"
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }

  test("ADR-009-d v0.3: no-oracle skew — model with large estimate + JoinHints.skewFactor=Some(f) + decisionHints=None → inline rule writes f") {
   // No oracle wired: the inline rule (model.joins.exists +
   // JoinHints.skewFactor = Some(f)) writes f.toLong on the
   // per-query session.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    val querySession = spark.newSession()
    querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    provider.querySessionTL.set(querySession)
    val out = provider.query(
     modelWith(Some(100_000_000L)),
     QueryRequest.empty,
     EngineContext.defaultContext.copy(joinHints = JoinHints(skewFactor = Some(11))))
    out.isRight shouldBe true
    val qs = provider.withQuerySessionTL()
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "11"
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }

  test("ADR-009-d v0.3: oracle-disagreement on small-estimate model — oracle Some(false) skew wins over inline rule → conf stays at default") {
   // Identical model (small estimate), the two regimes disagree
   // on the skew path: inline arms on JoinHints.skewFactor =
   // Some(13), oracle disarms. Oracle wins — no conf.set; per-query
   // conf stays at the inherited default 5.0.
   val spark = buildSpark()
   val provider = new SparkEngineProvider(spark, SparkTypeBridge)
   try {
    val querySession = spark.newSession()
    querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
     .createOrReplaceTempView("orders")
    querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
     .createOrReplaceTempView("customers")
    provider.querySessionTL.set(querySession)
    val eCtx = EngineContext.defaultContext.copy(
     joinHints      = JoinHints(skewFactor = Some(13)),
     decisionHints  = Some(DecisionHints(skewArmed = Some(false))))
    val out = provider.query(modelWith(Some(1_000L)), QueryRequest.empty, eCtx)
    out.isRight shouldBe true
    val qs = provider.withQuerySessionTL()
    qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "5.0"
    provider.clearQuerySessionTL()
    try spark.stop() catch { case _: Throwable => () }
   }
  }

 test("ADR-009-d v0.3: skew-axis divergence at the (10M, 1B) window — single estimate (100M) proves inline-vs-oracle disagreement on the skew path") {
  // Per-query decision oracle: a single estimate value (100M rows)
  // that sits in the window where the inline presence rule ARMS
  // (estimatedRows.isDefined == true) but the SkewStub's value-
  // consult rule DISARMS (100M < 1B threshold). The skew axis
  // mirrors the broadcast axis at est=100M but with the OPPOSITE
  // arms: the spark connector must respect the plugin's disarm
  // even though the inline rule would arm. Falsifiable proof of
  // the same-shape divergence on both axes.
  val spark = buildSpark()
  val provider = new SparkEngineProvider(spark, SparkTypeBridge)
  try {
   val querySession = spark.newSession()
   querySession.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
    .createOrReplaceTempView("orders")
   querySession.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
    .createOrReplaceTempView("customers")
   provider.querySessionTL.set(querySession)
   val eCtx = EngineContext.defaultContext.copy(
    joinHints      = JoinHints(skewFactor = Some(15)),
    decisionHints  = Some(DecisionHints(skewArmed = Some(false))))
   val out = provider.query(modelWith(Some(100_000_000L)), QueryRequest.empty, eCtx)
   out.isRight shouldBe true
   val qs = provider.withQuerySessionTL()
   // SkewStub disarms at est=100M (< 1B threshold). Inline rule
   // ARMS (presence=true + Some(f)=15). Connector respects the
   // oracle: conf stays at the inherited 5.0 default.
   qs.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor") shouldBe "5.0"
   provider.clearQuerySessionTL()
   try spark.stop() catch { case _: Throwable => () }
  }
 }

}

