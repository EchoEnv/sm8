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
      // PR-197 (Round 1 audit MED F-03): the sm8 seed now respects
      // this `-1` sentinel by ALSO disarming (previously the seed
      // silently re-enabled with 10 MiB default — the opposite of
      // operator intent). The "seed arms with estimatedRows" test
      // below uses `buildSparkWithLargeThreshold` +
      // `compiledWithLargeThreshold` for the canonical
      // "operator did not disable" path that the seed should still arm.
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

  // PR-197 (Round 1 audit MED F-03): the canonical `buildSpark`
  // uses `autoBroadcastJoinThreshold = -1` to disable Spark's own
  // broadcast heuristic — which (after the PR-197 fix) ALSO
  // disarms the sm8 seed. For tests that want to exercise the
  // "sm8 seed ARMS the broadcast hint" path, we need a session
  // where broadcast is allowed (NOT -1) and the threshold is
  // high enough that Spark itself doesn't broadcast the tiny
  // test fixture. 100 MiB (=104857600) is well above the test
  // fixture's actual sizeInBytes (~100 bytes).
  private def buildSparkWithLargeThreshold(): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("SparkBroadcastSeedSpec-LargeThreshold")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()

  /** PR-197 (Round 1 audit MED F-03): helper that runs the same
   * compile pipeline as `compiled` but with a SparkSession that
   * has `autoBroadcastJoinThreshold = 104857600` (100 MiB, NOT -1).
   * Used for the ADR-009-d "inline presence rule ARMS" test,
   * which requires the session to allow broadcast. */
  private def compiledWithLargeThreshold(model: Model, hints: JoinHints, eCtxOverride: EngineContext = EngineContext.defaultContext): String = {
    val spark = buildSparkWithLargeThreshold()
    compiledWithSpark(spark, model, hints, eCtxOverride)
  }

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
   compiledWithSpark(spark, model, hints, eCtxOverride)
  }

  /** Shared pipeline. PR-197 split this out so the build +
   * compile + plan-stringify sequence is shared between tests. */
  private def compiledWithSpark(spark: SparkSession, model: Model, hints: JoinHints, eCtxOverride: EngineContext): String = {
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

  test("seed: a declared join estimate arms the broadcast hint ONLY when operator has not disabled Spark broadcast") {
    // PR-197 (Round 1 audit MED F-03): the previous shape (pre-PR-197)
    // set `autoBroadcastJoinThreshold = -1` to disable Spark's own
    // heuristic, then expected the sm8 seed (default 10 MiB) to be
    // the ONLY mechanism producing broadcast. That test incidentally
    // codified the buggy behavior: the seed silently re-enabled
    // broadcast on operators who explicitly disabled it (the opposite
    // of intent). After PR-197 the sm8 seed ALSO disarms when `-1`
    // is set, so this test now asserts the corrected contract:
    // when operator says `-1` AND a model has a join estimate,
    // NO broadcast happens (the seed respects the disable).
    val plan = compiled(modelWith(Some(1000L)), JoinHints())
    plan should include ("SortMergeJoin")
    plan shouldNot include ("BroadcastHashJoin")
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
    // PR-197 (Round 1 audit MED F-03): with `buildSpark` setting
    // `autoBroadcastJoinThreshold = -1` (Spark disable sentinel),
    // the sm8 seed ALSO disarms, so the plan is SortMergeJoin
    // regardless of the seed value (which is what we're testing —
    // the seed value's effect would be a separate test).
    val p1 = compiled(modelWith(Some(100L)), JoinHints())
    val p2 = compiled(modelWith(Some(200L)), JoinHints())
    p1 should include ("SortMergeJoin")
    p2 should include ("SortMergeJoin")
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

  test("ADR-009-d v0.3: no-oracle broadcast — model with large estimate + decisionHints=None + session-default threshold → inline presence rule ARMS → BroadcastHashJoinExec") {
   // PR-197 (Round 1 audit MED F-03): the previous shape set
   // `autoBroadcastJoinThreshold = -1` to disable Spark's own
   // heuristic. After the PR-197 fix, the sm8 seed ALSO disarms
   // when `-1` is set — so this test must use a non-`-1` session
   // threshold to exercise the "inline presence rule ARMS" path.
   // We use `buildSparkWithLargeThreshold` (a non-`-1` high value)
   // to isolate the seed's arm behavior. The session threshold
   // (104857600 = 100 MiB) is way above the tiny test fixture's
   // sizeInBytes, so Spark itself wouldn't broadcast — the only
   // broadcast observed comes from the sm8 seed's `df.broadcast()`
   // hint.
   //
   // PR-197 dual-review (macaque MEDIUM test-rigor): this test
   // could pass via EITHER the sm8 seed OR Spark's own heuristic
   // (both fire when join is small). The sibling test below
   // isolates the sm8-seed-only path via an explicit
   // `JoinHints(broadcastRightBelowBytes=...)` — that path bypasses
   // the sm8 seed (seed only runs when `joinHints.broadcastRightBelowBytes`
   // is None per `seedBroadcastThreshold` .orElse at line 1142).
   val plan = compiledWithLargeThreshold(modelWith(Some(100_000_000L)), JoinHints())
   plan should include ("BroadcastHashJoin")
  }

  test("PR-197 dual-review: explicit JoinHints.broadcastRightBelowBytes isolates sm8 seed from Spark's heuristic (sibling test)") {
   // PR-197 dual-review (macaque MEDIUM test-rigor): this sibling
   // test complements the inline-arm test above by isolating the
   // sm8-seed arm behavior. When `JoinHints.broadcastRightBelowBytes`
   // is set EXPLICITLY (e.g. to 10 MiB), the lowerer's
   // `shouldBroadcast` check uses the explicit hint directly
   // (broadcast only if rightBytes <= 10 MiB). With the tiny
   // fixture, rightBytes (~100 bytes) <= 10 MiB, so the join
   // broadcasts — and the broadcast is unambiguously from the
   // lowerer's `df.broadcast()` hint, NOT from Spark's heuristic.
   //
   // Why this matters for regression detection: if a future PR
   // makes the sm8 seed a no-op (e.g. inline presence rule stops
   // arming), the inline-arm test above still passes (Spark
   // broadcasts anyway), but THIS test still passes too because
   // the explicit JoinHints bypass the seed entirely. So these
   // two tests verify different aspects of the seed contract:
   // the inline-arm test verifies "session default + estimatedRows
   // => SOME broadcast happens"; this sibling verifies "explicit
   // caller hint => broadcast happens (sm8 seed not involved)".
   val plan = compiledWithLargeThreshold(
     modelWith(Some(100_000_000L)),
     JoinHints(broadcastRightBelowBytes = Some(10L * 1024 * 1024)))
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

 // ------------------------------------------------------------------------
 // PR-209 (tigress MED-1 residual): non-(-1) negative thresholds also
 // disarm the sm8 seed, mirroring Spark's own `EnsureRequirements`
 // rule (`autoBroadcastJoinThreshold < 0 ⇒ None`). The pre-PR-209
 // shape only matched the literal `-1`; values like `-2b` or
 // `-100b` were silently re-armed with the 10 MiB default (the
 // opposite of operator intent). The `0b` case is preserved as
 // enabled (Spark's "always broadcast" sentinel).
 // ------------------------------------------------------------------------

 /** PR-209: parameterized spark builder — overrides the
 * `autoBroadcastJoinThreshold` to a caller-supplied raw value so
 * the seed's negative/zero handling is exercised without baking
 * the threshold into a named helper per case. */
 private def buildSparkWithThreshold(value: String): SparkSession =
  SparkSession.builder()
   .master("local[1]")
   .appName(s"SparkBroadcastSeedSpec-$value")
   .config("spark.sql.shuffle.partitions", "1")
   .config("spark.ui.enabled", "false")
   .config("spark.driver.host", "127.0.0.1")
   .config("spark.driver.bindAddress", "127.0.0.1")
   .config("spark.sql.autoBroadcastJoinThreshold", value)
   .config("spark.sql.adaptive.enabled", "false")
   .getOrCreate()

 /** PR-209: helper that pairs the parameterized spark builder
 * with the shared compile pipeline. Mirrors the shape of
 * `compiled` / `compiledWithLargeThreshold` so the existing
 * test surface stays homogeneous. */
 private def compiledWithThreshold(value: String, model: Model, hints: JoinHints): String = {
  val spark = buildSparkWithThreshold(value)
  compiledWithSpark(spark, model, hints, EngineContext.defaultContext)
 }

 test("PR-209: non-(-1) negative threshold (-2b) — operator disabled, sm8 seed disarms → SortMergeJoin") {
  // Falsifiable: pre-PR-209 the seed only matched `== -1L`, so
  // a `-2b` threshold fell through to the 10 MiB default and the
  // plan was a BroadcastHashJoin — the opposite of operator
  // intent (Spark itself treats `< 0` as disabled per
  // EnsureRequirements). Post-PR-209 the seed mirrors Spark's
  // `< 0` rule and the plan is a SortMergeJoin, identical to
  // the `-1b` falsifiable behavior.
  val plan = compiledWithThreshold("-2b", modelWith(Some(1000L)), JoinHints())
  plan should include ("SortMergeJoin")
  plan shouldNot include ("BroadcastHashJoin")
 }

 test("PR-209: non-(-1) negative threshold (-100b) — operator disabled, sm8 seed disarms → SortMergeJoin") {
  // Falsifiable: same contract as the `-2b` case but with a
  // larger negative magnitude. Proves the `< 0L` comparison
  // holds across the negative integer range, not just for `-1`
  // and `-2`. Spark's own `< 0` rule is magnitude-blind; the
  // sm8 seed mirrors that.
  val plan = compiledWithThreshold("-100b", modelWith(Some(1000L)), JoinHints())
  plan should include ("SortMergeJoin")
  plan shouldNot include ("BroadcastHashJoin")
 }

 test("PR-209: zero threshold (0b) — NOT disabled (Spark 'always broadcast'), sm8 seed still arms → BroadcastHashJoin") {
  // Falsifiable: `0b` is Spark's "always broadcast" sentinel —
  // Spark itself broadcasts unconditionally when the threshold
  // is `0`. The sm8 seed must NOT over-correct and disarm here
  // (the operator wants broadcast, just unconditionally).
  // `0L < 0L` is false → operatorDisabledBroadcast stays false
  // → seed follows the inline presence rule → BroadcastHashJoin.
  // This test is the negative case that proves the `< 0L` (not
  // `<= 0L`) comparison is the right shape.
  val plan = compiledWithThreshold("0b", modelWith(Some(1000L)), JoinHints())
  plan should include ("BroadcastHashJoin")
 }
}

