/*
 * SM8 Spark Connector — PortableQueryCompiler legacy compile path seed.
 *
 * Falsifiable coverage for the F-04 audit finding: the legacy
 * `PortableQueryCompiler.compile(model, ctx)` entry point (PR-K)
 * builds a join plan via `applyJoins`, which reads
 * `ctx.joinHints.broadcastRightBelowBytes` to decide whether to call
 * `functions.broadcast`. The broadcast seed — which arms the gate
 * from `model.joins[].estimatedRows` — historically lived only on
 * the request-layer paths (`SparkEngineProvider.query` and
 * `compileModelToDataFrame`). A caller invoking `compile(model, ctx)`
 * directly (test fixtures, replays, third-party drivers) read an
 * un-seeded `ctx` and the broadcast gate was silently OFF.
 *
 * Per [[scala-spark-batch-bugs-mindset]] §1: every assertion is on the evaluated
 * physical plan, not the intermediate logical hints. Adaptive Query
 * Execution is disabled in `buildSpark` so Catalyst cannot re-plan
 * the join around the assertion; the sm8 seed's explicit `broadcast()`
 * call (or its absence) is the only determinant of the physical
 * plan.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.{DecisionHints, EngineContext, EngineIdentity, JoinHints, ResolvedSource}
import io.sm8.core.model._
import io.sm8.core.rel.JoinKind
import io.sm8.core.schema.{Field, SealedDataType}

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PortableQueryCompilerLegacySeedSpec extends AnyFunSuite with Matchers {

  private val identity: EngineIdentity = EngineIdentity(
    name = "sm8-legacy-seed-test", nativeVersion = "3.5.8", engineAdapterVersion = "0.1.0",
  )

  private val ordersScan: ResolvedSource.Scan = ResolvedSource.Scan(
    source = SourceRef.ByName(table = "orders"),
    schema = List(
      Field("id",     SealedDataType.Int,     nullable = false),
      Field("region", SealedDataType.Varchar, nullable = false),
    ),
  )

  private def buildSpark(autoBroadcast: String = "10485760"): SparkSession =
    SparkSession.builder()
      .master("local[1]")
      .appName("PortableQueryCompilerLegacySeedSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      // Disable AQE so Catalyst cannot re-plan the join around the
      // assertion (per [[scala-spark-batch-bugs-mindset]] §1: what you wrote isn't
      // what runs — AQE is a second optimizer pass on top of the
      // explicit `functions.broadcast` call). The sm8 seed's explicit
      // `broadcast()` call (or its absence) is the only determinant.
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.sql.adaptive.skewJoin.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", autoBroadcast)
      .getOrCreate()

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

  /** Run the LEGACY `compile(model, ctx)` entry point and return
    * the evaluated physical plan string. The session construction is
    * inside the `try` so a `buildSpark` failure (port conflict,
    * classpath missing) does not leak the previous session. */
  private def compileLegacy(model: Model, hints: JoinHints, autoBroadcast: String = "10485760"): String = {
    var spark: SparkSession = null
    try {
      spark = buildSpark(autoBroadcast)
      spark.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
        .createOrReplaceTempView("orders")
      spark.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
        .createOrReplaceTempView("customers")
      val ctx = EngineContext.defaultContext.copy(joinHints = hints)
      val compiledE = new PortableQueryCompiler(spark).compile(model, ctx)
      compiledE.left.foreach(e => fail(s"compile failed: $e"))
      compiledE.toOption.get.queryExecution.executedPlan.toString
    } finally {
      if (spark != null) spark.stop()
    }
  }

  test("legacy compile path: sm8 seed fires when Spark heuristic is structurally OFF (autoBroadcast='0b')") {
    // FALSIFIABLE: `autoBroadcastJoinThreshold = 0b` is parsed by the
    // sm8 seed as `v=0` (=> `v > 0L` false => fallback to
    // BroadcastSeedDefaultBytes), and `operatorDisabledBroadcast` checks
    // `v == -1L` (not -1) so the seed ARMS with 10 MiB. Spark's
    // heuristic treats 0 as disabled (negative or zero disables
    // auto-broadcast). Pre-fix: applyJoins reads None, no explicit
    // broadcast call, Spark heuristic off => SortMergeJoin. Post-fix:
    // applyJoins reads Some(10 MiB), right-side fits, explicit
    // broadcast => BroadcastHashJoin. This is the only Spark conf where
    // the sm8 seed's behavior on `compile(model, ctx)` differs from
    // Spark's heuristic.
    val plan = compileLegacy(modelWith(Some(1000L)), JoinHints(), autoBroadcast = "0b")
    plan should include ("BroadcastHashJoin")
    plan shouldNot include ("SortMergeJoin")
  }

  test("legacy compile path: no estimate + no hint + Spark off => SortMergeJoin") {
    val plan = compileLegacy(modelWith(None), JoinHints(), autoBroadcast = "-1")
    plan should include ("SortMergeJoin")
    plan shouldNot include ("BroadcastHashJoin")
  }

  test("legacy compile path: explicit JoinHints.broadcastRightBelowBytes still overrides") {
    val plan = compileLegacy(
      modelWith(Some(1000L)),
      JoinHints(broadcastRightBelowBytes = Some(10L * 1024 * 1024)))
    plan should include ("BroadcastHashJoin")
  }

  test("legacy compile path: oracle path arms with plugin-set broadcastThresholdBytes (no estimate, Spark heuristic off)") {
    // FALSIFIABLE: Spark's `autoBroadcastJoinThreshold = -1` (heuristic
    // OFF) AND no inline `JoinHints.broadcastRightBelowBytes` (caller
    // didn't set) AND no `joins[].estimatedRows` (model has no
    // estimate to arm the inline rule). The ONLY broadcast trigger is
    // the plugin's oracle arm. Pre-fix (no seed on legacy path):
    // `applyJoins` reads `None` → no explicit broadcast call → no
    // broadcast → `SortMergeJoin`. Post-fix: seed arms from the oracle
    // → `seeded = Some(oracle.broadcastThresholdBytes)` → explicit
    // broadcast → `BroadcastHashJoin`.
    val oracle = DecisionHints(
      broadcastArmed          = Some(true),
      broadcastThresholdBytes = Some(10L * 1024 * 1024),
    )
    val modelNoEst = modelWith(None)
    val ctx = EngineContext.defaultContext.copy(
      joinHints      = JoinHints(),
      decisionHints  = Some(oracle),
    )
    var spark: SparkSession = null
    try {
      spark = buildSpark(autoBroadcast = "-1")
      spark.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
        .createOrReplaceTempView("orders")
      spark.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
        .createOrReplaceTempView("customers")
      val compiledE = new PortableQueryCompiler(spark).compile(modelNoEst, ctx)
      compiledE.left.foreach(e => fail(s"compile failed: $e"))
      val plan = compiledE.toOption.get.queryExecution.executedPlan.toString
      plan should include ("BroadcastHashJoin")
      plan shouldNot include ("SortMergeJoin")
    } finally {
      if (spark != null) spark.stop()
    }
  }

  test("PortableQueryCompiler.seedBroadcastThreshold: null spark returns eCtx unchanged") {
    val model = modelWith(Some(1000L))
    val ctx   = EngineContext.defaultContext.copy(joinHints = JoinHints())
    val out   = PortableQueryCompiler.seedBroadcastThreshold(null, ctx, model)
    out should be theSameInstanceAs ctx
  }

  test("PortableQueryCompiler.seedBroadcastThreshold: explicit caller hint is preserved (no override)") {
    val model = modelWith(Some(1000L))
    val explicit: Long = 7L * 1024 * 1024
    val ctx = EngineContext.defaultContext.copy(
      joinHints = JoinHints(broadcastRightBelowBytes = Some(explicit)))
    var spark: SparkSession = null
    try {
      spark = buildSpark()
      val out = PortableQueryCompiler.seedBroadcastThreshold(spark, ctx, model)
      out.joinHints.broadcastRightBelowBytes shouldBe Some(explicit)
    } finally {
      if (spark != null) spark.stop()
    }
  }

  // The 2-arg `compileRelOp(relOp, ctx)` and 3-arg
  // `compileRelOp(relOp, ctx, preFilteredDf)` overloads do NOT self-seed
  // (they receive only a `RelOp`, no `Model`; the oracle path requires
  // `model.joins`). Production request-layer paths (`SparkEngineProvider.query`
  // line 342, `compileModelToDataFrame` line 901) seed before calling.
  // Direct callers of the 2-/3-arg overloads must seed `ctx` themselves;
  // this is documented at the overload signatures.

  test("compileRelOp 5-arg overload: seeds ctx before validation, broadcast via oracle (Spark off)") {
    // FALSIFIABLE: `compileRelOp(model, relOp, ctx, scan, preFilteredDf)`
    // is the canonical entry point with model access. With Spark's
    // `autoBroadcastJoinThreshold = -1` (heuristic OFF), no inline
    // `JoinHints.broadcastRightBelowBytes`, and an oracle arm, the
    // ONLY broadcast trigger is the seed (the post-fix `seededCtx`
    // that `compileRelOp` produces for the lowerer).
    val oracle = DecisionHints(
      broadcastArmed          = Some(true),
      broadcastThresholdBytes = Some(10L * 1024 * 1024),
    )
    val m = modelWith(None)
    val ctx = EngineContext.defaultContext.copy(decisionHints = Some(oracle))
    var spark: SparkSession = null
    try {
      spark = buildSpark(autoBroadcast = "-1")
      spark.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
        .createOrReplaceTempView("orders")
      spark.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
        .createOrReplaceTempView("customers")
      val resolver = new SparkSourceResolver(
        spark, SparkSourceResolver.SessionCatalogModelRegistry)
      val relOpE = io.sm8.core.query.QueryBuilder.build(m, resolver, identity)
      relOpE.left.foreach(e => fail(s"build failed: $e"))
      val compiledE = new PortableQueryCompiler(spark)
        .compileRelOp(m, relOpE.toOption.get, ctx, ordersScan, None)
      compiledE.left.foreach(e => fail(s"compile failed: $e"))
      val plan = compiledE.toOption.get.queryExecution.executedPlan.toString
      plan should include ("BroadcastHashJoin")
      plan shouldNot include ("SortMergeJoin")
    } finally {
      if (spark != null) spark.stop()
    }
  }
}