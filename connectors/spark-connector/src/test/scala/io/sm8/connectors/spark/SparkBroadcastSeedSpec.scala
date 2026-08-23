/*
 * SM8 Spark Connector — SparkBroadcastSeedSpec.
 *
 * ADR-009-a v0.1: a model that declares ANY join `estimatedRows`
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
 * `BroadcastHashJoinExec` can appear is the ADR-009-a seed arming
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

import io.sm8.core.engine.{EngineContext, EngineIdentity, JoinHints, QueryRequest, ResolvedSource}
import io.sm8.core.expr.Expr
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

  private def compiled(model: Model, hints: JoinHints): String = {
    val spark = buildSpark()
    try {
      spark.sql("SELECT * FROM VALUES (1,'east'),(2,'west') AS t(id, region)")
        .createOrReplaceTempView("orders")
      spark.sql("SELECT * FROM VALUES ('east'),('west') AS t(region)")
        .createOrReplaceTempView("customers")
      // Drive the REAL ADR-009-a contract: the seed helper + the
      // lowerer's byte-gate. The helper is what query()/explain()
      // use; passing its output into the shared RelOp compile path
      // exercises the exact seeded broadcast decision.
      val resolver = new SparkSourceResolver(
        spark, SparkSourceResolver.SessionCatalogModelRegistry)
      val relOpE = QueryBuilder.build(model, resolver, identity)
      relOpE.left.foreach(e => fail(s"build failed: $e"))
      val relOp = relOpE.toOption.get
      val ctx = EngineContext.defaultContext.copy(joinHints = hints)
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

}
