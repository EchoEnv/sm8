/*
 * SM8 query-frequency-observer Plugin tests (Ticket 6 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md).
 *
 * Acceptance criteria under test:
 * - observer counts query shapes (feeding Ticket 2's
 *   instrumentation): per-(model, version, measures, dimensions)
 *   frequency, canonical keys (set-equal shapes collide), hot-shape
 *   ordering, cardinality cap, meta publication for `sm8 inspect`.
 * - hook contract: PostExecute observer (always fires), no captured
 *   state, Serializable.
 */
package io.sm8.plugins.queryfreqobs

import io.sm8.core.engine.{EngineContext, EngineHookRequest, EngineIdentity}
import io.sm8.core.engine.QueryRequest
import io.sm8.core.expr.Expr
import io.sm8.core.model.{
  AuditPolicy, CachePolicy, Dimension, MaterializePolicy, Measure,
  Model, ModelPolicyDefaults, SourceRef
}
import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.sdk.{Context, HookStage, PipelineStage}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QueryFrequencyObserverPluginSpec extends AnyFunSuite with Matchers with org.scalatest.BeforeAndAfterEach {

  private def model(name: String = "flights"): Model =
    Model.of(
      name = name,
      version = 1,
      dimensions = List(Dimension.field("carrier", "carrier")),
      measures = List(
        Measure("rows", AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")),
        Measure.aggregate("total_fare", AggregateFn.Sum, Expr.FieldRef("fare"))),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw")
    ).right.get

  private def request(dims: List[String], meas: List[String]): QueryRequest =
    QueryRequest(
      model = "flights",
      dimensions = dims,
      aggregateMeasures = meas.map(n =>
        io.sm8.core.rel.TypedAggregateCall.of[Nothing](name = n, fn = AggregateFn.Count)))

  private def hookContext(model: Model, request: QueryRequest): Context =
    Context(
      stage = PipelineStage.Execute,
      request = EngineHookRequest(model, request, cacheKey = "k"))

  private val plugin = new QueryFrequencyObserverPlugin

  /** Fresh counters per test (the counters object is JVM-global). */
  override def beforeEach(): Unit = QueryShapeCounters.reset()

  test("observer counts one shape per invocation; snapshot orders hottest first") {
    val hook = plugin.hook
    val req = request(List("carrier"), List("rows"))
    // 3x shape A, 1x shape B (different dim set)
    hook.run(hookContext(model(), req))
    hook.run(hookContext(model(), req))
    hook.run(hookContext(model(), req))
    hook.run(hookContext(model(), request(List("carrier", "dest"), List("rows", "total_fare"))))
    val snap = QueryShapeCounters.snapshot()
    snap.size shouldBe 2
    snap.head._2 shouldBe 3L // hottest first
    snap(1)._2 shouldBe 1L
  }

  test("canonical key: set-equal shapes with different member ORDER collide") {
    val hook = plugin.hook
    hook.run(hookContext(model(), request(List("a", "b"), List("m1", "m2"))))
    hook.run(hookContext(model(), request(List("b", "a"), List("m2", "m1"))))
    QueryShapeCounters.snapshot().size shouldBe 1
    QueryShapeCounters.snapshot().head._2 shouldBe 2L
    // Key itself is canonically sorted:
    QueryShapeCounters.shapeKey(model(), request(List("b", "a"), List("m2", "m1"))) should
      include("d=a,b")
  }

  test("counts publish into context.meta for the meta-inspector (sm8 inspect surface)") {
    val hook = plugin.hook
    val out = hook.run(hookContext(model(), request(List("carrier"), List("rows"))))
    val counts = out.meta.get("io.sm8.plugins.queryfreqobs:counts")
    counts shouldBe defined
    counts.get shouldBe a[List[_]]
    counts.get.asInstanceOf[List[_]].nonEmpty shouldBe true
  }

  test("cardinality cap: beyond MaxShapes distinct shapes, counts land under __overflow__") {
    QueryShapeCounters.increment("__seed__") // 1 slot used
    val keys = (1 until QueryShapeCounters.MaxShapes).map(i => s"k$i")
    keys.foreach(QueryShapeCounters.increment) // fill to the cap
    // One more distinct key -> overflow, not unbounded growth.
    val n = QueryShapeCounters.increment("brand-new-shape")
    n shouldBe 1L
    QueryShapeCounters.snapshot().find(_._1 == QueryShapeCounters.OverflowKey) shouldBe defined
    // An EXISTING key still increments normally after the cap:
    QueryShapeCounters.increment("k5") shouldBe 2L
  }

  test("hook contract: PostExecute observer, always fires, no captured state") {
    val hook = plugin.hook
    hook.stage shouldBe HookStage.PostExecute
    hook.runsOnStop shouldBe true
    plugin.closedOverVars shouldBe Seq.empty
    plugin.name shouldBe "query-frequency-observer"
  }

  test("non-EngineHookRequest context: observer is a no-op passthrough (never throws)") {
    val plain = Context(
      stage = PipelineStage.Execute,
      request = new io.sm8.sdk.Request {})
    val out = plugin.hook.run(plain)
    out.meta.get("io.sm8.plugins.queryfreqobs:counts") shouldBe None
  }
}
