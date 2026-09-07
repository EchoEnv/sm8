/*
 * SM8 rollup-refusal-observer Plugin tests.
 *
 * Acceptance criteria under test:
 * - hook contract: PostExecute observer (always fires), no captured
 *   state, Serializable, stable name.
 * - publishing semantics: snapshot is written into context.meta
 *   under the stable key `io.sm8.plugins.rolluprefusalobs:counters`,
 *   with the rewrites / refusals / refusalsPermanent /
 *   refusalsByReason values populated from the registered sink.
 * - sink swap behavior: a fresh MetricsSink implementation
 *   (`TestSink`) is registered around each test to verify the hook
 *   reads from the JVM-global registry without holding its own
 *   counter state.
 */
package io.sm8.plugins.rolluprefusalobs

import io.sm8.core.cache.{MetricsRegistry, MetricsSink, RollupCountersSnapshot}
import io.sm8.core.rel.RollupRewriter
import io.sm8.sdk.{Context, HookStage, PipelineStage}

import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Test sink that holds its own counters so each test can verify
  * the plugin publishes exactly what the sink exposes.
  */
private final class TestSink(
    rewritesInit: Long = 0L,
    refusalsInit: Long = 0L,
    refusalsPermanentInit: Long = 0L,
    refusalsByReasonInit: List[(String, Long)] = Nil
) extends MetricsSink {
  private val rewrites          = new AtomicLong(rewritesInit)
  private val refusals          = new AtomicLong(refusalsInit)
  private val refusalsPermanent = new AtomicLong(refusalsPermanentInit)
  private val refusalsByReason  = new java.util.concurrent.ConcurrentHashMap[String, AtomicLong]()

  refusalsByReasonInit.foreach { case (k, v) =>
    refusalsByReason.put(k, new AtomicLong(v))
  }

  override def recordRollupRewrite(): Unit = rewrites.incrementAndGet()
  override def recordRollupRefusal(reason: RollupRewriter.RollupRewriteRefusal): Unit = {
    refusals.incrementAndGet()
    if (RollupRewriter.RollupRewriteRefusal.isPermanent(reason)) {
      refusalsPermanent.incrementAndGet()
    }
    val key = RollupRewriter.RollupRewriteRefusal.reasonName(reason)
    refusalsByReason
      .computeIfAbsent(key, _ => new AtomicLong(0))
      .incrementAndGet()
  }
  override def rollupSnapshot(): RollupCountersSnapshot =
    RollupCountersSnapshot(
      rewrites          = rewrites.get,
      refusals          = refusals.get,
      refusalsPermanent = refusalsPermanent.get,
      refusalsByReason  = refusalsByReason.asScala.toList
                            .map { case (k, v) => (k, v.get) }
                            .sortBy { case (k, _) => k })
}

class RollupRefusalObserverPluginSpec extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  private val plugin = new RollupRefusalObserverPlugin

  /** Fresh sink per test (the registry holds a JVM-global; reset
    * keeps tests isolated). */
  override def beforeEach(): Unit = {
    MetricsRegistry.register(new TestSink())
  }

  private def emptyContext: Context = Context(
    stage   = PipelineStage.Execute,
    request = io.sm8.core.engine.EngineHookRequest(
      model      = io.sm8.core.model.Model.of(
                    name = "x", version = 1,
                    dimensions = Nil, measures = Nil,
                    source = io.sm8.core.model.SourceRef.ByName(table = "t")).right.get,
      mcpRequest = io.sm8.core.engine.QueryRequest(model = "x"),
      cacheKey   = "k"),
    result  = None,
    meta    = Map.empty,
    stop    = false
  )

  test("plugin name is stable: rollup-refusal-observer") {
    plugin.name shouldBe "rollup-refusal-observer"
  }

  test("closedOverVars is Nil (no captured state — journal-safe)") {
    plugin.closedOverVars shouldBe Nil
  }

  test("hook is a PostExecute observer at priority 910, runsOnStop = true") {
    val h = plugin.hook
    h.stage shouldBe HookStage.PostExecute
    h.priority shouldBe 910
    h.runsOnStop shouldBe true
    h.name shouldBe "rollup-refusal-observer"
  }

  test("hook publishes the sink snapshot into context.meta under the stable key") {
    MetricsRegistry.register(
      new TestSink(
        rewritesInit          = 7L,
        refusalsInit          = 3L,
        refusalsPermanentInit = 1L,
        refusalsByReasonInit  = List(
          "grainMismatch"          -> 2L,
          "unsplittableAggregate"  -> 1L)))
    val ctx0 = emptyContext
    val ctx1 = plugin.hook.run(ctx0)
    val key = "io.sm8.plugins.rolluprefusalobs:counters"
    ctx1.meta should contain key key
    val snap = ctx1.meta(key).asInstanceOf[RollupCountersSnapshot]
    snap.rewrites shouldBe 7L
    snap.refusals shouldBe 3L
    snap.refusalsPermanent shouldBe 1L
    snap.refusalsByReason shouldBe List(
      "grainMismatch"         -> 2L,
      "unsplittableAggregate" -> 1L)
  }

  test("hook reads fresh counts from the registered sink on every call (no own state)") {
    MetricsRegistry.register(new TestSink())
    val ctx0 = emptyContext
    val ctx1 = plugin.hook.run(ctx0)
    val snap1 = ctx1.meta("io.sm8.plugins.rolluprefusalobs:counters")
                               .asInstanceOf[RollupCountersSnapshot]
    snap1.rewrites shouldBe 0L

    // Simulate one rewrite + one permanent refusal flowing through
    // the routing-invocation PR (which would normally live in
    // EngineService.runQueryWithHooks). The plugin must reflect
    // these without holding any internal counter.
    val sink = MetricsRegistry.sink()
    sink.recordRollupRewrite()
    sink.recordRollupRefusal(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
    val ctx2 = plugin.hook.run(emptyContext)
    val snap2 = ctx2.meta("io.sm8.plugins.rolluprefusalobs:counters")
                               .asInstanceOf[RollupCountersSnapshot]
    snap2.rewrites shouldBe 1L
    snap2.refusals shouldBe 1L
    snap2.refusalsPermanent shouldBe 1L
    snap2.refusalsByReason shouldBe List("unsplittableAggregate" -> 1L)
  }

  test("with the default no-op sink, the hook still runs and publishes the empty snapshot") {
    MetricsRegistry.register(MetricsSink.NoOp)
    val ctx1 = plugin.hook.run(emptyContext)
    val snap = ctx1.meta("io.sm8.plugins.rolluprefusalobs:counters")
                               .asInstanceOf[RollupCountersSnapshot]
    snap shouldBe RollupCountersSnapshot.empty
  }
}
