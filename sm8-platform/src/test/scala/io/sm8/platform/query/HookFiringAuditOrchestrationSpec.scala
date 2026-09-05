/*
 * SM8 Platform — HookFiringAuditOrchestrationSpec.
 *
 * Production-wiring integration spec for the hook-firing-audit
 * plugin: drives the plugin through HookRunnerOrchestration +
 * EngineHookDispatcher — the real production entry point — covering
 * the completed path, cross-stage short-circuits, and same-stage
 * probe/stopper ordering.
 *
 * This spec exists because the silent-inertness-detection plugin
 * validates the production dispatch seam directly, which the
 * hook.run() isolation tests cannot. Direct hook.run() calls miss
 * the cross-stage short-circuit semantics that drive this plugin's
 * design.
 */
package io.sm8.platform.query

import io.sm8.core.EngineImpl
import io.sm8.core.engine.EngineError
import io.sm8.platform.query.hooks.{EngineHookDispatcher, HookRunnerOrchestration}
import io.sm8.plugins.hookfiringaudit.{HookFiringAuditKeys, HookFiringAuditPlugin}
import io.sm8.sdk.{Context, HookStage, PipelineStage, PreHook, Request, Result}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HookFiringAuditOrchestrationSpec extends AnyFunSuite with Matchers {

  private case object HookFiringAuditProbeRequest extends Request
  private case object HookFiringAuditProbeResult extends Result

  /** A pre-hook that sets Context.stop at its declared stage. */
  private final class StopperHook(stopStage: HookStage, override val priority: Int) extends PreHook {
    override val name: String = s"stopper-${stopStage}-${priority}"

    /** The attachment point this stopper sits at.
      *
      * @return the stage this stopper binds
      */
    override def stage: HookStage = stopStage

    /** Sets Context.stop = true so the dispatcher halts at the
      * orchestrator's next stage boundary.
      *
      * @param context the incoming context
      * @return the context with stop = true
      */
    override def run(context: Context): Context = context.copy(stop = true)
  }

  private def orchestratorFor(engine: EngineImpl): HookRunnerOrchestration =
    HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))

  /** Stopper registered BEFORE the plugin (higher seq at the same
    * priority sort). With probes at priority 1 the ordering of
    * registration does not matter — probes always sort first. */
  private def wiredEngine(stopper: Option[(HookStage, Int)]): EngineImpl = {
    val engine = EngineImpl()
    stopper.foreach { case (s, p) =>
      engine.hooks.registerPreHook(s, new StopperHook(s, p), p)
    }
    engine.use(new HookFiringAuditPlugin)
    engine
  }

  private val initial: Context =
    Context(
      stage = PipelineStage.Parse,
      request = HookFiringAuditProbeRequest,
      result = None,
      meta = Map.empty,
      stop = false
    )

  private def run(engine: EngineImpl): Either[EngineError, Context] =
    orchestratorFor(engine).run(initial,
      c => Right(c.copy(result = Some(HookFiringAuditProbeResult))))

  test("hook-firing-audit: completed pipeline writes a report with all 8 fired and no anomaly") {
    val Right(finalCtx) = run(wiredEngine(stopper = None))

    val report = finalCtx.meta(HookFiringAuditKeys.ReportKey).asInstanceOf[Map[String, Any]]
    report("fired") shouldBe List(
      "post:execute", "post:format", "post:parse", "post:resolve",
      "pre:execute", "pre:format", "pre:parse", "pre:resolve"
    )
    report("skipped") shouldBe List.empty
    report("missing") shouldBe List.empty
    report("stopped") shouldBe false
    finalCtx.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }

  test("hook-firing-audit: cross-stage short-circuit at pre:resolve fires the terminal reporter at post:resolve") {
    val Right(finalCtx) = run(wiredEngine(stopper = Some((HookStage.PreResolve, 50))))

    finalCtx.stop shouldBe true
    finalCtx.meta.contains(HookFiringAuditKeys.ReportKey) shouldBe true

    val report = finalCtx.meta(HookFiringAuditKeys.ReportKey).asInstanceOf[Map[String, Any]]
    report("stopped") shouldBe true
    // The probe (priority 1) fires before the same-stage stopper
    // (priority 50); the terminal reporter is the PostResolve one.
    report("fired") shouldBe List("post:parse", "post:resolve", "pre:parse", "pre:resolve")
    report("skipped") shouldBe
      List("post:execute", "post:format", "pre:execute", "pre:format")
    report("missing") shouldBe List.empty
    finalCtx.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }

  test("hook-firing-audit: typed anomaly is never produced by a healthy completed pipeline") {
    val outcome = run(wiredEngine(stopper = None))
    outcome.isRight shouldBe true
    val finalCtx = outcome.toOption.get
    finalCtx.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }
}
