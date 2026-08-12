/*
 * SM8 Core — HookDispatchSpec.
 *
 * Step 4: direct unit tests for HookManager priority dispatch and
 * Pipeline hook integration (RFC §8, §9). This is the "covering
 * tests" that the codegraph flagged as missing in the Step 3 audit.
 *
 * Per [[debug-mantra-mindset]]: these tests assert real behavior —
 * not silent passes (the Step 3 audit caught one such test and
 * replaced it with `cancel`; this spec lands the direct tests).
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * three tests cover the three new behaviors (priority order,
 * fail-fast, stop-flag).
 */
package io.sm8.core

import io.sm8.sdk._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HookDispatchSpec extends AnyFlatSpec with Matchers {

  // ---- Test fixtures ----

  /**
   * Recording PreHook — appends its name to `trace` every time it
   * runs. Uses `var` (test fixture only) per the audit's acceptance
   * of `var` in test code.
   */
  private final class RecordingPreHook(override val name: String, prio: Int, trace: scala.collection.mutable.ListBuffer[String])
      extends PreHook {
    override val priority: Int = prio
    override def stage: HookStage = HookStage.PreExecute
    override def run(context: Context): Context = {
      trace += s"pre:$name"
      context
    }
  }

  /** Throwing PreHook — fails per RFC §9 (fail-fast on throw). */
  private final class ThrowingPreHook extends PreHook {
    override val name: String = "boom"
    override val priority: Int = 50
    override def stage: HookStage = HookStage.PreExecute
    override def run(context: Context): Context =
      throw new RuntimeException("hook intentionally throws")
  }

  /** Stop-setting PreHook — sets context.stop = true. */
  private final class StopPreHook(override val name: String, prio: Int, trace: scala.collection.mutable.ListBuffer[String])
      extends PreHook {
    override val priority: Int = prio
    override def stage: HookStage = HookStage.PreExecute
    override def run(context: Context): Context = {
      trace += s"pre:$name"
      context.copy(stop = true)
    }
  }

  // ---- Tests ----

  "HookManager" should "dispatch hooks in priority order, registration order on ties" in {
    val hooks  = new HookManagerImpl
    val trace  = scala.collection.mutable.ListBuffer.empty[String]

    // Register out of priority order to prove sort-by-priority works.
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("b-prio-50", 50, trace), priority = 50)
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("a-prio-10", 10, trace), priority = 10)
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("c-prio-30", 30, trace), priority = 30)

    val dispatched = hooks.preHooksFor(HookStage.PreExecute).map(_._1.name)
    dispatched shouldBe List("a-prio-10", "c-prio-30", "b-prio-50")
  }

  it should "tie-break by registration order when priorities are equal" in {
    val hooks = new HookManagerImpl
    val trace = scala.collection.mutable.ListBuffer.empty[String]

    // Same priority (50); expect registration order.
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("first-registered", 50, trace), priority = 50)
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("second-registered", 50, trace), priority = 50)
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("third-registered", 50, trace), priority = 50)

    val dispatched = hooks.preHooksFor(HookStage.PreExecute).map(_._1.name)
    dispatched shouldBe List("first-registered", "second-registered", "third-registered")
  }

  it should "isolate hooks by stage — PreExecute hooks do not fire on PreResolve" in {
    val hooks = new HookManagerImpl
    val trace = scala.collection.mutable.ListBuffer.empty[String]
    hooks.registerPreHook(HookStage.PreExecute, new RecordingPreHook("only-on-execute", 10, trace), priority = 10)
    hooks.preHooksFor(HookStage.PreResolve) shouldBe empty
    hooks.preHooksFor(HookStage.PreExecute).map(_._1.name) shouldBe List("only-on-execute")
  }

  "Pipeline" should "abort on a throwing hook (RFC §9 fail-fast)" in {
    val engine = EngineImpl()
    engine.hooks.registerPreHook(HookStage.PreExecute, new ThrowingPreHook, priority = 10)

    val request  = ConnectorRequest(connectorName = "anything", query = new SemanticQuery {})
    val thrown = the [RuntimeException] thrownBy engine.run(request)
    thrown.getMessage shouldBe "hook intentionally throws"
  }

  it should "short-circuit subsequent stages when a hook sets Context.stop = true" in {
    val engine = EngineImpl()
    val trace  = scala.collection.mutable.ListBuffer.empty[String]
    // Register a StopPreHook at PreParse (before the parse stage).
    engine.hooks.registerPreHook(HookStage.PreParse, new StopPreHook("stopper", 1, trace), priority = 1)

    // Stage bodies add to trace themselves in this spec; here we
    // verify the hook ran (so stop was set) and that the Pipeline
    // short-circuited (result remains None since execute never ran).
    val result = engine.run(ConnectorRequest(connectorName = "anything", query = new SemanticQuery {}))

    trace.toList shouldBe List("pre:stopper")
    // No stage set a result → stub empty ConnectorResult.
    result shouldBe a [ConnectorResult]
    val cr = result.asInstanceOf[ConnectorResult]
    cr.connectorName shouldBe ""  // sentinel: no stage produced a result
  }
}