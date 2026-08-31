/*
 * SM8 Core — EngineSmokeSpec.
 *
 * End-to-end smoke for the SM8 Engine:
 *   1. Construct an Engine
 *   2. Register a Plugin via `engine.use(plugin)` (Plugin.setup
 *      registers a hook against the engine)
 *   3. Send a request to `engine.run` and observe the typed Result
 *
 * This is the proof that Step 3's machinery (Engine + registries +
 * Pipeline) wires together correctly.
 *
 * Engine dispatch (which provider serves a query) is owned by the
 * `EngineProvider` family + `EngineRegistry` (ServiceLoader
 * discovery) and is covered by the per-connector specs under
 * `connectors/` — this spec covers the engine's plugin portal + the
 * Pipeline's typed-failure contract (an unknown request type must
 * never pass through as a silent success).
 */
package io.sm8.core

import io.sm8.core.engine.EngineError
import io.sm8.sdk._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineSmokeSpec extends AnyFlatSpec with Matchers {

  // ---- Test fixtures ----

  /** Minimal PreHook that records nothing — proves registration only. */
  private final class NoopPreHook extends PreHook {
    override val name: String     = "smoke-hook"
    override val priority: Int    = 10
    override val stage: HookStage = HookStage.PreParse
    override def run(context: Context): Context = context
  }

  /** Stop-setting PreHook — sets context.stop = true (short-circuit). */
  private final class StopPreHook extends PreHook {
    override val name: String     = "stopper"
    override val priority: Int    = 1
    override val stage: HookStage = HookStage.PreParse
    override def run(context: Context): Context =
      context.copy(stop = true)
  }

  "Engine" should "register a Plugin that adds a hook via setup(engine)" in {
    val engine = EngineImpl()
    val plugin  = new Plugin {
      override def setup(engine: Engine): Unit = {
        engine.hooks.registerPreHook(
          HookStage.PreParse, new NoopPreHook, priority = 10, origin = HookOrigin.Core)
      }
    }
    engine.use(plugin)
    engine.hooks.preHooksFor(HookStage.PreParse).map(_._1.name) shouldBe List("smoke-hook")
  }

  it should "be forgiving when a Plugin's setup throws" in {
    val engine = EngineImpl()
    val bad    = new Plugin {
      override def setup(engine: Engine): Unit =
        throw new RuntimeException("simulated setup failure")
    }
    noException should be thrownBy engine.use(bad) // bad plugin warns, never crashes
  }

  it should "surface an unknown request type as a typed PipelineError(UnsupportedCapability), never pass-through" in {
    val engine  = EngineImpl()
    val request = new Request {} // not a known request type — nothing can execute it
    val result  = engine.run(request)
    // An unrecognized request type must travel in the Result envelope
    // as a typed failure, not pass through unchanged (a silent
    // success the caller cannot distinguish from a real result).
    result shouldBe a [PipelineError]
    val err = result.asInstanceOf[PipelineError]
    err.engine shouldBe "-"
    err.error shouldBe a [EngineError.UnsupportedCapability]
    val unsupported = err.error.asInstanceOf[EngineError.UnsupportedCapability]
    unsupported.engine shouldBe "pipeline"
    unsupported.capability shouldBe "RequestType"
    unsupported.message should include (request.getClass.getName)
  }

  it should "short-circuit when a hook sets Context.stop and return PipelineSkipped naming the halt stage" in {
    val engine = EngineImpl()
    engine.hooks.registerPreHook(
      HookStage.PreParse, new StopPreHook, priority = 1, origin = HookOrigin.Core)

    val result = engine.run(new Request {})
    // The hook set stop before any stage body ran — the explicit
    // short-circuit marker names the stage where the pipeline halted.
    result shouldBe a [PipelineSkipped]
    result.asInstanceOf[PipelineSkipped].stage shouldBe PipelineStage.Parse
  }
}
