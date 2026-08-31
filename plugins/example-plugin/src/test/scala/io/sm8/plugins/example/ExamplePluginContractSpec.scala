/*
 * SM8 example Plugin — conformance contracts.
 *
 * Proves the plugin + its hook satisfy the unified `HookContractSpec`
 * / `PluginContractSpec` bases from sm8-core's test-jar. Every real
 * plugin in this repo ships one of these; when you copy this plugin,
 * keep this file and update it to exercise YOUR hook instead.
 *
 * The hook is registered privately by the plugin; the test pulls the
 * registered instance back from the engine's HookManager. The on-disk
 * SPI registration (META-INF/services) is covered separately by the
 * behavioral spec's classpath scan — see ExamplePluginSpec.
 */
package io.sm8.plugins.example

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

/** Minimal no-op Request for the baseline Context. The trace hook
  * only reads `request.getClass`, so any subtype works. */
case object ExampleConformanceRequest extends Request

/** Minimal no-op Result for the baseline Context. */
case object ExampleConformanceResult extends Result

/** No-op PreHook stub for the contract spec's PreHook slot (this
  * plugin ships a PostHook only; the base requires a PreHook). */
final class NoopPreHook(override val name: String, override val priority: Int)
    extends PreHook {
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = context
}

/** No-op Transformer stub for the contract spec's Transformer slot
  * (this plugin has no Transformer). */
final class NoopTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context = context
}

class ExamplePluginContractSpec extends HookContractSpec {

  /** The plugin's only hook — read back from a fresh engine. */
  override def postHook: PostHook = {
    val engine = EngineImpl()
    engine.use(new ExamplePlugin)
    engine.hooks.postHooksFor(HookStage.PostExecute).head._1
  }

  override def preHook: PreHook =
    new NoopPreHook(name = "example-conformance-pre", priority = 200)

  override def transformer: Transformer =
    new NoopTransformer(name = "example-conformance-transformer", priority = 200)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Execute,
      request = ExampleConformanceRequest,
      result  = Some(ExampleConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class ExamplePluginContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new ExamplePlugin

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
