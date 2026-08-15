/*
 * SM8 broadcast Plugin — Conformance contract spec (PR-D per ADR-007).
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract`. The broadcast-plugin ships one
 * PreHook at HookStage.PreExecute, priority 250, name "broadcast".
 *
 * The hook is read back via `EngineImpl().use(plugin)`.
 *
 * Per RFC §13 DoD spirit: future contributors cannot skip the
 * unified conformance check; `PreHook` refactors in `sm8-core`
 * cannot silently break the broadcast-plugin contract rules.
 *
 * Per [[karpathy-guidelines-mindset]]: smallest correct change.
 */
package io.sm8.plugins.broadcast

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

case object BroadcastConformanceRequest extends Request
case object BroadcastConformanceResult   extends Result

final class NoopPreHook(override val name: String, override val priority: Int)
    extends PreHook {
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = context
}

final class NoopPostHook(override val name: String, override val priority: Int)
    extends PostHook {
  override def stage: HookStage = HookStage.PostExecute
  override def run(context: Context): Context = context
}

final class NoopTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context = context
}

class BroadcastPluginContractSpec extends HookContractSpec {

  override def preHook: PreHook = {
    val engine = EngineImpl()
    engine.use(new BroadcastPlugin)
    engine.hooks.preHooksFor(HookStage.PreExecute).head._1
  }

  override def postHook: PostHook =
    new NoopPostHook(name = "broadcast-conformance-post", priority = 250)

  override def transformer: Transformer =
    new NoopTransformer(name = "broadcast-conformance-transformer", priority = 250)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Execute,
      request = BroadcastConformanceRequest,
      result  = Some(BroadcastConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class BroadcastPluginContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new BroadcastPlugin

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
