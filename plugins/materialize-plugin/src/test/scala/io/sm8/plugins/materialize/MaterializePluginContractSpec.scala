/*
 * SM8 materialize Plugin — Conformance contract spec (PR-D per ADR-007).
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract`. The materialize-plugin ships both a
 * PreHook AND a PostHook, both at priority 250 (FirstParty range),
 * with `HookOrigin.FirstParty` (the explicit-origin overload).
 *
 * The hook is read back via `EngineImpl().use(plugin)`.
 *
 * Per RFC §13 DoD spirit: structural inheritance from the unified
 * contract base.
 *
 * Per [[karpathy-guidelines-mindset]]: smallest correct change.
 */
package io.sm8.plugins.materialize

import io.sm8.core.EngineImpl
import io.sm8.plugins.materialize.PersistLevel
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

case object MaterializeConformanceRequest extends Request
case object MaterializeConformanceResult   extends Result

final class NoopTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context = context
}

class MaterializePluginContractSpec extends HookContractSpec {

  private val plugin: MaterializePlugin =
    new MaterializePlugin(PersistLevel.MemoryAndDisk)

  override def preHook: PreHook = {
    val engine = EngineImpl()
    engine.use(plugin)
    engine.hooks.preHooksFor(HookStage.PreExecute).head._1
  }

  override def postHook: PostHook = {
    val engine = EngineImpl()
    engine.use(plugin)
    engine.hooks.postHooksFor(HookStage.PostExecute).head._1
  }

  override def transformer: Transformer =
    new NoopTransformer(name = "materialize-conformance-transformer", priority = 250)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Execute,
      request = MaterializeConformanceRequest,
      result  = Some(MaterializeConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class MaterializePluginContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin =
    new MaterializePlugin(PersistLevel.MemoryAndDisk)

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
