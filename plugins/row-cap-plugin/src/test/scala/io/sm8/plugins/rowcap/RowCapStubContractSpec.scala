/*
 * SM8 row-cap Plugin — Conformance contract spec (PR-D per ADR-007).
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract`. The row-cap-plugin ships one PostHook
 * at HookStage.PostExecute, priority 200, name "row-cap". The plugin
 * constructor requires a `RowCapConfig`.
 *
 * The hook is read back via `EngineImpl().use(plugin)`.
 *
 * Per RFC §13 DoD spirit: structural inheritance from the unified
 * contract base.
 *
 */
package io.sm8.plugins.rowcap

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

case object RowCapConformanceRequest extends Request
case object RowCapConformanceResult   extends Result

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

class RowCapStubContractSpec extends HookContractSpec {

  private val config: RowCapConfig = RowCapConfig(maxRows = 1000)

  override def preHook: PreHook =
    new NoopPreHook(name = "row-cap-conformance-pre", priority = 200)

  override def postHook: PostHook = {
    val engine = EngineImpl()
    val plugin = new RowCapStub(config)
    engine.use(plugin)
    engine.hooks.postHooksFor(HookStage.PostExecute).head._1
  }

  override def transformer: Transformer =
    new NoopTransformer(name = "row-cap-conformance-transformer", priority = 200)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Execute,
      request = RowCapConformanceRequest,
      result  = Some(RowCapConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class RowCapStubContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new RowCapStub(RowCapConfig(maxRows = 1000))

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
