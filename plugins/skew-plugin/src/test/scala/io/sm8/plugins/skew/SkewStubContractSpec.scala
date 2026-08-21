/*
 * SM8 skew Plugin — Conformance contract spec (PR-D per ADR-007).
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract`. The skew-plugin ships one PreHook
 * at HookStage.PreExecute, priority 250, name "skew-stub"".
 *
 * The hook is read back via `EngineImpl().use(plugin)`.
 *
 * Per RFC §13 DoD spirit: structural inheritance from the unified
 * contract base.
 *
 */
package io.sm8.plugins.skew

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

case object SkewConformanceRequest extends Request
case object SkewConformanceResult   extends Result

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

class SkewStubContractSpec extends HookContractSpec {

  override def preHook: PreHook = {
    val engine = EngineImpl()
    engine.use(new SkewStub)
    engine.hooks.preHooksFor(HookStage.PreExecute).head._1
  }

  override def postHook: PostHook =
    new NoopPostHook(name = "skew-conformance-post", priority = 250)

  override def transformer: Transformer =
    new NoopTransformer(name = "skew-conformance-transformer", priority = 250)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Execute,
      request = SkewConformanceRequest,
      result  = Some(SkewConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class SkewStubContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new SkewStub

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
