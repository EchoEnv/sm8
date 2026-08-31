/*
 * SM8 semantic-graph Plugin — conformance contract spec.
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract` so the semantic-graph plugin's hook +
 * plugin shapes are verified by inheritance from the abstract base,
 * matching the contract-spec coverage the other six plugin families
 * already carry (audit, broadcast, cache, materialize, row-cap, skew).
 *
 * The plugin registers two hooks — `JoinPathPreHook` (PreResolve,
 * priority 120) and `GraphPostResolveObserver` (PostResolve,
 * priority 120). The hooks are read back via
 * `EngineImpl().use(plugin)` — the plugin registers them privately;
 * the contract test pulls the registered instances from the engine's
 * HookManager.
 *
 * The Transformer slot in HookContractSpec is supplied as a no-op
 * stub (the semantic-graph plugin has none).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

/** Minimal no-op Request for the baseline Context (the semantic-graph
  * hooks inspect `Model` state via the request's model reference, not
  * the request envelope itself). */
case object SemanticGraphConformanceRequest extends Request

/** Minimal no-op Result for the baseline Context (the PostResolve
  * observer publishes to `context.meta`; it does not inspect the
  * Result). */
case object SemanticGraphConformanceResult extends Result

/** No-op PreHook stub for the contract spec's PreHook slot (the
  * semantic-graph plugin's real PreHook is exercised via the plugin
  * registration below). */
final class NoopPreHook(override val name: String, override val priority: Int)
    extends PreHook {
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = context
}

/** No-op Transformer stub for the contract spec's Transformer slot
  * (the semantic-graph plugin has no Transformer). */
final class NoopTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context = context
}

class SemanticGraphContractSpec extends HookContractSpec {

  /** The plugin's real pre-resolve validator — read back from a
    * fresh engine. */
  override def preHook: PreHook = {
    val engine = EngineImpl()
    engine.use(new SemanticGraphPlugin)
    engine.hooks.preHooksFor(HookStage.PreResolve).head._1
  }

  /** The plugin's real post-resolve observer — read back from the
    * same engine registration. */
  override def postHook: PostHook = {
    val engine = EngineImpl()
    engine.use(new SemanticGraphPlugin)
    engine.hooks.postHooksFor(HookStage.PostResolve).head._1
  }

  override def transformer: Transformer =
    new NoopTransformer(name = "semantic-graph-conformance-transformer", priority = 120)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Resolve,
      request = SemanticGraphConformanceRequest,
      result  = Some(SemanticGraphConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class SemanticGraphContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new SemanticGraphPlugin

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
