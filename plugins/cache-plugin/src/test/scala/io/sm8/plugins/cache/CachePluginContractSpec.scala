/*
 * SM8 cache Plugin — Conformance contract spec (PR-D per ADR-007).
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract`. The cache-plugin ships both a
 * PreHook (`cache-read` at HookStage.PreExecute, priority 50) AND
 * a PostHook (`cache-write` at HookStage.PostExecute, priority 60)
 * — the only plugin among the 6 reference plugins that has both.
 *
 * Per RFC §13 DoD spirit: structural inheritance from the unified
 * contract base — the cache-plugin is verified by the same assertions
 * as every other plugin (name non-empty, priority in reserved range,
 * `context.request` unchanged after `run(ctx)`).
 *
 * The hook is read back via `EngineImpl().use(plugin)`.
 *
 * Per [[karpathy-guidelines-mindset]]: smallest correct change.
 */
package io.sm8.plugins.cache

import io.sm8.core.EngineImpl
import io.sm8.core.cache.ResultCache
import io.sm8.plugins.cache.InMemoryResultCache
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

case object CacheConformanceRequest extends Request
case object CacheConformanceResult   extends Result

final class NoopTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context = context
}

class CachePluginContractSpec extends HookContractSpec {

  private val cache: ResultCache = new InMemoryResultCache

  override def preHook: PreHook = {
    val engine = EngineImpl()
    val plugin = new CachePlugin(cache)
    engine.use(plugin)
    engine.hooks.preHooksFor(HookStage.PreExecute).head._1
  }

  override def postHook: PostHook = {
    val engine = EngineImpl()
    val plugin = new CachePlugin(cache)
    engine.use(plugin)
    engine.hooks.postHooksFor(HookStage.PostExecute).head._1
  }

  override def transformer: Transformer =
    new NoopTransformer(name = "cache-conformance-transformer", priority = 100)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Execute,
      request = CacheConformanceRequest,
      result  = Some(CacheConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class CachePluginContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new CachePlugin(new InMemoryResultCache)

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
