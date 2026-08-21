/*
 * SM8 audit Plugin — Conformance contract spec (PR-D per ADR-007).
 *
 * Extends the unified `HookContractSpec` + `PluginContractSpec`
 * from `io.sm8.sdk.contract` so the audit-plugin hook + plugin
 * shapes are verified by inheritance from the abstract base —
 * not by inspection of the per-plugin hand-rolled spec.
 *
 * Per RFC §13 DoD spirit: a 7th contributor adding a new reference
 * plugin cannot skip the unified conformance check (compile-time
 * inheritance); a refactor of `PostHook` / `Plugin` in `sm8-core`
 * cannot silently break the audit-plugin contract rules.
 *
 * The hook is read back via `EngineImpl().use(plugin)` —
 * the plugin registers the hook privately; the contract test
 * pulls the registered instance from the engine's HookManager.
 *
 * The plugin ships one PostHook at HookStage.PostFormat, priority 150,
 * name "audit-stub"". The PreHook / Transformer slots in HookContractSpec
 * are supplied as no-op stubs (the audit-plugin has none of those).
 *
 * one contract spec per plugin; 
 * `EngineImpl` is fresh per test, no static state.
 */
package io.sm8.plugins.audit

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PostHook, PreHook, Request, Result, Transformer}
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

/** Minimal no-op Request for the baseline Context (the audit-plugin does
  * not pattern-match on the request, so a marker subtype is sufficient). */
case object AuditConformanceRequest extends Request

/** Minimal no-op Result for the baseline Context (the audit-plugin's
  * PostHook fires once per engine.run; it does not inspect the Result). */
case object AuditConformanceResult extends Result

/** No-op PreHook stub for the audit-plugin contract spec (the
  * audit-plugin has no PreHook). */
final class NoopPreHook(override val name: String, override val priority: Int)
    extends PreHook {
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = context
}

/** No-op PostHook stub for the audit-plugin contract spec
  * (HookContractSpec asserts the post-hook preserves `context.request`;
  * since the audit-plugin has only one PostHook, this stub is never
  * exercised — but the contract requires it be supplied). */
final class NoopPostHook(override val name: String, override val priority: Int)
    extends PostHook {
  override def stage: HookStage = HookStage.PostFormat
  override def run(context: Context): Context = context
}

/** No-op Transformer stub for the audit-plugin contract spec
  * (the audit-plugin has no Transformer; this stub satisfies the
  * abstract member). */
final class NoopTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context = context
}

class AuditStubContractSpec extends HookContractSpec {

  /** The audit-plugin's only hook — read back from a fresh engine. */
  override def preHook: PreHook =
    new NoopPreHook(name = "audit-conformance-pre", priority = 150)

  override def postHook: PostHook = {
    val engine = EngineImpl()
    engine.use(new AuditStub)
    engine.hooks.postHooksFor(HookStage.PostFormat).head._1
  }

  override def transformer: Transformer =
    new NoopTransformer(name = "audit-conformance-transformer", priority = 150)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Format,
      request = AuditConformanceRequest,
      result  = Some(AuditConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}

class AuditStubContractPluginSpec extends PluginContractSpec {

  override def plugin: Plugin = new AuditStub

  override def engine: io.sm8.sdk.Engine =
    io.sm8.sdk.contract.PluginContractSpecStubs.NoopEngine
}
