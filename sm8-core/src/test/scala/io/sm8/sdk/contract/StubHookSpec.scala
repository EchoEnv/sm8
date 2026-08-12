/*
 * SM8 Core — StubHookSpec.
 *
 * Concrete test that extends `HookContractSpec` and proves the hook
 * conformance assertions (name, priority range, request immutability,
 * Transformer determinism) work end-to-end against minimal Hook
 * implementations.
 *
 * If this test fails, the conformance contract is broken — fix the
 * contract base, not this stub.
 */
package io.sm8.sdk.contract

import io.sm8.sdk.{Context, HookStage, PostHook, PreHook, Request, Result, Transformer}

/** Minimal no-op Request for the baseline Context. */
case object StubRequest extends Request

/** Minimal no-op Result for the baseline Context. */
case object StubResult extends Result

/** Minimal `PreHook` that records the order it ran (for future ordering tests). */
final class StubPreHook(override val name: String, override val priority: Int)
    extends PreHook {
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context = context
}

/** Minimal `PostHook` that just returns ctx. */
final class StubPostHook(override val name: String, override val priority: Int)
    extends PostHook {
  override def stage: HookStage = HookStage.PostExecute
  override def run(context: Context): Context = context
}

/** Minimal `Transformer` that returns ctx with a transformed `result`. */
final class StubTransformer(override val name: String, override val priority: Int)
    extends Transformer {
  override def transform(context: Context): Context =
    context.copy(result = context.result) // identity, deterministic
}

/**
 * Extends `HookContractSpec` and supplies the abstract hook + baseline
 * Context. Real Hook Plugins follow this same shape.
 */
class StubHookSpec extends HookContractSpec {

  override def preHook: PreHook =
    new StubPreHook(name = "stub-pre", priority = 100)

  override def postHook: PostHook =
    new StubPostHook(name = "stub-post", priority = 110)

  override def transformer: Transformer =
    new StubTransformer(name = "stub-transformer", priority = 150)

  override def baselineContext: Context =
    Context(
      stage   = io.sm8.sdk.PipelineStage.Execute,
      request = StubRequest,
      result  = Some(StubResult),
      meta    = Map.empty,
      stop    = false
    )
}