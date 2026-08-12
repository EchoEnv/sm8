/*
 * SM8 Core — HookContractSpec.
 *
 * Abstract base class for testing any `io.sm8.sdk.PreHook`,
 * `io.sm8.sdk.PostHook`, or `io.sm8.sdk.Transformer` implementation.
 *
 * Per RFC §8 + hooks.md + the SM8 plan, the rules are:
 *   - name should be non-empty (RFC hooks.md)
 *   - priority should be in the reserved range for the hook's origin
 *     (RFC §8: 0-99 core, 100-899 first-party, 900+ community)
 *   - context.request must be unchanged after `run(ctx)` (RFC hooks.md
 *     Rule 2 — hooks must not mutate the original input)
 *   - a hook that throws must surface as a thrown exception (RFC §9
 *     fail-fast — silent partial failures are worse than a crash)
 *
 * Priority ordering (lower runs first; ties broken by registration
 * order) and the fail-fast abort semantics are tested at the
 * HookManager level in Step 4 — they require the HookManager, which
 * is not in scope for Step 2.
 */
package io.sm8.sdk.contract

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import io.sm8.sdk.{Context, HookStage, PipelineStage, Plugin, PreHook, PostHook, Transformer}

abstract class HookContractSpec extends AnyFlatSpec with Matchers {

  // ---- Abstract test data — every concrete spec MUST supply these ----

  /** The PreHook under test. Concrete `PreHookSpec`s override this. */
  def preHook: PreHook

  /** The PostHook under test. Concrete `PostHookSpec`s override this. */
  def postHook: PostHook

  /** A baseline Context to pass to `run`. */
  def baselineContext: Context

  // ---- assertions common to PreHook and PostHook ----

  private def assertCommonHookContract(
      name: String,
      priority: Int,
      stage: HookStage,
      hookKind: String
  ): Unit = {
    withClue(s"$hookKind.name: ") {
      name should not be empty
    }
    withClue(s"$hookKind.priority $priority is outside any reserved range (0-99 core, 100-899 first-party, 900+ community): ") {
      priority match {
        case p if p >= 0 && p <= 99   => succeed
        case p if p >= 100 && p <= 899 => succeed
        case p if p >= 900            => succeed
        case p                        => fail(s"priority $p not in any reserved range")
      }
    }
    withClue(s"$hookKind.stage wireName: ") {
      io.sm8.sdk.HookStage.wireName(stage) should fullyMatch regex """(pre|post):(parse|resolve|execute|format)"""
    }
  }

  "PreHook" should "have a non-empty name and a valid priority range" in {
    assertCommonHookContract(preHook.name, preHook.priority, preHook.stage, "PreHook")
  }

  "PreHook" should "preserve context.request after run(ctx) (RFC hooks.md Rule 2)" in {
    val requestBefore = baselineContext.request
    val resultCtx     = preHook.run(baselineContext)
    withClue("PreHook mutated context.request — forbidden (RFC hooks.md Rule 2): ") {
      resultCtx.request shouldBe requestBefore
    }
  }

  "PostHook" should "have a non-empty name and a valid priority range" in {
    assertCommonHookContract(postHook.name, postHook.priority, postHook.stage, "PostHook")
  }

  "PostHook" should "preserve context.request after run(ctx) (RFC hooks.md Rule 2)" in {
    val requestBefore = baselineContext.request
    val resultCtx     = postHook.run(baselineContext)
    withClue("PostHook mutated context.request — forbidden (RFC hooks.md Rule 2): ") {
      resultCtx.request shouldBe requestBefore
    }
  }

  // ---- Transformer contract (per Q3 = Transformer: Y) ----

  /** The Transformer under test. Concrete `TransformerSpec`s override this. */
  def transformer: Transformer

  "Transformer" should "have a non-empty name" in {
    transformer.name should not be empty
  }

  it should "have priority in the first-party or higher range (100+)" in {
    withClue(s"Transformer priority ${transformer.priority} is below the 100 first-party floor: ") {
      transformer.priority should be >= 100
    }
  }

  it should "preserve context.request after transform(ctx)" in {
    val requestBefore = baselineContext.request
    val resultCtx     = transformer.transform(baselineContext)
    resultCtx.request shouldBe requestBefore
  }

  it should "be deterministic — same Context → same Context" in {
    val first  = transformer.transform(baselineContext)
    val second = transformer.transform(baselineContext)
    first shouldBe second
  }
}

/**
 * Minimal `Request` shape for the abstract base's `baselineContext`.
 * Real shape lands in Step 0.
 */
final case class RequestStub(value: String) extends io.sm8.sdk.Request

/**
 * Minimal `Result` shape for the abstract base's `baselineContext`.
 * Real shape lands in Step 0.
 */
final case class ResultStub(payload: Map[String, Any] = Map.empty) extends io.sm8.sdk.Result