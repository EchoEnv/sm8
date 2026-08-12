/*
 * SM8 Core — HookContractSpec.
 *
 * Step-1 skeleton. Asserts that the three hook protocols (PreHook,
 * PostHook, Transformer) and the HookStage sealed hierarchy exist and
 * have the expected shape.
 *
 * Step 2 promotes this to the full conformance suite enforcing
 * (per RFC hooks.md):
 *   - priority ordering (lower runs first; ties broken by registration
 *     order)
 *   - context.request is read-only (hooks.md Rule 2)
 *   - the 8 named HookStages are exhaustive (sealed)
 *   - priority ranges (0-99 core, 100-899 first-party, 900+ community)
 *   - fail-fast semantics on throw (RFC §9)
 */
package io.sm8.sdk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HookContractSpec extends AnyFlatSpec with Matchers {

  "PreHook" should "expose name, priority, stage, run" in {
    val methods = classOf[PreHook].getMethods.map(_.getName).toSet
    Seq("name", "priority", "stage", "run").foreach { m =>
      methods should contain(m)
    }
  }

  "PostHook" should "expose name, priority, stage, run" in {
    val methods = classOf[PostHook].getMethods.map(_.getName).toSet
    Seq("name", "priority", "stage", "run").foreach { m =>
      methods should contain(m)
    }
  }

  "Transformer" should "expose name, priority, transform" in {
    val methods = classOf[Transformer].getMethods.map(_.getName).toSet
    Seq("name", "priority", "transform").foreach { m =>
      methods should contain(m)
    }
  }

  "HookStage" should "be sealed with exactly 8 named attachment points" in {
    val cases = sealedChildrenOf[HookStage]
    cases should have size 8
    HookStage.wireName(HookStage.PreParse)    shouldBe "pre:parse"
    HookStage.wireName(HookStage.PostParse)   shouldBe "post:parse"
    HookStage.wireName(HookStage.PreResolve)  shouldBe "pre:resolve"
    HookStage.wireName(HookStage.PostResolve) shouldBe "post:resolve"
    HookStage.wireName(HookStage.PreExecute)  shouldBe "pre:execute"
    HookStage.wireName(HookStage.PostExecute) shouldBe "post:execute"
    HookStage.wireName(HookStage.PreFormat)   shouldBe "pre:format"
    HookStage.wireName(HookStage.PostFormat)  shouldBe "post:format"
  }

  it should "be implementable by a minimal logging PreHook" in {
    val hook = new PreHook {
      def name: String = "logging-test"
      def priority: Int = 100
      def stage: HookStage = HookStage.PreExecute
      def run(context: Context): Context = context
    }
    hook.name shouldBe "logging-test"
    hook.priority shouldBe 100
    hook.stage shouldBe HookStage.PreExecute
  }

  /** Helper: enumerate the sealed children of a sealed trait. */
  private def sealedChildrenOf[T](implicit ct: scala.reflect.ClassTag[T]): Set[String] = {
    import scala.reflect.runtime.{universe => ru}
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val sym = mirror.classSymbol(ct.runtimeClass)
    sym.knownDirectSubclasses.map(_.name.toString).toSet
  }
}