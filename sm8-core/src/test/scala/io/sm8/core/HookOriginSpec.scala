/*
 * SM8 Core — HookOriginSpec (RFC §8 conformance test).
 *
 * Pure data + validator test for `HookOrigin.validate`. Covers:
 *   - each origin's inclusive lower/upper bounds
 *   - on-boundary priorities accepted
 *   - off-boundary priorities rejected (typed-Either shape — no
 *     try/catch needed for the assertion)
 *   - out-of-range rejected across origin boundaries (Core 99 vs
 *     FirstParty 100)
 */
package io.sm8.core

import io.sm8.sdk.HookOrigin

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HookOriginSpec extends AnyFunSuite with Matchers {

  test("HookOrigin.Core accepts 0-99 inclusive") {
    HookOrigin.validate(HookOrigin.Core, 0)   shouldBe Right(())
    HookOrigin.validate(HookOrigin.Core, 50)  shouldBe Right(())
    HookOrigin.validate(HookOrigin.Core, 99)  shouldBe Right(())
  }

  test("HookOrigin.Core rejects 100+ (FirstParty boundary)") {
    HookOrigin.validate(HookOrigin.Core, 100) shouldBe a [Left[_, _]]
    HookOrigin.validate(HookOrigin.Core, 250) shouldBe a [Left[_, _]]
  }

  test("HookOrigin.FirstParty accepts 100-899 inclusive") {
    HookOrigin.validate(HookOrigin.FirstParty, 100) shouldBe Right(())
    HookOrigin.validate(HookOrigin.FirstParty, 150) shouldBe Right(())
    HookOrigin.validate(HookOrigin.FirstParty, 899) shouldBe Right(())
  }

  test("HookOrigin.FirstParty rejects 0-99 (Core boundary)") {
    HookOrigin.validate(HookOrigin.FirstParty, 99)  shouldBe a [Left[_, _]]
    HookOrigin.validate(HookOrigin.FirstParty, 50)  shouldBe a [Left[_, _]]
    HookOrigin.validate(HookOrigin.FirstParty, 900) shouldBe a [Left[_, _]]
  }

  test("HookOrigin.Community accepts 900+") {
    HookOrigin.validate(HookOrigin.Community, 900)               shouldBe Right(())
    HookOrigin.validate(HookOrigin.Community, 10000)            shouldBe Right(())
    HookOrigin.validate(HookOrigin.Community, Int.MaxValue)     shouldBe Right(())
  }

  test("HookOrigin.Community rejects 0-899") {
    HookOrigin.validate(HookOrigin.Community, 899) shouldBe a [Left[_, _]]
    HookOrigin.validate(HookOrigin.Community, 0)   shouldBe a [Left[_, _]]
  }

  test("validate: negative priority rejected at any origin") {
    HookOrigin.validate(HookOrigin.Core,       -1)  shouldBe a [Left[_, _]]
    HookOrigin.validate(HookOrigin.FirstParty, -1)  shouldBe a [Left[_, _]]
    HookOrigin.validate(HookOrigin.Community,  -1)  shouldBe a [Left[_, _]]
  }

  test("HookOrigin contains: each origin reports its band") {
    HookOrigin.contains(HookOrigin.Core,       0)   shouldBe true
    HookOrigin.contains(HookOrigin.Core,       50)  shouldBe true
    HookOrigin.contains(HookOrigin.Core,       99)  shouldBe true
    HookOrigin.contains(HookOrigin.Core,       100) shouldBe false

    HookOrigin.contains(HookOrigin.FirstParty, 100) shouldBe true
    HookOrigin.contains(HookOrigin.FirstParty, 899) shouldBe true
    HookOrigin.contains(HookOrigin.FirstParty, 99)  shouldBe false
    HookOrigin.contains(HookOrigin.FirstParty, 900) shouldBe false

    HookOrigin.contains(HookOrigin.Community,  900) shouldBe true
    HookOrigin.contains(HookOrigin.Community,  899) shouldBe false
  }
}

/**
 * Boundary checks on the SDK `HookManager` impl:
 * - 3-int overload enforces ONLY non-negative (back-compat contract
 *   preserved — the SDK's documented throw on negative priority).
 * - 4-arg overload enforces BOTH non-negative AND origin range.
 * Plugin authors who declare their origin (Core/FirstParty/Community)
 * get strict RFC §8 conformance.
 */
class HookManagerOriginSpec extends AnyFunSuite with Matchers {

  private def stubHook(prio: Int = 100, name: String = "x"): io.sm8.sdk.PreHook =
    new io.sm8.sdk.PreHook {
      override val name: String                       = name
      override val priority: Int                     = prio
      override def stage: io.sm8.sdk.HookStage        = io.sm8.sdk.HookStage.PreExecute
      override def run(c: io.sm8.sdk.Context): io.sm8.sdk.Context = c
    }

  private def stubPostHook(prio: Int = 100, name: String = "y"): io.sm8.sdk.PostHook =
    new io.sm8.sdk.PostHook {
      override val name: String                       = name
      override val priority: Int                     = prio
      override def stage: io.sm8.sdk.HookStage        = io.sm8.sdk.HookStage.PostExecute
      override def run(c: io.sm8.sdk.Context): io.sm8.sdk.Context = c
    }

  test("HookManagerImpl: 3-int overload enforces non-negative but NOT range (back-compat)") {
    val hooks = new HookManagerImpl
    noException should be thrownBy
      hooks.registerPreHook(io.sm8.sdk.HookStage.PreExecute, stubHook(prio = 10), 10)
  }

  test("HookManagerImpl: 3-int overload rejects negative priority") {
    val hooks = new HookManagerImpl
    val ex = intercept[IllegalArgumentException] {
      hooks.registerPreHook(io.sm8.sdk.HookStage.PreExecute, stubHook(), -1)
    }
    ex.getMessage should include ("priority must be non-negative")
  }

  test("HookManagerImpl: 4-arg overload accepts Core-range priority under Core origin") {
    val hooks = new HookManagerImpl
    noException should be thrownBy
      hooks.registerPreHook(io.sm8.sdk.HookStage.PreExecute, stubHook(prio = 10), 10, HookOrigin.Core)
  }

  test("HookManagerImpl: 4-arg overload rejects Community-range priority under Core origin") {
    val hooks = new HookManagerImpl
    val ex = intercept[IllegalArgumentException] {
      hooks.registerPreHook(io.sm8.sdk.HookStage.PreExecute, stubHook(prio = 1000), 1000, HookOrigin.Core)
    }
    ex.getMessage should include ("outside the reserved range")
  }

  test("HookManagerImpl: 4-arg overload rejects FirstParty-range priority under Core origin") {
    val hooks = new HookManagerImpl
    val ex = intercept[IllegalArgumentException] {
      hooks.registerPreHook(io.sm8.sdk.HookStage.PreExecute, stubHook(prio = 150), 150, HookOrigin.Core)
    }
    ex.getMessage should include ("outside the reserved range")
  }

  test("HookManagerImpl.Post: 4-arg overload accepts FirstParty-range priority under FirstParty") {
    val hooks = new HookManagerImpl
    noException should be thrownBy
      hooks.registerPostHook(io.sm8.sdk.HookStage.PostExecute, stubPostHook(prio = 200), 200, HookOrigin.FirstParty)
  }

  test("HookManagerImpl.Post: 4-arg overload rejects Core-range priority under FirstParty") {
    val hooks = new HookManagerImpl
    val ex = intercept[IllegalArgumentException] {
      hooks.registerPostHook(io.sm8.sdk.HookStage.PostExecute, stubPostHook(prio = 50), 50, HookOrigin.FirstParty)
    }
    ex.getMessage should include ("outside the reserved range")
  }
}
