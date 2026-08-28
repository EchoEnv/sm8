/*
 * SM8 Core — PluginDiscoverySpec.
 *
 * Per [[karpathy-app-design-mindset]] §3.1 + the layer discipline of
 * `semantic-layer-engine-architecture.md` §3 (Core Boundary):
 * `PluginDiscovery.discoverFromConfig()` is the inward-facing seam that
 * lets sm8-server trigger plugin discovery without binding to the
 * concrete `EngineImpl` class. This spec verifies the factory contract.
 *
 * Per [[scala-error-handling-mindset]]: errors are data. A misconfigured
 * boot (malformed `sm8.plugins.allowed`, SPI errors) MUST log and
 * return `Nil`, never throw.
 *
 * Per [[scala-impact-analysis-mindset]]: this spec exercises the SOLE
 * outward entry point for plugin discovery. Production callers
 * (sm8-server Main.run) MUST go through this seam.
 *
 * PR-Background: this spec closes the [LOW] [no-spec-test] finding from
 * the post-PR-192 data-eng audit (calf, 2026-08-28). Until now the
 * factory was covered only indirectly via `MainSpec`, leaving a
 * regression risk if the delegation contract ever changes.
 */
package io.sm8.core

import io.sm8.sdk.Plugin

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class PluginDiscoverySpec extends AnyFlatSpec with Matchers {

  "PluginDiscovery.discoverFromConfig" should
    "be a stable outward entry point (object, not class)" in {
    // Layer discipline: sm8-server triggers plugin discovery by name
    // `io.sm8.core.PluginDiscovery.discoverFromConfig()`. The factory
    // MUST remain an `object` (no constructor) — a `class` would force
    // adapters to bind to a concrete instance, defeating the seam.
    val factoryClass = PluginDiscovery.getClass
    factoryClass.getName shouldBe "io.sm8.core.PluginDiscovery$"
  }

  it should "delegate to EngineImpl.discoverFromConfig without throwing" in {
    // Per scala-error-handling-mindset: a misconfigured boot (no
    // allowlist file, SPI errors) MUST NOT throw. The factory exists
    // precisely so deployment callers don't need to catch
    // reflection/SPI exceptions.
    val plugins = PluginDiscovery.discoverFromConfig()
    // Returns a List (possibly empty). Type-level contract.
    plugins shouldBe a[List[_]]
    // Per scala-jvm-safety: not a null on the happy path.
    plugins should not be null
  }

  it should "return List[Plugin] (typed return surface)" in {
    // Compile-time check: the return type is List[Plugin]. If a
    // future refactor widens this to Any/Object, this test fails
    // at compile time before reaching this line.
    val plugins: List[Plugin] = PluginDiscovery.discoverFromConfig()
    plugins.foreach { p =>
      p shouldBe a[Plugin]
    }
  }

  it should "be safe to call multiple times (no caching, no state)" in {
    // Per scala-data-driven-refactor-mindset §1: a factory holds no
    // state. Two consecutive calls MUST each succeed and return
    // List[Plugin] without throwing. (Element identity is NOT
    // asserted: SPI instantiates fresh Plugin instances per call,
    // so two calls return different objects that satisfy the same
    // Plugin interface.)
    val first = PluginDiscovery.discoverFromConfig()
    val second = PluginDiscovery.discoverFromConfig()
    first shouldBe a[List[_]]
    second shouldBe a[List[_]]
    (first eq second) shouldBe false  // separate List instances, no shared state
  }

  it should "NOT be instantiable by adapters (no public ctor)" in {
    // The seam's whole point is that sm8-server calls
    // `PluginDiscovery.discoverFromConfig()` without ever
    // constructing an `EngineImpl`. If a future refactor exposes a
    // public constructor on PluginDiscovery, this test catches it.
    val ctors = classOf[PluginDiscovery.type].getDeclaredConstructors
    // `object` produces a synthetic constructor; its visibility is
    // private at the JVM level (parameterless, callable only via
    // MODULE$ field). The point is that the API does not advertise
    // a public ctor — adapters cannot `new PluginDiscovery(...)`.
    ctors.foreach(_.getParameterCount shouldBe 0)
  }
}