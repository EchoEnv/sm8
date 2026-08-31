/*
 * SM8 Core — PluginContractSpec.
 *
 * Abstract base class for testing any `io.sm8.sdk.Plugin`
 * implementation.
 *
 * Per RFC plugins.md Rule 1: setup must be idempotent-safe to call once
 * at startup. Calling setup twice on the same Engine must NOT
 * duplicate state or raise an exception.
 *
 * Per RFC plugins.md Rule 2: a plugin should have one clear purpose —
 * this rule is a code-review concern, not a runtime assertion. It's
 * documented in the Plugin scaladoc; this contract test does not
 * enforce it.
 *
 * Per RFC plugins.md Rule 3: plugins depend on core's contracts, never
 * on each other directly. This is a structural rule — also enforced
 * at code-review time, not at runtime.
 *
 * The contract test enforces only what is mechanically checkable
 * without a real Engine implementation: idempotent setup.
 *
 * The Engine parameter passed to setup is a no-op stub for Step 2 —
 * a real Engine lands in Step 3.
 */
package io.sm8.sdk.contract

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import io.sm8.core.{HookManagerImpl, TransformerRegistryImpl}
import io.sm8.sdk.{Engine, Plugin, Request, Result}

abstract class PluginContractSpec extends AnyFlatSpec with Matchers {

  // ---- Abstract test data — every concrete spec MUST supply these ----

  /** The Plugin under test. */
  def plugin: Plugin

  /**
   * The Engine instance the plugin's setup will run against. For
   * Step 2 this is a no-op stub; the real Engine lands in Step 3.
   */
  def engine: Engine

  // ---- RFC plugins.md Rule 1: idempotent setup ----

  "Plugin (RFC plugins.md Rule 1)" should "setup without throwing on a fresh Engine" in {
    noException should be thrownBy plugin.setup(engine)
  }

  it should "be idempotent — calling setup twice on the same Engine does not raise" in {
    // Idempotency means the second call is a no-op (or at least does
    // not throw). Real verification that state is unchanged requires
    // a real Engine (Step 3); here we only assert no exception.
    noException should be thrownBy plugin.setup(engine)
    noException should be thrownBy plugin.setup(engine)
  }
}

/**
 * No-op Engine stub for Step 2 contract tests. Real Engine lands in
 * Step 3. The stub exists so Plugin authors can write their
 * `PluginContractSpec` extension without depending on the full Engine.
 *
 * Step 3 update: implements the new `connectors` / `hooks` /
 * `transformers` accessors added to the Engine trait in Step 3.
 * Uses the real internal implementations so Plugin.setup can
 * actually register Connectors / Hooks against the stub.
 */
object PluginContractSpecStubs {

  /** Minimal no-op Engine for testing Plugin.setup idempotency. */
  val NoopEngine: Engine = new Engine {
    private val _hooks:        HookManagerImpl         = new HookManagerImpl
    private val _transformers: TransformerRegistryImpl = new TransformerRegistryImpl

    override def use(p: Plugin): Engine = {
      p.setup(this)
      this
    }

    override def run(request: Request): Result =
      throw new UnsupportedOperationException(
        "NoopEngine is a Step 2 contract-test stub; real Engine.run lands in Step 3"
      )

    override def hooks: io.sm8.sdk.HookManager               = _hooks
    override def transformers: io.sm8.sdk.TransformerRegistry = _transformers
  }
}