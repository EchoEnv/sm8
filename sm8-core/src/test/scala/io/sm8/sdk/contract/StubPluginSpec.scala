/*
 * SM8 Core — StubPluginSpec.
 *
 * Concrete test that extends `PluginContractSpec` and proves the
 * Plugin idempotency assertion works end-to-end against a minimal
 * `Plugin` implementation.
 *
 * If this test fails, the conformance contract is broken — fix the
 * contract base, not this stub.
 */
package io.sm8.sdk.contract

import io.sm8.sdk.{Plugin, Request, Result}

/** Minimal no-op `Plugin` that records how many times setup was called. */
final class StubPlugin extends Plugin {
  var setupCalls: Int = 0

  override def setup(engine: io.sm8.sdk.Engine): Unit = {
    setupCalls += 1
  }
}

/**
 * Extends `PluginContractSpec` and supplies the abstract plugin +
 * engine. Real Plugins follow this same shape.
 */
class StubPluginSpec extends PluginContractSpec {

  override def plugin: io.sm8.sdk.Plugin = new StubPlugin

  override def engine: io.sm8.sdk.Engine =
    PluginContractSpecStubs.NoopEngine
}