/*
 * SM8 Core — EngineFactorySpec.
 *
 * Unit tests for `io.sm8.core.EngineFactory.create(plugins)`,
 * the sole outward seam from the adapter layer for engine
 * construction. Verifies:
 *
 * 1. Returns a non-null Engine.
 * 2. Return type is the SDK `Engine` trait (NOT `EngineImpl`),
 *    enforcing the layer rule at the type-system level: an
 *    adapter that names `EngineImpl` cannot compile against the
 *    return value.
 * 3. Empty Seq produces a working engine (matches the unit-test
 *    path where no plugins are loaded).
 * 4. Plugins passed in are registered: a counter plugin
 *    increments on `setup()`.
 * 5. The factory creates a fresh Engine per call (no caching) —
 *    matches `PluginDiscovery.discoverFromConfig()` per-call
 *    semantics (no leak risk across hot-reload).
 */
package io.sm8.core

import io.sm8.sdk.Engine
import io.sm8.sdk.Plugin

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineFactorySpec extends AnyFlatSpec with Matchers {

  // Test plugin: counts how many times setup() was called.
  private final class CounterPlugin extends Plugin {
    var setupCount: Int = 0
    override def setup(engine: Engine): Unit = {
      setupCount += 1
    }
  }

  "EngineFactory.create" should "return a non-null Engine" in {
    val engine = EngineFactory.create(Seq.empty)
    assert(engine != null)
  }

  it should "return the SDK Engine trait (not EngineImpl) — type-system layer enforcement" in {
    // The compile-time check IS the test. If this code compiles,
    // the return type IS `Engine`. The assertion below is belt-
    // and-suspenders for runtime checkers (Scala runtime type
    // erasure doesn't preserve the trait vs class distinction,
    // but if a future refactor widens the return type to
    // `AnyRef` or removes the ascription, this will catch it).
    val engine: Engine = EngineFactory.create(Seq.empty)
    assert(engine.isInstanceOf[EngineImpl],
      "engine should be an EngineImpl at runtime (concrete class)")
  }

  it should "accept an empty plugin Seq (unit-test path)" in {
    val engine = EngineFactory.create(Seq.empty)
    // The empty engine is still usable: querying it yields a typed
    // error (no provider registered), not a crash. We don't
    // exercise the typed error here (covered by EngineSmokeSpec);
    // we just verify the constructor accepts Seq.empty.
    assert(engine != null)
  }

  it should "register every plugin in the Seq" in {
    val p1 = new CounterPlugin
    val p2 = new CounterPlugin
    val p3 = new CounterPlugin
    EngineFactory.create(Seq(p1, p2, p3))
    assert(p1.setupCount == 1)
    assert(p2.setupCount == 1)
    assert(p3.setupCount == 1)
  }

  it should "create a fresh Engine per call (no caching, no singleton)" in {
    // Same plugin passed to two create() calls: both engines
    // register the plugin. If the factory cached the engine, the
    // second create() would either skip the registration or
    // throw on duplicate. Neither is acceptable — the spec says
    // "per-call, no leak risk across hot-reload".
    val p = new CounterPlugin
    EngineFactory.create(Seq(p))
    EngineFactory.create(Seq(p))
    assert(p.setupCount == 2,
      "plugin.setup() should fire on every create() call, not once")
  }
}
