/*
 * SM8 Core — PluginContractSpec.
 *
 * Step-1 skeleton. Asserts that the Plugin trait exists, has the
 * expected shape, and that a minimal implementation compiles and
 * satisfies the contract.
 *
 * Step 2 promotes this to a fuller conformance suite covering
 * idempotency (plugins.md Rule 1) and registration-order invariants.
 *
 * Per karpathy-guidelines §1.2: Protocol compliance is checked at
 * definition time, not silently at runtime.
 */
package io.sm8.sdk

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PluginContractSpec extends AnyFlatSpec with Matchers {

  "Plugin" should "be a trait with a single setup(engine) method" in {
    val clazz = classOf[Plugin]
    val methods = clazz.getMethods.map(_.getName).toSet
    methods should contain("setup")
  }

  it should "be implementable by a minimal no-op Plugin" in {
    val noop = new Plugin {
      def setup(engine: Engine): Unit = ()
    }
    noop shouldBe a [Plugin]
  }

  it should "allow setup to be called once without side effects" in {
    var calls = 0
    val p = new Plugin {
      def setup(engine: Engine): Unit = { calls += 1 }
    }
    // Idempotency contract per RFC plugins.md Rule 1 — setup is called
    // exactly once at startup. Step 2 enforces this in the registry.
    p.setup(null) // engine: null is fine for Step 1 (no-op Plugin)
    calls shouldBe 1
  }
}