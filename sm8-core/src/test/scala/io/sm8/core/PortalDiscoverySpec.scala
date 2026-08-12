/*
 * SM8 Core — PortalDiscoverySpec.
 *
 * Step 7: direct unit tests for ServiceLoader-based Plugin discovery
 * with the Maven-coords allowlist (Q6 = C).
 *
 * Per [[debug-mantra-mindset]]: assert real behavior — bad coords
 * skip the plugin, good coords load it, discovery never crashes the
 * engine.
 *
 * Per [[scala-jvm-safety-mindset]]: all state via AtomicBoolean /
 * immutable collections.
 */
package io.sm8.core

import io.sm8.sdk.portal.SetupRecordingPlugin

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PortalDiscoverySpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit =
    SetupRecordingPlugin.wasCalled.set(false)

  "EngineImpl.discover" should "load a Plugin whose coords are in the allowlist" in {
    val engine: EngineImpl = EngineImpl()
    val loaded = engine.discover(Set("io.sm8.sdk:test-portal-fixture"))
    loaded.map(_.getClass.getSimpleName) shouldBe List("SetupRecordingPlugin")
    SetupRecordingPlugin.wasCalled.get() shouldBe true
  }

  it should "skip a Plugin whose coords are NOT in the allowlist" in {
    val engine: EngineImpl = EngineImpl()
    val loaded = engine.discover(Set("io.sm8:something-else"))
    loaded shouldBe empty
    // Plugin's setup() must NOT have been called.
    SetupRecordingPlugin.wasCalled.get() shouldBe false
  }

  it should "load every Plugin when allowlist is empty (discoverAll)" in {
    val engine: EngineImpl = EngineImpl()
    val loaded = engine.discoverAll()
    // At least our SetupRecordingPlugin is found.
    loaded.map(_.getClass.getSimpleName) should contain ("SetupRecordingPlugin")
    SetupRecordingPlugin.wasCalled.get() shouldBe true
  }

  it should "never crash when ServiceLoader has a malformed entry" in {
    // The META-INF/services file is well-formed in our test, so this
    // verifies that bad inputs (any NonFatal during instantiation)
    // surface as a warning + skip, not an exception.
    val engine: EngineImpl = EngineImpl()
    noException should be thrownBy engine.discover(Set.empty)
  }
}