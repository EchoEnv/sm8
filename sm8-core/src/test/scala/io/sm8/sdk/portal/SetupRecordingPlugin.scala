/*
 * SM8 Core — test fixture for PortalDiscoverySpec.
 *
 * Public, no-arg-constructible Plugin that records that
 * `setup(engine)` was called. Used to verify ServiceLoader discovery
 * via the `META-INF/services/io.sm8.sdk.Plugin` file in test
 * resources.
 *
 * Lives in test scope only — not shipped in the published JAR.
 * Per [[scala-jvm-safety-mindset]]: uses AtomicBoolean (no `var`).
 */
package io.sm8.sdk.portal

import java.util.concurrent.atomic.AtomicBoolean

import io.sm8.sdk.{Engine, Plugin}

/**
 * Recording Plugin for Portal tests. The shared `wasCalled` flag is
 * reset in `PortalDiscoverySpec` before each test (per ScalaTest
 * `BeforeAndAfterEach`).
 */
final class SetupRecordingPlugin extends Plugin {

  override def setup(engine: Engine): Unit = {
    SetupRecordingPlugin.wasCalled.set(true)
  }
}

object SetupRecordingPlugin {
  /** Reset by each PortalDiscoverySpec test. */
  val wasCalled: AtomicBoolean = new AtomicBoolean(false)

  /** No-arg factory (ServiceLoader requires this). */
  def newInstance(): SetupRecordingPlugin = new SetupRecordingPlugin
}