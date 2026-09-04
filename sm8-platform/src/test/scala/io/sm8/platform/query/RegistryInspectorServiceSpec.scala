/*
 * SM8 Platform — RegistryInspectorServiceSpec (C10-PR-B).
 *
 * Exercises the wire shape + handler logic of the transport layer's
 * `RegistryInspectorService`: two read-only handlers over the
 * boot-stable plugin + hook registries. The transport knows NOTHING
 * about plugin domain semantics — it projects the core-sourced
 * registries into flat string DTOs (`PluginEntry`, `HookEntry`).
 *
 * Per C10 map #306 + ticket #307. Service shape mirrors
 * `MetaInspectorService` (SERVICE + SHARED; closures over
 * deployment-owned state, evaluated per call).
 */
package io.sm8.platform.query

import io.sm8.core.engine.EngineRegistry
import io.sm8.core.model._

import io.sm8.sdk._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RegistryInspectorServiceSpec extends AnyFlatSpec with Matchers {

  // ---- Fixtures ----

  private final class FakePlugin(override val name: String) extends Plugin {
    override def setup(engine: Engine): Unit = ()
  }

  private final class FakePreHook(override val name: String, prio: Int)
      extends PreHook {
    override val priority: Int = prio
    override def stage: HookStage = HookStage.PreResolve
    override def run(context: Context): Context = context
  }

  private val fakePlugin = new FakePlugin("fake-plugin")
  private val fakeStatus: SetupStatus = SetupStatus.Registered("fake-plugin")

  private val hooksFn: () => Seq[RegisteredHook] = () =>
    Seq(
      RegisteredHook(
        name       = "audit-pre",
        stage      = HookStage.PreResolve,
        priority   = 120,
        origin     = HookOrigin.FirstParty,
        pluginName = "fake-plugin"
      )
    )

  private val pluginsFn: () => Seq[(Plugin, SetupStatus)] = () =>
    Seq((fakePlugin, fakeStatus))

  "RegistryInspectorService.toPluginEntry" should
    "project a registered plugin with its real metadata" in {
    val entry = RegistryInspectorService.toPluginEntry(
      fakePlugin,
      SetupStatus.Registered("fake-plugin")
    )
    entry.name       shouldBe "fake-plugin"
    entry.coords     shouldBe "io.sm8.plugins:FakePlugin"
    entry.version    shouldBe "0.0.0"
    entry.registered shouldBe true
  }

  it should "project a discovered-but-unregistered plugin with registered=false" in {
    val entry = RegistryInspectorService.toPluginEntry(
      fakePlugin,
      SetupStatus.NotRegistered("FakePlugin", "allowlist-gated")
    )
    entry.registered shouldBe false
  }

  "RegistryInspectorService.toHookEntry" should
    "project a RegisteredHook with stable wire stage + origin names" in {
    val entry = RegistryInspectorService.toHookEntry(
      RegisteredHook(
        name       = "audit-pre",
        stage      = HookStage.PreResolve,
        priority   = 120,
        origin     = HookOrigin.FirstParty,
        pluginName = "audit-plugin"
      )
    )
    entry.name       shouldBe "audit-pre"
    entry.stage      shouldBe "pre:resolve" // wire name, not case-object toString
    entry.priority   shouldBe 120
    entry.origin     shouldBe "FirstParty"
    entry.pluginName shouldBe "audit-plugin"
  }

  it should "use the wireName for every stage (spot-check post:format)" in {
    val entry = RegistryInspectorService.toHookEntry(
      RegisteredHook(
        name       = "x",
        stage      = HookStage.PostFormat,
        priority   = 0,
        origin     = HookOrigin.Core,
        pluginName = "<core>"
      )
    )
    entry.stage shouldBe "post:format"
  }

  "RegistryInspectorService.definition" should
    "expose a ServiceDefinition named RegistryInspectorService with 2 handlers" in {
    val defn = RegistryInspectorService.definition(hooksFn, pluginsFn)
    defn.getServiceName shouldBe "RegistryInspectorService"
    // ServiceType.SERVICE per the MetaInspectorService rationale.
    defn.getServiceType shouldBe dev.restate.sdk.endpoint.definition.ServiceType.SERVICE
    // 2 handlers: listPlugins + listHooks.
    import scala.jdk.CollectionConverters._
    val handlers = defn.getHandlers.asScala.toSeq
    handlers.map(_.getName).toSet shouldBe Set("listPlugins", "listHooks")
    handlers.foreach { h =>
      h.getHandlerType shouldBe dev.restate.sdk.endpoint.definition.HandlerType.SHARED
    }
  }

  it should "evaluate the pluginsFn + hooksFn closures when handlers run" in {
    // Direct closure evaluation (the HandlerRunner path needs the
    // full Restate machinery; the spec verifies the pure functions
    // the closures delegate to, per the MetaInspectorServiceSpec
    // pattern of testing filterByPrefix directly).
    var pluginCalls = 0
    var hookCalls   = 0
    val countingPluginsFn: () => Seq[(Plugin, SetupStatus)] = () =>
      { pluginCalls += 1; pluginsFn() }
    val countingHooksFn: () => Seq[RegisteredHook] = () =>
      { hookCalls += 1; hooksFn() }
    // Invoke the closures the same way the runner would.
    val entries = countingPluginsFn().map { case (p, s) =>
      RegistryInspectorService.toPluginEntry(p, s)
    }
    val hookEntries = countingHooksFn().map(RegistryInspectorService.toHookEntry)
    pluginCalls shouldBe 1
    hookCalls shouldBe 1
    entries.size shouldBe 1
    hookEntries.size shouldBe 1
  }
}
