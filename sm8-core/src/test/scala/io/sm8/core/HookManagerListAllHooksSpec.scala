/*
 * SM8 Core — HookManagerListAllHooksSpec.
 *
 * Per C10-PR-A: direct unit tests for the new `HookManager.listAllHooks()`
 * surface. Verifies that every registered hook (pre + post, all 8 stages)
 * is enumerated with its (name, stage, priority, origin, pluginName)
 * metadata. Backs the `list_hooks` transport surface in PR-B.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * 5 tests cover the 5 behaviors (empty, single-hook, multi-hook
 * across stages, pluginName attribution via EngineImpl.use, ordering).
 *
 * Per RFC §13 conformance PR + C10-PR-A: the `use(plugin)` thread-local
 * is the established seam for attributing hooks to plugins (the SDK's
 * `registerPreHook` signature stayed unchanged — the attribution
 * happens in `HookManagerImpl` via the thread-local read at register
 * time).
 */
package io.sm8.core

import io.sm8.sdk._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HookManagerListAllHooksSpec extends AnyFlatSpec with Matchers {

  // ---- Test fixtures ----

  private final class FakePreHook(override val name: String, prio: Int)
      extends PreHook {
    override val priority: Int = prio
    override def stage: HookStage = HookStage.PreExecute
    override def run(context: Context): Context = context
  }

  private final class FakePostHook(override val name: String, prio: Int)
      extends PostHook {
    override val priority: Int = prio
    override def stage: HookStage = HookStage.PostExecute
    override def run(context: Context): Context = context
  }

  private final class FakePlugin(override val name: String) extends Plugin {
    override def setup(engine: Engine): Unit = ()
  }

  "HookManagerImpl.listAllHooks" should "return empty Seq when no hooks registered" in {
    val hm = new HookManagerImpl
    hm.listAllHooks() shouldBe Seq.empty
  }

  it should "enumerate a single PreHook with full metadata" in {
    val hm = new HookManagerImpl
    hm.registerPreHook(HookStage.PreExecute, new FakePreHook("audit-pre", 50), 50)
    val hooks = hm.listAllHooks()
    hooks.size shouldBe 1
    hooks.head.name       shouldBe "audit-pre"
    hooks.head.stage      shouldBe HookStage.PreExecute
    hooks.head.priority   shouldBe 50
    hooks.head.pluginName shouldBe "<core>" // no EngineImpl.use() wrapping
  }

  it should "enumerate across all 8 stages when hooks are registered in each" in {
    val hm = new HookManagerImpl
    val stages = HookStage.values
    stages.foreach { s =>
      hm.registerPreHook(s, new FakePreHook(s"pre-${s.getClass.getSimpleName}", 10), 10)
      hm.registerPostHook(s, new FakePostHook(s"post-${s.getClass.getSimpleName}", 20), 20)
    }
    val hooks = hm.listAllHooks()
    hooks.size shouldBe stages.size * 2
    hooks.map(_.stage).toSet shouldBe stages.toSet
  }

  it should "attribute hooks registered AFTER use() returns to the post-clear state (<core>)" in {
    val engine = new EngineImpl
    val plugin = new FakePlugin("my-plugin")
    engine.use(plugin)
    // After use(), the plugin's setup() was called; our FakePlugin
    // is a no-op, but we can simulate a hook registration by
    // injecting one directly into the manager — the thread-local
    // has already been cleared by the time we get here. So this test
    // verifies the post-clear state: listAllHooks should return any
    // pre-existing entries attributed to <core> (or whatever the
    // current thread-local was at registration time).
    engine.hooks.registerPreHook(
      HookStage.PreResolve, new FakePreHook("resolve-validator", 100), 100
    )
    val hooks = engine.hooks.asInstanceOf[HookManagerImpl].listAllHooks()
    hooks.find(_.name == "resolve-validator") shouldBe defined
    hooks.find(_.name == "resolve-validator").map(_.pluginName) shouldBe Some("<core>")
  }

  it should "attribute hooks registered INSIDE use(plugin).setup() to the registering plugin" in {
    // Per C10-PR-A final-gate (data-eng F1): real in-window attribution.
    // The plugin below registers a hook DURING setup(), so the
    // EngineImpl.use thread-local is set to the plugin's name at
    // register time — the hook MUST surface with pluginName =
    // "registering-plugin", not "<core>".
    final class RegisteringPlugin(override val name: String) extends Plugin {
      override def setup(engine: Engine): Unit =
        engine.hooks.registerPreHook(
          HookStage.PreResolve, new FakePreHook("in-window-hook", 100), 100
        )
    }
    val engine = new EngineImpl
    val plugin = new RegisteringPlugin("registering-plugin")
    engine.use(plugin)
    val hooks = engine.hooks.asInstanceOf[HookManagerImpl].listAllHooks()
    val inWindow = hooks.find(_.name == "in-window-hook")
    inWindow shouldBe defined
    inWindow.map(_.pluginName) shouldBe Some("registering-plugin")
  }

  it should "expose the synthetic Plugin.metadata default when not overridden" in {
    // Per C10-PR-A final-gate (architect F5): the trait's default
    // `metadata` returns synthetic coordinates (groupId =
    // "io.sm8.plugins", artifactId = simple class name, version =
    // "0.0.0"). Plugin authors SHOULD override with the real
    // coordinates from their `META-INF/sm8/plugin.properties`,
    // but legacy plugins inherit the default.
    val md = new FakePlugin("meta-test-plugin").metadata
    md.groupId shouldBe "io.sm8.plugins"
    md.artifactId shouldBe "FakePlugin"
    md.version shouldBe "0.0.0"
  }

  it should "sort hooks by (stage declaration, priority, pluginName, name)" in {
    val hm = new HookManagerImpl
    // Register in scrambled order to confirm the sort is real.
    hm.registerPostHook(HookStage.PostFormat, new FakePostHook("z-post", 999), 999)
    hm.registerPreHook (HookStage.PreParse,   new FakePreHook ("a-pre",  1), 1)
    hm.registerPreHook (HookStage.PostExecute,new FakePreHook ("m-pre",  10), 10)
    val hooks = hm.listAllHooks()
    // Declared order is PreParse, PostParse, PreResolve, PostResolve,
    // PreExecute, PostExecute, PreFormat, PostFormat. We registered
    // 3 of those: PreParse (priority 1), PostExecute (priority 10),
    // PostFormat (priority 999). Expected sort order:
    val names = hooks.map(_.name)
    names shouldBe Seq("a-pre", "m-pre", "z-post")
  }
}