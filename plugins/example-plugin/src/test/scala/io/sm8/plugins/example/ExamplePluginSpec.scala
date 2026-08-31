/*
 * SM8 example Plugin — behavioral spec.
 *
 * The concrete, copyable test pattern for a plugin: register the
 * plugin on a fresh engine, run a request, and assert the hook did
 * its one job (stamped the trace tag; counted the fire). Also proves
 * the closure-safety contract: the plugin round-trips through Java
 * serialization, which is what the engine's journal capture does.
 *
 * When you copy this plugin, keep this shape and swap the
 * assertions for your hook's behavior.
 */
package io.sm8.plugins.example

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.core.EngineImpl
import io.sm8.sdk.{Context, HookStage, PipelineStage, PostHook, Request}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Named-probe Request so the integration assertions check the
  * concrete tag value rather than a tautology (request.getClass
  * returns the same string both sides of the hook). The object's
  * simple class name is `TraceProbeRequest$`, matching the hook's
  * `getSimpleName`-derived tag. */
case object TraceProbeRequest extends Request

class ExamplePluginSpec extends AnyFlatSpec with Matchers {

  "ExamplePlugin.setup" should "register a single Post-hook at PostExecute" in {
    val engine: EngineImpl = EngineImpl()
    engine.use(new ExamplePlugin)

    engine.hooks.postHooksFor(HookStage.PostExecute).map(_._1.name) shouldBe
      List("example-trace")
  }

  it should "stamp the trace tag into context.meta and count the fire" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new ExamplePlugin
    engine.use(plugin)

    // Capture the final Context through a late-priority PostFormat
    // observer so the integration path asserts the meta write,
    // not only the counter. The engine's `run` returns only the
    // Result envelope; the capture hook sees the Context threaded
    // through every post-stage.
    val capturedMeta = scala.collection.mutable.Map.empty[String, Any]
    engine.hooks.registerPostHook(
      HookStage.PostFormat,
      new PostHook with java.io.Serializable {
        override val name: String = "example-meta-capture"
        override val priority: Int = 900
        override def stage: HookStage = HookStage.PostFormat
        override def run(context: Context): Context = {
          capturedMeta ++= context.meta
          context
        }
      },
      priority = 900
    )

    // Named probe request: the tag embeds its simple class name,
    // so the assertion is specific rather than tautological.
    engine.run(TraceProbeRequest)
    plugin.fires.get() shouldBe 1
    capturedMeta.get(ExamplePlugin.TraceTagKey) shouldBe defined
    capturedMeta(ExamplePlugin.TraceTagKey).toString shouldBe "example:TraceProbeRequest$"
  }

  it should "write a namespaced, deterministic tag for a given request" in {
    val engine: EngineImpl = EngineImpl()
    engine.use(new ExamplePlugin)

    // Read the registered hook back and run it against a baseline
    // context — same shape the engine uses on the real path.
    val hook = engine.hooks.postHooksFor(HookStage.PostExecute).head._1
    val ctx = Context(
      stage   = PipelineStage.Execute,
      request = TraceProbeRequest,
      result  = None,
      meta    = Map.empty,
      stop    = false
    )
    val out = hook.run(ctx)
    // The tag embeds the request class name so downstream consumers
    // can attribute it (namespaced key per RFC plugins.md Rule 3).
    out.meta(ExamplePlugin.TraceTagKey) shouldBe "example:TraceProbeRequest$"
  }

  it should "pass a null request through without throwing (observer contract)" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new ExamplePlugin
    engine.use(plugin)
    val hook = engine.hooks.postHooksFor(HookStage.PostExecute).head._1
    // A malformed Context with null request must pass through with
    // no meta write — an observer never kills the pipeline on input
    // it doesn't understand.
    val ctx = Context(
      stage   = PipelineStage.Execute,
      request = null,
      result  = None,
      meta    = Map.empty,
      stop    = false
    )
    val out = hook.run(ctx)
    out.meta.get(ExamplePlugin.TraceTagKey) shouldBe empty
    plugin.fires.get() shouldBe 1
  }

  it should "be deterministic — same request, same tag" in {
    val engine: EngineImpl = EngineImpl()
    engine.use(new ExamplePlugin)
    val hook = engine.hooks.postHooksFor(HookStage.PostExecute).head._1
    val ctx = Context(
      stage   = PipelineStage.Execute,
      request = new Request {},
      result  = None,
      meta    = Map.empty,
      stop    = false
    )
    val first  = hook.run(ctx).meta(ExamplePlugin.TraceTagKey)
    val second = hook.run(ctx).meta(ExamplePlugin.TraceTagKey)
    first shouldBe second
  }

  "ExamplePlugin" should "survive Java-serialization round-trip (closure-safety)" in {
    val plugin = new ExamplePlugin
    val bytes = {
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(plugin)
      oos.close()
      bos.toByteArray
    }
    val back = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[ExamplePlugin]
    }
    back.fires.get() shouldBe 0
  }

  "ExamplePlugin" should "be discoverable via META-INF/services (SPI registered)" in {
    // This is the only test in the template that scans the build-
    // classpath for META-INF/services files rather than registering
    // on a fresh EngineImpl: it verifies the on-disk SPI shape, not
    // the in-memory registration (covered by the contract spec).
    val loader = getClass.getClassLoader
    val discovered = scala.collection.mutable.ListBuffer[String]()
    val it = loader.getResources("META-INF/services/io.sm8.sdk.Plugin")
    while (it.hasMoreElements) {
      val src = scala.io.Source.fromURL(it.nextElement())
      try discovered ++= src.getLines().map(_.trim).filter(_.nonEmpty)
      finally src.close()
    }
    discovered should contain ("io.sm8.plugins.example.ExamplePlugin")
  }
}
