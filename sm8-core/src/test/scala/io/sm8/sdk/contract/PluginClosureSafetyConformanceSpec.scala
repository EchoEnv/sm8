/*
 * SM8 Core - PluginClosureSafetyConformanceSpec.
 *
 * Per the spark-batch-bugs-mindset mantra #1: every Plugin in
 * the reactor must close over only Serializable state. This spec
 * ties together the per-Plugin contracts (commit 1's
 * PluginSerializationSpec, the per-plugin closedOverVars overrides
 * from commit 2) into a reactor-wide conformance gate.
 *
 * Per karpathy-guidelines-mindset 'smallest correct core': the
 * gate is parameterized over the Plugin contract surface (a test
 * fixture that captures a state field) - the contract is sealed
 * at the trait level via 'Plugin extends java.io.Serializable'
 * (commit 1's trait change). Per-plugin conformance is verified
 * per-module by the per-plugin test suites; the reactor-wide
 * assertion here is that the contract SHAPE holds.
 */
package io.sm8.sdk.contract

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.sdk.{Plugin, PreHook, HookStage, Context, Engine}

private final class FixturePlugin(override val name: String) extends Plugin {

  val captured: java.util.concurrent.atomic.AtomicInteger =
    new java.util.concurrent.atomic.AtomicInteger(0)

  override def setup(engine: Engine): Unit = ()

  override def closedOverVars: Seq[String] = Seq("name", "captured")
}

private final class FixtureHook(
    counter: java.util.concurrent.atomic.AtomicInteger
) extends PreHook with java.io.Serializable {
  override val name: String  = "fixture-hook"
  override val priority: Int = 100
  override def stage: HookStage = HookStage.PreExecute
  override def run(c: Context): Context = { counter.incrementAndGet(); c }
}

class PluginClosureSafetyConformanceSpec extends AnyFunSuite with Matchers {

  private def roundTripViaJavaSerialization[T <: AnyRef](value: T): T = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  test("Plugin contract: 'extends Plugin with java.io.Serializable' is enforced by the trait") {
    val traitClass = classOf[Plugin]
    traitClass.getInterfaces.exists(_ == classOf[java.io.Serializable]) shouldBe true
  }

  test("Plugin contract: every Plugin MUST be Serializable - fixture round-trip") {
    val original = new FixturePlugin("audit-style")
    val restored = roundTripViaJavaSerialization(original)
    restored should not be null
    restored.name shouldBe "audit-style"
    restored.captured should not be null
  }

  test("Plugin contract: closedOverVars lists every captured state field") {
    val p = new FixturePlugin("broadcast-style")
    p.closedOverVars should contain theSameElementsAs Seq("name", "captured")
  }

  test("Plugin contract: default closedOverVars is Seq.empty for a no-capture Plugin") {
    val p = new Plugin {
      override def setup(engine: Engine): Unit = ()
    }
    p.closedOverVars shouldBe Seq.empty
  }

  test("Hook class: closure-safe baseline (Round-trip via ObjectOutputStream)") {
    val counter = new java.util.concurrent.atomic.AtomicInteger(7)
    val original = new FixtureHook(counter)
    val restored = roundTripViaJavaSerialization(original)
    restored.name shouldBe "fixture-hook"
    restored.priority shouldBe 100
  }
}
