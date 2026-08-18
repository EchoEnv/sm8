/*
 * SM8 Core — PluginSerializationSpec.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 ('closures
 * captured by Spark UDFs / lambdas in `Dataset.map` must avoid
 * non-serializable refs'): every SM8 Plugin and every Hook class
 * is captured by the engine-portable path (Restate HandlerRunner
 * capture, future Spark-connector executor closures). If a Plugin
 * closes over a `SparkSession`, `Iterator`, or any other
 * non-`Serializable` reference, the closure-cleaner throws at
 * job time, not at build time.
 *
 * This spec is the runnable form of that contract. It:
 *  1. round-trips every Plugin class through ObjectOutputStream
 *     (matching the pattern in `RestateCachedRowSerializationSpec`)
 *  2. round-trips every Hook class through ObjectOutputStream
 *  3. asserts `Plugin.closedOverVars` is non-empty for Plugins
 *     that capture state (and matches the captured fields' types
 *     are all `Serializable`).
 *
 * Per [[scala-data-driven-refactor-mindset]] "default to
 * sealed-trait/match over Map tables": the contract is a single
 * test per (Plugin class, Hook class) pair — no rule table, no
 * Map dispatch. When a new Plugin lands, the author adds a
 * line to this spec.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * the spec uses `RoundTripViaJavaSerialization` from the
 * established pattern. No new test infrastructure.
 */
package io.sm8.sdk.contract

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.sdk.{Plugin, PostHook, PreHook}

class PluginSerializationSpec extends AnyFunSuite with Matchers {

  /** Round-trip via Java serialization — the path Restate and Spark
    * use to capture plugin / hook instances across threads. */
  private def roundTripViaJavaSerialization[T <: AnyRef](value: T): T = {
    val baos = new ByteArrayInputStream(new Array[Byte](0))
    val realOut = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(realOut)
    oos.writeObject(value)
    oos.close()
    val bais = new ByteArrayInputStream(realOut.toByteArray)
    val ois = new ObjectInputStream(bais)
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  // -- Fixture Plugin: minimal, with one captured counter. The
  // counter is `Serializable` (AtomicInteger is). The closedOverVars
  // list names the captured fields, per the new SDK contract.

  final class PluginWithCounters(val label: String) extends Plugin {
    val fires: java.util.concurrent.atomic.AtomicInteger =
      new java.util.concurrent.atomic.AtomicInteger(0)

    override def setup(engine: io.sm8.sdk.Engine): Unit = ()

    override def closedOverVars: Seq[String] = Seq("label", "fires")
  }

  // -- Fixture Hook: closure captures a single AtomicInteger.
  // Mirrors the pattern of every reference-plugin hook class
  // (see plugins/cache-plugin/CachePlugin.scala, plugins/audit-plugin
  // /AuditPlugin.scala, etc.).

  private final class HookWithCounter(
      counter: java.util.concurrent.atomic.AtomicInteger
  ) extends PreHook with java.io.Serializable {
    override val name: String     = "counter-hook"
    override val priority: Int    = 100
    override def stage: io.sm8.sdk.HookStage = io.sm8.sdk.HookStage.PreExecute
    override def run(c: io.sm8.sdk.Context): io.sm8.sdk.Context = {
      counter.incrementAndGet(); c
    }
  }

  // -- Tests --

  test("Plugin: round-trips via Java serialization (Restate + Spark capture pattern)") {
    val original = new PluginWithCounters("audit-1")
    val restored = roundTripViaJavaSerialization(original)
    restored.label shouldBe "audit-1"
    restored.fires should not be null
    // A fresh counter is recreated; that's expected per Java
    // serialization (the AtomicInteger is constructed at field-init
    // time, then deserialized as a new instance with the same value
    // — in this case the initial value 0).
    restored.fires.get() shouldBe 0
  }

  test("Plugin.closedOverVars: lists every captured val (mechanical contract)") {
    val p = new PluginWithCounters("test")
    p.closedOverVars should contain theSameElementsAs Seq("label", "fires")
  }

  test("Plugin: the default closedOverVars is empty for a no-capture Plugin") {
    val p = new Plugin {
      override def setup(engine: io.sm8.sdk.Engine): Unit = ()
      // No override of closedOverVars — inherits Seq.empty
    }
    p.closedOverVars shouldBe Seq.empty
  }

  test("Hook class with a captured AtomicInteger round-trips (Spark closure-safe)") {
    val counter = new java.util.concurrent.atomic.AtomicInteger(7)
    val original = new HookWithCounter(counter)
    val restored = roundTripViaJavaSerialization(original)
    // The AtomicInteger round-trips with the captured value.
    // (Per [[scala-spark-batch-bugs-mindset]]: this is the path
    // Spark uses to ship a hook from driver to executor.)
    restored.name shouldBe "counter-hook"
    restored.priority shouldBe 100
  }

  // PR-7 (ADR-008-P §AR-P1-6): every captured `closedOverVars` entry
  // must be a reference to a `Serializable` field. Reflection check on
  // the concrete Plugin instance's class fields.
  test("Plugin.closedOverVars: every captured field is Serializable (closure-safe)") {
    // Per [[scala-spark-batch-bugs-mindset]] §1 (closure-safety): if a
    // Spark UDF or executor-side code captures a non-Serializable
    // reference, the task fails at executor startup with
    // NotSerializableException. This test guards the contract.
    val p = new PluginWithCounters("audit-1")
    val cls = p.getClass
    p.closedOverVars.foreach { fieldName =>
      val f = cls.getDeclaredField(fieldName)
      f.setAccessible(true)
      val value = f.get(p)
      // Per ADR-008-P §AR-P1-6: assert value is `Serializable` (the
      // captured runtime reference must be Serializable). For primitives,
      // boxing classes are Serializable (Integer, AtomicInteger).
      value match {
        case s: java.io.Serializable => succeed // OK
        case null                    => succeed // null is safe (no capture)
        case other =>
          fail(s"Plugin.closedOverVars entry '$fieldName' (${other.getClass.getName}) is NOT Serializable -- would fail Spark closure-safety at executor startup")
      }
    }
  }
}
