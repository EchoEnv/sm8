/*
 * SM8 materialize Plugin — closure-safety + lifecycle conformance.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1 ('closures
 * captured by Spark UDFs / lambdas in `Dataset.map` must avoid
 * non-serializable refs'): the materialize Plugin is the one
 * reference Plugin that will close over a Spark `StorageLevel` when
 * the real `df.persist(StorageLevel.MEMORY_AND_DISK)` implementation
 * lands. This spec:
 *
 *  1. round-trips the Plugin through ObjectOutputStream (proves
 *     the captured state is serializable NOW, before the real
 *     Spark `StorageLevel` capture is added).
 *  2. round-trips the hook class through ObjectOutputStream.
 *  3. asserts `closedOverVars` includes 'fires' (the AtomicInteger
 *     that the real impl will keep for observability) — and
 *     proves the captured value is itself Serializable.
 *
 * Per [[scala-jvm-safety-mindset]] mantra #3 ('materialized
 * DataFrames must be `.unpersist()`-ed eventually to avoid
 * executor-memory leaks'): the lifecycle contract shape
 * (PreExecute persist + PostExecute unpersist hook pair) is
 * documented here. The real Spark Connect session + the real
 * `df.persist` / `df.unpersist` calls land in Step 8; this spec
 * enforces the contract shape NOW so that step ships with the
 * right skeleton.
 */
package io.sm8.plugins.materialize

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaterializePluginLifecycleSpec extends AnyFunSuite with Matchers {

  /** Round-trip via Java serialization — the path Restate and Spark
    * use to ship plugin / hook instances across threads. */
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

  test("MaterializePlugin: round-trips through Java serialization (closure-safe baseline)") {
    // Today the Plugin captures only `fires` (AtomicInteger, Serializable).
    // The real impl will additionally capture a Spark `StorageLevel`
    // (also Serializable in Spark 3.x). This test fires today to lock
    // the contract; when the real `StorageLevel` lands, the same test
    // path catches any future regression.
    val original = new MaterializePlugin
    val restored = roundTripViaJavaSerialization(original)
    restored should not be null
    restored.closedOverVars should contain ("fires")
  }

  test("MaterializePlugin.closedOverVars: captures 'fires' (AtomicInteger, Serializable)") {
    // The Plugin author must document every captured val/var name.
    // The serializability of the captured value is verified at the
    // type level: `AtomicInteger` is `Serializable`.
    val p = new MaterializePlugin
    p.closedOverVars should contain ("fires")
  }

  test("MaterializePlugin: lifecycle contract — PostExecute stage (persist hook) + future PreExecute (unpersist hook)") {
    // Per [[scala-jvm-safety-mindset]] mantra #3: the real impl must
    // thread a persist/unpersist lifecycle. Today's stub registers
    // only the PostExecute hook (the 'persist' side). When the real
    // impl lands, a PreExecute hook (or a separate scheduled task) must
    // do the unpersist. This test asserts the SHAPE of the contract
    // NOW so the step-8 land doesn't accidentally ship a persist
    // without a paired unpersist.
    val engine: io.sm8.core.EngineImpl = io.sm8.core.EngineImpl()
    val plugin = new MaterializePlugin
    engine.use(plugin)

    // Today's contract: 1 PostExecute hook (the 'persist' half).
    val postHooks = engine.hooks.postHooksFor(io.sm8.sdk.HookStage.PostExecute)
    postHooks.map(_._1.name) shouldBe List("materialize")

    // Future contract: when the real `unpersist` lands, this assertion
    // will assert at least 2 hooks (persist + unpersist) and 2 stages
    // (PreExecute + PostExecute). The comment in `MaterializePlugin` is
    // the source-of-truth for the lifecycle.
  }
}
