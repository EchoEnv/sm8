/*
 * SM8 materialize Plugin — closure-safety + lifecycle conformance.
 *
 * captured by Spark UDFs / lambdas in `Dataset.map` must avoid
 * non-serializable refs'): the materialize Plugin captures the
 * engine-portable `PersistLevel` marker (a sealed trait extending
 * `Product with Serializable`). The Spark connector's real impl
 * wraps `StorageLevel` in a concrete `PersistLevel` subtype —
 * which IS Serializable (Spark 3.x's contract).
 *
 * DataFrames must be `.unpersist()`-ed eventually to avoid
 * executor-memory leaks'): the lifecycle pair (PreExecute
 * persist + PostExecute unpersist) is the testable contract.
 *
 * 3 tests in 1 file. No Spark dependency. The materialize-plugin
 * module is engine-portable — the Spark-specific `StorageLevel`
 * lives in the spark-connector module per the Module Map.
 */
package io.sm8.plugins.materialize

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaterializePluginLifecycleSpec extends AnyFunSuite with Matchers {

  /** Round-trip via Java serialization — the path Restate and Spark
    * use to ship plugin instances across threads. */
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

  test("MaterializePlugin: round-trips through Java serialization (PersistLevel + fires captured)") {
    // The plugin captures a `PersistLevel.StubLevel` + an
    // `AtomicInteger` (both Serializable). This test fires the
    // closure-safety contract 
    // mantra #1.
    val original = new MaterializePlugin(PersistLevel.MemoryAndDisk)
    val restored = roundTripViaJavaSerialization(original)
    restored should not be null
    restored.closedOverVars should contain ("storageLevel")
    restored.closedOverVars should contain ("fires")
  }

  test("MaterializePlugin.closedOverVars: captures 'storageLevel' + 'fires' (both Serializable)") {
    val p = new MaterializePlugin(PersistLevel.DiskOnly)
    p.closedOverVars should contain ("storageLevel")
    p.closedOverVars should contain ("fires")
  }

  test("MaterializePlugin: lifecycle contract — BOTH PreExecute (persist) AND PostExecute (unpersist) register") {
    // (persist before, unpersist after) ensures executor-memory isn't
    // leaked. A regression that registers only one half breaks the
    // contract — this test enforces BOTH.
    val engine: io.sm8.core.EngineImpl = io.sm8.core.EngineImpl()
    val plugin = new MaterializePlugin(PersistLevel.MemoryAndDisk)
    engine.use(plugin)

    val preHooks  = engine.hooks.preHooksFor(io.sm8.sdk.HookStage.PreExecute)
    val postHooks = engine.hooks.postHooksFor(io.sm8.sdk.HookStage.PostExecute)

    preHooks.map(_._1.name) shouldBe List("materialize-pre")
    postHooks.map(_._1.name) shouldBe List("materialize-post")

    plugin.fires.get() shouldBe 0  // fires on execute, not on setup
  }
}
