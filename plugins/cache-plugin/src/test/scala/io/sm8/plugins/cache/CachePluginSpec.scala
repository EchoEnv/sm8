/*
 * SM8 cache Plugin — standalone spec.
 *
 * observable contract of the plugin (RFC §13 DoD observability).
 *
 * `with java.io.Serializable`. The full integration test
 * (HIT/MISS via the platform's EngineService) lives in sm8-platform
 * where the platform wiring is consumed; this spec confirms the
 * plugin's own corner.
 *
 * business state beyond the cache reference; we verify that the
 * counters + cache are the only captured state.
 *
 * ==What this spec verifies==
 *
 * 1. Plugin implements SDK Plugin + Serializable.
 * 2. closedOverVars is exactly { cache, readFires, writeFires,
 *    hits, misses } (mantra #1: nothing else captured).
 * 3. setup() registers ONE PreExecute + ONE PostExecute hook.
 * 4. Hooks have the documented priorities (50/60); RFC §8 Core origin.
 * 5. The plugin + its hooks round-trip through ObjectOutputStream.
 * 6. Portal service file declares the plugin class.
 */
package io.sm8.plugins.cache

import io.sm8.core.cache._
import io.sm8.core.EngineImpl

import io.sm8.sdk.{HookStage, Plugin}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CachePluginSpec extends AnyFunSuite with Matchers {

  /** Tiny in-spec ResultCache stub — the plugin takes the
    * abstract contract, not a concrete impl. The InMemoryResultCache
    * spec lives in sm8-platform (default impl moves in Phase 3). */
  private final class LocalCache extends ResultCache {
    private val store = scala.collection.mutable.Map.empty[String, RestateCachedRow]
    override def getJournaled(key: String): Option[RestateCachedRow] = store.get(key)
    override def putJournaledWithModelAndVersion(
        key: String, value: RestateCachedRow, model: String, version: Int
    ): Unit = { store.update(key, value); () }
  }

  private def newPlugin: CachePlugin = new CachePlugin(new LocalCache)

  // -- SDK contract --

  test("CachePlugin: implements SDK Plugin trait") {
    val p: Plugin = newPlugin
    p shouldBe a [Plugin]
  }

  test("CachePlugin: extends Serializable (closure-safety contract PR #36)") {
    newPlugin shouldBe a [java.io.Serializable]
  }

  // -- closedOverVars (mantra #1) --

  test("CachePlugin.closedOverVars: exactly the 5 documented captured vars") {
    val p = newPlugin
    p.closedOverVars.toSet shouldBe Set(
      "cache", "readFires", "writeFires", "hits", "misses"
    )
  }

  // -- Hook registration (RFC §8) --

  test("CachePlugin.setup: registers 1 PreExecute + 1 PostExecute hook") {
    val engine = new EngineImpl
    newPlugin.setup(engine)
    engine.hooks.preHooksFor(HookStage.PreExecute).size shouldBe 1
    engine.hooks.postHooksFor(HookStage.PostExecute).size shouldBe 1
  }

  test("CachePlugin.setup: PreExecute hook priority = 50 (RFC §8)") {
    val engine = new EngineImpl
    newPlugin.setup(engine)
    val (hook, priority) = engine.hooks.preHooksFor(HookStage.PreExecute).head
    priority shouldBe 50
    hook.stage    shouldBe HookStage.PreExecute
  }

  test("CachePlugin.setup: PostExecute hook priority = 60 (RFC §8)") {
    val engine = new EngineImpl
    newPlugin.setup(engine)
    val (hook, priority) = engine.hooks.postHooksFor(HookStage.PostExecute).head
    priority shouldBe 60
    hook.stage    shouldBe HookStage.PostExecute
  }

  // -- Serializable round-trip (per user's 'must be serializable every part') --

  test("CachePlugin: full Java-serialization round-trip preserves class + counters") {
    val p = newPlugin
    p.hits.incrementAndGet()
    p.misses.incrementAndGet()
    val bytes = {
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(p); oos.close(); bos.toByteArray
    }
    val back = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[CachePlugin]
    }
    back.hits.get   shouldBe 1
    back.misses.get shouldBe 1
    back.closedOverVars.toSet shouldBe Set(
      "cache", "readFires", "writeFires", "hits", "misses"
    )
  }

  // -- Portal discovery (RFC §11 + Step 9) --

  test("Portal: META-INF/services/io.sm8.sdk.Plugin declares this plugin") {
    val acc = scala.collection.mutable.Set[String]()
    val urls = classOf[CachePlugin].getClassLoader.getResources("META-INF/services/io.sm8.sdk.Plugin")
    import scala.jdk.CollectionConverters._
    urls.asScala.foreach { u =>
      scala.io.Source.fromURL(u).getLines().foreach(acc += _)
    }
    acc.toSet should contain (classOf[CachePlugin].getName)
  }
}
