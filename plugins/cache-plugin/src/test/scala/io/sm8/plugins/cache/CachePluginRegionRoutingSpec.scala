/*
 * SM8 cache Plugin — region-namespaced cache-key regression test.
 *
 * Asserts that `CachePolicy.ReadThrough(name)` / `CachePolicy.WriteThrough(name)`
 * route cache reads and writes to region-scoped keys, so two regions
 * with identical query shapes never share an entry. The composite key
 * is length-prefixed so `region="a::b" + key="x"` cannot collide with
 * `region="a" + key="b::x"`.
 */
package io.sm8.plugins.cache

import io.sm8.core.cache._
import io.sm8.core.engine.{EngineHookRequest, EngineHookResult, QueryRequest}
import io.sm8.core.model.CachePolicy
import io.sm8.sdk.{Context, PipelineStage}

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CachePluginRegionRoutingSpec extends AnyFunSuite with Matchers {

  /** Recording cache: captures every put (so tests can assert the
    * namespaced key) and serves a get only when the exact namespaced
    * key was written. Mirrors `InMemoryResultCache` semantics with
    * observable write/read logs. */
  private final class RecordingCache extends ResultCache {
    val puts = scala.collection.mutable.ListBuffer.empty[String]
    private val stored = scala.collection.mutable.Map.empty[String, RestateCachedRow]
    override def getJournaled(key: String): Option[RestateCachedRow] =
      stored.get(key)
    override def putJournaledWithModelAndVersion(
        key: String, value: RestateCachedRow, model: String, version: Int
    ): Unit = {
      puts += key
      stored(key) = value
    }
  }

  private def model(): io.sm8.core.model.Model = io.sm8.core.model.Model.of(
    name    = "region-routing",
    version = 1,
    description = None,
    dimensions  = Nil,
    measures    = Nil,
    defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
      materialize = io.sm8.core.model.MaterializePolicy.None,
      cache       = CachePolicy.NoCache,
      audit       = io.sm8.core.model.AuditPolicy.NoAudit),
    source  = io.sm8.core.model.SourceRef.ByName(table = "t"),
    status  = io.sm8.core.model.ModelStatus.Draft,
    filters = Nil,
    calculatedMeasures = Nil,
    joins   = Nil
  ).toOption.get

  private def pqr(): io.sm8.core.engine.PortableQueryResult =
    io.sm8.core.engine.PortableQueryResult(
      schema   = io.sm8.core.engine.ResultSchema(List(
        io.sm8.core.schema.Field(
          name     = "a",
          dataType = io.sm8.core.schema.SealedDataType.Varchar,
          nullable = false))),
      rows     = Vector(io.sm8.core.engine.ResultRow(
        values = List(io.sm8.core.engine.ResultValue.StringV("v")),
        schema = io.sm8.core.engine.ResultSchema(Nil))),
      metadata = Map.empty)

  test("regionKey returns the original key when region is empty or null") {
    CachePlugin.regionKey("",   "abc") shouldBe "abc"
    CachePlugin.regionKey(null, "abc") shouldBe "abc"
  }

  test("regionKey namespaces a non-empty region with a length-prefixed delimiter") {
    CachePlugin.regionKey("users",    "abc") shouldBe "00000005:users:abc"
    CachePlugin.regionKey("orders",   "abc") shouldBe "00000006:orders:abc"
    CachePlugin.regionKey("region-a", "x")   shouldBe "00000008:region-a:x"
  }

  test("regionKey uses length-prefixed encoding so composite keys are unambiguous") {
    val composite1 = CachePlugin.regionKey("a::b", "x")
    val composite2 = CachePlugin.regionKey("a",    "b::x")
    composite1 should not equal composite2
    composite1 shouldBe "00000004:a::b:x"
    composite2 shouldBe "00000001:a:b::x"
  }

  test("ReadThrough under one region does not see entries written under another region") {
    val cache   = new RecordingCache
    val reads   = new AtomicInteger(0)
    val hits    = new AtomicInteger(0)
    val misses  = new AtomicInteger(0)
    val pre     = new CacheReadPreHook(cache, reads, hits, misses)
    val writes  = new AtomicInteger(0)
    val post    = new CacheWritePostHook(cache, writes)
    val md      = model()
    val key     = "shared-query-key"

    val writeCtx = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = Some(EngineHookResult(pqr())),
      meta    = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("region-a")),
      stop    = false)
    post.run(writeCtx)
    cache.puts.toList shouldBe List("00000008:region-a:" + key)

    val readCtxB = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = None,
      meta    = Map("sm8.cache.policy" -> CachePolicy.ReadThrough("region-b")),
      stop    = false)
    val outB = pre.run(readCtxB)
    misses.get() shouldBe 1
    outB.stop    shouldBe false
    outB.result  shouldBe None

    val readCtxA = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = None,
      meta    = Map("sm8.cache.policy" -> CachePolicy.ReadThrough("region-a")),
      stop    = false)
    val outA = pre.run(readCtxA)
    hits.get() shouldBe 1
    outA.stop   shouldBe true
    outA.result shouldBe defined
  }

  test("WriteThrough writes under the region-namespaced key, not the raw key") {
    val cache  = new RecordingCache
    val writes = new AtomicInteger(0)
    val post   = new CacheWritePostHook(cache, writes)
    val md     = model()

    val ctx = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = "k"),
      result  = Some(EngineHookResult(pqr())),
      meta    = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("prod")),
      stop    = false)
    post.run(ctx)
    cache.puts.toList shouldBe List("00000004:prod:k")
  }

  test("WriteThrough pre-hook reads under the region-namespaced key on HIT and MISS") {
    val cache  = new RecordingCache
    val reads  = new AtomicInteger(0)
    val hits   = new AtomicInteger(0)
    val misses = new AtomicInteger(0)
    val pre    = new CacheReadPreHook(cache, reads, hits, misses)
    val md     = model()
    val key    = "wt-key"

    val ctx = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = None,
      meta    = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("wt-region")),
      stop    = false)
    val miss = pre.run(ctx)
    misses.get() shouldBe 1
    miss.stop   shouldBe false

    val writes = new AtomicInteger(0)
    val post   = new CacheWritePostHook(cache, writes)
    val writeCtx = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = Some(EngineHookResult(pqr())),
      meta    = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("wt-region")),
      stop    = false)
    post.run(writeCtx)

    val hit = pre.run(ctx.copy(
      meta = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("wt-region"))))
    hits.get() shouldBe 1
    hit.stop   shouldBe true
  }

  test("empty-region ReadThrough and WriteThrough consult the raw cache key (legacy default)") {
    val cache  = new RecordingCache
    val reads  = new AtomicInteger(0)
    val hits   = new AtomicInteger(0)
    val misses = new AtomicInteger(0)
    val pre    = new CacheReadPreHook(cache, reads, hits, misses)
    val writes = new AtomicInteger(0)
    val post   = new CacheWritePostHook(cache, writes)
    val md     = model()
    val key    = "raw-key"

    val writeCtx = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = Some(EngineHookResult(pqr())),
      meta    = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("")),
      stop    = false)
    post.run(writeCtx)
    cache.puts.toList shouldBe List(key)

    val readCtx = Context(
      stage   = PipelineStage.Execute,
      request = EngineHookRequest(model = md, mcpRequest = QueryRequest.empty, cacheKey = key),
      result  = None,
      meta    = Map("sm8.cache.policy" -> CachePolicy.ReadThrough("")),
      stop    = false)
    val out = pre.run(readCtx)
    hits.get() shouldBe 1
    out.stop   shouldBe true
  }
}