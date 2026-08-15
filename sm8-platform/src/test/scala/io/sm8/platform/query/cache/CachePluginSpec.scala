/*
 * SM8 Platform — CachePlugin integration test.
 *
 * Per the cache-as-hook extraction PR: these tests PROVE the
 * cache is invoked as a PreExecute + PostExecute hook pair, not
 * inline in the executor. RFC §13 DoD observability.
 *
 * Per [[debug-mantra-mindset]] §3 (falsify hypothesis): the HIT
 * test was originally broken because the test primed the cache
 * with a hand-crafted string. The right shape is a TWO-CALL
 * sequence — the first MISS writes the cache (via the
 * PostExecute hook), the second HIT reads it (via the PreExecute
 * hook, short-circuiting the executor). That exercises the real
 * plugin behavior without guessing the SHA-256 key shape.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: hooks are
 * `with java.io.Serializable`. Test fixtures do not close over
 * SparkSession / Iterator / Connection.
 */
package io.sm8.platform.query.cache

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.engine.{
  EngineContext,
  EngineError,
  EngineIdentity,
  MCPEngineProvider,
  MCPEngineRegistry,
  MCPQueryRequest,
  PortableQueryResult,
  ResultRow,
  ResultSchema,
  ResultValue
}
import io.sm8.core.model.{
  Dimension,
  Measure,
  Model,
  ModelStatus,
  SourceRef
}
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.platform.query.hooks.EngineHookDispatcher
import io.sm8.sdk.Plugin
import io.sm8.platform.query.{EngineService, QueryRequest}

private final class FakeProvider(
    val name: String,
    val stubPqr: PortableQueryResult
) extends MCPEngineProvider {

  override val identity: EngineIdentity =
    EngineIdentity(name = name, nativeVersion = "test", engineAdapterVersion = "1.0")

  val callCount: AtomicInteger = new AtomicInteger(0)

  override def available: Boolean = true

  override def query(
      model: Model,
      mcpReq: MCPQueryRequest,
      ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] = {
    callCount.incrementAndGet()
    Right(stubPqr)
  }

  override def explain(
      model: Model,
      mcpReq: MCPQueryRequest,
      ctx: EngineContext
  ): Either[EngineError, String] = Right("fake")
}

private final class CountingCacheDelegate(
    underlying: InMemoryResultCache,
    getCounter: AtomicInteger,
    putCounter: AtomicInteger
) extends ResultCache {

  override def getJournaled(key: String): Option[io.sm8.platform.query.cache.RestateCachedRow] = {
    getCounter.incrementAndGet()
    underlying.getJournaled(key)
  }

  override def putJournaledWithModelAndVersion(
      key: String,
      value: io.sm8.platform.query.cache.RestateCachedRow,
      model: String,
      version: Int
  ): Unit = {
    putCounter.incrementAndGet()
    underlying.putJournaledWithModelAndVersion(key, value, model, version)
  }
}

class CachePluginSpec extends AnyFunSuite with Matchers {

  private def stubPqr: PortableQueryResult = PortableQueryResult(
    schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int))),
    rows   = Vector(ResultRow(
      values = List(ResultValue.IntV(42L)),
      schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int)))
    ))
  )

  private def sampleModel: Model =
    Model.of(
      name       = "m",
      version    = 1,
      source     = SourceRef.ByName("n", "t"),
      status     = ModelStatus.Draft,
      dimensions = List(Dimension("d", "d")),
      measures   = List(Measure("v", "v"))
    ).toOption.get

  private def registryWith(p: FakeProvider): MCPEngineRegistry = {
    val engines: Map[String, MCPEngineProvider] = Map(p.name -> p)
    MCPEngineRegistry(engines, p.name)
  }

  private def engineWith(plugin: Plugin): EngineHookDispatcher = {
    val engineImpl = new io.sm8.core.EngineImpl
    plugin.setup(engineImpl)
    EngineHookDispatcher(engineImpl.hooks)
  }

  private val sampleRequest: QueryRequest =
    QueryRequest("m", Nil, Nil, "", "")

  test("CachePlugin: HIT short-circuits engine on second call (canonical 2-call sequence)") {
    val provider = new FakeProvider("test-engine", stubPqr)
    val cache    = new InMemoryResultCache(maxEntries = 16)

    val dispatcher = engineWith(new CachePlugin(cache))

    // Call 1 — MISS path. Plugin reads (None) → engine runs → cache write.
    val first = EngineService.runQueryWithHooks(
      request    = sampleRequest,
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    assert(first.isRight, s"first call should be Right: ${first.swap.toOption.map(_.toString).getOrElse("")}")

    // Call 2 — HIT path. Same engine instance, same dispatcher, same
    // underlying cache. Plugin reads → Some(row) → engine skipped.
    val second = EngineService.runQueryWithHooks(
      request    = sampleRequest,
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    assert(second.isRight, s"second call should be Right: ${second.swap.toOption.map(_.toString).getOrElse("")}")

    // Engine count is the key assertion: first call ran it (count = 1),
    // second call did NOT (still 1, not 2). The PostExecute hook
    // registered the row between calls 1 and 2.
    provider.callCount.get() shouldBe 1
  }

  test("CachePlugin: MISS fires engine + writes back via PostExecute (counted)") {
    val getCalls = new AtomicInteger(0)
    val putCalls = new AtomicInteger(0)
    val provider = new FakeProvider("test-engine", stubPqr)
    val cache    = new CountingCacheDelegate(
      underlying = new InMemoryResultCache(maxEntries = 16),
      getCounter = getCalls,
      putCounter = putCalls
    )

    val dispatcher = engineWith(new CachePlugin(cache))
    val result = EngineService.runQueryWithHooks(
      request    = sampleRequest,
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    result.isRight shouldBe true
    getCalls.get() shouldBe 1
    putCalls.get() shouldBe 1
    provider.callCount.get() shouldBe 1
  }
}
