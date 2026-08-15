/*
 * SM8 Platform — EngineService.runQueryWithHooksSpec.
 *
 * Per the cache-as-hook extraction PR #35 + the RFC §13 architectural
 * audit: the hook-aware engine-portable entry-point
 * (`EngineService.runQueryWithHooks`) is the canonical path. These
 * tests give it DIRECT unit coverage without going through the
 * Restate HandlerRunner handler (which is what `QueryServiceSpec`
 * covers transitively).
 *
 * Per [[debug-mantra-mindset]] §5 (verify): these tests prove
 * the no-inline-cache claim at the function level. The 9 legacy
 * `runQuery` tests in `EngineServiceSpec` were migrated to
 * `runQueryWithHooks` as part of the same change; this file
 * retains the DIRECT coverage that the legacy test file had
 * (now via the hook path).
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: hooks
 * themselves are `with java.io.Serializable`; the test fixtures
 * do not close over SparkSession / Iterator / Connection.
 */
package io.sm8.platform.query

import io.sm8.core.cache._
import io.sm8.platform.query.cache._
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
  AuditPolicy,
  CachePolicy,
  MaterializePolicy,
  Model,
  ModelPolicyDefaults,
  ModelStatus,
  SourceRef
}
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.platform.query.cache.CachePlugin
import io.sm8.platform.query.hooks.EngineHookDispatcher

class EngineServiceRunQueryWithHooksSpec extends AnyFunSuite with Matchers {

  // -- Test fixtures (mirroring EngineServiceSpec shape) --

  private val dummyModel: Model = Model(
    name = "m",
    version = 1,
    description = None,
    dimensions = Nil,
    measures = Nil,
    defaultPolicies = ModelPolicyDefaults(
      MaterializePolicy.None,
      CachePolicy.NoCache,
      AuditPolicy.NoAudit),
    source = SourceRef.ByName("m", "t"),
    status = ModelStatus.Draft,
    filters = Nil
  )

  // A well-formed PQR: 1 field + 1 row with 1 matching cell. The cache
  // wire format (RestateCachedRow) requires row.length == fieldNames.size
  // (per the require() in RestateCachedRow.<init>). A mismatched PQR
  // throws on the write-through path even when the engine result is correct.
  private val wiringPortable: PortableQueryResult = PortableQueryResult(
    schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int))),
    rows     = Vector(ResultRow(
      values = List(ResultValue.IntV(42L)),
      schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int)))
    ))
  )

  private final class StubProvider(
      override val identity: EngineIdentity,
      override val available: Boolean,
      var queryResult: Either[EngineError, PortableQueryResult] =
        Right(wiringPortable),
      var queryThrowable: RuntimeException = null,
      val callCount: AtomicInteger = new AtomicInteger(0)
  ) extends MCPEngineProvider {

    override def query(
        model: Model,
        request: MCPQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = {
      callCount.incrementAndGet()
      if (queryThrowable != null) throw queryThrowable
      queryResult
    }

    override def explain(
        model: Model,
        request: MCPQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, String] = Right("fake")
  }

  private def makeRegistry(
      providers: Map[String, MCPEngineProvider],
      default: String = "test-engine"
  ): MCPEngineRegistry = MCPEngineRegistry(providers, default)

  /**
   * Build a dispatcher with the cache plugin registered. This
   * mirrors the production wiring (per the QueryServiceSpec
   * sibling-fix in PR #35).
   */
  private def hookDispatcherWith(cache: ResultCache): EngineHookDispatcher = {
    val engineImpl = new io.sm8.core.EngineImpl
    new CachePlugin(cache).setup(engineImpl)
    EngineHookDispatcher(engineImpl.hooks)
  }

  // -- Tests --

  test("runQueryWithHooks: success path returns Right(QueryResult)") {
    val spark = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry   = makeRegistry(Map("test-engine" -> spark))
    val dispatcher = hookDispatcherWith(ResultCache.NoOp)

    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    out.isRight shouldBe true
    spark.callCount.get() shouldBe 1
  }

  test("runQueryWithHooks: cache HIT short-circuits engine via PreExecute hook") {
    val spark = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry = makeRegistry(Map("test-engine" -> spark))
    val cache    = new io.sm8.platform.query.cache.InMemoryResultCache(maxEntries = 16)
    val dispatcher = hookDispatcherWith(cache)

    // First call: MISS path. Engine runs, cache writes.
    val first = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    first.isRight shouldBe true
    spark.callCount.get() shouldBe 1

    // Second call: HIT path. PreExecute hook sets stop=true; engine skipped.
    val second = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    second.isRight shouldBe true
    spark.callCount.get() shouldBe 1   // unchanged — engine skipped
  }

  test("runQueryWithHooks: engine unavailable → Left(EngineUnavailable)") {
    val spark = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry   = makeRegistry(Map("test-engine" -> spark), default = "test-engine")
    val dispatcher = hookDispatcherWith(ResultCache.NoOp)

    // Request asks for "missing" — registry's select returns Left.
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "missing"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.EngineUnavailable]
  }
}
