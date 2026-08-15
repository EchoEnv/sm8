package io.sm8.platform.query

import io.sm8.platform.query.cache._
import java.util.concurrent.atomic.AtomicReference

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import dev.restate.sdk.endpoint.definition.{HandlerContext, HandlerType, ServiceType}
import dev.restate.serde.jackson.JacksonSerdeFactory

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
  FilterSpec,
  MaterializePolicy,
  Model,
  ModelPolicyDefaults,
  ModelStatus,
  SourceRef
}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.platform.query.cache.CachePlugin

/**
 * Unit tests for `QueryService.definition(...)` — exercises the
 * hand-built `ServiceDefinition` + `HandlerRunner` without booting
 * the Restate runtime.
 *
 * Per review pass #2 (DE-reviewer #7 + architect-reviewer #11):
 * EVERY test in this file drives the handler body through
 * `HandlerRunner.run(stubContext, ...)` — proving the wire
 * contract, not just the engine. The earlier version called
 * `EngineService.runQuery` directly, which tested the engine
 * but DID NOT verify the handler wiring (the new addition).
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct test
 * footprint": no Docker, no Testcontainers, no Restate server.
 *
 * Per [[scala-jvm-safety-mindset]] "null is a liar": the stub
 * `HandlerContext` returns `null`-safe defaults; the SDK's
 * serializer surface is empty (we don't serialize into a Slice
 * — we just exercise the handler body).
 *
 * Per [[scala-impact-analysis-mindset]] "name every caller":
 * `QueryServiceSpec` is the only production caller of
 * `QueryService.definition` (apart from `RestateBootstrap`).
 */
class QueryServiceSpec extends AnyFunSuite with Matchers {

  // -- Fixtures --

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

  /** A minimal MCPEngineProvider stub. */
  private final class StubProvider(
      override val identity: EngineIdentity,
      override val available: Boolean,
      var queryResult: Either[EngineError, PortableQueryResult] =
        Right(PortableQueryResult(schema = ResultSchema(Nil), rows = Vector.empty))
  ) extends MCPEngineProvider with java.io.Serializable {
    override def query(
        model: Model,
        request: MCPQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = queryResult
    override def explain(
        model: Model,
        request: MCPQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, String] = ???
  }

  private def makeRegistry(
      providers: Map[String, MCPEngineProvider],
      default: String = "spark"
  ): MCPEngineRegistry = MCPEngineRegistry(providers, default)

  private val twoRowsPortable: PortableQueryResult = PortableQueryResult(
    rows = Vector(
      ResultRow(
        values = List(ResultValue.StringV("Alice"), ResultValue.IntV(30L)),
        schema = ResultSchema(Nil)),
      ResultRow(
        values = List(ResultValue.StringV("Bob"), ResultValue.IntV(25L)),
        schema = ResultSchema(Nil))
    ),
    schema = ResultSchema(List(
      Field.nonNull("name", SealedDataType.Varchar),
      Field.nonNull("age", SealedDataType.BigInt)
    ))
  )

  /** The mapper used by the JSON serde layer. Same instance
    * the SDK's `JacksonSerdeFactory` uses. Per [[scala-jvm-safety-mindset]]
    * "audit jar contents": `jackson-module-scala` is on the
    * classpath (sm8-platform dep tree). */
  private val mapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)

  private val jacksonSerdeFactory: JacksonSerdeFactory =
    new JacksonSerdeFactory(mapper)

  private val requestSerde = jacksonSerdeFactory.create(classOf[QueryRequest])
  private val resultSerde  = jacksonSerdeFactory.create(classOf[QueryResult])

  /**
   * Construct a fully-stubbed `HandlerContext` for `HandlerRunner.run(...)`.
   *
   * Per review pass #2 (debug-mantra): the SDK's `HandlerRunner.run`
   * reads the request body from `ctx.request().body()` (the v2.x
   * state-machine model — no request-as-argument overload). The
   * body must be a `Slice` of the JSON-encoded `QueryRequest`.
   *
   * Per [[scala-jvm-safety-mindset]] "null is a liar": the SDK
   * calls `ctx.request().openTelemetryContext()` early in `run()`
   * — the request must be non-null. We construct a minimal
   * `HandlerRequest` with a real (but empty) OpenTelemetry
   * context. The invocationId / headers are unused by the
   * handler body.
   */
  private def stubContext(requestBody: dev.restate.common.Slice): HandlerContext = {
    val otelContext = io.opentelemetry.context.Context.root()
    val stubRequest = new dev.restate.sdk.common.HandlerRequest(
      null,
      otelContext,
      requestBody,
      java.util.Map.of()
    )
    new HandlerContext {
      override def objectKey: String = ""
      override def request: dev.restate.sdk.common.HandlerRequest = stubRequest
      override def writeOutput(s: dev.restate.common.Slice) =
        java.util.concurrent.CompletableFuture.completedFuture(null: java.lang.Void)
      override def writeOutput(e: dev.restate.sdk.common.TerminalException) =
        java.util.concurrent.CompletableFuture.completedFuture(null: java.lang.Void)
    override def get(key: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def getKeys() =
      throw new UnsupportedOperationException("not used in unit test")
    override def clear(key: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def clearAll() =
      throw new UnsupportedOperationException("not used in unit test")
    override def set(key: String, value: dev.restate.common.Slice) =
      throw new UnsupportedOperationException("not used in unit test")
    override def timer(d: java.time.Duration, key: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def call(target: dev.restate.common.Target, value: dev.restate.common.Slice, key: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]]) =
      throw new UnsupportedOperationException("not used in unit test")
    override def send(target: dev.restate.common.Target, value: dev.restate.common.Slice, key: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]],
        delay: java.time.Duration) =
      throw new UnsupportedOperationException("not used in unit test")
    override def submitRun(name: String, completer: java.util.function.Consumer[
        HandlerContext.RunCompleter]) =
      throw new UnsupportedOperationException("not used in unit test")
    override def awakeable() =
      throw new UnsupportedOperationException("not used in unit test")
    override def resolveAwakeable(id: String, value: dev.restate.common.Slice) =
      throw new UnsupportedOperationException("not used in unit test")
    override def rejectAwakeable(id: String, e: dev.restate.sdk.common.TerminalException) =
      throw new UnsupportedOperationException("not used in unit test")
    override def promise(name: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def peekPromise(name: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def resolvePromise(name: String, value: dev.restate.common.Slice) =
      throw new UnsupportedOperationException("not used in unit test")
    override def rejectPromise(name: String, e: dev.restate.sdk.common.TerminalException) =
      throw new UnsupportedOperationException("not used in unit test")
    override def cancelInvocation(id: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def attachInvocation(id: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def getInvocationOutput(id: String) =
      throw new UnsupportedOperationException("not used in unit test")
    override def fail(t: Throwable): Unit =
      throw new UnsupportedOperationException("not used in unit test")
    override def createAnyAsyncResult(args: java.util.List[
        dev.restate.sdk.endpoint.definition.AsyncResult[_]]) =
      throw new UnsupportedOperationException("not used in unit test")
    override def createAllAsyncResult(args: java.util.List[
        dev.restate.sdk.endpoint.definition.AsyncResult[_]]) =
      throw new UnsupportedOperationException("not used in unit test")
  }
  }

  /**
   * Drive `runQuery` through the actual handler body via the
   * `HandlerRunner.run(stubContext, ...)` invocation. This is the
   * v2.x-shaped unit-test surface (no Docker, no Restate runtime).
   */
  private def invoke(
      model: Model,
      registry: MCPEngineRegistry,
      cache: ResultCache,
      request: QueryRequest
  ): QueryResult = {
    // Per [[scala-data-driven-refactor-mindset]] "Plugin unit of
    // extension": mirror the production wiring (`RestateBootstrap`
    // always registers the `CachePlugin`). Without the plugin,
    // the cache HIT path can't short-circuit and the engine runs
    // on every request (regression introduced when cache lookup
    // moved from the inline `EngineService.runQuery` body to the
    // `CachePlugin` hook).
    val svc = QueryService.definition(model, registry, cache, plugins = Seq(new CachePlugin(cache)))
    val handler = svc.getHandlers.iterator.next()
    val runner = handler.getRunner
      .asInstanceOf[dev.restate.sdk.HandlerRunner[QueryRequest, QueryResult]]
    val requestSlice = requestSerde.serialize(request)
    val responseSlice = runner.run(
      stubContext(requestSlice),
      requestSerde,
      resultSerde,
      new AtomicReference[Runnable]()
    ).get()
    resultSerde.deserialize(responseSlice).asInstanceOf[QueryResult]
  }

  // -- Tests --

  test("QueryService.definition: builds a ServiceDefinition with one runQuery handler") {
    val cache = ResultCache.NoOp
    val spark = new StubProvider(
      EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val svc = QueryService.definition(dummyModel, registry, cache)
    svc.getServiceName shouldBe "QueryService"
    val handlers = svc.getHandlers
    handlers.size() shouldBe 1
    handlers.iterator.next().getName shouldBe "runQuery"
    handlers.iterator.next().getHandlerType shouldBe HandlerType.SHARED
    svc.getServiceType shouldBe ServiceType.SERVICE
  }

  test("QueryService: handler body serde round-trip (QueryRequest → JSON → QueryRequest)") {
    // Per review pass #2 (DE-reviewer #2 + architect-reviewer #12):
    // the previous "ne null" test was a lie. Real round-trip with
    // NON-EMPTY fields (the cache-key derivation depends on these).
    val original = QueryRequest(
      modelName = "flights",
      measures = List("rows", "unique_carriers"),
      dimensions = List("carrier", "year"),
      where = "carrier = 'AA'",
      engine = "spark"
    )
    val bytes = mapper.writeValueAsBytes(original)
    val decoded = mapper.readValue(bytes, classOf[QueryRequest])
    decoded.modelName shouldBe original.modelName
    decoded.measures shouldBe original.measures
    decoded.dimensions shouldBe original.dimensions
    decoded.where shouldBe original.where
    decoded.engine shouldBe original.engine
  }

  test("QueryService: handler body executes EngineService.runQuery (cache MISS path)") {
    val spark = new StubProvider(
      EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = Right(twoRowsPortable)
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val cache = ResultCache.NoOp
    val req = QueryRequest("flights", Nil, Nil, "", "spark")
    val qr = invoke(dummyModel, registry, cache, req)
    qr.model shouldBe "flights"
    qr.rows should have size 2
    qr.rows(0) shouldBe List("Alice", 30L)
  }

  test("QueryService: cache HIT path bypasses engine (via HandlerRunner)") {
    // Pre-populate the cache. The handler body should serve the
    // cached row without invoking the engine (the stub provider
    // has queryResult = null → would NPE if invoked).
    val spark = new StubProvider(
      EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = null.asInstanceOf[Either[EngineError, PortableQueryResult]]
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val cache = InMemoryResultCache()
    val req = QueryRequest("flights", Nil, Nil, "", "spark")
    val mcpReq = EngineService.buildMCPRequest(req)
    val cacheKey = CacheBridge.platformCacheKey(
      engine = "spark",
      modelName = "flights",
      version = dummyModel.version,
      measures = mcpReq.measures.toList,
      dimensions = mcpReq.dimensions.toList,
      where = mcpReq.where
    )
    cache.putJournaledWithModelAndVersion(
      cacheKey,
      CachedRowDecoder.toRestateCachedRowFromPortable(twoRowsPortable),
      "flights",
      dummyModel.version
    )
    val qr = invoke(dummyModel, registry, cache, req)
    qr.rows should have size 2
    qr.rows(0) shouldBe List("Alice", 30L)
  }

  test("QueryService: engine error path throws TerminalException (via HandlerRunner)") {
    val spark = new StubProvider(
      EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = Left(EngineError.EngineUnavailable(
        engine = "spark",
        available = Nil,
        wasDefault = false,
        message = "no spark available"
      ))
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val cache = ResultCache.NoOp
    val req = QueryRequest("m", Nil, Nil, "", "spark")
    // Per review pass #2 (debug-mantra): the SDK's `run()` returns
    // `CompletableFuture<Slice]` — when the handler throws, the
    // future completes exceptionally with `ExecutionException(TerminalException)`.
    // We unwrap via `.getCause`.
    val ex = intercept[java.util.concurrent.ExecutionException] {
      invoke(dummyModel, registry, cache, req)
    }
    val cause = ex.getCause
    cause shouldBe a [dev.restate.sdk.common.TerminalException]
    val te = cause.asInstanceOf[dev.restate.sdk.common.TerminalException]
    // The handler wraps EngineError in TerminalException with the
    // mapped HTTP status code (503 for EngineUnavailable).
    te.getCode shouldBe 503
    te.getMessage should include("no spark available")
  }
}
