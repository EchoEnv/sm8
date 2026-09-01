/*
 * SM8 Platform — HttpTransportPluginWiringSpec.
 *
 * Acceptance tests for the P1-A1 / SM8-03 / A1 / E6 fix: the
 * production HTTP path previously wired ZERO plugins
 * (`HttpTransport.start` called `QueryService.definition(..., plugins =
 * Nil)`), so no plugin hook (cache, audit, row-cap, broadcast/skew
 * oracle, materialize) ever fired and the DecisionHints oracle was
 * dead; `MetaInspectorService` was never bound, so `sm8 inspect <key>`
 * 404'd.
 *
 * The fix threads `plugins` into `HttpTransport` (→ `QueryService.definition`
 * → the hook dispatcher) and binds `MetaInspectorService` on the same
 * endpoint when a `metaInspectorEngineFn` is supplied.
 *
 * Per [[debug-mantra-mindset]] §1 (reproduce): each test is a fast,
 * deterministic signal that the previously-dead path is now live. The
 * tests drive the REAL `Endpoint` built by `HttpTransport` (via the
 * package-private `endpoint` accessor) through the real Restate
 * `HandlerRunner` — no socket is bound, so they stay unit-level.
 *
 * Backward compat (scala-impact-analysis): existing 3-arg
 * `HttpTransport(model, registry, cache)` call sites are unchanged —
 * the new params default to `Nil` / `None`.
 */
package io.sm8.platform.query

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import dev.restate.sdk.endpoint.definition.HandlerContext
import dev.restate.serde.jackson.JacksonSerdeFactory

import io.sm8.core.cache.ResultCache
import io.sm8.core.engine.{
  EngineContext,
  EngineError,
  EngineIdentity,
  EngineProvider,
  EngineRegistry,
  PortableQueryResult,
  QueryRequest => CoreQueryRequest,
  ResultSchema
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
import io.sm8.sdk.{Context, Engine, HookStage, Plugin, PostHook}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

class HttpTransportPluginWiringSpec extends AnyFunSuite with Matchers {

  // -- Fixtures (mirror HttpTransportSpec) --

  private final class StubProvider extends EngineProvider {
    override val identity: EngineIdentity = EngineIdentity("test", "1.0", "0")
    override val available: Boolean = true
    override def explain(
        m: Model,
        r: CoreQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, String] = Right(s"test plan for ${m.name}")
    override def query(
        m: Model,
        r: CoreQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] =
      Right(PortableQueryResult(ResultSchema(Nil), Vector.empty, Map.empty))
  }

  private def makeModel: Model = Model(
    name    = "test-model",
    version = 1,
    description = None,
    dimensions = Nil,
    measures = Nil,
    defaultPolicies = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache = CachePolicy.NoCache,
      audit = AuditPolicy.NoAudit),
    source = SourceRef.ByName(table = "test_table"),
    status = ModelStatus.Draft,
    filters = Nil
  )

  private def makeRegistry: EngineRegistry =
    EngineRegistry(Map("test" -> new StubProvider), "test")

  /** A recorder plugin: its PostExecute hook increments `counter` on
    * every run. If the plugin is registered on the dispatcher, the
    * counter fires when a query flows through the handler. */
  private final class RecorderPlugin(counter: AtomicInteger)
      extends Plugin with java.io.Serializable {
    override def setup(engine: Engine): Unit = {
      engine.hooks.registerPostHook(
        HookStage.PostExecute,
        new PostHook with java.io.Serializable {
          override val name: String = "RecorderPostHook"
          override val stage: HookStage = HookStage.PostExecute
          override val priority: Int = 100
          override def run(context: Context): Context = {
            counter.incrementAndGet()
            context
          }
        },
        100
      )
    }
    override def closedOverVars: Seq[String] = Seq("counter")
  }

  private val mapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
  private val serdeFactory: JacksonSerdeFactory =
    new JacksonSerdeFactory(mapper)

  /** Hand-rolled HandlerContext stub (mirrors
    * MetaInspectorServiceE2ESpec / QueryServiceSpec). */
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

  private def queryServiceRunQueryHandler(
      transport: HttpTransport
  ): dev.restate.sdk.HandlerRunner[QueryRequest, QueryResult] = {
    val services = transport.endpoint.getServiceDefinitions.iterator().asScala.toList
    val queryService = services
      .find(_.getServiceName == "QueryService")
      .getOrElse(fail("QueryService not bound on endpoint"))
    val handler = queryService.getHandlers.asScala
      .find(_.getName == "runQuery")
      .getOrElse(fail("runQuery handler not found"))
    handler.getRunner
      .asInstanceOf[dev.restate.sdk.HandlerRunner[QueryRequest, QueryResult]]
  }

  // -- Acceptance: plugins are actually threaded into the dispatcher --

  test("plugins passed to HttpTransport have their PostExecute hook fire on runQuery") {
    val counter = new AtomicInteger(0)
    val transport = HttpTransport(
      makeModel,
      makeRegistry,
      ResultCache.NoOp,
      plugins = Seq(new RecorderPlugin(counter))
    )
    try {
      val runner = queryServiceRunQueryHandler(transport)
      val requestSerde = serdeFactory.create(classOf[QueryRequest])
      val resultSerde = serdeFactory.create(classOf[QueryResult])
      val request = QueryRequest(
        modelName  = "test-model",
        measures   = Nil,
        dimensions = Nil,
        where      = "",
        engine     = ""
      )
      val responseSlice = runner
        .run(stubContext(requestSerde.serialize(request)), requestSerde, resultSerde,
          new AtomicReference[Runnable]())
        .get()
      val result = resultSerde.deserialize(responseSlice).asInstanceOf[QueryResult]
      result.model shouldBe "test-model"
      // The recorder PostExecute hook fired exactly once => the plugin
      // reached the dispatcher (was NOT silently dropped by plugins=Nil).
      counter.get() shouldBe 1
    } finally transport.stop()
  }

  test("HttpTransport with no plugins leaves its dispatcher with no recorder hook fired") {
    val counter = new AtomicInteger(0)
    val transport = HttpTransport(
      makeModel,
      makeRegistry,
      ResultCache.NoOp,
      plugins = Nil
    )
    try {
      // Build the endpoint (forces dispatcher construction) — no hook
      // is registered, so a run must NOT touch the recorder counter.
      val runner = queryServiceRunQueryHandler(transport)
      val requestSerde = serdeFactory.create(classOf[QueryRequest])
      val resultSerde = serdeFactory.create(classOf[QueryResult])
      val request = QueryRequest(
        modelName  = "test-model",
        measures   = Nil,
        dimensions = Nil,
        where      = "",
        engine     = ""
      )
      runner
        .run(stubContext(requestSerde.serialize(request)), requestSerde, resultSerde,
          new AtomicReference[Runnable]())
        .get()
      counter.get() shouldBe 0
    } finally transport.stop()
  }

  // -- Acceptance: MetaInspectorService is bound on the same endpoint --

  test("metaInspectorEngineFn binds MetaInspectorService on the endpoint; None does not") {
    val withInspector = HttpTransport(
      makeModel,
      makeRegistry,
      ResultCache.NoOp,
      Nil,
      Some(() => Map("k" -> Map("a" -> 1)))
    )
    val withoutInspector = HttpTransport(makeModel, makeRegistry, ResultCache.NoOp)
    try {
      val withNames = withInspector.endpoint.getServiceDefinitions.iterator()
        .asScala.map(_.getServiceName).toList
      withNames should contain ("QueryService")
      withNames should contain ("MetaInspectorService")

      val withoutNames = withoutInspector.endpoint.getServiceDefinitions.iterator()
        .asScala.map(_.getServiceName).toList
      // Per [[ADR-012-a]] + [[ADR-012-b]]: QueryService + ModelService
      // + MetricsService are always bound (read-only DTOs of the
      // loaded model and placeholder counters). MetaInspectorService is
      // bound only when metaInspectorEngineFn is Some(_).
      // The exact ordering is an implementation detail of the
      // Restate SDK's `Endpoint.getServiceDefinitions` (alphabetical
      // in 2.x); we assert by Set-equality below and check length,
      // not order, to stay robust against SDK iteration-order changes.
      withoutNames.toSet shouldBe Set("QueryService", "ModelService", "MetricsService")
      withoutNames.size shouldBe 3
    } finally {
      withInspector.stop()
      withoutInspector.stop()
    }
  }

  test("getMeta on the transport-bound endpoint serves the metaInspectorEngineFn map") {
    val key = "io.sm8.plugins.semanticgraph:graph-snapshot"
    val transport = HttpTransport(
      makeModel,
      makeRegistry,
      ResultCache.NoOp,
      Nil,
      Some(() => Map(key -> Map("hasCycle" -> false)))
    )
    try {
      val services = transport.endpoint.getServiceDefinitions.iterator().asScala.toList
      val inspector = services
        .find(_.getServiceName == "MetaInspectorService")
        .getOrElse(fail("MetaInspectorService not bound"))
      val handler = inspector.getHandlers.asScala
        .find(_.getName == "getMeta")
        .getOrElse(fail("getMeta handler not found"))
      val runner = handler.getRunner
        .asInstanceOf[dev.restate.sdk.HandlerRunner[MetaRequest, MetaResponse]]
      val requestSerde = serdeFactory.create(classOf[MetaRequest])
      val resultSerde = serdeFactory.create(classOf[MetaResponse])
      val responseSlice = runner
        .run(stubContext(requestSerde.serialize(MetaRequest(key = key))),
          requestSerde, resultSerde, new AtomicReference[Runnable]())
        .get()
      val resp = resultSerde.deserialize(responseSlice).asInstanceOf[MetaResponse]
      resp.key shouldBe key
      resp.present shouldBe true
      resp.value.get shouldBe Map("hasCycle" -> false)
    } finally transport.stop()
  }
}