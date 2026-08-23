/*
 * SM8 Platform — MetaInspectorServiceE2ESpec (PR-158, data-eng
 * WARN-2 from 3rd-pass cumulative-session-review).
 *
 * Verifies the wire-value round-trip for `MetaInspectorService`:
 *  1. An in-memory snapshot is constructed (mirroring the
 *     shape the plugin's `GraphSnapshot` writes to
 *     `context.meta`).
 *  2. The `MetaInspectorService.getMeta` handler is invoked
 *     with `MetaRequest(GraphSnapshot.MetaKey)`.
 *  3. The handler returns `MetaResponse(present = true,
 *     value = Some(snapshot.toMetaValue))`.
 *  4. The wire `value` map matches the in-memory snapshot's
 *     projection.
 *
 * Per data-eng WARN-2: `CliIntegrationSpec` tests `inspect`
 * against a mock HTTP server. This spec wires a real
 * `MetaInspectorService.definition` (not a mock) and asserts
 * the wire value matches the in-memory snapshot.
 */
package io.sm8.platform.query

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import dev.restate.sdk.endpoint.definition.HandlerContext
import dev.restate.serde.jackson.JacksonSerdeFactory

import io.sm8.core.engine.EngineRegistry
import io.sm8.core.model._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

class MetaInspectorServiceE2ESpec extends AnyFunSuite with Matchers {

  // Stub snapshot shape (mirrors the plugin's GraphSnapshot;
  // see plugins/semantic-graph-plugin/.../GraphSnapshot.scala).
  private final case class StubGraphNode(model: String, field: String)
  private final case class StubGraphSnapshot(
      vertices: List[StubGraphNode],
      edges: List[(StubGraphNode, StubGraphNode)],
      hasCycle: Boolean,
      cycleError: Option[String],
      danglingRightNodes: List[StubGraphNode]
  ) {
    def toMetaValue: Map[String, Any] = Map(
      "vertices" -> vertices.map(n => Map("model" -> n.model, "field" -> n.field)),
      "edges" -> edges.map { case (from, to) =>
        Map(
          "from" -> Map("model" -> from.model, "field" -> from.field),
          "to"   -> Map("model" -> to.model,   "field" -> to.field)
        )
      },
      "hasCycle" -> hasCycle,
      "cycleError" -> cycleError,
      "danglingRightNodes" -> danglingRightNodes.map(n =>
        Map("model" -> n.model, "field" -> n.field)
      )
    )
  }

  private val stubModel: Model = Model
    .of(
      name = "e2e_stub",
      version = 1,
      description = None,
      dimensions = List.empty,
      measures = List.empty,
      defaultPolicies = ModelPolicyDefaults(
        MaterializePolicy.None,
        CachePolicy.NoCache,
        AuditPolicy.NoAudit
      ),
      source = SourceRef.byName("in-memory", "x"),
      status = ModelStatus.Published,
      filters = List.empty,
      calculatedMeasures = List.empty,
      joins = List.empty
    )
    .toOption
    .get

  // EngineRegistry requires the default to be in the engines map;
  // we never invoke the provider, so a `null` is safe.
  // EngineRegistry requires the default to be in the engines map
  // AND the provider to be non-null + have `available: Boolean`.
  // We use a minimal in-test stub.
  private final class StubEngineProvider extends io.sm8.core.engine.EngineProvider {
    override val identity: io.sm8.core.engine.EngineIdentity =
      io.sm8.core.engine.EngineIdentity("default", "0.0.0", "0.0.0")
    override val available: Boolean = true
    override def query(model: io.sm8.core.model.Model, request: io.sm8.core.engine.QueryRequest, ctx: io.sm8.core.engine.EngineContext): io.sm8.core.engine.EngineError Either io.sm8.core.engine.PortableQueryResult = ???
    override def explain(model: io.sm8.core.model.Model, request: io.sm8.core.engine.QueryRequest, ctx: io.sm8.core.engine.EngineContext): io.sm8.core.engine.EngineError Either String = ???
    override def close(): Unit = ()
  }

  private val stubRegistry: EngineRegistry = EngineRegistry(
    Map("default" -> new StubEngineProvider),
    "default"
  )

  private val snapshot = StubGraphSnapshot(
    vertices = List(
      StubGraphNode(model = "e2e_stub", field = "amount"),
      StubGraphNode(model = "e2e_stub", field = "total")
    ),
    edges = List(
      (StubGraphNode("e2e_stub", "amount"), StubGraphNode("e2e_stub", "total"))
    ),
    hasCycle = false,
    cycleError = None,
    danglingRightNodes = List.empty
  )

  private val mapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)
  private val jacksonSerdeFactory: JacksonSerdeFactory =
    new JacksonSerdeFactory(mapper)
  private val requestSerde = jacksonSerdeFactory.create(classOf[MetaRequest])
  private val resultSerde  = jacksonSerdeFactory.create(classOf[MetaResponse])

  /** Hand-rolled HandlerContext stub (mirrors QueryServiceSpec). */
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

  test("end-to-end: MetaInspectorService.getMeta returns the snapshot's wire projection") {
    // The handler expects `meta.get(key)` to return a `Map[String, Any]`
    // (the wire projection). The plugin's `GraphPostResolveObserver`
    // writes the `GraphSnapshot` instance directly to `context.meta`;
    // for the test we pre-project via `toMetaValue` to mirror the
    // production wire shape (the plugin's observer is tested
    // separately in `SemanticGraphBuilderSpec`).
    val stubMetaKey = "io.sm8.plugins.semanticgraph:graph-snapshot"
    val engineFn = () => Map(stubMetaKey -> snapshot.toMetaValue)
    val serviceDef = MetaInspectorService.definition(
      model = stubModel,
      registry = stubRegistry,
      engineFn = engineFn
    )
    val getMetaHandler = serviceDef
      .getHandlers
      .asScala
      .find(_.getName == "getMeta")
      .getOrElse(fail("getMeta handler not found"))
    val runner = getMetaHandler
      .getRunner
      .asInstanceOf[dev.restate.sdk.HandlerRunner[MetaRequest, MetaResponse]]

    val requestSlice = requestSerde.serialize(MetaRequest(key = stubMetaKey))
    val responseSlice = runner
      .run(stubContext(requestSlice), requestSerde, resultSerde, new AtomicReference[Runnable]())
      .get()
    val resp = resultSerde.deserialize(responseSlice).asInstanceOf[MetaResponse]
    resp.key shouldBe stubMetaKey
    resp.present shouldBe true
    resp.value shouldBe defined
    val wireMap = resp.value.get
    // The JSON wire round-trips `Option[T]` as either the T value
    // or `null` when absent (per the Jackson DefaultScalaModule
    // convention). We accept both `null` and `None` as "absent".
    // Jackson deserializes a missing JSON key as `null`, not `None`.
    val cycleError = wireMap("cycleError")
    (cycleError == null || cycleError == None) shouldBe true
    wireMap("danglingRightNodes").asInstanceOf[List[Map[String, Any]]] shouldBe empty
    val vertices = wireMap("vertices").asInstanceOf[List[Map[String, Any]]]
    vertices should have size 2
    vertices.exists(v => v("model") == "e2e_stub" && v("field") == "amount") shouldBe true
    vertices.exists(v => v("model") == "e2e_stub" && v("field") == "total") shouldBe true

    val edges = wireMap("edges").asInstanceOf[List[Map[String, Any]]]
    edges should have size 1
    val edge = edges.head
    val from = edge("from").asInstanceOf[Map[String, Any]]
    val to   = edge("to").asInstanceOf[Map[String, Any]]
    from("field") shouldBe "amount"
    to("field") shouldBe "total"
  }

  test("end-to-end: getMeta returns present=false for an absent key") {
    val engineFn = () => Map.empty[String, Any]
    val serviceDef = MetaInspectorService.definition(
      model = stubModel,
      registry = stubRegistry,
      engineFn = engineFn
    )
    val getMetaHandler = serviceDef
      .getHandlers
      .asScala
      .find(_.getName == "getMeta")
      .getOrElse(fail("getMeta handler not found"))
    val runner = getMetaHandler
      .getRunner
      .asInstanceOf[dev.restate.sdk.HandlerRunner[MetaRequest, MetaResponse]]

    val requestSlice = requestSerde.serialize(MetaRequest(key = "absent.key"))
    val responseSlice = runner
      .run(stubContext(requestSlice), requestSerde, resultSerde, new AtomicReference[Runnable]())
      .get()
    val resp = resultSerde.deserialize(responseSlice).asInstanceOf[MetaResponse]

    resp.key shouldBe "absent.key"
    resp.present shouldBe false
    resp.value shouldBe None
  }
}
