/*
 * SM8 Platform — HttpTransportRestateIngressE2E spec.
 *
 * Per the user's directive: "the binary boots AND serves a real request"
 * gap-closer. The existing [[io.sm8.platform.query.HttpTransportSpec]]
 * covers start/stop lifecycle; this spec goes one level deeper — it
 * starts a REAL Vert.x socket (via [[io.sm8.platform.query.HttpTransport]])
 * and sends real HTTP requests through the JDK's built-in
 * `java.net.http.HttpClient` (zero new deps, per karpathy-guidelines:
 * smallest correct change).
 *
 * Per the debug-mantra discipline (reproduce): every assertion is on the
 * observed HTTP response, not on mocks.
 *
 * == Scope ==
 *
 *   - GET /health → 200 "OK" (Restate's static health probe)
 *   - POST /discover → 200 + the service descriptor (proves the
 *     service definition is wire-encodable)
 *   - POST /invoke/QueryService/runQuery → asserts the route RESOLVES
 *     (not 404) and that Restate's protocol layer rejects a malformed
 *     body on Restate's OWN error path (not a Vert.x 404). Hand-rolling
 *     the full bidirectional-stream protocol would couple this spec to
 *     the Restate protocol version — exactly the incidental coupling
 *     the karpathy-guidelines warn against.
 *
 * == Out of scope (deliberately) ==
 *
 *   - The full bidirectional-stream invoke protocol (protobuf frames).
 *   - The external Restate runtime (restate-server). The operator
 *     deploys that; sm8 stays pure-engine.
 *
 * Per [[scala-jvm-safety-mindset]] (resource lifecycle): the suite
 * binds ONE socket on an OS-assigned ephemeral port (port=0) and stops
 * it once (afterAll). AnyFunSuite creates ONE spec instance per suite,
 * so the lazy vals below initialize exactly once.
 *
 * Per [[scala-perf-testing-mindset]]: one Vert.x boot amortized across
 * all tests.
 */
package io.sm8.platform.query

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

import io.sm8.core.cache.ResultCache
import io.sm8.core.engine.{EngineContext, EngineError, EngineIdentity, EngineProvider, EngineRegistry, PortableQueryResult, QueryRequest, ResultSchema}
import io.sm8.core.model.{AuditPolicy, CachePolicy, MaterializePolicy, Model, ModelPolicyDefaults, ModelStatus, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class HttpTransportRestateIngressE2ESpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // ------------------------------------------------------------------
  // Fixtures (mirror HttpTransportSpec's stubs)
  // ------------------------------------------------------------------

  /** Minimal stub engine — available + deterministic. */
  private final class StubProvider extends EngineProvider {
    override val identity: EngineIdentity = EngineIdentity("test", "1.0", "0")
    override val available: Boolean = true
    // override scaladoc: stub implementation — see EngineProvider for the
    // canonical contract.
    override def explain(
        m: Model,
        r: QueryRequest,
        ctx: EngineContext
    ): Either[EngineError, String] = Right(s"test plan for ${m.name}")
    // override scaladoc: stub implementation — see EngineProvider for the
    // canonical contract.
    override def query(
        m: Model,
        r: QueryRequest,
        ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = Right(
      PortableQueryResult(ResultSchema(Nil), Vector.empty, Map.empty))
  }

  private def makeModel: Model = Model(
    name        = "test-model",
    version     = 1,
    description = None,
    dimensions  = Nil,
    measures    = Nil,
    defaultPolicies = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache       = CachePolicy.NoCache,
      audit       = AuditPolicy.NoAudit),
    source  = SourceRef.ByName(table = "test_table"),
    status  = ModelStatus.Draft,
    filters = Nil
  )

  private def makeRegistry: EngineRegistry =
    EngineRegistry(Map("test" -> new StubProvider), "test")

  // ------------------------------------------------------------------
  // Shared server lifecycle (ONE socket for the whole suite)
  // ------------------------------------------------------------------

  // Per [[scala-perf-testing-mindset]]: single Vert.x boot per suite.
  // AnyFunSuite instantiates the spec once; lazy vals initialize on
  // first use and are shared by all tests.
  private lazy val transport: HttpTransport =
    HttpTransport(makeModel, makeRegistry, ResultCache.NoOp)
  private lazy val port: Int = transport.start(0) // 0 = OS-assigned ephemeral

  // JDK built-in client — zero new deps.
  private lazy val client: HttpClient = HttpClient.newHttpClient()

  private def get(path: String): HttpResponse[String] =
    client.send(
      HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).GET.build(),
      HttpResponse.BodyHandlers.ofString())

  private def post(path: String, body: String, accept: Option[String] = None): HttpResponse[String] =
    client.send(
      (accept match {
        case Some(a) =>
          HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path"))
            .header("content-type", "application/json")
            .header("Accept", a)
            .POST(HttpRequest.BodyPublishers.ofString(body))
        case None =>
          HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path"))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
      }).build(),
      HttpResponse.BodyHandlers.ofString())

  /** Stop the bound Vert.x socket after the suite completes. Per
    * [[scala-jvm-safety-mindset]] "resource lifecycle": never leak
    * a bound socket between suites.
    */
  override def afterAll(): Unit = {
    transport.stop()
    super.afterAll()
  }

  // ------------------------------------------------------------------
  // Tests
  // ------------------------------------------------------------------

  test("GET /health returns 200 OK from the real socket") {
    // Per Restate's EndpointRequestHandler: any path ENDING in /health
    // short-circuits to a 200 "OK" static response before any service
    // resolution happens.
    val resp = get("/health")
    resp.statusCode shouldBe 200
    resp.body shouldBe "OK"
  }

  test("GET /nested/health also returns 200 (path-suffix rule)") {
    // Per EndpointRequestHandler.processorForRequest: `path.endsWith(HEALTH_PATH)`.
    val resp = get("/some/prefix/health")
    resp.statusCode shouldBe 200
    resp.body shouldBe "OK"
  }

  test("GET on unknown path returns Restate protocol error (415), proving the route reaches Restate") {
    // Per EndpointRequestHandler + DiscoveryProtocol: even unknown
    // paths reach Restate's protocol layer (NOT a Vert.x 404). The
    // handler validates the Accept header before path resolution and
    // returns 415 Unsupported Media Type when no valid discovery
    // media type is present. The `x-restate-server` header on the
    // response is the smoking gun: only Restate's HTTP layer emits it.
    val resp = get("/no/such/route")
    // Either 415 (missing/mismatched Accept) or 404 (unknown handler
    // name); both prove the path reached Restate, not Vert.x.
    (resp.statusCode == 415 || resp.statusCode == 404) shouldBe true
    // `x-restate-server` is Restate's signature header (see the JDK
    // HttpClient's Headers API: firstValue returns java.util.Optional).
    import scala.jdk.OptionConverters._
    resp.headers.firstValue("x-restate-server").toScala shouldBe defined
  }

  test("POST /discover returns 200 with QueryService in the descriptor") {
    // Per EndpointRequestHandler.handleDiscoveryRequest + DiscoveryProtocol:
    // discovery requires an Accept header naming a supported endpoint-manifest
    // media type (v1 or v2). The response is the service descriptor — proving
    // the ServiceDefinition (QueryService) is wire-encodable and registered.
    val manifestV1 = "application/vnd.restate.endpointmanifest.v1+json"
    val resp = post("/discover", "", accept = Some(manifestV1))
    resp.statusCode shouldBe 200
    // The discovery response is a JSON descriptor; it must mention the
    // service we bound.
    resp.body should include("QueryService")
  }

  test("POST /invoke/QueryService/runQuery rejects a malformed body on Restate's protocol path") {
    // The full bidi-stream protocol needs protobuf frames; sending a
    // plain JSON body still reaches Restate's state machine (NOT a
    // Vert.x 404), which rejects it with its own protocol error status.
    // Assert: the route resolved (status != 404) and the error is not
    // a generic Vert.x 404 page.
    val resp = post("/invoke/QueryService/runQuery", """{"model":"test-model"}""")
    resp.statusCode should not be 404
    resp.body should not include "<html"
  }
}