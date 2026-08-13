package io.sm8.platform.query

import io.sm8.core.engine.{EngineError, MCPEngineProvider, MCPEngineRegistry, MCPQueryRequest}
import io.sm8.core.model.{AuditPolicy, CachePolicy, FilterSpec, MaterializePolicy, Model, ModelPolicyDefaults, ModelStatus, SourceRef}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for PR-C5a: `EngineService.buildMCPRequest` + `selectEngine`.
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure data → Either
 * dispatch, no Map-based rule tables. The `buildMCPRequest` helper
 * is exhaustive over `Option` (Scala native). The `selectEngine`
 * helper delegates to `MCPEngineRegistry.select` (already tested
 * in `MCPEngineRegistrySpec`).
 *
 * Per [[scala-error-handling-mindset]]: errors are data. The
 * `selectEngine` returns `Either[EngineError, MCPEngineProvider]`
 * — no `throw` at the boundary. The caller (PR-C5b) wraps the
 * error path.
 *
 * Per [[scala-impact-analysis-mindset]]: 0 callers of the Java
 * `QueryService.runQueryViaEngineRegistry` in our reactor. These
 * tests are the wire-contract proof that the Scala version
 * preserves the legacy behavior — minus the `providerHolder[0]`
 * JVM-safety bug fix.
 */
class EngineServiceSpec extends AnyFunSuite with Matchers {

  // -- Test fixtures --

  /** A minimal Model for tests that don't care about model fields. */
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
      override val identity: io.sm8.core.engine.EngineIdentity,
      override val available: Boolean
  ) extends MCPEngineProvider {
    override def query(
        model: Model,
        request: MCPQueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, io.sm8.core.engine.PortableQueryResult] = ???
    override def explain(
        model: Model,
        request: MCPQueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, String] = ???
  }

  private def makeRegistry(
      providers: Map[String, MCPEngineProvider],
      default: String = "spark"
  ): MCPEngineRegistry = MCPEngineRegistry(providers, default)

  // -- buildMCPRequest tests --

  test("buildMCPRequest: dimensions converted from Scala List to Scala Seq") {
    val req = QueryRequest(
      modelName = "flights",
      measures = Nil,
      dimensions = List("carrier", "year"),
      where = "",
      engine = ""
    )
    val mcp = EngineService.buildMCPRequest(req)
    mcp.model shouldBe "flights"
    mcp.dimensions shouldBe Seq("carrier", "year")
    mcp.measures shouldBe Seq.empty
  }

  test("buildMCPRequest: measures converted from Scala List to Scala Seq") {
    val req = QueryRequest(
      modelName = "flights",
      measures = List("rows", "unique_carriers"),
      dimensions = Nil,
      where = "",
      engine = ""
    )
    val mcp = EngineService.buildMCPRequest(req)
    mcp.measures shouldBe Seq("rows", "unique_carriers")
    mcp.dimensions shouldBe Seq.empty
  }

  test("buildMCPRequest: null dimensions → empty Seq") {
    val req = QueryRequest(
      modelName = "m",
      measures = Nil,
      dimensions = null,
      where = "",
      engine = ""
    )
    EngineService.buildMCPRequest(req).dimensions shouldBe Seq.empty
  }

  test("buildMCPRequest: null measures → empty Seq") {
    val req = QueryRequest(
      modelName = "m",
      measures = null,
      dimensions = Nil,
      where = "",
      engine = ""
    )
    EngineService.buildMCPRequest(req).measures shouldBe Seq.empty
  }

  test("buildMCPRequest: null where → None") {
    val req = QueryRequest(
      modelName = "m",
      measures = Nil,
      dimensions = Nil,
      where = null,
      engine = ""
    )
    EngineService.buildMCPRequest(req).where shouldBe None
  }

  test("buildMCPRequest: blank where → None") {
    val req = QueryRequest(
      modelName = "m",
      measures = Nil,
      dimensions = Nil,
      where = "   ",
      engine = ""
    )
    EngineService.buildMCPRequest(req).where shouldBe None
  }

  test("buildMCPRequest: non-blank where → Some(value)") {
    val req = QueryRequest(
      modelName = "m",
      measures = Nil,
      dimensions = Nil,
      where = "carrier = 'AA'",
      engine = ""
    )
    EngineService.buildMCPRequest(req).where shouldBe Some("carrier = 'AA'")
  }

  test("buildMCPRequest: filters is empty (typed FilterSpec deferred)") {
    val req = QueryRequest(
      modelName = "m",
      measures = Nil,
      dimensions = Nil,
      where = "",
      engine = ""
    )
    EngineService.buildMCPRequest(req).filters shouldBe Nil
  }

  test("buildMCPRequest: full wire contract preservation") {
    val req = QueryRequest(
      modelName = "flights",
      measures = List("rows"),
      dimensions = List("carrier"),
      where = "carrier = 'AA'",
      engine = "spark"
    )
    val mcp = EngineService.buildMCPRequest(req)
    mcp.model shouldBe "flights"
    mcp.dimensions shouldBe Seq("carrier")
    mcp.measures shouldBe Seq("rows")
    mcp.where shouldBe Some("carrier = 'AA'")
    mcp.filters shouldBe Nil
    mcp.limit shouldBe None
    mcp.timeGrain shouldBe None
    mcp.timeRange shouldBe None
  }

  // -- selectEngine tests --

  test("selectEngine: registry.select returns Right(provider) for a registered engine") {
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val req = QueryRequest("m", Nil, Nil, "", "spark")
    EngineService.selectEngine(dummyModel, req, registry) shouldBe Right(spark)
  }

  test("selectEngine: blank engine field → uses registry default") {
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark), default = "spark")
    val req = QueryRequest("m", Nil, Nil, "", "")
    EngineService.selectEngine(dummyModel, req, registry) shouldBe Right(spark)
  }

  test("selectEngine: null engine field → uses registry default") {
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark), default = "spark")
    val req = QueryRequest("m", null, null, null, null)
    EngineService.selectEngine(dummyModel, req, registry) shouldBe Right(spark)
  }

  test("selectEngine: Unicode whitespace (NBSP) engine field uses registry default") {
    // NBSP-only engine name should fall back to default (matches
    // Java `isBlank()` semantics). Regression test for the
    // Unicode whitespace bug found in PR-C5a senior data
    // engineer review.
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark), default = "spark")
    val req = QueryRequest("m", Nil, Nil, "", new String(Array(' ')))
    EngineService.selectEngine(dummyModel, req, registry) shouldBe Right(spark)
  }

  test("selectEngine: unknown engine → Left(EngineUnavailable)") {
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark), default = "spark")
    val req = QueryRequest("m", Nil, Nil, "", "trino")
    val out = EngineService.selectEngine(dummyModel, req, registry)
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.EngineUnavailable]
  }

  test("selectEngine: registered but unavailable → Left(EngineUnavailable)") {
    // Registry's `require` (line 45) forbids unavailability in the
    // DEFAULT engine. We test the unavailable-non-default case:
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val trino = new StubProvider(
      io.sm8.core.engine.EngineIdentity("trino", "0.286", "0.2.4"),
      available = false
    )
    val registry = makeRegistry(Map("spark" -> spark, "trino" -> trino), default = "spark")
    val req = QueryRequest("m", Nil, Nil, "", "trino")
    val out = EngineService.selectEngine(dummyModel, req, registry)
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.EngineUnavailable]
  }

  test("selectEngine: NO providerHolder[0] array — direct Either return") {
    // The headline JVM-safety bug fix. The old Java code used a
    // 1-element array as a mutable cell to escape the Either. The
    // Scala version returns the Either directly. This test exists
    // to document the change — no array involved.
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val req = QueryRequest("m", Nil, Nil, "", "spark")
    val out: Either[EngineError, MCPEngineProvider] =
      EngineService.selectEngine(dummyModel, req, registry)
    // Pattern match the Either directly — no index, no holder,
    // no unwrap-then-then-NPE.
    out match {
      case Right(p)  => p shouldBe spark
      case Left(err) => fail(s"unexpected Left: $err")
    }
  }

  // -- Wire contract: integration --

  test("selectEngine + buildMCPRequest: full pipeline for a typical request") {
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true
    )
    val registry = makeRegistry(Map("spark" -> spark))
    val req = QueryRequest(
      modelName = "flights",
      measures = List("rows"),
      dimensions = List("carrier"),
      where = "carrier = 'AA'",
      engine = ""
    )

    val mcp = EngineService.buildMCPRequest(req)
    val out = EngineService.selectEngine(dummyModel, req, registry)
    val provider = out.toOption.get

    mcp.model shouldBe "flights"
    mcp.dimensions shouldBe Seq("carrier")
    mcp.measures shouldBe Seq("rows")
    mcp.where shouldBe Some("carrier = 'AA'")
    provider shouldBe spark
  }
}