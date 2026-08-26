package io.sm8.platform.query

import io.sm8.platform.query.cache._
import io.sm8.core.engine.{EngineContext, EngineError, EngineProvider, EngineRegistry, PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.sm8.core.engine.{ QueryRequest => CoreQueryRequest }
import io.sm8.core.model.{AuditPolicy, CachePolicy, FilterSpec, MaterializePolicy, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for PR-C5a: `EngineService.buildMCPRequest` + `selectEngine`.
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure data → Either
 * dispatch, no Map-based rule tables. The `buildMCPRequest` helper
 * is exhaustive over `Option` (Scala native). The `selectEngine`
 * helper delegates to `EngineRegistry.select` (already tested
 * in `EngineRegistrySpec`).
 *
 * Per [[scala-error-handling-mindset]]: errors are data. The
 * `selectEngine` returns `Either[EngineError, EngineProvider]`
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
    source = SourceRef.ByName(table = "t"),
    status = ModelStatus.Draft,
    filters = Nil
  )

  /** A minimal EngineProvider stub. The `queryResult` and
    * `queryThrowable` are set per-test to control behavior. */
  private final class StubProvider(
      override val identity: io.sm8.core.engine.EngineIdentity,
      override val available: Boolean,
      var queryResult: Either[EngineError, PortableQueryResult] =
        Right(PortableQueryResult(schema = ResultSchema(Nil), rows = Vector.empty)),
      var queryThrowable: RuntimeException = null
  ) extends EngineProvider with java.io.Serializable {
    override def query(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, io.sm8.core.engine.PortableQueryResult] = {
      if (queryThrowable != null) throw queryThrowable
      queryResult
    }
    override def explain(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, String] = ???
  }

  private def makeRegistry(
      providers: Map[String, EngineProvider],
      default: String = "spark"
  ): EngineRegistry = EngineRegistry(providers, default)

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
    val out: Either[EngineError, EngineProvider] =
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

  // -- executeEngine tests (PR-C5b) --

  private def emptyPortableResult: PortableQueryResult = PortableQueryResult(
    rows = Vector.empty,
    schema = ResultSchema(Nil)
  )

  private def portableResultWithRows: PortableQueryResult = PortableQueryResult(
    rows = Vector(
      ResultRow(
        values = List(
          ResultValue.StringV("Alice"),
          ResultValue.IntV(30L)),
        schema = ResultSchema(Nil)),
      ResultRow(
        values = List(
          ResultValue.StringV("Bob"),
          ResultValue.IntV(25L)),
        schema = ResultSchema(Nil))
    ),
    schema = ResultSchema(List(
      io.sm8.core.schema.Field.nonNull("name", io.sm8.core.schema.SealedDataType.Varchar),
      io.sm8.core.schema.Field.nonNull("age", io.sm8.core.schema.SealedDataType.BigInt)
    ))
  )

  test("executeEngine: returns Right(PortableQueryResult) on success") {
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = Right(emptyPortableResult)
    )
    val mcpReq = CoreQueryRequest(model = "flights")
    val out = EngineService.executeEngine(dummyModel, mcpReq, spark)
    out shouldBe Right(emptyPortableResult)
  }

  test("executeEngine: returns Left(ProviderInvocationFailed) on NonFatal IOException (P1-S2)") {
    // P1-S2: the catch was `case e: RuntimeException` — a provider
    // throwing a checked `IOException` (e.g. a network/IO fault at
    // the engine boundary) escaped as a raw throw instead of the
    // typed `ProviderInvocationFailed`. After the NonFatal widening
    // it is converted.
    final class IoStubProvider extends EngineProvider with java.io.Serializable {
      override val identity: io.sm8.core.engine.EngineIdentity =
        io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4")
      override val available: Boolean = true
      override def query(
          model: Model,
          request: CoreQueryRequest,
          ctx: io.sm8.core.engine.EngineContext
      ): Either[EngineError, io.sm8.core.engine.PortableQueryResult] =
        throw new java.io.IOException("x")
      override def explain(
          model: Model,
          request: CoreQueryRequest,
          ctx: io.sm8.core.engine.EngineContext
      ): Either[EngineError, String] = ???
    }
    val mcpReq = CoreQueryRequest(model = "flights")
    val out = EngineService.executeEngine(dummyModel, mcpReq, new IoStubProvider)
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.ProviderInvocationFailed]
    val err = out.left.get.asInstanceOf[EngineError.ProviderInvocationFailed]
    err.name shouldBe "spark"
    err.message shouldBe "x"
  }

  test("executeEngine: returns Left(EngineError) on engine-typed failure") {
    val typedError = EngineError.QueryTimedOut(
      engine = "spark", cancelStatus = "cancelled", message = "timed out"
    )
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = Left(typedError)
    )
    val mcpReq = CoreQueryRequest(model = "flights")
    EngineService.executeEngine(dummyModel, mcpReq, spark) shouldBe Left(typedError)
  }

  test("executeEngine: uses default EngineContext.defaultContext") {
    // No explicit ctx parameter → uses EngineContext.defaultContext.
    // Per executeEngine, the test just confirms the success path works.
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = Right(emptyPortableResult)
    )
    val mcpReq = CoreQueryRequest(model = "flights")
    EngineService.executeEngine(dummyModel, mcpReq, spark) shouldBe Right(emptyPortableResult)
  }

  // -- toQueryResultFromPortable tests (PR-C5b) --

  test("toQueryResultFromPortable: empty portable → QueryResult with empty rows") {
    val portable = PortableQueryResult(
      rows = Vector.empty,
      schema = ResultSchema(Nil)
    )
    val req = QueryRequest("m", Nil, Nil, "", "")
    val out = EngineService.toQueryResultFromPortable(portable, req)
    out.model shouldBe "m"
    out.measures shouldBe Nil
    out.rows shouldBe Nil
    out.rowCount shouldBe 0L
    out.truncated shouldBe false
  }

  test("toQueryResultFromPortable: 2-row portable with mixed types → decoded rows") {
    val portable = portableResultWithRows
    val req = QueryRequest("users", Nil, Nil, "", "spark")
    val out = EngineService.toQueryResultFromPortable(portable, req)
    out.model shouldBe "users"
    out.rows should have size 2
    out.rows(0) shouldBe List("Alice", 30L)
    out.rows(1) shouldBe List("Bob", 25L)
    out.rowCount shouldBe 2L
    out.truncated shouldBe false
  }

  test("toQueryResultFromPortable: rowCount = rows.size.toLong") {
    val portable = PortableQueryResult(
      rows = Vector.tabulate(5)(i =>
        ResultRow(
          values = List(ResultValue.IntV(i.toLong)),
          schema = ResultSchema(Nil))),
      schema = ResultSchema(Nil)
    )
    val req = QueryRequest("m", Nil, Nil, "", "")
    val out = EngineService.toQueryResultFromPortable(portable, req)
    out.rowCount shouldBe 5L
  }

  test("toQueryResultFromPortable: model name comes from request, not portable") {
    val portable = PortableQueryResult(
      rows = Vector.empty,
      schema = ResultSchema(Nil)
    )
    val req = QueryRequest("requested_model", Nil, Nil, "", "")
    val out = EngineService.toQueryResultFromPortable(portable, req)
    out.model shouldBe "requested_model"
  }

  test("toQueryResultFromPortable: forwards truncated from portable (true + false — ADR-009-e)") {
    // Acceptance #3: the engine flag reaches the platform wire
    // JSON. true stays true; false stays false (nothing hardcodes
    // it anymore).
    val req = QueryRequest("m", Nil, Nil, "", "")
    def fwd(t: Boolean): Boolean =
      EngineService.toQueryResultFromPortable(
        PortableQueryResult(schema = ResultSchema(Nil), rows = Vector.empty, truncated = t),
        req).truncated
    fwd(true)  shouldBe true
    fwd(false) shouldBe false
  }

  test("toQueryResultFromPortable: integration — executeEngine + toQueryResultFromPortable") {
    // The realistic pipeline: executeEngine returns Right(pqr);
    // toQueryResultFromPortable converts to the wire response.
    val spark = new StubProvider(
      io.sm8.core.engine.EngineIdentity("spark", "3.5.8", "0.2.4"),
      available = true,
      queryResult = Right(portableResultWithRows)
    )
    val mcpReq = CoreQueryRequest(model = "flights")
    val req = QueryRequest("flights", Nil, Nil, "", "")
    val either = EngineService.executeEngine(dummyModel, mcpReq, spark)
    val result = EngineService.toQueryResultFromPortable(either.toOption.get, req)
    result.model shouldBe "flights"
    result.rows should have size 2
    result.rows(0) shouldBe List("Alice", 30L)
  }

  test("toQueryResultFromPortable: schema.fields → measures (non-empty)") {
    // Regression for the architect's review: the `schema.fields →
    // measures` path was untested when all tests used
    // `ResultSchema(Nil)`. PR-C5b-extension's cache-hit path
    // relies on this for the rowCount wire field.
    val schema = ResultSchema(List(
      Field.nonNull("name", SealedDataType.Varchar),
      Field.nonNull("age", SealedDataType.BigInt)
    ))
    val portable = PortableQueryResult(
      rows = Vector(ResultRow(
        values = List(ResultValue.StringV("Alice"), ResultValue.IntV(30L)),
        schema = schema)),
      schema = schema
    )
    val req = QueryRequest("users", Nil, Nil, "", "spark")
    val out = EngineService.toQueryResultFromPortable(portable, req)
    out.measures shouldBe List("name", "age")
  }

}
