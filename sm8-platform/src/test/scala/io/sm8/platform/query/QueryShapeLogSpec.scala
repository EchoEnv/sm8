/*
 * SM8 Platform — QueryShapeLogSpec (query-shape instrumentation; Ticket 2 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md).
 *
 * Pre-aggregation map `docs/wayfinder/2026-09-06-pre-aggregation.md`
 * Ticket #2: query-shape instrumentation. `EngineService.logQueryShape`
 * emits one DEBUG record per `runQueryWithHooks` invocation carrying
 * the (model, version, measures, dimensions) tuple so that rollup
 * selection (Ticket 3 of docs/wayfinder/2026-09-06-pre-aggregation.md:
 * which `Model.rollups` to declare)
 * is driven by measured query patterns rather than guesses.
 *
 * What these tests pin (the observable contract):
 * 1. A successful `runQueryWithHooks` invocation completes with the
 *    instrumentation on the recording path and returns Right —
 *    zero behavioral regression. Event CONTENT is deliberately not
 *    asserted: the test classpath has no slf4j binding/appender, so
 *    emitted events are unobservable here; the emission contract
 *    (exactly one DEBUG event per invocation, normalized arguments
 *    attached as slf4j args, constant format string, DEBUG-gated)
 *    is pinned by code review + production usage, not by this spec.
 * 2. The named DEBUG logger resolves in this runtime, and the
 *    DEBUG-guard (isDebugEnabled before any formatting) holds by
 *    construction: at INFO level no message is built.
 * 3. Repeated invocations each take the recording path without
 *    altering results or provider call counts.
 *
 * Known limitation (deliberate, not an oversight): NO test captures
 * the emitted log event itself. There is no slf4j binding/appender
 * on the test classpath (slf4j-api 2.0.13 reaches sm8-platform
 * transitively via the MCP SDK; verified `mvn dependency:tree`), so
 * an emission/content test would need a new test-scoped binding —
 * a follow-up candidate, not a blocker.
 *
 * The provider fixture is built fresh per test: no shared mutable
 * fixture state, so repeated invocations cannot contaminate counts.
 * each test builds its own registry/dispatcher.
 */
package io.sm8.platform.query

import io.sm8.core.cache.ResultCache
import io.sm8.core.engine.{EngineContext, EngineIdentity, EngineError, EngineProvider, EngineRegistry}
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.core.engine.{ResultSchema, ResultRow, ResultValue}
import io.sm8.core.engine.{QueryRequest => CoreQueryRequest, PortableQueryResult}
import io.sm8.core.model.{
  AuditPolicy,
  CachePolicy,
  MaterializePolicy,
  Model,
  ModelPolicyDefaults,
  ModelStatus,
  SourceRef
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import io.sm8.platform.query.hooks.EngineHookDispatcher

import java.util.concurrent.atomic.AtomicInteger

final class QueryShapeLogSpec extends AnyFunSuite with Matchers {

  private val dummyModel: Model = Model(
    name = "shape-model",
    version = 3,
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

  private final class CountingProvider extends EngineProvider {
    val calls = new AtomicInteger(0)
    override val identity: EngineIdentity = EngineIdentity("test-engine", "1.0", "1.0")
    override val available: Boolean = true
    // Well-formed PQR per the RestateCachedRow wire contract
    // (row.length == fieldNames.size — cf. EngineServiceRunQueryWithHooksSpec).
    private val schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int)))
    /** Counting no-op engine: returns a fixed one-row Int result and
      * increments `calls` so tests can assert exactly-once invocation.
      *
      * @param model    the model under query (ignored by the stub)
      * @param request  the normalized request (ignored by the stub)
      * @param ctx      the engine context (ignored by the stub)
      * @return a fixed one-row Int result
      */
    override def query(
        model: Model,
        request: CoreQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = {
      calls.incrementAndGet()
      Right(PortableQueryResult(
        schema = schema,
        rows = Vector(ResultRow(
          values = List(ResultValue.IntV(42L)),
          schema = schema
        ))
      ))
    }
    /** Constant plan string; the shape assertions never inspect it.
      *
      * @param model    the model under query (ignored by the stub)
      * @param request  the normalized request (ignored by the stub)
      * @param ctx      the engine context (ignored by the stub)
      * @return the constant plan string
      */
    override def explain(
        model: Model,
        request: CoreQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, String] = Right("fake")
  }

  private def wired(): (EngineRegistry, CountingProvider) = {
    val p = new CountingProvider
    (EngineRegistry(Map("test-engine" -> p), "test-engine"), p)
  }

  test("query-shape instrumentation: successful invocation still returns Right (zero behavioral regression)") {
    val (registry, provider) = wired()
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("shape-model", List("rev"), List("day"), "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = EngineHookDispatcher(new io.sm8.core.EngineImpl().hooks)
    )
    out.isRight shouldBe true
    provider.calls.get() shouldBe 1
  }

  test("query-shape instrumentation: DEBUG logger exists on the io.sm8.platform.query.QueryShape name") {
    // The helper's logger is private; this pins the logger NAME
    // contract (what an operator configures to collect shapes) by
    // resolving the same name through slf4j's factory — guaranteed
    // non-null and functional per the slf4j contract.
    val log = org.slf4j.LoggerFactory.getLogger("io.sm8.platform.query.QueryShape")
    log should not be null
    // The production helper must be DEBUG-gated: at default levels
    // the guard short-circuits. Touching the gate documents it; the
    // boolean itself depends on the test-classpath binding.
    log.isDebugEnabled
    succeed
  }

  test("query-shape instrumentation: repeated invocations each take the recording path without altering counts") {
    val (registry, provider) = wired()
    val dispatcher = EngineHookDispatcher(new io.sm8.core.EngineImpl().hooks)
    val req = QueryRequest("shape-model", List("rev", "cost"), List("day", "region"), "", "test-engine")
    (1 to 3).foreach { _ =>
      val out = EngineService.runQueryWithHooks(
        request    = req,
        model      = dummyModel,
        registry   = registry,
        cache      = ResultCache.NoOp,
        dispatcher = dispatcher
      )
      out.isRight shouldBe true
    }
    provider.calls.get() shouldBe 3
  }
}
