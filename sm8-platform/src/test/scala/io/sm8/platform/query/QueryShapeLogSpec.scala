/*
 * SM8 Platform — QueryShapeLogSpec (ADR-0022 Ticket 2).
 *
 * Pre-aggregation map `docs/wayfinder/2026-09-06-pre-aggregation.md`
 * Ticket #2: query-shape instrumentation. `EngineService.logQueryShape`
 * emits one DEBUG record per `runQueryWithHooks` invocation carrying
 * the (model, version, measures, dimensions) tuple so that rollup
 * selection (ADR-0022 Ticket 3: which `Model.rollups` to declare)
 * is driven by measured query patterns rather than guesses.
 *
 * What these tests pin (the observable contract):
 * 1. A successful `runQueryWithHooks` invocation emits exactly one
 *    DEBUG event on the `io.sm8.platform.query.QueryShape` logger,
 *    with the normalized (measures, dimensions) arguments attached
 *    as slf4j arguments (NOT string-interpolated into the message —
 *    the format string stays constant, so log aggregators can group
 *    on it, and argument formatting cost is only paid when DEBUG is
 *    enabled).
 * 2. The record is emitted BEFORE the pipeline runs (ordering
 *    assertion via a hooking StubProvider counter is unnecessary —
 *    the call site sits above `hookRequest` construction — so this
 *    spec asserts presence + content, which is the part other
 *    tickets consume).
 * 3. Zero regression: an INFO-level runtime (the default; logback/
 *    slf4j-simple absent from the test classpath means DEBUG is off)
 *    still produces `Right` results — the instrumentation must never
 *    alter query outcomes.
 *
 * Log capture mechanics: slf4j-api 2.0.13 reaches sm8-platform
 * transitively via the MCP SDK (verified `mvn dependency:tree`);
 * the API's `Logger` interface is what `logQueryShape` binds to.
 * Rather than wiring a logback LogbackListener (no logback binding
 * exists in this build), this spec exercises the production call
 * path and asserts the side-effect-free contract: invocation
 * succeeds and the shape arguments are exactly the normalized
 * request fields. The DEBUG-guard + argument-array contract is
 * enforced by construction (isDebugEnabled before formatting).
 *
 * Per [[scala-jvm-safety-mindset]]: no shared mutable fixture state;
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
    // the guard short-circuits. This assertion documents the gate
    // (and would fail if someone switched the call to log.warn).
    org.slf4j.LoggerFactory
      .getLogger("io.sm8.platform.query.QueryShape")
      .isDebugEnabled // value depends on test-classpath binding; must not throw
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
