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
import io.sm8.core.cache._
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.engine.{
  EngineContext,
  EngineError,
  EngineIdentity,
  EngineProvider,
  EngineRegistry,
  QueryRequest => CoreQueryRequest,
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
import io.sm8.core.rel.JoinKind
import io.sm8.core.model.JoinSpec
import io.sm8.plugins.cache.CachePlugin
import io.sm8.sdk.{Context, HookStage, PreHook, Request}
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
    source = SourceRef.ByName(table = "t"),
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
      val callCount: AtomicInteger = new AtomicInteger(0),
      // ADR per-query decision oracle: the stub captures the
      // EngineContext it received so the tests can assert that
      // EngineService folded the post-hook decision into
      // ctx.decisionHints.
      val capturedCtx: java.util.concurrent.atomic.AtomicReference[Option[EngineContext]] =
        new java.util.concurrent.atomic.AtomicReference(None)
  ) extends EngineProvider {

    override def query(
        model: Model,
        request: CoreQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = {
      callCount.incrementAndGet()
      capturedCtx.set(Some(ctx))
      if (queryThrowable != null) throw queryThrowable
      queryResult
    }

    override def explain(
        model: Model,
        request: CoreQueryRequest,
        ctx: EngineContext
    ): Either[EngineError, String] = Right("fake")
  }

  private def makeRegistry(
      providers: Map[String, EngineProvider],
      default: String = "test-engine"
  ): EngineRegistry = EngineRegistry(providers, default)

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
    val cache    = new io.sm8.plugins.cache.InMemoryResultCache(maxEntries = 16)
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

  test("runQueryWithHooks: non-EngineHookRequest Context returns Left(ProviderInvocationFailed) without NonLocalReturnControl (P1-S3)") {
    // P1-S3: the engineExecutor thunk used a non-local `return Left(...)`
    // for a Context whose request is not an EngineHookRequest. It only
    // worked because the dispatcher runs the thunk synchronously; the
    // correct composition is a typed Either (no NonLocalReturnControl).
    // Here a PreExecute hook replaces the request with a non-hook
    // Request, forcing the executor onto the unexpected-type path.
    final case class ForeignRequest(name: String) extends Request with Serializable

    val spark = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry = makeRegistry(Map("test-engine" -> spark))
    val engineImpl = new io.sm8.core.EngineImpl
    engineImpl.use(new io.sm8.sdk.Plugin {
      override def setup(engine: io.sm8.sdk.Engine): Unit = {
        engine.hooks.registerPreHook(
          HookStage.PreExecute,
          new PreHook {
            override val name: String = "swap-request"
            override val priority: Int = 10
            override def stage: HookStage = HookStage.PreExecute
            override def run(c: Context): Context =
              c.copy(request = ForeignRequest("not-a-hook-request"))
          },
          10
        )
      }
    })
    val dispatcher = EngineHookDispatcher(engineImpl.hooks)

    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    // Typed failure, NOT a thrown NonLocalReturnControl.
    out.isLeft shouldBe true
    val err = out.left.get
    err shouldBe a [EngineError.ProviderInvocationFailed]
    err.asInstanceOf[EngineError.ProviderInvocationFailed].reason shouldBe "UnexpectedRequestType"
    // The engine must never be invoked on this path.
    spark.callCount.get() shouldBe 0
  }

  test("runQueryWithHooks: missing engine in registry returns Left(EngineUnavailable)") {
    // EngineRegistry requires the default to be available at
    // startup. Put an available default in the map + request a
    // non-default name ("ghost") that's not in the map at all ->
    // select returns Left(EngineUnavailable).
    val defaultProvider = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry = EngineRegistry(
      engines = Map("test-engine" -> defaultProvider),
      default = "test-engine"
    )
    val dispatcher = hookDispatcherWith(ResultCache.NoOp)

    // Request asks for "ghost" — not in the registry at all.
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "ghost"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    out.isLeft shouldBe true
    out.left.get shouldBe a [EngineError.EngineUnavailable]
  }
  private val broadcastStubRegistry: (io.sm8.core.EngineImpl, EngineRegistry) = {
    val engineImpl = new io.sm8.core.EngineImpl
    engineImpl.use(new io.sm8.plugins.broadcast.BroadcastStub)
    engineImpl.use(new io.sm8.plugins.skew.SkewStub)
    val spark = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry = makeRegistry(Map("test-engine" -> spark))
    (engineImpl, registry)
  }

  test("ADR-009-d v0.3: no-oracle path — EngineService.runQueryWithHooks folds empty decisionHints; inline fallback fires") {
    // No BroadcastStub/SkewStub registered on the engine impl.
    val engineImpl = new io.sm8.core.EngineImpl
    val spark = new StubProvider(
      EngineIdentity("test-engine", "1.0", "1.0"),
      available = true
    )
    val registry = makeRegistry(Map("test-engine" -> spark))
    val dispatcher = EngineHookDispatcher(engineImpl.hooks)
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    out.isRight shouldBe true
    val capturedCtx = spark.capturedCtx.get.getOrElse(fail("engine ctx was not captured"))
    // No oracle: decisionHints is Some(DecisionHints(...all None...))
    // (the fold ran; no plugin wrote any keys).
    capturedCtx.decisionHints shouldBe defined
    capturedCtx.decisionHints.get.broadcastArmed shouldBe None
    capturedCtx.decisionHints.get.skewArmed shouldBe None
    capturedCtx.decisionHints.get.broadcastThresholdBytes shouldBe None
  }

  test("ADR-009-d v0.3: oracle-wired path — BroadcastStub + SkewStub arm the decisionHints fields on the per-query EngineContext") {
    // Both stubs registered: the post-PreExecute Context.meta
    // carries the arm Booleans + threshold bytes; the fold
    // populates EngineContext.decisionHints.
    val (engineImpl, registry) = broadcastStubRegistry
    val spark: StubProvider = registry.select("test-engine").fold(
      _ => fail("registry did not return the test provider"),
      p => p.asInstanceOf[StubProvider])
    val dispatcher = EngineHookDispatcher(engineImpl.hooks)
    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
      model      = dummyModel,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )
    out.isRight shouldBe true
    // The StubProvider captured the EngineContext that
    // EngineService passed to engineExecutor (the fold
    // populated decisionHints from the post-PreExecute Context.meta).
    val capturedCtx: EngineContext = spark.capturedCtx.get.getOrElse(
      fail("engine ctx was not captured by the stub provider"))
    val hints = capturedCtx.decisionHints.getOrElse(
      fail("decisionHints was not populated (the fold did not run)"))
    hints.broadcastArmed shouldBe Some(false)
  }
  test("ADR-009-d v0.3: throwing-oracle path — a PreExecute hook that throws yields Left(EngineError.HookFailed) and the engine is NEVER invoked") {
   // Per ADR-008-AF v1.0 + the v0.3 P1-C closure (dispatcher owns
   // the 5-field HookFailed construction): a plugin hook that
   // throws in PreExecute.run propagates to the dispatcher's
   // existing catch — engineExecutor never runs, so the
   // StubProvider's callCount stays at 0 and the capturedCtx
   // stays at None (no ThreadLocal leak; no engine invocation).
   val engineImpl = new io.sm8.core.EngineImpl
   engineImpl.hooks.registerPreHook(
     HookStage.PreExecute,
     new ThrowingPreStubHook,
     priority = 250)
   val spark = new StubProvider(
     EngineIdentity("test-engine", "1.0", "1.0"),
     available = true)
   val registry   = makeRegistry(Map("test-engine" -> spark))
   val dispatcher = EngineHookDispatcher(engineImpl.hooks)
   val out = EngineService.runQueryWithHooks(
     request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
     model      = dummyModel,
     registry   = registry,
     cache      = ResultCache.NoOp,
     dispatcher = dispatcher
   )
   // Left(HookFailed) with all 5 fields populated.
   out.isLeft shouldBe true
   val err = out.left.get
   err shouldBe a [EngineError.HookFailed]
   val hf = err.asInstanceOf[EngineError.HookFailed]
   hf.engine   shouldBe "<dispatcher>"
   hf.name     shouldBe "throwing-stub"
   hf.priority shouldBe 250
   hf.stage    shouldBe "PreExecute"
   // Sanitized message (the dispatcher's Option(getMessage).getOrElse(...)
   // rule — synthetic Throwable with a real .getMessage).
   hf.message  shouldBe "boom"
   // Engine never invoked; capturedCtx stays None (no ThreadLocal
   // leak — EngineService never populated it for this query).
   spark.callCount.get() shouldBe 0
   spark.capturedCtx.get() shouldBe None
  }

  test("ADR-009-d v0.3: oracle-wired path with real join — estimatedRows=Some(1M) arms broadcast (≤10M) and disarms skew (<1B)") {
   // F4 strengthen: the existing oracle-wired test (L254) used a
   // no-joins model so both stubs computed arm=false. This test
   // wires a model with one join whose estimatedRows = 1_000_000
   // (small enough for the broadcast value-consult to arm; well
   // below the 1B skew threshold so skew disarms). Falsifiable
   // proof that the broadcast value-consult genuinely fires.
   val modelWithSmallJoin: Model = dummyModel.copy(
     joins = List(JoinSpec(
       name        = "orders.customers",
       rightModel  = "customers",
       kind        = JoinKind.Inner,
       keys        = List("region" -> "region"),
       estimatedRows = Some(1_000_000L))))
   val (engineImpl, registry) = broadcastStubRegistry
   val spark: StubProvider = registry.select("test-engine").fold(
     _ => fail("registry did not return the test provider"),
     p => p.asInstanceOf[StubProvider])
   val dispatcher = EngineHookDispatcher(engineImpl.hooks)
   val out = EngineService.runQueryWithHooks(
     request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
     model      = modelWithSmallJoin,
     registry   = registry,
     cache      = ResultCache.NoOp,
     dispatcher = dispatcher
   )
   out.isRight shouldBe true
   val capturedCtx: EngineContext = spark.capturedCtx.get.getOrElse(
     fail("engine ctx was not captured by the stub provider"))
   val hints = capturedCtx.decisionHints.getOrElse(
     fail("decisionHints was not populated (the fold did not run)"))
   // Broadcast value-consult: 1M <= 10M threshold → arm = true.
   hints.broadcastArmed shouldBe Some(true)
   // Skew value-consult: 1M < 1B threshold → arm = false.
   hints.skewArmed shouldBe Some(false)
  hints.broadcastThresholdBytes shouldBe Some(10L * 1024L * 1024L: Long)
  }

  test("ADR-009-d v0.3: oracle-wired path with real join — estimatedRows=Some(2B) arms skew (≥1B) and disarms broadcast (>10M)") {
   // Mirror of the previous test with a join whose estimatedRows
   // is well past BOTH thresholds: skew arms (2B >= 1B), broadcast
   // disarms (2B > 10M).
   val modelWithLargeJoin: Model = dummyModel.copy(
     joins = List(JoinSpec(
       name        = "orders.events",
       rightModel  = "events",
       kind        = JoinKind.Inner,
       keys        = List("region" -> "region"),
       estimatedRows = Some(2_000_000_000L))))
   val (engineImpl, registry) = broadcastStubRegistry
   val spark: StubProvider = registry.select("test-engine").fold(
     _ => fail("registry did not return the test provider"),
     p => p.asInstanceOf[StubProvider])
   val dispatcher = EngineHookDispatcher(engineImpl.hooks)
   val out = EngineService.runQueryWithHooks(
     request    = QueryRequest("m", Nil, Nil, "", "test-engine"),
     model      = modelWithLargeJoin,
     registry   = registry,
     cache      = ResultCache.NoOp,
     dispatcher = dispatcher
   )
   out.isRight shouldBe true
   val capturedCtx: EngineContext = spark.capturedCtx.get.getOrElse(
     fail("engine ctx was not captured by the stub provider"))
   val hints = capturedCtx.decisionHints.getOrElse(
     fail("decisionHints was not populated (the fold did not run)"))
   // Broadcast disarmed (2B > 10M).
   hints.broadcastArmed shouldBe Some(false)
   // Skew armed (2B >= 1B).
   hints.skewArmed shouldBe Some(true)
  hints.broadcastThresholdBytes shouldBe Some(10L * 1024L * 1024L: Long)
  }
}

/**
 * Test fixture: a PreExecute hook whose `run` always throws. Used
 * to verify the dispatcher's throw-catch path produces a typed
 * `EngineError.HookFailed` (v0.3 P1-C closure: NO try/catch in
 * the hook; the dispatcher's existing catch is the single owner
 * of the 5-field HookFailed construction). Per ADR-008-AF v1.0
 * the dispatcher sanitizes the message via
 * `Option(e.getMessage).getOrElse(...)`.
 */
private final class ThrowingPreStubHook
    extends PreHook with java.io.Serializable {
  override val name: String = "throwing-stub"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute
  override def run(context: Context): Context =
    throw new RuntimeException("boom")
}
