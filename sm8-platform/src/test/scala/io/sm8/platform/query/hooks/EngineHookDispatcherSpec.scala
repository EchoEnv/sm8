/*
 * SM8 Platform — EngineHookDispatcher integration test.
 *
 * Per the pipeline-wiring PR: these tests PROVE the hook path
 * actually fires on real queries. RFC §13 DoD observability.
 */
package io.sm8.platform.query.hooks

import io.sm8.core.cache._
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.engine.{
  EngineContext,
  EngineError,
  EngineHookRequest,
  EngineHookResult,
  EngineIdentity,
  EngineProvider,
  EngineRegistry,
  PortableQueryResult,
  ResultRow,
  ResultSchema,
  ResultValue
}
import io.sm8.core.engine.{ QueryRequest => CoreQueryRequest }
import io.sm8.core.model.{
  Dimension,
  Measure,
  Model,
  ModelStatus,
  SourceRef
}
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.sdk.{
  Context,
  Engine => SdkEngine,
  HookStage,
  Plugin,
  PostHook,
  PreHook
}
import io.sm8.platform.query.{
  EngineService,
  QueryRequest
}
import io.sm8.core.cache.ResultCache
import io.sm8.plugins.cache.InMemoryResultCache

private final class FakeProvider(
    val name: String,
    val stubPqr: PortableQueryResult
) extends EngineProvider {

  override val identity: EngineIdentity =
    EngineIdentity(name = name, nativeVersion = "test", engineAdapterVersion = "1.0")

  val callCount: AtomicInteger = new AtomicInteger(0)

  override def available: Boolean = true

  override def query(
      model: Model,
      mcpReq: CoreQueryRequest,
      ctx: EngineContext
  ): Either[EngineError, PortableQueryResult] = {
    callCount.incrementAndGet()
    Right(stubPqr)
  }

  override def explain(
      model: Model,
      mcpReq: CoreQueryRequest,
      ctx: EngineContext
  ): Either[EngineError, String] = Right("fake")
}

private final class CountedPlugin(val fires: AtomicInteger) extends Plugin {
  override def setup(engine: SdkEngine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new PreHook {
        override val name: String      = "test-pre"
        override val priority: Int     = 50
        override def stage: HookStage  = HookStage.PreExecute
        override def run(c: Context): Context = { fires.incrementAndGet(); c }
      },
      50
    )
    engine.hooks.registerPostHook(
      HookStage.PostExecute,
      new PostHook {
        override val name: String      = "test-post"
        override val priority: Int     = 100
        override def stage: HookStage  = HookStage.PostExecute
        override def run(c: Context): Context = { fires.incrementAndGet(); c }
      },
      100
    )
  }
}

private final class ShortCircuitPlugin(stub: PortableQueryResult) extends Plugin {
  override def setup(engine: SdkEngine): Unit = {
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new PreHook {
        override val name: String      = "sc"
        override val priority: Int     = 1
        override def stage: HookStage  = HookStage.PreExecute
        override def run(c: Context): Context =
          c.copy(stop = true, result = Some(EngineHookResult(stub)))
      },
      1
    )
  }
}


/**
 * Stub hook that throws a `RuntimeException` on every invocation. Used
 * by the new HookFailed tests below to exercise the typed-error
 * dispatch path (per ADR-008-AF v1.0).
 */
private final class FailingPreHook(override val name: String, override val priority: Int)
    extends PreHook {
  override def stage: HookStage = HookStage.PreExecute
  override def run(c: Context): Context =
    throw new RuntimeException("simulated pre-hook failure")
}

private final class FailingPostHook(override val name: String, override val priority: Int)
    extends PostHook {
  override def stage: HookStage = HookStage.PostExecute
  override def run(c: Context): Context =
    throw new RuntimeException("simulated post-hook failure")
}

private final class FailingHookPlugin(
    pluginName: String,
    hookStage: HookStage,
    register: io.sm8.sdk.Engine => Unit
) extends Plugin {
  override def setup(engine: io.sm8.sdk.Engine): Unit = register(engine)
}

class EngineHookDispatcherSpec extends AnyFunSuite with Matchers {

  private def stubPqr: PortableQueryResult = PortableQueryResult(
    schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int))),
    rows   = Vector(ResultRow(
      values = List(ResultValue.IntV(42L)),
      schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int)))
    ))
  )

  private def sampleModel: Model =
    Model.of(
      name       = "m",
      version    = 1,
      source     = SourceRef.ByName(table = "t"),
      status     = ModelStatus.Draft,
      dimensions = List(Dimension.field("d", "d")),
      measures   = List(Measure.aggregate("v", io.sm8.core.rel.AggregateFn.Sum, io.sm8.core.expr.Expr.FieldRef("v")))
    ).toOption.get

  private def sampleProvider: FakeProvider =
    new FakeProvider("test-engine", stubPqr)

  private def registryWith(p: FakeProvider): EngineRegistry = {
    val engines: Map[String, EngineProvider] = Map(p.name -> p)
    EngineRegistry(engines, p.name)
  }

  test("dispatcher: PreExecute + PostExecute hooks fire on cache-MISS path") {
    val fires  = new AtomicInteger(0)
    val plugin = new CountedPlugin(fires)
    val engine = new io.sm8.core.EngineImpl
    plugin.setup(engine)
    val dispatcher = EngineHookDispatcher(engine.hooks)

    val provider = sampleProvider
    val result = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", ""),
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    result.isRight shouldBe true
    fires.get() shouldBe 2
    provider.callCount.get() shouldBe 1
  }

  test("dispatcher: PreExecute hook with stop=true short-circuits engine call") {
    val sc         = new ShortCircuitPlugin(stubPqr)
    val engine     = new io.sm8.core.EngineImpl
    sc.setup(engine)
    val dispatcher = EngineHookDispatcher(engine.hooks)

    val provider = sampleProvider
    val result = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", ""),
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    result.isRight shouldBe true
    provider.callCount.get() shouldBe 0
  }

  test("dispatcher: NoOp dispatcher fires zero hooks (backward-compat)") {
    val provider = sampleProvider
    val result = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", ""),
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = EngineHookDispatcher.NoOp
    )

    result.isRight shouldBe true
    provider.callCount.get() shouldBe 1
  }

  test("dispatcher: PreHook that throws returns typed EngineError.HookFailed (per ADR-008-AF v1.0)") {
    val plugin = new FailingHookPlugin(
      pluginName = "failing-pre",
      hookStage = HookStage.PreExecute,
      register = engine => {
        engine.hooks.registerPreHook(
          HookStage.PreExecute,
          new FailingPreHook("failing-pre", 50),
          50
        )
      }
    )
    val engine = new io.sm8.core.EngineImpl
    plugin.setup(engine)
    val dispatcher = EngineHookDispatcher(engine.hooks)

    val provider = sampleProvider
    val result = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", ""),
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [io.sm8.core.engine.EngineError.HookFailed]
    err.asInstanceOf[io.sm8.core.engine.EngineError.HookFailed].name shouldBe "failing-pre"
    err.asInstanceOf[io.sm8.core.engine.EngineError.HookFailed].priority shouldBe 50
    err.asInstanceOf[io.sm8.core.engine.EngineError.HookFailed].stage shouldBe "PreExecute"
    err.asInstanceOf[io.sm8.core.engine.EngineError.HookFailed].message should include ("simulated")
  }

  test("dispatcher: PostHook that throws returns typed EngineError.HookFailed") {
    val plugin = new FailingHookPlugin(
      pluginName = "failing-post",
      hookStage = HookStage.PostExecute,
      register = engine => {
        engine.hooks.registerPostHook(
          HookStage.PostExecute,
          new FailingPostHook("failing-post", 60),
          60
        )
      }
    )
    val engine = new io.sm8.core.EngineImpl
    plugin.setup(engine)
    val dispatcher = EngineHookDispatcher(engine.hooks)

    val provider = sampleProvider
    val result = EngineService.runQueryWithHooks(
      request    = QueryRequest("m", Nil, Nil, "", ""),
      model      = sampleModel,
      registry   = registryWith(provider),
      cache      = ResultCache.NoOp,
      dispatcher = dispatcher
    )

    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err shouldBe a [io.sm8.core.engine.EngineError.HookFailed]
    err.asInstanceOf[io.sm8.core.engine.EngineError.HookFailed].name shouldBe "failing-post"
  }
}
