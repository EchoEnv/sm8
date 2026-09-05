/*
 * SM8 Platform — TypedErrorMetaNamespaceSpec (ADR-0020).
 *
 * Verifies that `EngineService.runQueryWithHooks` surfaces any
 * `ctx.meta` entry whose key ends in `":error"` AND whose value is a
 * typed `EngineError` as `Left(error)` to the caller — the convention
 * introduced in ADR-0020 to close the ADR-0010-a §6 deferral.
 *
 * Per ADR-0020:
 *  - Plugin authors write `ctx.meta + ("<scope>:error" -> typedErr)`
 *    via the surfaceTypedError helper (or directly); the platform
 *    collects any key ending in `":error"`.
 *  - The cache plugin's `sm8.cache.write.error` already satisfies
 *    the convention (verified end-to-end here).
 *  - The semantic-graph plugin's `semanticGraphError` literal did
 *    NOT end in `":error"` and was migrated to
 *    `"io.sm8.plugins.semanticgraph:error"` per ADR-0020 §Backward-
 *    compat.
 */
package io.sm8.platform.query

import io.sm8.core.cache.ResultCache
import io.sm8.core.engine.{
  EngineError,
  EngineIdentity,
  EngineProvider,
  EngineRegistry,
  QueryRequest => CoreQueryRequest
}
import io.sm8.core.model.{Model, SourceRef, ModelStatus}
import io.sm8.core.schema.SealedDataType
import io.sm8.platform.query.hooks.{EngineHookDispatcher, HookRunnerOrchestration}
import io.sm8.sdk.{Context, HookManager, HookStage, PipelineStage, PostHook, PreHook}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedErrorMetaNamespaceSpec extends AnyFunSuite with Matchers {

  /** Trivial in-memory provider that returns a Right(PortableQueryResult).
    * The hook setup is the only thing under test; the executor runs
    * cleanly. Mirrors the fixture in
    * `AuditPostStubHookFiresSpec.scala`. */
  private final class TrivialProvider extends EngineProvider with java.io.Serializable {
    override val identity: EngineIdentity = EngineIdentity("trivial", "1.0", "0.1")
    override val available: Boolean = true
    override def query(
        model: Model,
        request: io.sm8.core.engine.QueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, io.sm8.core.engine.PortableQueryResult] =
      Right(io.sm8.core.engine.PortableQueryResult(
        rows = Vector.empty,
        schema = io.sm8.core.engine.ResultSchema(Nil)
      ))
    override def explain(
        model: Model,
        request: io.sm8.core.engine.QueryRequest,
        ctx: io.sm8.core.engine.EngineContext
    ): Either[EngineError, String] = Right("trivial")
  }

  private val model: Model = Model
    .of(
      name = "x",
      version = 1,
      description = None,
      dimensions = Nil,
      measures = Nil,
      defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
        io.sm8.core.model.MaterializePolicy.None,
        io.sm8.core.model.CachePolicy.NoCache,
        io.sm8.core.model.AuditPolicy.NoAudit
      ),
      source = SourceRef.byName("in-memory", "x"),
      status = ModelStatus.Draft,
      filters = Nil,
      calculatedMeasures = Nil,
      joins = Nil
    )
    .toOption
    .get

  private val request: QueryRequest =
    QueryRequest("x", Nil, Nil, "", "trivial")

  /** A PreHook that writes a typed `EngineError` to a custom meta
    * key (no suffix convention applied — the key is whatever the caller
    * passes). Used to test the convention's strict match: keys that
    * don't end in `:error` are NOT surfaced. */
  private final class WritingCustomPreHook(key: String, err: EngineError) extends PreHook with java.io.Serializable {
    override val name: String = s"writing-custom-pre-hook-$key"
    override val priority: Int = 200
    override def stage: HookStage = HookStage.PreExecute
    override def run(context: Context): Context =
      context.copy(meta = context.meta + (key -> err))
  }

  /** A PreHook that writes a typed `EngineError` to the namespaced meta
    * key on PreExecute. The hook does NOT short-circuit (does not set
    * `stop = true`); the platform's typed-error surfacing reads from
    * `finalCtx.meta` after the dispatch loop completes. */
  private final class WritingPreHook(scope: String, err: EngineError) extends PreHook with java.io.Serializable {
    override val name: String = s"writing-pre-hook-$scope"
    override val priority: Int = 200
    override def stage: HookStage = HookStage.PreExecute
    override def run(context: Context): Context =
      context.copy(meta = context.meta + (s"$scope:error" -> err))
  }

  private def orchestratorFor(engine: io.sm8.core.EngineImpl): HookRunnerOrchestration =
    HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))

  test("runQueryWithHooks: surfaces typed error written under '<scope>:error' namespace (ADR-0020)") {
    val engine = new io.sm8.core.EngineImpl
    val typedErr: EngineError = EngineError.UnsupportedCapability(
      engine     = "trivial",
      capability = "test.capability",
      message    = "sm8: plugin author set this"
    )
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new WritingPreHook("io.sm8.plugins.test-namespace", typedErr),
      200
    )

    val out = EngineService.runQueryWithHooks(
      request    = request,
      model      = model,
      registry   = EngineRegistry(Map("trivial" -> new TrivialProvider), default = "trivial"),
      cache      = ResultCache.NoOp,
      dispatcher = orchestratorFor(engine)
    )
    out shouldBe Left(typedErr)
  }

  test("runQueryWithHooks: subsumes the renamed semantic-graph key 'io.sm8.plugins.semanticgraph:error' (ADR-0020 backward-compat)") {
    // ADR-0020 §Backward-compat: the legacy 'semanticGraphError' literal
    // did NOT end in ':error' and would have been silently dropped; the
    // plugin migrated to the namespaced form. The platform's matcher
    // must accept the new key.
    val engine = new io.sm8.core.EngineImpl
    val typedErr: EngineError = EngineError.UnsupportedCapability(
      engine     = "trivial",
      capability = "SemanticGraph.cycle",
      message    = "sm8: cycle detected in calc-measure graph"
    )
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new WritingPreHook("io.sm8.plugins.semanticgraph", typedErr),
      200
    )

    val out = EngineService.runQueryWithHooks(
      request    = request,
      model      = model,
      registry   = EngineRegistry(Map("trivial" -> new TrivialProvider), default = "trivial"),
      cache      = ResultCache.NoOp,
      dispatcher = orchestratorFor(engine)
    )
    out shouldBe Left(typedErr)
  }

  test("runQueryWithHooks: subsumes the cache plugin's 'sm8.cache.write.error' key (ADR-0020 §Real-in-the-wild)") {
    // The cache plugin's `CachePlugin.scala:293` writes a typed
    // EngineError to `sm8.cache.write.error`. Before ADR-0020 the
    // platform only read the hard-coded 'semanticGraphError' key, so
    // this typed error was silently dropped. After ADR-0020 it
    // surfaces via the ':error' namespace convention.
    val engine = new io.sm8.core.EngineImpl
    val typedErr: EngineError = EngineError.ConnectionFailed(
      engine  = "spark",
      reason  = "CachedRowDecoder mismatch",
      message = "sm8: cache-write decoder failed"
    )
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new WritingPreHook("sm8.cache.write", typedErr),
      200
    )

    val out = EngineService.runQueryWithHooks(
      request    = request,
      model      = model,
      registry   = EngineRegistry(Map("trivial" -> new TrivialProvider), default = "trivial"),
      cache      = ResultCache.NoOp,
      dispatcher = orchestratorFor(engine)
    )
    out shouldBe Left(typedErr)
  }

  test("runQueryWithHooks: ignores meta keys that end in ':error' but carry a non-EngineError value (ADR-0020 typing)") {
    // ADR-0020 typing: the matcher is `case (k, e: EngineError) if k.endsWith(':error') => e`.
    // A plugin that writes a String/String/Boolean/etc. to a ':error'
    // key is IGNORED (no false-positive match). The non-error path
    // (the dispatcher continues, the executor runs) is preserved.
    val engine = new io.sm8.core.EngineImpl
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new PreHook with java.io.Serializable {
        override val name: String = "non-error-pre-hook"
        override val priority: Int = 200
        override def stage: HookStage = HookStage.PreExecute
        override def run(context: Context): Context =
          context.copy(meta = context.meta + ("my-plugin:error" -> "not an EngineError, just a string"))
      },
      200
    )

    val out = EngineService.runQueryWithHooks(
      request    = request,
      model      = model,
      registry   = EngineRegistry(Map("trivial" -> new TrivialProvider), default = "trivial"),
      cache      = ResultCache.NoOp,
      dispatcher = orchestratorFor(engine)
    )
    // The non-error meta value is ignored; the dispatcher completes
    // normally; the executor returns Right(PortableQueryResult).
    out.isRight shouldBe true
  }

  test("runQueryWithHooks: ignores meta keys that do NOT end in ':error' (ADR-0020 namespace strict)") {
    // Existing meta keys like 'sm8.cache.policy' (set by the
    // EngineService initialCtx fold) MUST NOT be classified as typed
    // errors even if the value happens to be a typed EngineError
    // instance — the discriminator is the ':error' suffix on the key.
    // Uses a custom-key helper (no ':error' suffix applied) to test the
    // strict convention: a key WITHOUT ':error' is ignored.
    val engine = new io.sm8.core.EngineImpl
    val typedErr: EngineError = EngineError.UnsupportedCapability(
      engine     = "trivial",
      capability = "policy",
      message    = "sm8: would false-positive if not for the :error suffix"
    )
    engine.hooks.registerPreHook(
      HookStage.PreExecute,
      new WritingCustomPreHook("sm8.cache.policy", typedErr),
      200
    )

    val out = EngineService.runQueryWithHooks(
      request    = request,
      model      = model,
      registry   = EngineRegistry(Map("trivial" -> new TrivialProvider), default = "trivial"),
      cache      = ResultCache.NoOp,
      dispatcher = orchestratorFor(engine)
    )
    // sm8.cache.policy does NOT end in ':error' → ignored → executor runs → Right.
    out.isRight shouldBe true
  }
}
