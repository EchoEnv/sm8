/*
 * SM8 Platform — GraphPostResolveObserverSnapshotSpec (ADR-010-a v0.3).
 *
 * Regression test for the silent-no-op defect discovered in the
 * 2026-08-26 dual senior codebase review: `GraphPostResolveObserver`
 * (registered at `HookStage.PostResolve`) was silently inert in
 * production since PR #32 (commit `daac360`, 2026-08-14). The
 * snapshot it writes to `ctx.meta` at `GraphSnapshot.MetaKey` is
 * the only writer the `MetaInspectorService` reads from; without
 * the orchestrator driving PostResolve, `sm8 inspect` returns
 * `present=false` forever.
 *
 * This spec wires a PostResolve capture observer (mirroring the
 * `MetaCaptureObserver` pattern from `sm8-server`) onto the
 * engine + the real `GraphPostResolveObserver`, runs a request
 * through `EngineService.runQueryWithHooks`, and asserts the
 * `AtomicReference` carries `GraphSnapshot.MetaKey`.
 *
 * Per [[debug-mantra-mindset]] §5 (verify): the assertion is on
 * observable state (the `AtomicReference`'s payload), not on
 * internal log lines. The orchestrator's end-to-end behavior
 * is what we're verifying.
 *
 * Per ADR-009-d ctx.meta fold pattern: the snapshot lives at the
 * canonical `GraphSnapshot.MetaKey` namespace.
 */
package io.sm8.platform.query

import java.util.concurrent.atomic.AtomicReference

import io.sm8.core.cache._
import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineRegistry,
  PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.sm8.core.engine.{ QueryRequest => CoreQueryRequest }
import io.sm8.core.expr.Expr
import io.sm8.core.model.{CalculatedMeasure, Dimension, MaterializePolicy, CachePolicy, AuditPolicy,
  Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.sm8.core.schema.{Field, SealedDataType}
import io.sm8.platform.query.hooks.{EngineHookDispatcher, HookRunnerOrchestration}
import io.sm8.plugins.semanticgraph.{GraphPostResolveObserver, GraphSnapshot}
import io.sm8.sdk.{Context, HookStage, Plugin, PostHook}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GraphPostResolveObserverSnapshotSpec extends AnyFunSuite with Matchers {

  // A trivial engine provider (returns a single-row PQR so the
  // happy path completes).
  private final class TrivialProvider extends EngineProvider {
    override val identity: EngineIdentity = EngineIdentity("trivial", "0.0.0", "0.0.0")
    override val available: Boolean = true
    override def query(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext): Either[EngineError, PortableQueryResult] =
      Right(PortableQueryResult(
        schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int))),
        rows   = Vector(ResultRow(
          values = List(ResultValue.IntV(1L)),
          schema = ResultSchema(List(Field.nonNull("v", SealedDataType.Int)))
        ))
      ))
    override def explain(
        model: Model,
        request: CoreQueryRequest,
        ctx: io.sm8.core.engine.EngineContext): Either[EngineError, String] = Right("trivial")
  }

  /**
   * A plugin that mirrors `sm8-server`'s `MetaCaptureObserver`: at
   * PostExecute (high priority), snapshots the most recent
   * `context.meta` into a caller-supplied `AtomicReference`. This
   * stands in for the production reader; we use PostExecute
   * (rather than PostResolve) so the capture fires AFTER the
   * graph snapshot has been written by `GraphPostResolveObserver`.
   *
   * Observer semantics: always fires, even on the short-circuit
   * (cache-HIT) path.
   */
  private final class CaptureMetaAtPostExec(target: AtomicReference[Map[String, Any]])
      extends Plugin with java.io.Serializable {
    override def setup(engine: io.sm8.sdk.Engine): Unit = {
      engine.hooks.registerPostHook(
        HookStage.PostExecute,
        new PostHook with java.io.Serializable {
          override val name: String = "TestCaptureMeta"
          override val stage: HookStage = HookStage.PostExecute
          override val priority: Int = 999
          override def run(context: Context): Context = {
            target.set(context.meta)
            context
          }
          override val runsOnStop: Boolean = true
        },
        999
      )
    }
  }

  /** A model with one calc-measure + one dimension — the snapshot
    * builder needs at least one vertex + edge to populate the
    * `dependents` map. */
  private val model: Model = Model.of(
    name = "graph_fixture",
    version = 1,
    description = None,
    dimensions = List(
      Dimension(name = "amount", expr = Expr.FieldRef("amount"),
        dataType = Some(SealedDataType.Int))
    ),
    measures = Nil,
    defaultPolicies = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache       = CachePolicy.NoCache,
      audit       = AuditPolicy.NoAudit
    ),
    source = SourceRef.byName("in-memory", "x"),
    status = ModelStatus.Draft,
    filters = Nil,
    calculatedMeasures = List(
      CalculatedMeasure(
        name = "total",
        expr = Expr.Add(
          Expr.FieldRef("amount"),
          Expr.Literal(io.sm8.core.expr.LiteralValue.LongValue(0L), SealedDataType.Int)
        )
      )
    ),
    joins = Nil
  ).toOption.get

  test("runQueryWithHooks: PostResolve fires — GraphSnapshot.MetaKey appears in captured context.meta") {
    // The orchestrator's job: drive the PostResolve stage. Before
    // the fix, PostResolve hooks never fired (dispatcher hardcoded
    // PipelineStage.Execute). After the fix, the GraphSnapshot
    // observer publishes a typed snapshot at MetaKey.
    val engine = new io.sm8.core.EngineImpl
    // Real GraphPostResolveObserver (the inert plugin that becomes
    // live after the orchestrator fix). Register via the engine's
    // HookManager since the observer is a PostHook, not a Plugin.
    val observer = new GraphPostResolveObserver
    engine.hooks.registerPostHook(observer.stage, observer, observer.priority)
    val captured = new AtomicReference[Map[String, Any]](Map.empty)
    new CaptureMetaAtPostExec(captured).setup(engine)
    val orchestrator = HookRunnerOrchestration(EngineHookDispatcher(engine.hooks))

    val registry = EngineRegistry(
      Map("trivial" -> new TrivialProvider),
      default = "trivial"
    )

    val out = EngineService.runQueryWithHooks(
      request    = QueryRequest("graph_fixture", Nil, Nil, "", "trivial"),
      model      = model,
      registry   = registry,
      cache      = ResultCache.NoOp,
      dispatcher = orchestrator
    )

    out.isRight shouldBe true

    // The capture observer's AtomicReference now carries the
    // post-PostResolve context.meta, which includes the
    // GraphSnapshot at its well-known MetaKey.
    val capturedMeta = captured.get()
    capturedMeta.contains(GraphSnapshot.MetaKey) shouldBe true
    // The snapshot itself is a typed GraphSnapshot.
    val snapshot = capturedMeta(GraphSnapshot.MetaKey)
    snapshot shouldBe a [GraphSnapshot]
    snapshot.asInstanceOf[GraphSnapshot].hasCycle shouldBe false
  }
}