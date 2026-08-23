/*
 * SM8 Semantic Graph PostResolve Observer Spec (PR-161).
 *
 * Verifies that the observer publishes a `GraphSnapshot` to
 * `context.meta` at `GraphSnapshot.MetaKey`, and that the snapshot's
 * `dependents` field is correctly populated (the impact-analysis
 * data). The dependents list is the reverse-closure of every node:
 * "if I change field X, which calc-measures break?" — answered by
 * `dependents(dim X)`.
 *
 * Per the established `Context` API (final case class at
 * sm8-core/.../sdk/Context.scala): we construct a Context
 * directly and assert on the `meta` field after the observer
 * returns. The observer's `run` returns `context.copy(meta = ...)`.
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.{EngineHookRequest, QueryRequest => CoreQueryRequest}
import io.sm8.core.expr.Expr
import io.sm8.core.model._
import io.sm8.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GraphPostResolveObserverSpec extends AnyFunSuite with Matchers {

  // Fixture: a calc-measure that reads a dimension field directly.
  // The graph walker creates edge `total -> amount`, so
  // dependents("amount") = {"total"}.
  private val fixtureModel: Model = Model
    .of(
      name = "obs_fixture",
      version = 1,
      description = None,
      dimensions = List(
        Dimension(
          name = "amount",
          expr = Expr.FieldRef("amount"),
          dataType = Some(SealedDataType.Int)
        )
      ),
      measures = List.empty,
      defaultPolicies = ModelPolicyDefaults(
        MaterializePolicy.None,
        CachePolicy.NoCache,
        AuditPolicy.NoAudit
      ),
      source = SourceRef.byName("in-memory", "x"),
      status = ModelStatus.Published,
      filters = List.empty,
      calculatedMeasures = List(
        CalculatedMeasure(
          name = "total",
          expr = Expr.Add(
            Expr.FieldRef("amount"),
            Expr.Literal(io.sm8.core.expr.LiteralValue.LongValue(0L), SealedDataType.Int)
          )
        )
      ),
      joins = List.empty
    )
    .toOption
    .get

  test("observer publishes GraphSnapshot to context.meta at MetaKey") {
    val observer = new GraphPostResolveObserver
    val baseCtx = io.sm8.sdk.Context(
      stage = io.sm8.sdk.PipelineStage.Resolve,
      request = EngineHookRequest(
        fixtureModel,
        CoreQueryRequest(model = "obs_fixture"),
        "obs-cache-key"
      )
    )
    val afterRun = observer.run(baseCtx)
    afterRun.meta.contains(GraphSnapshot.MetaKey) shouldBe true
    val snapshot =
      afterRun.meta(GraphSnapshot.MetaKey).asInstanceOf[GraphSnapshot]
    snapshot.hasCycle shouldBe false
    snapshot.cycleError shouldBe None
    snapshot.danglingRightNodes shouldBe empty
  }

  test("observer populates snapshot.dependents for impact analysis") {
    val observer = new GraphPostResolveObserver
    val baseCtx = io.sm8.sdk.Context(
      stage = io.sm8.sdk.PipelineStage.Resolve,
      request = EngineHookRequest(
        fixtureModel,
        CoreQueryRequest(model = "obs_fixture"),
        "obs-cache-key"
      )
    )
    val afterRun = observer.run(baseCtx)
    val snapshot =
      afterRun.meta(GraphSnapshot.MetaKey).asInstanceOf[GraphSnapshot]

    // Per the fixture: 'total' (calc-measure) reads 'amount' field.
    // Edge: total -> amount. dependents("amount") = {"total"}.
    val amountNode = GraphNode("obs_fixture", "amount")
    val amountDeps = snapshot.dependents.getOrElse(amountNode, List.empty)
    amountDeps should contain (GraphNode("obs_fixture", "total"))

    // 'total' itself has no incoming edges; its dependents list
    // is empty.
    val totalNode = GraphNode("obs_fixture", "total")
    snapshot.dependents.getOrElse(totalNode, List.empty) shouldBe empty
  }
}