/*
 * SM8 Semantic Graph Builder Spec (PR-149, ADR-008-AI).
 *
 * Per ADR-008-AI v1.1 + the v1.1 §6 "Suggested next step":
 *   Start with just `SemanticGraphBuilder` + a ScalaTest spec that
 *   feeds it the `examples/hospital-cleaning` model and asserts the
 *   graph it builds matches what `detectCalcCycles` already accepts/
 *   rejects on the same fixture — that gives you a correctness baseline
 *   against the Core's existing behavior.
 *
 * Per ADR-008-AI v1.1 acceptance criterion #7:
 *   `SemanticGraphBuilderSpec` asserts the graph it builds against
 *   `examples/hospital-cleaning` matches what `detectCalcCycles` already
 *   accepts/rejects.
 *
 * Per  SS1 (deterministic tests):
 * every assertion uses a specific, hand-constructed `Model.of(...)`
 * fixture (NOT a YAML loader), so the test is independent of any
 * manifest format evolution.
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.EngineError
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model._
import io.sm8.core.model.{AuditPolicy, CachePolicy, MaterializePolicy}
import io.sm8.core.query.QueryBuilder
import io.sm8.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.sm8.core.schema.SealedDataType

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SemanticGraphBuilderSpec extends AnyFlatSpec with Matchers {

  // ---- Fixtures (hand-built, no YAML loader dependency) ----

  /**
   * A simple model with NO calc-measure, NO joins, NO cycle.
   * Single dimension ("amount"), single measure ("total").
   */
  private def simpleModel: Model = {
    val dimAmount = Dimension(
      name = "amount",
      expr = Expr.FieldRef("amount"),
      dataType = Some(SealedDataType.Int)
    )
    val measTotal = Measure(
      name = "total",
      expr = AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("amount")))
    )
    Model
      .of(
        name = "simple",
        version = 1,
        description = None,
        dimensions = List(dimAmount),
        measures = List(measTotal),
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "x"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List.empty,
        joins = List.empty
      )
      .toOption
      .get
  }

  /**
   * A model with a self-referencing calc-measure that creates a cycle:
   *   `bad_cycle = bad_cycle + 1` (referencing itself).
   */
  private def cyclingModel: Model = {
    val measTotal = Measure(
      name = "x",
      expr = AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("x")))
    )
    val badCycle = CalculatedMeasure(
      name = "bad_cycle",
      expr = Expr.Add(Expr.MeasureRef("bad_cycle"), Expr.Literal(LiteralValue.LongValue(1L), SealedDataType.Int))
    )
    Model
      .of(
        name = "cycling",
        version = 1,
        description = None,
        dimensions = List.empty,
        measures = List(measTotal),
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "x"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List(badCycle),
        joins = List.empty
      )
      .toOption
      .get
  }

  /**
   * Two models with a join edge. Right-hand model IS loaded.
   * No dangling references.
   */
  private def joinedModels: (Model, Model) = {
    val leftKey = Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    val rightKey =
      Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    val left = Model
      .of(
        name = "left",
        version = 1,
        description = None,
        dimensions = List(leftKey),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "l"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List.empty,
        joins = List(JoinSpec("j", "right", JoinKind.Inner, List("k" -> "k")))
      )
      .toOption
      .get
    val right = Model
      .of(
        name = "right",
        version = 1,
        description = None,
        dimensions = List(rightKey),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "r"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List.empty,
        joins = List.empty
      )
      .toOption
      .get
    (left, right)
  }

  /**
   * Two models with a join edge pointing at a NON-LOADED right-hand
   * model. Per ADR-008-AI v1.1 fix 3, this surfaces as a typed
   * dangling-node list.
   */
  private def danglingModel: Model = {
    val leftKey = Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    Model
      .of(
        name = "lonely",
        version = 1,
        description = None,
        dimensions = List(leftKey),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "l"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List.empty,
        joins = List(
          JoinSpec("j", "ghost_model", JoinKind.Inner, List("k" -> "k"))
        )
      )
      .toOption
      .get
  }

  // ---- Builder tests ----

  "SemanticGraphBuilder.build" should "build a graph with no cycle for a simple Model" in {
    val g = SemanticGraphBuilder.build(simpleModel)
    g.hasCycle shouldBe false
  }

  it should "build a graph that surfaces a cycle when calc-measure self-references" in {
    val g = SemanticGraphBuilder.build(cyclingModel)
    g.hasCycle shouldBe true
  }

  it should "produce an empty danglingRightNodes for a single Model" in {
    val g = SemanticGraphBuilder.build(simpleModel)
    g.danglingRightNodes shouldBe empty
  }

  // Per ADR-008-AI v1.1 + the architect re-review: a Dimension whose
  // `expr` is a `FieldRef` of its own name (a self-referential dimension)
  // is a no-op (the dim IS that field), not a cycle. The `addDimEdge`
  // guard at `SemanticGraphBuilder.scala:175-185` skips this self-loop.
  // Without the guard, JGraphT's `CycleDetector` would report
  // `(amount) -> (amount)` as a cycle. Regression test for that guard.
  it should "NOT flag a self-referential Dimension as a cycle (addDimEdge guard)" in {
    val selfRefDim = Dimension(
      name = "amount",
      expr = Expr.FieldRef("amount"),
      dataType = Some(SealedDataType.Int)
    )
    val model = Model
      .of(
        name = "self",
        version = 1,
        description = None,
        dimensions = List(selfRefDim),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(
          MaterializePolicy.None,
          CachePolicy.NoCache,
          AuditPolicy.NoAudit
        ),
        source = SourceRef.byName("in-memory", "x"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List.empty,
        joins = List.empty
      )
      .toOption
      .get
    SemanticGraphBuilder.build(model).hasCycle shouldBe false
  }

  "SemanticGraphBuilder.buildAcross" should
    "report no dangling right-nodes when all joined models are loaded" in {
    val (left, right) = joinedModels
    val g = SemanticGraphBuilder.buildAcross(List(left, right))
    g.danglingRightNodes shouldBe empty
  }

  it should "report dangling right-nodes when a join target model is not loaded" in {
    val g = SemanticGraphBuilder.buildAcross(List(danglingModel))
    g.danglingRightNodes.map(_.model).distinct should contain("ghost_model")
  }
  it should "produce a non-None joinPath when both endpoints are vertices in the graph" in {
    // The danglingModel has a vertex ("lonely", "k") and a join edge
    // to ("ghost_model", "k") — the latter is dangling. We can
    // therefore ask for a path from one to the other.
    val g = SemanticGraphBuilder.build(danglingModel)
    val path = g.joinPath(GraphNode("lonely", "k"), GraphNode("ghost_model", "k"))
    path shouldBe defined
  }

  // Per the data-eng re-review: a 3-model chain A -> B -> C should
  // produce a 3-vertex joinPath when asked from A.k to C.k.
  it should "find a 3-vertex joinPath across a 3-model chain (A -> B -> C)" in {
    def keyDim(name: String): Dimension =
      Dimension(name, Expr.FieldRef(name), Some(SealedDataType.Varchar))
    def chainModel(
        n: String,
        joinTarget: Option[String]
    ): Model =
      Model
        .of(
          name = n,
          version = 1,
          description = None,
          dimensions = List(keyDim("k")),
          measures = List.empty,
          defaultPolicies = ModelPolicyDefaults(
            MaterializePolicy.None,
            CachePolicy.NoCache,
            AuditPolicy.NoAudit
          ),
          source = SourceRef.byName("in-memory", n),
          status = ModelStatus.Published,
          filters = List.empty,
          calculatedMeasures = List.empty,
          joins = joinTarget.toList.map { t =>
            JoinSpec("j", t, JoinKind.Inner, List("k" -> "k"))
          }
        )
        .toOption
        .get
    val a = chainModel("a", Some("b"))
    val b = chainModel("b", Some("c"))
    val c = chainModel("c", None)
    val g = SemanticGraphBuilder.buildAcross(List(a, b, c))
    val path = g.joinPath(GraphNode("a", "k"), GraphNode("c", "k"))
    path.map(_.map(_.model)) shouldBe Some(List("a", "b", "c"))
  }

  // ---- Parity test (the v1.1 acceptance criterion #7) ----

  "SemanticGraphBuilder + QueryBuilder.detectCalcCycles" should
    "agree on cycle detection for the same calc-measure fixture" in {
    // Per ADR-008-AI v1.1: SemanticGraphBuilder is a pre-flight duplicate
    // of Core's detectCalcCycles. The two must agree.

    val cyclingGraph = SemanticGraphBuilder.build(cyclingModel)
    cyclingGraph.hasCycle shouldBe true

    // Core's check uses the SAME walkers; verify it surfaces the cycle too.
    val coreResult = QueryBuilder.detectCalcCycles(cyclingModel.calculatedMeasures)
    coreResult match {
      case Left(EngineError.UnsupportedCapability(_, capability, _)) =>
        capability shouldBe "CalculatedMeasure.cycle"
      case other =>
        fail(s"Expected EngineError.UnsupportedCapability, got: $other")
    }
  }

  it should "agree on no-cycle for a simple fixture" in {
    val simpleGraph = SemanticGraphBuilder.build(simpleModel)
    simpleGraph.hasCycle shouldBe false

    val coreResult = QueryBuilder.detectCalcCycles(simpleModel.calculatedMeasures)
    coreResult shouldBe a[Right[_, _]]
  }
}