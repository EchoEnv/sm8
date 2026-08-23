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

  it should "report the models referencing a target model via a join (cross-model discovery)" in {
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
    g.referencingModels("c") shouldBe List("b")
    g.referencingModels("b") shouldBe List("a")
    g.referencingModels("a") shouldBe List.empty
  }

  it should "exclude same-model edges from cross-model discovery" in {
    // A join that targets the model's OWN key (a self-join) creates a
    // same-model edge; referencingModels must never surface a model
    // as referencing itself, so the guard (source.model != targetModel)
    // is what makes this assertion pass. Without the guard this model
    // would report ["single"].
    val keyDim = Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    val model = Model
      .of(
        name = "single",
        version = 1,
        description = None,
        dimensions = List(keyDim),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(
          MaterializePolicy.None,
          CachePolicy.NoCache,
          AuditPolicy.NoAudit
        ),
        source = SourceRef.byName("in-memory", "s"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List.empty,
        joins = List(JoinSpec("self", "single", JoinKind.Inner, List("k" -> "k")))
      )
      .toOption
      .get
    val g = SemanticGraphBuilder.build(model)
    // The self-join edge (single.k -> single.k) exists, proving the
    // guard excludes it: the model must not report itself.
    g.edges should contain((GraphNode("single", "k"), GraphNode("single", "k")))
    g.referencingModels("single") shouldBe List.empty
  }

  it should "expose a join cardinality estimate when a join declares one" in {
    val leftKey = Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    val rightKey =
      Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    def build(n: String, est: Option[Long]): Model =
      Model
        .of(
          name = n,
          version = 1,
          description = None,
          dimensions = List(leftKey),
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
          joins = if (n == "left")
            List(JoinSpec("j", "right", JoinKind.Inner, List("k" -> "k"),
              estimatedRows = est))
            else List.empty
        )
        .toOption
        .get
    val left = build("left", Some(5000L))
    val right = build("right", None)
    val g = SemanticGraphBuilder.buildAcross(List(left, right))
    g.joinCardinality(GraphNode("left", "k"), GraphNode("right", "k")) shouldBe
      Some(5000L)
    g.joinCardinalities shouldBe Map(
      (GraphNode("left", "k"), GraphNode("right", "k")) -> 5000L
    )
  }

  it should "report no cardinality for a join without an estimate" in {
    val (left, right) = joinedModels
    val g = SemanticGraphBuilder.buildAcross(List(left, right))
    g.joinCardinality(GraphNode("left", "k"), GraphNode("right", "k")) shouldBe None
    g.joinCardinalities shouldBe empty
  }


  it should "keep joinCardinality consistent with edge weights for duplicate key-pair joins" in {
    // Two differently-named joins aliasing the SAME (leftKey, rightKey)
    // pair with different estimates must not drift: the graph keeps
    // the FIRST edge's weight (JGraphT addEdge is a no-op on an
    // existing pair) and the exposure must report the same number.
    val leftKey = Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    val rightKey =
      Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    def build(n: String, joins: List[JoinSpec]): Model =
      Model
        .of(
          name = n,
          version = 1,
          description = None,
          dimensions = List(leftKey),
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
          joins = joins
        )
        .toOption
        .get
    val left = build(
      "left",
      List(
        JoinSpec("j1", "right", JoinKind.Inner, List("k" -> "k"), estimatedRows = Some(100L)),
        JoinSpec("j2", "right", JoinKind.Inner, List("k" -> "k"), estimatedRows = Some(200L))
      )
    )
    val right = build("right", List.empty)
    val g = SemanticGraphBuilder.buildAcross(List(left, right))
    val node = (GraphNode("left", "k"), GraphNode("right", "k"))
    // The edge keeps the first-wins weight; the exposure must agree.
    g.joinCardinality(node._1, node._2) shouldBe Some(100L)
    g.joinCardinalities shouldBe Map(node -> 100L)
    g.edges should contain(node)
  }

  it should "not surface the placeholder when the first alias of a duplicate pair lacks an estimate" in {
    // P2 (final review): when the FIRST join for a key pair declares
    // no estimate but a LATER alias does, the graph's first-wins edge
    // carries the 1.0 placeholder. The pair must NOT be reported as
    // estimated — joinCardinality returns None (no user-supplied
    // value on the winning edge), never the placeholder.
    val leftKey = Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    val rightKey =
      Dimension("k", Expr.FieldRef("k"), Some(SealedDataType.Varchar))
    def build(n: String, joins: List[JoinSpec]): Model =
      Model
        .of(
          name = n,
          version = 1,
          description = None,
          dimensions = List(leftKey),
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
          joins = joins
        )
        .toOption
        .get
    val left = build(
      "left",
      List(
        JoinSpec("j1", "right", JoinKind.Inner, List("k" -> "k")), // no estimate
        JoinSpec("j2", "right", JoinKind.Inner, List("k" -> "k"), estimatedRows = Some(200L))
      )
    )
    val right = build("right", List.empty)
    val g = SemanticGraphBuilder.buildAcross(List(left, right))
    val from = GraphNode("left", "k")
    val to = GraphNode("right", "k")
    g.joinCardinality(from, to) shouldBe None
    g.joinCardinalities shouldBe empty
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
  // -- Impact analysis: dependents() (PR-161) --
  //
  // dependents(node) returns every node that transitively depends
  // on `node` (the reverse-closure). Used for impact analysis:
  // "which calculated measures break if this dimension changes?"
  //
  // Graph edge semantics: the calc-measure walker creates edge
  // `(calc-name) -> (ref)` where `ref` is a field/measure the calc
  // references. So `dependents(d)` returns every calc-measure
  // that (transitively) references `d`.

  it should "dependents() returns the direct reverse-closure of a node" in {
    // Model where `total` (calc-measure) reads field `amount`:
    //   Edge: total -> amount
    //   dependents(amount) = {total}
    val model = Model
      .of(
        name = "dependents_model",
        version = 1,
        description = None,
        dimensions = List(
          Dimension(name = "amount", expr = Expr.FieldRef("amount"), dataType = Some(SealedDataType.Int))
        ),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "x"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List(
          CalculatedMeasure(
            name = "total",
            expr = Expr.Add(Expr.FieldRef("amount"), Expr.Literal(LiteralValue.LongValue(0L), SealedDataType.Int))
          )
        ),
        joins = List.empty
      )
      .toOption
      .get
    val g = SemanticGraphBuilder.build(model)
    val deps = g.dependents(GraphNode("dependents_model", "amount"))
    deps should contain (GraphNode("dependents_model", "total"))
    deps.size shouldBe 1
  }

  it should "dependents() returns the transitive reverse-closure" in {
    // Model where:
    //   - calc-measure `b` reads field `c` (edge: b -> c)
    //   - calc-measure `a` reads calc-measure `b` (edge: a -> b)
    // dependents(c) should include BOTH b (direct) and a (transitive).
    val model = Model
      .of(
        name = "deep",
        version = 1,
        description = None,
        dimensions = List(
          Dimension(name = "c", expr = Expr.FieldRef("c"), dataType = Some(SealedDataType.Int))
        ),
        measures = List.empty,
        defaultPolicies = ModelPolicyDefaults(MaterializePolicy.None, CachePolicy.NoCache, AuditPolicy.NoAudit),
        source = SourceRef.byName("in-memory", "x"),
        status = ModelStatus.Published,
        filters = List.empty,
        calculatedMeasures = List(
          CalculatedMeasure(
            name = "b",
            expr = Expr.Add(Expr.FieldRef("c"), Expr.Literal(LiteralValue.LongValue(0L), SealedDataType.Int))
          ),
          CalculatedMeasure(
            name = "a",
            expr = Expr.Add(Expr.MeasureRef("b"), Expr.Literal(LiteralValue.LongValue(0L), SealedDataType.Int))
          )
        ),
        joins = List.empty
      )
      .toOption
      .get
    val g = SemanticGraphBuilder.build(model)
    val deps = g.dependents(GraphNode("deep", "c"))
    deps should contain (GraphNode("deep", "b"))
    deps should contain (GraphNode("deep", "a"))
    // `c` itself should NOT be in its own dependents.
    deps should not contain (GraphNode("deep", "c"))
  }
}
