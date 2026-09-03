/*
 * SM8 Semantic Graph Snapshot Spec (C9-T4).
 *
 * Verifies the `toMetaValue` wire-shape invariant that the prior
 * sort lambdas depended on by structural accident:
 *
 *   - `dependents` is sorted by `(node.model, node.field)`.
 *   - `joinCardinalities` is sorted by `((from.model, from.field),
 *     (to.model, to.field))`.
 *   - Sort is stable across calls (deterministic).
 *
 * Per C9 research (chick @ #298): the prior `asInstanceOf[Map[String,
 * Any]]("model").toString` lambdas at lines 75-76, 82-85 were type-
 * correct only because the literal two lines up was literally
 * `Map("model" -> n.model, "field" -> n.field)`. C9-T4(a) extracted
 * a `nodeKey` helper. This spec pins the sort contract so any
 * future refactor that breaks the invariant fails loudly.
 *
 * Read-only verification: this spec is the regression test for the
 * typing fix. The existing `GraphPostResolveObserverSpec` does NOT
 * call `toMetaValue` (it asserts on typed `GraphSnapshot` fields),
 * so this spec fills the coverage gap.
 *
 * Per AGENTS.md and the established project convention, this spec
 * uses `AnyFlatSpec with Matchers` (not `AnyFunSuite`).
 */
package io.sm8.plugins.semanticgraph

import io.sm8.core.engine.EngineError

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GraphSnapshotSpec extends AnyFlatSpec with Matchers {

  // Fixture helpers

  private def node(model: String, field: String): GraphNode =
    GraphNode(model, field)

  private def snapshot(
      dependents: Map[GraphNode, List[GraphNode]] = Map.empty,
      joinCardinalities: Map[(GraphNode, GraphNode), Long] = Map.empty
  ): GraphSnapshot =
    GraphSnapshot(
      vertices = Nil,
      edges = Nil,
      hasCycle = false,
      cycleError = None,
      danglingRightNodes = Nil,
      dependents = dependents,
      joinCardinalities = joinCardinalities
    )

  // nodeKey behavior — verified through the public toMetaValue surface

  "GraphSnapshot.toMetaValue" should "sort dependents by (node.model, node.field)" in {
    // Inserted out-of-order on purpose; expected order = alpha by (model, field).
    val nA = node("a", "x")
    val nB = node("b", "x")
    val nC = node("a", "y")
    val s = snapshot(dependents = Map(
      nB -> List(nC),
      nA -> List(nB),
      nC -> List(nA)
    ))
    val wire = s.toMetaValue
    val dependentsWire = wire("dependents").asInstanceOf[List[Map[String, Any]]]
    // Pull (model, field) for each "node" entry to verify order.
    def k(m: Map[String, Any]): (String, String) =
      (m("node").asInstanceOf[Map[String, Any]]("model").toString,
       m("node").asInstanceOf[Map[String, Any]]("field").toString)
    dependentsWire.map(k) shouldBe List(("a", "x"), ("a", "y"), ("b", "x"))
  }

  it should "sort joinCardinalities by ((from.model, from.field), (to.model, to.field))" in {
    val from1 = node("orders", "id")
    val to1 = node("users", "id")
    val from2 = node("orders", "customer_id")
    val to2 = node("users", "id")
    val s = snapshot(joinCardinalities = Map(
      (from2, to2) -> 50L,
      (from1, to1) -> 10L
    ))
    val wire = s.toMetaValue
    val jcWire = wire("joinCardinalities").asInstanceOf[List[Map[String, Any]]]
    def pair(m: Map[String, Any]): ((String, String), (String, String)) = {
      val f = m("from").asInstanceOf[Map[String, Any]]
      val t = m("to").asInstanceOf[Map[String, Any]]
      ((f("model").toString, f("field").toString),
       (t("model").toString, t("field").toString))
    }
    jcWire.map(pair) shouldBe List(
      (("orders", "customer_id"), ("users", "id")),
      (("orders", "id"),         ("users", "id"))
    )
  }

  it should "be deterministic across calls (same input → same output)" in {
    val n1 = node("m", "a")
    val n2 = node("m", "b")
    val n3 = node("m", "c")
    val s = snapshot(
      dependents = Map(n1 -> List(n2), n2 -> List(n3), n3 -> List(n1)),
      joinCardinalities = Map((n1, n2) -> 1L, (n2, n3) -> 2L, (n3, n1) -> 3L)
    )
    val first = s.toMetaValue
    val second = s.toMetaValue
    first("dependents") shouldBe second("dependents")
    first("joinCardinalities") shouldBe second("joinCardinalities")
  }

  it should "round-trip cycleError: Option[EngineError] via .toString" in {
    val s = GraphSnapshot(
      vertices = Nil,
      edges = Nil,
      hasCycle = true,
      cycleError = Some(EngineError.UnsupportedCapability(
        engine = "semantic-graph-plugin",
        capability = "SemanticGraph.cycle",
        message = "test cycle"
      )),
      danglingRightNodes = Nil
    )
    val cycleWire = s.toMetaValue("cycleError")
    // cycleError is wrapped in `cycleError.map(_.toString)` at line 67,
    // so the wire value is `Option[String]` — Jackson sees Some/None.
    cycleWire shouldBe a [Some[_]]
    val cycleStr = cycleWire.asInstanceOf[Some[String]].value
    cycleStr should include ("SemanticGraph.cycle")
    cycleStr should include ("test cycle")
  }

  it should "have cycleError = None when there is no cycle" in {
    val s = GraphSnapshot(
      vertices = Nil,
      edges = Nil,
      hasCycle = false,
      cycleError = None,
      danglingRightNodes = Nil
    )
    s.toMetaValue("cycleError") shouldBe None
  }

  it should "produce empty sorted lists for empty dependents / joinCardinalities" in {
    val s = snapshot()
    val wire = s.toMetaValue
    wire("dependents") shouldBe Nil
    wire("joinCardinalities") shouldBe Nil
  }
}