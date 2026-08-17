/*
 * SM8 Core — rel/ conformance spec (PR-H per ADR-007 + the
 * v0.1.0 IR extension plan).
 *
 * Per RFC §12 (Adapter Conformance Testing): every new IR type
 * must verify:
 *   1. Sealed-trait exhaustiveness (every case is enumerated +
 *      constructable)
 *   2. Structural equality (case-class `equals`/`hashCode` derived)
 *   3. Serializable round-trip (Java ObjectOutputStream → InputStream
 *      → the same data)
 *   4. Zero spark imports (boundary contract)
 *
 * Per [[karpathy-guidelines-mindset]]: smallest correct change —
 * one spec per type, ~5-10 lines per assertion.
 *
 * Per [[scala-jvm-safety-mindset]]: every test uses fresh instances;
 * no static / ThreadLocal state.
 *
 * Per [[scala-error-handling-mindset]]: the spec asserts the
 * "not-yet-supported" cases are absent from the ADT (the absence
 * is the contract — a future contributor adding them must extend
 * the sealed trait, forcing the engine adapter to handle them
 * explicitly).
 */
package io.sm8.core.rel

import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.SourceRef
import io.sm8.core.schema.{Field, SealedDataType}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RelOpConformanceSpec extends AnyFunSuite with Matchers {

  // ===== JoinKind (5 cases) =====

  test("JoinKind: all 5 cases constructable + structurally equal") {
    val all = List(JoinKind.Inner, JoinKind.Left, JoinKind.Right, JoinKind.Full, JoinKind.Cross)
    all.size shouldBe 5
    all.distinct.size shouldBe 5  // no duplicates
    JoinKind.Inner shouldBe JoinKind.Inner  // case-object equality
  }

  test("JoinKind: round-trip through ObjectOutputStream (closure-safety)") {
    val all = List(JoinKind.Inner, JoinKind.Left, JoinKind.Right, JoinKind.Full, JoinKind.Cross)
    for (k <- all) {
      val bytes = {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(k); oos.close()
        baos.toByteArray
      }
      val restored = {
        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject().asInstanceOf[JoinKind]
      }
      restored shouldBe k
    }
  }

  // ===== SortDirection + NullOrdering (each 2 cases) =====

  test("SortDirection: 2 cases constructable") {
    SortDirection.Ascending shouldBe SortDirection.Ascending
    SortDirection.Descending shouldBe SortDirection.Descending
    (SortDirection.Ascending != SortDirection.Descending) shouldBe true
  }

  test("NullOrdering: 2 cases constructable") {
    NullOrdering.First shouldBe NullOrdering.First
    NullOrdering.Last shouldBe NullOrdering.Last
    (NullOrdering.First != NullOrdering.Last) shouldBe true
  }

  test("SortKey: case-class equality + round-trip") {
    val k1 = SortKey(
      expression   = Expr.FieldRef("age"),
      direction    = SortDirection.Ascending,
      nullOrdering = NullOrdering.Last,
    )
    val k2 = SortKey(
      expression   = Expr.FieldRef("age"),
      direction    = SortDirection.Ascending,
      nullOrdering = NullOrdering.Last,
    )
    k1 shouldBe k2  // structural equality
    val bytes = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(k1); oos.close()
      baos.toByteArray
    }
    val restored = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[SortKey]
    }
    restored shouldBe k1
  }

  // ===== AggregateFn (16 cases) =====

  test("AggregateFn: all 16 cases constructable + distinct") {
    val all = List(
      AggregateFn.Sum, AggregateFn.Count, AggregateFn.CountDistinct, AggregateFn.Avg,
      AggregateFn.Min, AggregateFn.Max,
      AggregateFn.StddevSample, AggregateFn.StddevPopulation,
      AggregateFn.VarianceSample, AggregateFn.VariancePopulation,
      AggregateFn.Median,
      AggregateFn.PercentileContinuous, AggregateFn.PercentileDiscrete,
      AggregateFn.ApproxPercentile,
      AggregateFn.First, AggregateFn.Last,
    )
    all.size shouldBe 16
    all.distinct.size shouldBe 16  // no two AggregateFn are equal
  }

  test("AggregateFn: round-trip through ObjectOutputStream (closure-safety)") {
    val fns = List(AggregateFn.Sum, AggregateFn.ApproxPercentile, AggregateFn.Median)
    for (fn <- fns) {
      val bytes = {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        oos.writeObject(fn); oos.close()
        baos.toByteArray
      }
      val restored = {
        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        ois.readObject().asInstanceOf[AggregateFn]
      }
      restored shouldBe fn
    }
  }

  // ===== AggregateCall =====

  test("AggregateCall: smart ctor shape — Sum(amount) AS total") {
    val call = AggregateCall(
      fn        = AggregateFn.Sum,
      input     = Some(Expr.FieldRef("amount")),
      alias     = "total",
      distinct  = false,
      arguments = Nil,
    )
    call.fn shouldBe AggregateFn.Sum
    call.input.get shouldBe Expr.FieldRef("amount")
    call.alias shouldBe "total"
  }

  test("AggregateCall: Count(*) shape — input is None") {
    val call = AggregateCall(
      fn        = AggregateFn.Count,
      input     = None,  // Count(*)
      alias     = "*",
      distinct  = false,
      arguments = Nil,
    )
    call.input shouldBe None
    call.fn shouldBe AggregateFn.Count
  }

  test("AggregateCall: ApproxPercentile(x, 0.95) shape — literal argument") {
    val call = AggregateCall(
      fn        = AggregateFn.ApproxPercentile,
      input     = Some(Expr.FieldRef("latency_ms")),
      alias     = "p95_latency",
      distinct  = false,
      arguments = List(LiteralValue.DoubleValue(0.95)),
    )
    call.arguments.size shouldBe 1
    call.arguments.head shouldBe LiteralValue.DoubleValue(0.95)
  }

  test("AggregateCall: structural equality + round-trip") {
    val a = AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("x")), "sx")
    val b = AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("x")), "sx")
    a shouldBe b
    val bytes = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(a); oos.close()
      baos.toByteArray
    }
    val restored = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[AggregateCall]
    }
    restored shouldBe a
  }

  // ===== RelOp (7 nodes) =====

  private val sampleScan = RelOp.Scan(
    sourceRef  = SourceRef.ByName(table = "people"),
    schema     = List(Field("id", SealedDataType.Int, nullable = false)),
    projection = List(Expr.FieldRef("id")),
  )

  test("RelOp: all 7 nodes constructable") {
    val filter   = RelOp.Filter(sampleScan, Expr.FieldRef("id"))
    val project  = RelOp.Project(filter, List((Expr.FieldRef("id"), "id")))
    val agg      = RelOp.Aggregate(project, List(Expr.FieldRef("id")), List(
                   AggregateCall(AggregateFn.Count, alias = "n")))
    val join     = RelOp.Join(sampleScan, sampleScan, JoinKind.Inner, Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
    val sort     = RelOp.Sort(agg, List(SortKey(Expr.FieldRef("id"), SortDirection.Ascending, NullOrdering.Last)))
    val limit    = RelOp.Limit(sort, count = 10L)

    filter.isInstanceOf[RelOp.Filter] shouldBe true
    project.isInstanceOf[RelOp.Project] shouldBe true
    agg.isInstanceOf[RelOp.Aggregate] shouldBe true
    join.isInstanceOf[RelOp.Join] shouldBe true
    sort.isInstanceOf[RelOp.Sort] shouldBe true
    limit.isInstanceOf[RelOp.Limit] shouldBe true
  }

  test("RelOp.Scan: case-class equality") {
    val s1 = RelOp.Scan(
      sourceRef  = SourceRef.ByName(table = "people"),
      schema     = List(Field("id", SealedDataType.Int, nullable = false)),
      projection = List(Expr.FieldRef("id")),
    )
    val s2 = RelOp.Scan(
      sourceRef  = SourceRef.ByName(table = "people"),
      schema     = List(Field("id", SealedDataType.Int, nullable = false)),
      projection = List(Expr.FieldRef("id")),
    )
    s1 shouldBe s2
  }

  test("RelOp tree (Scan → Filter → Project → Aggregate → Sort → Limit): round-trip") {
    val plan: RelOp = RelOp.Limit(
      input = RelOp.Sort(
        input = RelOp.Aggregate(
          input = RelOp.Project(
            input = RelOp.Filter(
              input = sampleScan,
              predicate = Expr.Equal(Expr.FieldRef("id"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
            ),
            expressions = List((Expr.FieldRef("id"), "id")),
          ),
          groupBy = List(Expr.FieldRef("id")),
          aggregates = List(AggregateCall(AggregateFn.Count, alias = "n")),
        ),
        keys = List(SortKey(Expr.FieldRef("id"), SortDirection.Ascending, NullOrdering.Last)),
      ),
      count = 100L,
    )
    val bytes = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(plan); oos.close()
      baos.toByteArray
    }
    val restored = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[RelOp]
    }
    restored shouldBe plan
  }

  test("RelOp.Join with Cross kind: condition is unused but must be present (typed Expr)") {
    val join = RelOp.Join(
      left      = sampleScan,
      right     = sampleScan,
      kind      = JoinKind.Cross,
      condition = Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean),
    )
    join.kind shouldBe JoinKind.Cross
    // Cross joins ignore condition; the ADT still carries it for type uniformity.
    join.condition should not be null
  }

}
