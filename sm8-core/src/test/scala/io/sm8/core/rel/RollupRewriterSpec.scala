/*
 * SM8 Core — RollupRewriterSpec (Ticket 4 of
 * docs/wayfinder/2026-09-06-pre-aggregation.md).
 *
 * Acceptance criteria under test:
 * 1. match: dims ⊆ + composable aggregates + filter-evaluability
 *    + grain agreement -> plan re-scans from the rollup table.
 * 2. no-match: any failed criterion -> the ORIGINAL plan instance
 *    returned byte-identical (===, fail-open).
 * 3. partial-overlap: request group set NOT contained in the
 *    rollup's -> Unchanged(NoGroupSetMatch).
 * 4. unsplittable-aggregate: Positional / Holistic / Approximable
 *    refused (Ticket 1 vocabulary); Algebraic refused in v1 until
 *    Ticket 5 wires partial-state columns.
 * 5. Algebraic NULL/undefined guards (humpback finding):
 *    stddev_samp with total n < 2 -> NULL (engine parity with the
 *    base path); the n=1-single-observation-group edge is in this
 *    regression set (guard builders pinned at the Expr level).
 * 6. Determinism: first matching rollup in declaration order wins.
 */
package io.sm8.core.rel

import io.sm8.core.expr.Expr
import io.sm8.core.expr.LiteralValue
import io.sm8.core.model.{
  AuditPolicy,
  CachePolicy,
  CalculatedMeasure,
  Dimension,
  MaterializePolicy,
  Measure,
  Model,
  ModelPolicyDefaults,
  RollupSpec,
  SourceRef
}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RollupRewriterSpec extends AnyFunSuite with Matchers {

  // -- fixtures --

  private val baseSchema: List[Field] = List(
    Field.nonNull("carrier", SealedDataType.Varchar),
    Field.nonNull("dest_region", SealedDataType.Varchar),
    Field.nonNull("fare", SealedDataType.Double),
    Field.nonNull("flight_date", SealedDataType.Date)
  )

  private val dims = List(
    Dimension.field("carrier", "carrier"),
    Dimension.field("dest_region", "dest_region"))

  private val meas = List(
    // rows = true row count: Count(*) (no input). Count(WITH input)
    // is a non-null count and is refused by the rewriter in v1
    // (F1: the state contract stores row counts only).
    Measure("rows", AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")),
    Measure.aggregate("total_fare", AggregateFn.Sum, Expr.FieldRef("fare")))

  private def model(rollups: List[RollupSpec]): Model =
    Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas,
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = rollups
    ).right.get

  /** Canonical query plan: Scan -> Aggregate(g=[carrier], a=[Count, Sum]). */
  private def canonicalPlan(): RelOp =
    RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(
        AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows"),
        AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("fare")), alias = "total_fare")))

  private val byCarrier =
    RollupSpec("by_carrier", List("carrier"), List("rows", "total_fare"), None)

  // ===== 1. match =====

  test("match: dims subset + Additive aggregates -> plan re-scans from the rollup table") {
    val plan = canonicalPlan()
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val r = out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
    r.rollupName shouldBe "by_carrier"
    // New plan re-scans <model>__<rollup>
    r.plan.toString should include("flights__by_carrier")
    // The rewritten Aggregate sums the rollup's count partials.
    val RelOp.Aggregate(_, g, aggs) = findAggregate(r.plan)
    g shouldBe List(Expr.FieldRef("carrier"))
    aggs.map(_.fn) shouldBe List(AggregateFn.Sum, AggregateFn.Sum) // Count -> Sum(count__rows)
    aggs.head.input shouldBe Some(Expr.FieldRef("count__rows"))
  }

  test("match preserves upper wrappers: Sort/Limit re-apply over the rollup scan") {
    val plan = RelOp.Limit(
      input = RelOp.Sort(input = canonicalPlan(), keys = Nil),
      count = 10L,
      offset = 0L)
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val r = out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
    // Wrappers rebuilt outermost-first: original Limit(Sort(Agg))
    // stays Limit(Sort(Agg')) with the aggregate re-based.
    val RelOp.Limit(sortInput, count, offset) = r.plan
    count shouldBe 10L
    offset shouldBe 0L
    sortInput shouldBe a[RelOp.Sort]
  }

  test("match with filters: filters re-apply over the rollup scan (evaluable columns)") {
    val plan = RelOp.Aggregate(
      input = RelOp.Filter(
        input = RelOp.Scan(
          sourceRef = SourceRef.ByName(table = "flights_raw"),
          schema = baseSchema,
          projection = Nil),
        predicate = Expr.Equal(Expr.FieldRef("carrier"), Expr.FieldRef("carrier"))),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
  }

  // ===== 2. no-match: fail-open byte-identical =====

  test("no rollups declared -> original instance, byte-identical (===)") {
    val plan = canonicalPlan()
    val out = RollupRewriter.rewrite(plan, model(Nil), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.NoGroupSetMatch)
    // Determinism of the refusal: repeated rewrites agree.
    RollupRewriter.rewrite(plan, model(Nil), None) shouldBe out
    // plan reference unchanged (the rewriter never mutates input).
    plan shouldBe canonicalPlan()
  }

  test("non-canonical shape (Join above Aggregate) -> Unchanged(NonCanonicalShape), same instance") {
    val plan = canonicalPlan()
    val joined: RelOp = RelOp.Join(
      left = plan,
      right = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "other"),
        schema = List(Field.nonNull("x", SealedDataType.Int)),
        projection = Nil),
      kind = JoinKind.Inner,
      condition = Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean))
    val out = RollupRewriter.rewrite(joined, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.NonCanonicalShape)
  }

  test("Project above Aggregate (pass-through FieldRefs) is peeled and routes to the rollup") {
    // The production shape: QueryBuilder.build emits
    // Project(Aggregate(...)) for every measure-bearing model, and the
    // pass-through projection (FieldRef -> same-name) re-emits the
    // names the rewritten plan produces, so it is semantically a
    // no-op wrapper. Routing MUST work through it.
    val plan = RelOp.Project(
      input = canonicalPlan(),
      expressions = List((Expr.FieldRef("carrier"), "carrier")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out match {
      case RollupRewriter.RollupRewriteResult.Rewritten(p, name) =>
        name shouldBe "by_carrier"
        // The projection wrapper is re-emitted on the rewritten plan,
        // with the SAME column names as the original (pass-through
        // contract).
        p shouldBe a[RelOp.Project]
        val project = p.asInstanceOf[RelOp.Project]
        project.expressions shouldBe List((Expr.FieldRef("carrier"), "carrier"))
      case other =>
        fail(s"expected Rewritten, got $other")
    }
  }

  test("Project above Aggregate with CALCULATED expressions (non-FieldRef) -> non-canonical in v1") {
    // The original v1 contract, preserved: a projection that computes
    // (e.g. Alias of a non-FieldRef expression, or any non-FieldRef
    // entry) changes semantics and is refused.
    val plan = RelOp.Project(
      input = canonicalPlan(),
      expressions = List(
        (Expr.Alias("carrier_plus_one",
          Expr.Add(Expr.FieldRef("carrier"), Expr.Literal(io.sm8.core.expr.LiteralValue.IntValue(1), io.sm8.core.schema.SealedDataType.Int))), "carrier")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.NonCanonicalShape)
  }

  // ===== 3. partial overlap =====

  test("partial overlap: query groups by dim NOT in the rollup -> NoGroupSetMatch") {
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("dest_region")), // not in by_carrier
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.NoGroupSetMatch)
  }

  test("within-list duplicate group refs dedupe to the same subset (Ticket 3 follow-up)") {
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier"), Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
  }

  // ===== 4. unsplittable aggregates (Ticket 1 vocabulary) =====

  test("Median (Holistic) -> Unchanged(UnsplittableAggregate)") {
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Median, input = Some(Expr.FieldRef("fare")), alias = "med")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("First (Positional) and CountDistinct (Approximable) refused in v1") {
    for (fn <- List(AggregateFn.First, AggregateFn.CountDistinct)) {
      val plan = RelOp.Aggregate(
        input = RelOp.Scan(
          sourceRef = SourceRef.ByName(table = "flights_raw"),
          schema = baseSchema,
          projection = Nil),
        groupBy = List(Expr.FieldRef("carrier")),
        aggregates = List(AggregateCall(fn = fn, input = Some(Expr.FieldRef("fare")), alias = "x")))
      val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
      out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
        RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
    }
  }

  test("Avg (Algebraic) refused in v1 until Ticket 5 wires partial-state columns") {
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Avg, input = Some(Expr.FieldRef("fare")), alias = "avg_fare")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  // ===== filter evaluability =====

  test("filter referencing a non-rollup column -> FilterNotEvaluable") {
    val plan = RelOp.Aggregate(
      input = RelOp.Filter(
        input = RelOp.Scan(
          sourceRef = SourceRef.ByName(table = "flights_raw"),
          schema = baseSchema,
          projection = Nil),
        predicate = Expr.GreaterThan(Expr.FieldRef("fare"), Expr.Literal(LiteralValue.DoubleValue(100.0), SealedDataType.Double))),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    // fare is NOT a rollup column (rollup carries dims + state cols; the
    // count rollup only pre-aggregates rows) -> filter not evaluable.
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.FilterNotEvaluable)
  }

  // ===== grain agreement =====

  test("grain mismatch -> GrainMismatch; equal grains match; both None match") {
    val dayRollup = RollupSpec("by_carrier_day", List("carrier"), List("rows", "total_fare"), Some("day"), Some("carrier"))
    val plan = canonicalPlan()
    // Query asks day, rollup is day -> match.
    RollupRewriter.rewrite(plan, model(List(dayRollup)), Some("day")) shouldBe
      a[RollupRewriter.RollupRewriteResult.Rewritten]
    // Normalization: "DAY " matches "day".
    RollupRewriter.rewrite(plan, model(List(dayRollup)), Some(" DAY ")) shouldBe
      a[RollupRewriter.RollupRewriteResult.Rewritten]
    // Query asks hour, rollup is day -> fail-safe no match.
    RollupRewriter.rewrite(plan, model(List(dayRollup)), Some("hour")) shouldBe
      RollupRewriter.RollupRewriteResult.Unchanged(RollupRewriter.RollupRewriteRefusal.GrainMismatch)
    // Query no grain, rollup day -> v1 conservative: no match.
    RollupRewriter.rewrite(plan, model(List(dayRollup)), None) shouldBe
      RollupRewriter.RollupRewriteResult.Unchanged(RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("blank grain normalizes to None (absent)") {
    RollupRewriter.normalizeGrain(Some("   ")) shouldBe None
    RollupRewriter.normalizeGrain(None) shouldBe None
    RollupRewriter.normalizeGrain(Some("Day")) shouldBe Some("day")
  }

  // ===== determinism =====

  test("non-FieldRef group key (calculated dim) -> NoGroupSetMatch in v1") {
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.Add(Expr.FieldRef("carrier"), Expr.FieldRef("dest_region"))),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.NoGroupSetMatch)
  }

  test("rollup without grain vs query WITH grain -> no match (conservative)") {
    val out = RollupRewriter.rewrite(canonicalPlan(), model(List(byCarrier)), Some("day"))
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("ByPath base source -> SourceKindUnsupported (v1 re-scans named tables only)") {
    val byPathModel = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas,
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByPath(format = "parquet", path = "/data/flights"),
      rollups = List(byCarrier)
    ).right.get
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByPath(format = "parquet", path = "/data/flights"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    val out = RollupRewriter.rewrite(plan, byPathModel, None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.SourceKindUnsupported)
  }

  test("COUNT(carrier) (Count WITH input) refused in v1: state stores row counts only") {
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = Some(Expr.FieldRef("carrier")), alias = "rows")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("measure-name filter (rows > 5) is HAVING semantics -> FilterNotEvaluable in v1") {
    val plan = RelOp.Aggregate(
      input = RelOp.Filter(
        input = RelOp.Scan(
          sourceRef = SourceRef.ByName(table = "flights_raw"),
          schema = baseSchema,
          projection = Nil),
        predicate = Expr.GreaterThan(Expr.FieldRef("rows"), Expr.Literal(LiteralValue.IntValue(5), SealedDataType.Int))),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.FilterNotEvaluable)
  }

  test("request call with same alias but different input than the declared measure refused") {
    // Declared total_fare = Sum(fare); request says Sum(dest_region)
    // under the same alias — identity mismatch -> permanent refusal
    // (never re-base against a different input's state column).
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.FieldRef("dest_region")), alias = "total_fare")))
    val out = RollupRewriter.rewrite(plan, model(List(byCarrier)), None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("composite-input Sum measure refused — no state column, no alias fallback") {
    val m = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = List(
        Measure.aggregate("gross", AggregateFn.Sum,
          Expr.Add(Expr.FieldRef("fare"), Expr.FieldRef("fare")))),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"), List("gross"), None))
    ).right.get
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(AggregateCall(fn = AggregateFn.Sum,
        input = Some(Expr.Add(Expr.FieldRef("fare"), Expr.FieldRef("fare"))), alias = "gross")))
    val out = RollupRewriter.rewrite(plan, m, None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  // ===== ADR-0023 algebraic routing (Ticket 7) =====

  private val algebraicModel = Model.of(
    name = "flights",
    version = 1,
    dimensions = dims,
    measures = meas :+ Measure.aggregate("avg_fare", AggregateFn.Avg, Expr.FieldRef("fare")),
    defaultPolicies = ModelPolicyDefaults(
      materialize = MaterializePolicy.None,
      cache = CachePolicy.NoCache,
      audit = AuditPolicy.NoAudit),
    source = SourceRef.ByName(table = "flights_raw"),
    rollups = List(RollupSpec("by_carrier", List("carrier"), List("rows", "total_fare", "avg_fare"), None))
  ).right.get

  private def algebraicPlan(aggs: AggregateCall*): RelOp =
    RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = aggs.toList)

  test("Algebraic measure with DECLARED identity + state columns -> ROUTES as two-phase Aggregate->Project") {
    val out = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.Avg, input = Some(Expr.FieldRef("fare")), alias = "avg_fare")),
      algebraicModel, None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val r = out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
    r.rollupName shouldBe "by_carrier"
    // Outer shape: Project over Aggregate (ADR-0023 two-phase).
    val proj = r.plan.asInstanceOf[RelOp.Project]
    // Contract #3: outer alias equals the REQUEST measure alias.
    // Output-schema parity (T8 fix): group-set columns prefix the
    // projection — [carrier] then measures.
    proj.expressions.map(_._2) shouldBe List("carrier", "avg_fare")
    proj.expressions.head._1 shouldBe Expr.FieldRef("carrier")
    // Derived: Divide(sum__fare_total, count__fare_total).
    proj.expressions(1)._1 shouldBe
      Expr.Divide(Expr.FieldRef("sum__fare_total"), Expr.FieldRef("count__fare_total"))
    // Inner Aggregate re-aggregates the state columns with Sum calls
    // using the <state>__<F>_total alias convention (contract #6).
    val agg = proj.input.asInstanceOf[RelOp.Aggregate]
    val innerAliases = agg.aggregates.map(_.alias)
    innerAliases should contain allOf ("sum__fare_total", "count__fare_total")
    innerAliases.foreach { a =>
      val call = agg.aggregates.find(_.alias == a).get
      call.fn shouldBe AggregateFn.Sum
    }
  }

  test("Algebraic measure on a rollup WITHOUT state columns -> AlgebraicStateNotWired (recoverable, contract #1)") {
    // Rollup declares only Additive measures -> no algebraic state
    // columns on the rollup schema -> recoverable refusal, base path.
    val additiveOnly = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas :+ Measure.aggregate("avg_fare", AggregateFn.Avg, Expr.FieldRef("fare")),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"), List("rows", "total_fare"), None))
    ).right.get
    val out = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.Avg, input = Some(Expr.FieldRef("fare")), alias = "avg_fare")),
      additiveOnly, None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.AlgebraicStateNotWired)
  }

  test("Mixed Additive+Algebraic request composes both arms in one plan (contract #6)") {
    val out = RollupRewriter.rewrite(
      algebraicPlan(
        AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows"),
        AggregateCall(fn = AggregateFn.Avg, input = Some(Expr.FieldRef("fare")), alias = "avg_fare")),
      algebraicModel, None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val proj = out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
      .plan.asInstanceOf[RelOp.Project]
    // Outer Project: group cols + additive passthrough + algebraic
    // derived, aliases = request names.
    proj.expressions.map(_._2) shouldBe List("carrier", "rows", "avg_fare")
    proj.expressions(1)._1 shouldBe Expr.FieldRef("rows")
    proj.expressions(2)._1 shouldBe
      Expr.Divide(Expr.FieldRef("sum__fare_total"), Expr.FieldRef("count__fare_total"))
    // Inner Aggregate carries ALL re-aggregates: the additive Count
    // re-base (Sum(count__rows) AS rows — keeps the request alias)
    // PLUS the algebraic totals.
    val agg = proj.input.asInstanceOf[RelOp.Aggregate]
    val innerAliases = agg.aggregates.map(_.alias).toSet
    innerAliases should contain allOf ("rows", "count__fare_total", "sum__fare_total")
    val countCall = agg.aggregates.find(_.alias == "rows").get
    countCall.fn shouldBe AggregateFn.Sum
    countCall.input shouldBe Some(Expr.FieldRef("count__rows"))
  }

  test("Count over a rollup that does NOT declare Count -> UnsplittableAggregate (permanent, state column missing)") {
    // Architect round-1 HIGH (same bug class as PR-338): identity
    // matched against the MODEL measure, but the rollup excludes the
    // measure so its state column is absent from the rollup schema.
    // rebaseAggregate would emit a dangling Sum(count__rows).
    val probe = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas :+ Measure.aggregate("avg_fare", AggregateFn.Avg, Expr.FieldRef("fare")),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"), List("avg_fare"), None))
    ).right.get
    val out = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")),
      probe, None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("Sum over a rollup that does NOT declare the Sum measure -> UnsplittableAggregate (same Additive gate)") {
    val probe = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas,
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"), List("rows"), None))
    ).right.get
    val out = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.Sum,
        input = Some(Expr.FieldRef("fare")), alias = "total_fare")),
      probe, None)
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("Avg(DISTINCT x) / StddevSample(DISTINCT x) -> UnsplittableAggregate (permanent, contract #4)") {
    val out1 = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.Avg,
        input = Some(Expr.FieldRef("fare")), alias = "avg_fare", distinct = true)),
      algebraicModel, None)
    out1 shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
    val out2 = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.StddevSample,
        input = Some(Expr.FieldRef("fare")), alias = "avg_fare", distinct = true)),
      algebraicModel, None)
    out2 shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
    // VarianceSample(DISTINCT) — same permanent class (architect L4).
    val out3 = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.VarianceSample,
        input = Some(Expr.FieldRef("fare")), alias = "avg_fare", distinct = true)),
      algebraicModel, None)
    out3 shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("Multi-algebraic alias parity: outer aliases equal request aliases in request order") {
    val stddevModel = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas :+
        Measure.aggregate("avg_fare", AggregateFn.Avg, Expr.FieldRef("fare")) :+
        Measure.aggregate("fare_stddev", AggregateFn.StddevSample, Expr.FieldRef("fare")),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"),
        List("rows", "total_fare", "avg_fare", "fare_stddev"), None))
    ).right.get
    val out = RollupRewriter.rewrite(
      algebraicPlan(
        AggregateCall(fn = AggregateFn.Avg, input = Some(Expr.FieldRef("fare")), alias = "avg_fare"),
        AggregateCall(fn = AggregateFn.StddevSample, input = Some(Expr.FieldRef("fare")), alias = "fare_stddev")),
      stddevModel, None)
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val proj = out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
      .plan.asInstanceOf[RelOp.Project]
    // Output-schema parity: group cols prefix, then measures in
    // request order.
    proj.expressions.map(_._2) shouldBe List("carrier", "avg_fare", "fare_stddev")
    // StddevSample derives as sqrt(guarded-variance): FunctionCall("sqrt", ...).
    proj.expressions(2)._1 shouldBe
      Expr.FunctionCall("sqrt", Seq(
        Expr.CaseWhen(
          List((
            Expr.LessThan(Expr.FieldRef("count__fare_total"),
              Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)),
            Expr.Literal(LiteralValue.NullValue, SealedDataType.Double))),
          Expr.Divide(
            Expr.FieldRef("m2__fare_total"),
            Expr.Subtract(Expr.FieldRef("count__fare_total"),
              Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))))
      ))
  }

  test("StddevSample(tax) on a rollup declaring Avg(tax) -> UnsplittableAggregate (identity refusal precedes decomposability)") {
    // algebraicModel's rollup carries avg_fare state columns
    // (count__fare, sum__fare, m2__fare — stateColumnsFor emits
    // all three for ANY algebraic measure), so build a rollup whose
    // declared algebraic measure has a DIFFERENT input field to
    // exercise the per-field availability check.
    val otherFieldModel = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas :+ Measure.aggregate("avg_tax", AggregateFn.Avg, Expr.FieldRef("tax")),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(RollupSpec("by_carrier", List("carrier"), List("rows", "total_fare", "avg_tax"), None))
    ).right.get
    // Request Avg(tax) AS alias matching DECLARED avg_tax measure:
    // the rollup DOES carry count__tax/sum__tax -> routes.
    val out1 = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.Avg, input = Some(Expr.FieldRef("tax")), alias = "avg_tax")),
      otherFieldModel, None)
    out1 shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    // Request Stddev(tax): identity check fails (declared measure is
    // Avg, not Stddev) -> permanent UnsplittableAggregate, NOT the
    // recoverable gate (identity is checked before decomposability).
    val out2 = RollupRewriter.rewrite(
      algebraicPlan(AggregateCall(fn = AggregateFn.StddevSample, input = Some(Expr.FieldRef("tax")), alias = "avg_tax")),
      otherFieldModel, None)
    out2 shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate)
  }

  test("first matching rollup in declaration order wins") {
    val coarse = RollupSpec("coarse", List("carrier", "dest_region"), List("rows", "total_fare"), None)
    val fine = RollupSpec("fine", List("carrier"), List("rows", "total_fare"), None)
    val out = RollupRewriter.rewrite(canonicalPlan(), model(List(coarse, fine)), None)
    out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].rollupName shouldBe "coarse"
    val out2 = RollupRewriter.rewrite(canonicalPlan(), model(List(fine, coarse)), None)
    out2.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].rollupName shouldBe "fine"
  }

  // ===== table naming + schema contract (Ticket 5 materializer) =====

  test("rollup table name convention <model>__<rollup>") {
    RollupRewriter.rollupTableName(model(List(byCarrier)), byCarrier) shouldBe "flights__by_carrier"
  }

  test("rollup schema carries dim columns + measure state columns (Ticket 5 contract)") {
    val m = model(List(byCarrier))
    val schema = RollupRewriter.rollupSchema(byCarrier, m)
    schema.map(_.name) should contain("carrier")
    schema.map(_.name) should contain("count__rows")
    schema.map(_.name) should contain("sum__fare")
    // Count state: integral + non-nullable (a NULL partial would
    // turn the re-aggregated Sum into NULL where base yields 0).
    val countCol = schema.find(_.name == "count__rows").get
    countCol.dataType shouldBe SealedDataType.BigInt
    countCol.nullable shouldBe false
    // Duplicate state columns from shared shapes are deduped.
    schema.map(_.name) should have size schema.map(_.name).distinct.size
  }

  // ===== 5. Algebraic NULL guards (humpback finding) =====

  test("variance_samp guard (Welford): total n < 2 -> NULL (engine parity with base path)") {
    val e = RollupRewriter.varianceSampGuardExpr("n", "m2")
    e shouldBe a[Expr.CaseWhen]
    val Expr.CaseWhen(branches, _) = e
    val (cond, res) = branches.head
    cond shouldBe Expr.LessThan(
      Expr.FieldRef("n"),
      Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    res shouldBe Expr.Literal(LiteralValue.NullValue, SealedDataType.Double)
    val Expr.CaseWhen(_, body) = e
    body shouldBe Expr.Divide(
      Expr.FieldRef("m2"),
      Expr.Subtract(Expr.FieldRef("n"), Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)))
  }

  test("variance_pop guard (Welford): n<=0 -> NULL, else M2/n") {
    val e = RollupRewriter.variancePopGuardExpr("n", "m2")
    e shouldBe a[Expr.CaseWhen]
    val Expr.CaseWhen(branches, _) = e
    val (cond, res) = branches.head
    cond shouldBe Expr.LessOrEqual(
      Expr.FieldRef("n"),
      Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))
    res shouldBe Expr.Literal(LiteralValue.NullValue, SealedDataType.Double)
    val Expr.CaseWhen(_, body) = e
    body shouldBe Expr.Divide(Expr.FieldRef("m2"), Expr.FieldRef("n"))
  }

  // -- helpers --

  private def findAggregate(plan: RelOp): RelOp.Aggregate = plan match {
    case a: RelOp.Aggregate => a
    case RelOp.Sort(input, _) => findAggregate(input)
    case RelOp.Limit(input, _, _) => findAggregate(input)
    case other => fail(s"no Aggregate under $other")
  }

  // ===== grain agreement: the four-arm matrix =====

  test("grain matrix: (None, None) matches, grained query over grain-less rollup refuses") {
    // Preserved v1 behavior: unchanged grain-less path.
    RollupRewriter.rewrite(canonicalPlan(), model(List(byCarrier)), None) shouldBe
      a[RollupRewriter.RollupRewriteResult.Rewritten]
    RollupRewriter.rewrite(canonicalPlan(), model(List(byCarrier)), Some("day")) shouldBe
      RollupRewriter.RollupRewriteResult.Unchanged(RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("grain matrix: grained rollup vs grain-less query still refuses (full-aggregate arm)") {
    val dayRollup = RollupSpec("by_carrier_day", List("carrier"), List("rows", "total_fare"), Some("day"), Some("carrier"))
    RollupRewriter.rewrite(canonicalPlan(), model(List(dayRollup)), None) shouldBe
      RollupRewriter.RollupRewriteResult.Unchanged(RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("grain matrix: coarsening (day rollup, month query) routes for Additive-only requests") {
    val dayRollup = RollupSpec("by_carrier_day", List("carrier"), List("rows", "total_fare"), Some("day"), Some("carrier"))
    val out = RollupRewriter.rewrite(canonicalPlan(), model(List(dayRollup)), Some("month"))
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val r = out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten]
    r.rollupName shouldBe "by_carrier_day"
  }

  test("grain matrix: non-bucket-safe request never coarsens (fail-safe GrainMismatch)") {
    // A composite-input Sum has no state column; the coarsening fn
    // gate refuses, and the deterministic diagnosis order reports
    // the grain gate first.
    val dayRollup = RollupSpec("by_carrier_day", List("carrier"), List("rows", "total_fare"), Some("day"), Some("carrier"))
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(
        AggregateCall(fn = AggregateFn.Sum, input = Some(Expr.Add(Expr.FieldRef("fare"), Expr.FieldRef("fare"))), alias = "x")))
    val out = RollupRewriter.rewrite(plan, model(List(dayRollup)), Some("month"))
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("grain matrix: dispersion measure never coarsens (Stddev + coarser query refuses)") {
    // The coarsening fn gate excludes dispersion fns (Stddev/
    // Variance) despite M2-additivity — v1 scope discipline. A
    // request carrying a Stddev measure at a coarser grain than the
    // rollup must fail safe to the base path.
    val dayRollup = RollupSpec(
      "by_carrier_day", List("carrier"), List("rows", "std_fare"), Some("day"), Some("carrier"))
    val stdModel = Model.of(
      name = "flights",
      version = 1,
      dimensions = dims,
      measures = meas :+ Measure.aggregate(
        "std_fare", AggregateFn.StddevSample, Expr.FieldRef("fare")),
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = List(dayRollup)
    ).right.get
    val plan = RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("carrier")),
      aggregates = List(
        AggregateCall(fn = AggregateFn.StddevSample, input = Some(Expr.FieldRef("fare")), alias = "std_fare")))
    val out = RollupRewriter.rewrite(plan, stdModel, Some("month"))
    out shouldBe RollupRewriter.RollupRewriteResult.Unchanged(
      RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("grain matrix: coarsening refuses to SPLIT (month rollup, day query)") {
    val monthRollup = RollupSpec("by_carrier_month", List("carrier"), List("rows", "total_fare"), Some("month"), Some("carrier"))
    RollupRewriter.rewrite(canonicalPlan(), model(List(monthRollup)), Some("day")) shouldBe
      RollupRewriter.RollupRewriteResult.Unchanged(RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("grain matrix: unknown grain label is fail-safe (never coarsens)") {
    val dayRollup = RollupSpec("by_carrier_day", List("carrier"), List("rows", "total_fare"), Some("day"), Some("carrier"))
    RollupRewriter.rewrite(canonicalPlan(), model(List(dayRollup)), Some("fortnight")) shouldBe
      RollupRewriter.RollupRewriteResult.Unchanged(RollupRewriter.RollupRewriteRefusal.GrainMismatch)
  }

  test("grainRank / finerThan: total order over KnownGrains, unknowns rank None") {
    RollupRewriter.grainRank("hour") shouldBe Some(0)
    RollupRewriter.grainRank("year") shouldBe Some(5)
    RollupRewriter.grainRank("fortnight") shouldBe None
    RollupRewriter.finerThan("day", "month") shouldBe true
    RollupRewriter.finerThan("month", "day") shouldBe false
    RollupRewriter.finerThan("day", "day") shouldBe false // equal is not finer
    RollupRewriter.finerThan("fortnight", "month") shouldBe false
  }

  // ===== coarsening emit: date_trunc on the grain dimension =====

  /** Canonical plan grouping by the grain dim (flight_date). */
  private def datePlan(): RelOp =
    RelOp.Aggregate(
      input = RelOp.Scan(
        sourceRef = SourceRef.ByName(table = "flights_raw"),
        schema = baseSchema,
        projection = Nil),
      groupBy = List(Expr.FieldRef("flight_date")),
      aggregates = List(
        AggregateCall(fn = AggregateFn.Count, input = None, alias = "rows")))

  private val dateDims = dims :+ Dimension.field("flight_date", "flight_date", SealedDataType.Date)

  private def dateModel(rollups: List[RollupSpec]): Model =
    Model.of(
      name = "flights",
      version = 1,
      dimensions = dateDims,
      measures = meas,
      defaultPolicies = ModelPolicyDefaults(
        materialize = MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = AuditPolicy.NoAudit),
      source = SourceRef.ByName(table = "flights_raw"),
      rollups = rollups
    ).right.get

  test("coarsening emit: date_trunc wraps the grain dim when re-grouping to the query grain") {
    val dayRollup = RollupSpec(
      "by_date_day", List("flight_date"), List("rows"), Some("day"), Some("flight_date"))
    val out = RollupRewriter.rewrite(datePlan(), dateModel(List(dayRollup)), Some("month"))
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val RelOp.Aggregate(_, g, _) = findAggregate(out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].plan)
    // The truncation is ALIASED back to the dim name: the Aggregate
    // output column must stay addressable as `flight_date` (Spark
    // would otherwise auto-name it date_trunc(month, flight_date),
    // and every downstream name-based reference would dangle).
    g shouldBe List(Expr.Alias("flight_date", Expr.FunctionCall(
      "date_trunc",
      Seq(Expr.Literal(LiteralValue.StringValue("month"), SealedDataType.Varchar),
          Expr.FieldRef("flight_date")))))
  }

  test("coarsening emit: exact-grain request keeps the raw dim column") {
    val dayRollup = RollupSpec(
      "by_date_day", List("flight_date"), List("rows"), Some("day"), Some("flight_date"))
    val out = RollupRewriter.rewrite(datePlan(), dateModel(List(dayRollup)), Some("day"))
    out shouldBe a[RollupRewriter.RollupRewriteResult.Rewritten]
    val RelOp.Aggregate(_, g, _) = findAggregate(out.asInstanceOf[RollupRewriter.RollupRewriteResult.Rewritten].plan)
    g shouldBe List(Expr.FieldRef("flight_date"))
  }

  // ===== grain-dim schema override (Timestamp parity seam) =====

  test("rollupSchema: grain dim overrides to Timestamp regardless of declared Date") {
    val dayRollup = RollupSpec(
      "by_date_day", List("flight_date"), List("rows"), Some("day"), Some("flight_date"))
    val schema = RollupRewriter.rollupSchema(dayRollup, dateModel(List(dayRollup)))
    val byName = schema.map(f => f.name -> f).toMap
    byName("flight_date").dataType shouldBe SealedDataType.Timestamp
  }

  test("rollupSchema: non-grained rollup keeps the declared dim type") {
    val schema = RollupRewriter.rollupSchema(byCarrier, model(List(byCarrier)))
    val byName = schema.map(f => f.name -> f).toMap
    byName("carrier").dataType shouldBe SealedDataType.Varchar
  }

  test("reconciledRollupSchema: grain dim resolves to Timestamp even when the scan says Date") {
    val dayRollup = RollupSpec(
      "by_date_day", List("flight_date"), List("rows"), Some("day"), Some("flight_date"))
    val scanTypes = Map("flight_date" -> SealedDataType.Date)
    val schema = RollupRewriter.reconciledRollupSchema(dayRollup, dateModel(List(dayRollup)), scanTypes)
    val byName = schema.map(f => f.name -> f).toMap
    byName("flight_date").dataType shouldBe SealedDataType.Timestamp
    byName("count__rows").dataType shouldBe SealedDataType.BigInt
  }
}

// ===== Schema-type reconciliation (Ticket 5 DE carry-item) =====

class ReconciledRollupSchemaSpec extends AnyFunSuite with Matchers {

  private val scanTypes = Map(
    "region" -> io.sm8.core.schema.SealedDataType.Varchar,
    "amount" -> io.sm8.core.schema.SealedDataType.BigInt,
    "units" -> io.sm8.core.schema.SealedDataType.Int)

  private val dims = List(
    Dimension.field("region", "region"),
    Dimension.field("units", "units"))

  private val meas = List(
    Measure("order_count", AggregateCall(fn = AggregateFn.Count, input = None, alias = "order_count")),
    Measure.aggregate("total_amount", AggregateFn.Sum, Expr.FieldRef("amount")),
    Measure.aggregate("min_units", AggregateFn.Min, Expr.FieldRef("units")))

  private val spec = RollupSpec("by_region", List("region"), List("order_count", "total_amount", "min_units"), None)

  private val m = Model.of(
    name = "sales", version = 1,
    dimensions = dims, measures = meas,
    source = SourceRef.ByName(table = "sales_base"),
    rollups = List(spec)
  ).right.get

  test("state column types derive from the resolved base-scan schema (not hardcoded Double)") {
    val schema = RollupRewriter.reconciledRollupSchema(spec, m, scanTypes)
    val byName = schema.map(f => f.name -> f).toMap
    // count__rows is Long (integral, matches base-path count semantics)
    byName("count__rows").dataType shouldBe io.sm8.core.schema.SealedDataType.BigInt
    byName("count__rows").nullable shouldBe false
    // sum__amount inherits amount's base type (BigInt), not Double
    byName("sum__amount").dataType shouldBe io.sm8.core.schema.SealedDataType.BigInt
    // min__units inherits units' base type (Int)
    byName("min__units").dataType shouldBe io.sm8.core.schema.SealedDataType.Int
    // dim types resolve from the scan
    byName("region").dataType shouldBe io.sm8.core.schema.SealedDataType.Varchar
  }

  test("count__rows is always present for a rollup with a Count measure") {
    val schema = RollupRewriter.reconciledRollupSchema(spec, m, scanTypes)
    schema.exists(_.name == "count__rows") shouldBe true
  }

  test("unknown base fields fall back to the declared dim type (Varchar default)") {
    val emptyScan = Map.empty[String, io.sm8.core.schema.SealedDataType]
    val schema = RollupRewriter.reconciledRollupSchema(spec, m, emptyScan)
    val byName = schema.map(f => f.name -> f).toMap
    byName("region").dataType shouldBe io.sm8.core.schema.SealedDataType.Varchar
    byName("count__rows").dataType shouldBe io.sm8.core.schema.SealedDataType.BigInt
  }

  // -- refusal label + permanence taxonomy (observer-facing surface) --

  private val allRefusals: List[RollupRewriter.RollupRewriteRefusal] = List(
    RollupRewriter.RollupRewriteRefusal.NonCanonicalShape,
    RollupRewriter.RollupRewriteRefusal.NoGroupSetMatch,
    RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate,
    RollupRewriter.RollupRewriteRefusal.FilterNotEvaluable,
    RollupRewriter.RollupRewriteRefusal.GrainMismatch,
    RollupRewriter.RollupRewriteRefusal.SourceKindUnsupported,
    RollupRewriter.RollupRewriteRefusal.AlgebraicStateNotWired,
    RollupRewriter.RollupRewriteRefusal.RollupSchemaStale,
    RollupRewriter.RollupRewriteRefusal.RollupBucketStale(
      Set(RollupRewriter.RollupRewriteRefusal.BucketKey("2026-09-08"))),
    // ADR-0031 cascade refusal siblings — every case must appear
    // here so the label-uniqueness + permanence tests cover the
    // FULL closed taxonomy (narwhal final-gate F4: a subset here
    // silently un-pins new cases).
    RollupRewriter.RollupRewriteRefusal.CascadeSourceNotFinal(
      Set(RollupRewriter.RollupRewriteRefusal.BucketKey("2026-09-08"))),
    RollupRewriter.RollupRewriteRefusal.CascadeSourceMissing,
    RollupRewriter.RollupRewriteRefusal.CascadeSourceFailed,
    RollupRewriter.RollupRewriteRefusal.CascadeCoverageUncovered(
      "2026-09-08 uncovered (source has 23/24 hours)"),
    RollupRewriter.RollupRewriteRefusal.CascadePartiallyEligible(
      Set("med"))
  )

  test("reasonName labels are unique across the whole refusal ADT") {
    val names = allRefusals.map(RollupRewriter.RollupRewriteRefusal.reasonName)
    names.distinct should have size allRefusals.size
    names.foreach(_.charAt(0).isLower shouldBe true) // stable camelCase convention
  }

  test("only UnsplittableAggregate is permanent; every other refusal is recoverable") {
    allRefusals.foreach { r =>
      val expected = r == RollupRewriter.RollupRewriteRefusal.UnsplittableAggregate
      RollupRewriter.RollupRewriteRefusal.isPermanent(r) shouldBe expected
    }
  }
}
