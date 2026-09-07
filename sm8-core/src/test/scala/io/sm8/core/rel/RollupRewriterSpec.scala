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

  test("Project above Aggregate (calculated dims) -> non-canonical in v1") {
    val plan = RelOp.Project(
      input = canonicalPlan(),
      expressions = List((Expr.FieldRef("carrier"), "carrier")))
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
    val dayRollup = RollupSpec("by_carrier_day", List("carrier"), List("rows", "total_fare"), Some("day"))
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
    proj.expressions.map(_._2) shouldBe List("avg_fare")
    // Derived: Divide(sum__fare_total, count__fare_total).
    proj.expressions.head._1 shouldBe
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
    // Outer Project: additive passthrough + algebraic derived, in
    // REQUEST order, aliases = request aliases.
    proj.expressions.map(_._2) shouldBe List("rows", "avg_fare")
    proj.expressions.head._1 shouldBe Expr.FieldRef("rows")
    proj.expressions(1)._1 shouldBe
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
    proj.expressions.map(_._2) shouldBe List("avg_fare", "fare_stddev")
    // StddevSample derives as sqrt(guarded-variance): FunctionCall("sqrt", ...).
    proj.expressions(1)._1 shouldBe
      Expr.FunctionCall("sqrt", Seq(
        Expr.CaseWhen(
          List((
            Expr.LessThan(Expr.FieldRef("count__fare_total"),
              Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int)),
            Expr.Literal(LiteralValue.NullValue, SealedDataType.Double))),
          Expr.Divide(
            Expr.Subtract(Expr.FieldRef("sumsq__fare_total"),
              Expr.Divide(Expr.Multiply(Expr.FieldRef("sum__fare_total"), Expr.FieldRef("sum__fare_total")),
                Expr.FieldRef("count__fare_total"))),
            Expr.Subtract(Expr.FieldRef("count__fare_total"),
              Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int))))
      ))
  }

  test("StddevSample(tax) on a rollup declaring Avg(tax) -> UnsplittableAggregate (identity refusal precedes decomposability)") {
    // algebraicModel's rollup carries avg_fare state columns
    // (count__fare, sum__fare, sumsq__fare — stateColumnsFor emits
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

  test("stddev_samp guard: total n < 2 -> NULL (engine parity with base path)") {
    val e = RollupRewriter.stddevSampGuardExpr("n", "s", "ss")
    e shouldBe a[Expr.CaseWhen]
    val Expr.CaseWhen(branches, _) = e
    val (cond, res) = branches.head
    // Condition references the caller-named re-aggregated total-n column.
    cond shouldBe Expr.LessThan(
      Expr.FieldRef("n"),
      Expr.Literal(LiteralValue.IntValue(2), SealedDataType.Int))
    // Result of the guarded branch is NULL.
    res shouldBe Expr.Literal(LiteralValue.NullValue, SealedDataType.Double)
  }

  test("n=1 single-observation group edge: population stddev guard (n<=0 -> NULL, else defined)") {
    val e = RollupRewriter.stddevPopGuardExpr("n", "s", "ss")
    e shouldBe a[Expr.CaseWhen]
    val Expr.CaseWhen(branches, _) = e
    val (cond, res) = branches.head
    cond shouldBe Expr.LessOrEqual(
      Expr.FieldRef("n"),
      Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int))
    // branches.head is the GUARDED branch (n<=0 -> NULL); assert it.
    res shouldBe Expr.Literal(LiteralValue.NullValue, SealedDataType.Double)
    // The non-guarded body computes the population form
    // (sumSq - sum^2/n) / n: an n=1 group yields the defined 0.0 at
    // eval time (x - x^2 = 0).
    val Expr.CaseWhen(_, body) = e
    body shouldBe a[Expr.Divide]
    body.toString shouldBe "Divide(Subtract(FieldRef(ss),Divide(Multiply(FieldRef(s),FieldRef(s)),FieldRef(n))),FieldRef(n))"
  }

  // -- helpers --

  private def findAggregate(plan: RelOp): RelOp.Aggregate = plan match {
    case a: RelOp.Aggregate => a
    case RelOp.Sort(input, _) => findAggregate(input)
    case RelOp.Limit(input, _, _) => findAggregate(input)
    case other => fail(s"no Aggregate under $other")
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
}
