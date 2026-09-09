/*
 * FreshnessPolicySpec — Tier 2 D3 core-contract tests (ADR-0030).
 *
 * Pins three pure-data contracts, none of which touch IO:
 *
 *   1. Validation: a freshness policy REQUIRES timeGrain +
 *      grainDimension (the staleness verdict is per bucket; a
 *      grain-less rollup has no buckets to gate). A policy on a
 *      well-grained rollup passes.
 *   2. Default absence: no policy = valid, and routing behavior is
 *      pre-Tier-2 (this spec pins the MODEL-side half: the field
 *      defaults to None).
 *   3. Refusal vocabulary: RollupBucketStale exists in the closed
 *      RollupRewriteRefusal ADT with the full non-final bucket set,
 *      carries BucketKey identity, and its tag is "rollupBucketStale"
 *      — the string-tag ↔ ADT correspondence the observer taxonomy
 *      reads (mirrors the RollupSchemaStale tag test's shape).
 *
 * The connector-side watermark lookup and the policy-gated emission
 * are OUT of core's scope (RFC §3) and are tested in the connector's
 * Tier 2 specs.
 */
package io.sm8.core.model

import io.sm8.core.engine.ResolvedSource
import io.sm8.core.expr.Expr
import io.sm8.core.rel.{AggregateCall, AggregateFn, RollupRewriter}
import io.sm8.core.schema.{Field, SealedDataType}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FreshnessPolicySpec extends AnyFunSuite with Matchers {

  private val peopleScan: ResolvedSource.Scan = ResolvedSource.Scan(
    source = io.sm8.core.model.SourceRef.ByName(table = "people"),
    schema = List(
      Field("id",     SealedDataType.Int,     nullable = false),
      Field("region", SealedDataType.Varchar, nullable = false),
      Field("amount", SealedDataType.Int,     nullable = false),
    ),
  )

  /** A grained rollup host model; freshness is the varying axis. */
  private def modelWith(freshness: Option[FreshnessPolicy]) =
    Model.of(
      name    = "fresh",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName(table = "people"),
      dimensions = List(
        io.sm8.core.model.Dimension(
          name = "day", expr = Expr.FieldRef("id"),
          dataType = Some(SealedDataType.Date)),
        io.sm8.core.model.Dimension.field("region", "region"),
      ),
      measures = List(io.sm8.core.model.Measure(
        "total",
        AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total"))),
      rollups = List(io.sm8.core.model.RollupSpec(
        "by_day", List("day"), List("total"),
        timeGrain = Some("day"), grainDimension = Some("day"),
        freshness = freshness)),
    )

  // ===== 1. validation: policy requires buckets =====

  test("freshness: FinalRequired on a grained rollup passes validation") {
    modelWith(Some(FreshnessPolicy.FinalRequired)) shouldBe a[Right[_, _]]
  }

  test("freshness: policy without grain fails loud (no buckets to gate)") {
    val model = Model.of(
      name    = "grainless",
      version = 1,
      source  = io.sm8.core.model.SourceRef.ByName(table = "people"),
      dimensions = List(
        io.sm8.core.model.Dimension.field("region", "region")),
      measures = List(io.sm8.core.model.Measure(
        "total",
        AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total"))),
      rollups = List(io.sm8.core.model.RollupSpec(
        "by_region", List("region"), List("total"),
        freshness = Some(FreshnessPolicy.FinalRequired))),
    )
    val errs = model.left.toOption.get
      .asInstanceOf[ModelValidationError.SchemaValidation].messages
    errs.exists(_.contains("freshness policy requires timeGrain + grainDimension")) shouldBe true
  }

  // ===== 2. default absence =====

  test("freshness: no policy is the valid default (pre-Tier-2 routing)") {
    modelWith(None) shouldBe a[Right[_, _]]
  }

  test("freshness: RollupSpec.freshness defaults to None") {
    io.sm8.core.model.RollupSpec(
      "by_day", List("day"), List("total"),
      timeGrain = Some("day"), grainDimension = Some("day"))
      .freshness shouldBe None
  }

  // ===== 3. refusal vocabulary =====

  test("vocabulary: RollupBucketStale carries the full bucket set + tag") {
    val buckets = Set(
      RollupRewriter.RollupRewriteRefusal.BucketKey("2026-09-08"),
      RollupRewriter.RollupRewriteRefusal.BucketKey("2026-09-09"))
    val refusal = RollupRewriter.RollupRewriteRefusal.RollupBucketStale(buckets)
    refusal.buckets should contain theSameElementsAs buckets
    RollupRewriter.RollupRewriteRefusal.reasonName(refusal) shouldBe "rollupBucketStale"
  }

  test("vocabulary: empty bucket set is representable (defensive shape)") {
    val refusal = RollupRewriter.RollupRewriteRefusal.RollupBucketStale(Set.empty)
    refusal.buckets shouldBe empty
  }

  // ===== schema-aware half: grain resolution feeds the validator =====

  test("freshness: schema-aware validation accepts grained + FinalRequired") {
    modelWith(Some(FreshnessPolicy.FinalRequired)).foreach { m =>
      ModelValidator.validateAgainstSchema(m, peopleScan) shouldBe Right(())
    }
  }
}
