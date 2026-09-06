/*
 * SM8 Core — AggregateFnDecomposabilitySpec (pre-aggregation map, Ticket 1; design record docs/adr/0022).
 *
 * Guards the typed `Decomposability` classification over the 16
 * aggregate functions. The prior prose section comments carried
 * three errors (Min/Max labeled non-additive; First labeled
 * "additive for some rollups"; CountDistinct sitting under the
 * additive banner) — this spec makes the corrected classification
 * executable, so a future RollupRewriter (ADR-0022 Ticket 4) can
 * consult `decomposability(fn)` without re-deriving aggregate
 * algebra from prose.
 *
 * Classification rationale (textbook decomposable-aggregate
 * theory, monoid/algebraic framing):
 * - Additive: f is associative (+ idempotent for Min/Max), so
 *   f over partials equals f over the whole. The prior comment
 *   misfiled Min/Max as non-additive; `min(min(p1), min(p2))` is
 *   the global min.
 * - Algebraic: computable from a fixed vector of named partial
 *   states — Avg from (sum, count); Stddev/Variance from
 *   (n, sum, sumSq). NOT the same as additive: AVG(avg-parts)
 *   is wrong; SUM(sum-parts)/SUM(count-parts) is right.
 * - Positional: First/Last depend on within-group ordering;
 *   re-aggregation needs argmin/argmax (value, order-key) state.
 * - Holistic: exact Median/Percentile* need the full within-group
 *   distribution; no bounded partial state suffices.
 * - Approximable: re-aggregable only via sketches (HLL, GK/
 *   DDsketch) and then approximate — serving these from a rollup
 *   silently swaps exact for approximate semantics, forbidden
 *   in v1 routing.
 */
package io.sm8.core.rel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class AggregateFnDecomposabilitySpec extends AnyFlatSpec with Matchers {

  private def all: List[(AggregateFn, Decomposability)] = List(
    (AggregateFn.Sum,                Decomposability.Additive),
    (AggregateFn.Count,              Decomposability.Additive),
    (AggregateFn.Min,                Decomposability.Additive),
    (AggregateFn.Max,                Decomposability.Additive),
    (AggregateFn.Avg,                Decomposability.Algebraic),
    (AggregateFn.StddevSample,       Decomposability.Algebraic),
    (AggregateFn.StddevPopulation,   Decomposability.Algebraic),
    (AggregateFn.VarianceSample,     Decomposability.Algebraic),
    (AggregateFn.VariancePopulation, Decomposability.Algebraic),
    (AggregateFn.CountDistinct,      Decomposability.Approximable),
    (AggregateFn.ApproxPercentile,   Decomposability.Approximable),
    (AggregateFn.Median,             Decomposability.Holistic),
    (AggregateFn.PercentileContinuous, Decomposability.Holistic),
    (AggregateFn.PercentileDiscrete,   Decomposability.Holistic),
    (AggregateFn.First,              Decomposability.Positional),
    (AggregateFn.Last,               Decomposability.Positional)
  )

  behavior.of("AggregateFn.decomposability")

  it should "classify all 16 aggregate functions (regression guards on the three previously-wrong prose entries)" in {
    all.foreach { case (fn, expected) =>
      withClue(s"${fn.toString}: ") {
        AggregateFn.decomposability(fn) shouldBe expected
      }
    }
  }

  it should "classify Min/Max as Additive (min(min(part)) = min(total); prior comment said non-additive)" in {
    // min and max are associative AND idempotent: combining partial
    // minima/maxima yields the global minimum/maximum.
    AggregateFn.decomposability(AggregateFn.Min) shouldBe Decomposability.Additive
    AggregateFn.decomposability(AggregateFn.Max) shouldBe Decomposability.Additive
  }

  it should "classify First/Last as Positional, NOT additive (prior comment said 'additive for some rollups')" in {
    // first(first(p1), first(p2)) depends on within-group ordering;
    // without argmin/argmax (value, order-key) state the merge is
    // undefined. No partial state any current connector exposes.
    AggregateFn.decomposability(AggregateFn.First) shouldBe Decomposability.Positional
    AggregateFn.decomposability(AggregateFn.Last)  shouldBe Decomposability.Positional
  }

  it should "classify CountDistinct as Approximable, NOT additive (re-aggregable only via HLL, and then approximate)" in {
    // Exact count-distinct re-aggregation needs the full per-group
    // distinct sets. HLL sketches make it re-aggregable but
    // approximate — a wire-contract change, never a rollup artifact
    // (ADR-0022).
    AggregateFn.decomposability(AggregateFn.CountDistinct) shouldBe Decomposability.Approximable
  }

  it should "classify Avg and the Stddev/Variance family as Algebraic with their named partial states" in {
    // avg = SUM(sum-parts) / SUM(count-parts) — re-aggregable from
    // (sum, count), not by applying avg to avg-partials.
    // stddev/variance re-aggregate from (n, sum, sumSq).
    val algebraic = all.collect { case (fn, Decomposability.Algebraic) => fn }.toSet
    algebraic shouldBe Set(
      AggregateFn.Avg,
      AggregateFn.StddevSample,
      AggregateFn.StddevPopulation,
      AggregateFn.VarianceSample,
      AggregateFn.VariancePopulation
    )
  }

  it should "classify the exact order-statistics as Holistic (no bounded partial state suffices)" in {
    val holistic = all.collect { case (fn, Decomposability.Holistic) => fn }.toSet
    holistic shouldBe Set(
      AggregateFn.Median,
      AggregateFn.PercentileContinuous,
      AggregateFn.PercentileDiscrete
    )
  }

  it should "be total over the sealed ADT: every case maps to exactly one class (16 fns, all classified)" in {
    // Exhaustiveness is compile-WARNED by the match (non-fatal under
    // this build), and case REMOVAL is caught by this spec failing to
    // compile (it references each case object). What the count pins
    // is the classification COVERAGE of this spec's table: if an
    // aggregate is added, the compiler warning plus review force the
    // arm + a row here.
    all.size shouldBe 16
    all.map(_._1).toSet.size shouldBe 16
  }

  it should "have a Serializable Decomposability taxonomy (closure-safety convention, cf. AggregateCallClosureSafetySpec)" in {
    // Decomposability is 5 stateless case objects on a sealed
    // Product with Serializable trait — serialization-safe by
    // construction; this round-trip pins that contract so a future
    // refactor carrying captured state fails here, not in Spark.
    import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
    val values = List(Decomposability.Additive, Decomposability.Algebraic,
      Decomposability.Positional, Decomposability.Holistic, Decomposability.Approximable)
    val buf = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(buf)
    oos.writeObject(values)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(buf.toByteArray))
    ois.readObject().asInstanceOf[List[Decomposability]] shouldBe values
  }
}
