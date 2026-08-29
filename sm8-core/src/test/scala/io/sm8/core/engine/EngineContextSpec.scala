package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// ADR-009-g Fix 2: the engine-side CachePolicy ADT was deleted; the test
// imports the single-source model-side ADT. The 4-cases test became a
// 3-cases test against the model-side ADT (ReadOnly is gone).
// The ReadThrough/WriteThrough references are case-class invocations
// carrying a required 'name: String'.
import io.sm8.core.model.CachePolicy

/** Phase 2 contract: prove `EngineContext` + its sub-ADTs work as
 * pure data. Each sub-ADT is a sealed trait with case objects or
 * final case classes; no behavior; no engine coupling.
 *
 * PR-199: the engine-side `AuditPolicy` ADT + the
 * `CancellationCapability` ADT + the `Duration` import were
 * removed (dead fields per the pre-existing pre-PR-199
 * audit). 6 tests that exercised these types are deleted.
 */
class EngineContextSpec extends AnyFunSuite with Matchers {

  // -- CachePolicy --

  // ADR-009-g Fix 2: model-side ADT has 3 cases (NoCache, ReadThrough(name),
  // WriteThrough(name)). The engine-side 4th case ReadOnly was deleted.
  // ReadThrough/WriteThrough are case classes — they carry a required name.
  test("CachePolicy has 3 cases: NoCache, ReadThrough, WriteThrough") {
    val all: Set[CachePolicy] = Set(
      CachePolicy.NoCache,
      CachePolicy.ReadThrough("default"),
      CachePolicy.WriteThrough("default"),
    )
    all.size shouldBe 3
  }

  // -- JoinHints --

  test("JoinHints default constructor has all fields None") {
    val h = JoinHints()
    h.broadcastRightBelowBytes shouldBe None
    h.skewFactor shouldBe None
    h.preferredStrategy shouldBe None
  }

  test("JoinHints with all fields set carries all values") {
    val h = JoinHints(
      broadcastRightBelowBytes = Some(10485760L),
      skewFactor = Some(10),
      preferredStrategy = Some(JoinStrategy.Broadcast),
    )
    h.broadcastRightBelowBytes shouldBe Some(10485760L)
    h.skewFactor shouldBe Some(10)
    h.preferredStrategy shouldBe Some(JoinStrategy.Broadcast)
  }

  test("JoinHints equality: same fields => equal") {
    JoinHints() shouldBe JoinHints()
    JoinHints(Some(1L)) shouldBe JoinHints(Some(1L))
  }

  // -- JoinStrategy --

  test("JoinStrategy has 3 cases: Broadcast, ShuffleHash, SortMerge") {
    val all: Set[JoinStrategy] = Set(
      JoinStrategy.Broadcast,
      JoinStrategy.ShuffleHash,
      JoinStrategy.SortMerge,
    )
    all.size shouldBe 3
  }

  // -- EngineContext --

  test("EngineContext.defaultContext has sensible defaults") {
    val ctx = EngineContext.defaultContext
    ctx.cachePolicy shouldBe CachePolicy.NoCache
    ctx.joinHints shouldBe JoinHints()
  }

  test("EngineContext holds cachePolicy + joinHints with arbitrary values") {
    val ctx = EngineContext(
      cachePolicy = CachePolicy.ReadThrough("default"),
      joinHints   = JoinHints(skewFactor = Some(5)),
    )
    ctx.cachePolicy shouldBe CachePolicy.ReadThrough("default")
    ctx.joinHints.skewFactor shouldBe Some(5)
  }

  test("EngineContext equality: same data => equal") {
    val a = EngineContext.defaultContext
    val b = EngineContext.defaultContext
    a shouldBe b
  }
}