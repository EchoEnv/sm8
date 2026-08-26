package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration

/** Phase 2 contract: prove `EngineContext` + its 5 sub-ADTs work as
  * pure data. Each sub-ADT is a sealed trait with case objects or
  * final case classes; no behavior; no engine coupling.
  */
class EngineContextSpec extends AnyFunSuite with Matchers {

  // -- CachePolicy --

  test("CachePolicy has 4 cases: NoCache, ReadThrough, WriteThrough, ReadOnly") {
    val all: Set[CachePolicy] = Set(
      CachePolicy.NoCache,
      CachePolicy.ReadThrough,
      CachePolicy.WriteThrough,
      CachePolicy.ReadOnly,
    )
    all.size shouldBe 4
  }

  // -- AuditPolicy --

  test("AuditPolicy has 2 cases: NoAudit, EngineDefault") {
    val all: Set[AuditPolicy] = Set(
      AuditPolicy.NoAudit,
      AuditPolicy.EngineDefault,
    )
    all.size shouldBe 2
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

  // -- CancellationCapability --

  test("CancellationCapability has 4 cases: Cooperative, SparkJobTag, RemoteStatement, Unsupported") {
    val all: Set[CancellationCapability] = Set(
      CancellationCapability.Cooperative("req-1"),
      CancellationCapability.SparkJobTag("req-2"),
      CancellationCapability.RemoteStatement("req-3"),
      CancellationCapability.Unsupported,
    )
    all.size shouldBe 4
  }

  test("CancellationCapability.Cooperative carries requestId") {
    CancellationCapability.Cooperative("req-42").requestId shouldBe "req-42"
  }

  test("CancellationCapability.SparkJobTag carries requestId") {
    CancellationCapability.SparkJobTag("req-42").requestId shouldBe "req-42"
  }

  test("CancellationCapability.RemoteStatement carries requestId") {
    CancellationCapability.RemoteStatement("req-42").requestId shouldBe "req-42"
  }

  // -- EngineContext --

  test("EngineContext.defaultContext has sensible defaults") {
    val ctx = EngineContext.defaultContext
    ctx.cachePolicy shouldBe CachePolicy.NoCache
    ctx.auditPolicy shouldBe AuditPolicy.NoAudit
    ctx.joinHints shouldBe JoinHints()
    ctx.timeout shouldBe Duration.Inf
    ctx.cancellation shouldBe CancellationCapability.Unsupported
  }

  test("EngineContext holds all 5 fields with arbitrary values") {
    val ctx = EngineContext(
      cachePolicy       = CachePolicy.ReadThrough,
      auditPolicy       = AuditPolicy.EngineDefault,
      joinHints         = JoinHints(skewFactor = Some(5)),
      timeout           = Duration("30 seconds"),
      cancellation      = CancellationCapability.RemoteStatement("req-99"),
    )
    ctx.cachePolicy shouldBe CachePolicy.ReadThrough
    ctx.auditPolicy shouldBe AuditPolicy.EngineDefault
    ctx.joinHints.skewFactor shouldBe Some(5)
    ctx.timeout shouldBe Duration("30 seconds")
    ctx.cancellation shouldBe CancellationCapability.RemoteStatement("req-99")
  }

  test("EngineContext equality: same data => equal") {
    val a = EngineContext.defaultContext
    val b = EngineContext.defaultContext
    a shouldBe b
  }
}