package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// ADR-009-g Fix 2: the engine-side CachePolicy ADT was deleted; the test
// imports the single-source model-side ADT. The 4-cases test became a
// 3-cases test against the model-side ADT (ReadOnly is gone).
// The ReadThrough/WriteThrough references are case-class invocations
// carrying a required 'name: String'.
import io.sm8.core.model.CachePolicy


import scala.concurrent.duration.Duration

/** Phase 2 contract: prove `EngineContext` + its 5 sub-ADTs work as
  * pure data. Each sub-ADT is a sealed trait with case objects or
  * final case classes; no behavior; no engine coupling.
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

  // ADR-009-g Fix 2: the model-side ReadThrough is a case class with
  // required 'name: String'. The case-object engine-side ReadThrough
  // is gone — the migration adds the ("default") literal.
  test("EngineContext holds all 5 fields with arbitrary values") {
    val ctx = EngineContext(
      cachePolicy       = CachePolicy.ReadThrough("default"),
      auditPolicy       = AuditPolicy.EngineDefault,
      joinHints         = JoinHints(skewFactor = Some(5)),
      timeout           = Duration("30 seconds"),
      cancellation      = CancellationCapability.RemoteStatement("req-99"),
    )
    ctx.cachePolicy shouldBe CachePolicy.ReadThrough("default")
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