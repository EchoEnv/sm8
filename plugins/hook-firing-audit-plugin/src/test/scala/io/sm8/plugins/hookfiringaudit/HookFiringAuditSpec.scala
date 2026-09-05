/*
 * Unit spec for the hook-firing-audit plugin: probe stamping behavior,
 * terminal-reporter selection, report diff logic (fired /
 * legitimately-skipped / missing), and inertness detection.
 *
 * The end-to-end production-wiring spec (probe + reporter through the
 * real orchestrator) lives in sm8-platform's test suite, which is the
 * module that owns the orchestrator dependency.
 */
package io.sm8.plugins.hookfiringaudit

import io.sm8.core.engine.EngineError
import io.sm8.sdk._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HookFiringAuditSpec extends AnyFlatSpec with Matchers {

  // ---- fixtures ----

  private case object TestRequest extends Request
  private case object TestResult extends Result

  private def ctx(
      stage: PipelineStage = PipelineStage.Parse,
      stop: Boolean = false,
      meta: Map[String, Any] = Map.empty
  ): Context =
    Context(stage = stage, request = TestRequest, result = None, meta = meta, stop = stop)

  private val AllWireNames: List[String] =
    List("pre:parse", "post:parse", "pre:resolve", "post:resolve",
         "pre:execute", "post:execute", "pre:format", "post:format")

  // ---- probe stamping ----

  "StageProbeHook" should "stamp its stage wireName into the accumulator" in {
    val out = new PreResolveProbe().run(ctx())
    out.meta(HookFiringAuditKeys.StampsKey) shouldBe Set("pre:resolve")
  }

  it should "accumulate across probes without losing earlier stamps" in {
    val afterPreParse = new PreParseProbe().run(ctx())
    val afterPostParse = new PostParseProbe().run(afterPreParse)
    val afterPreResolve = new PreResolveProbe().run(afterPostParse)

    afterPreResolve.meta(HookFiringAuditKeys.StampsKey) shouldBe
      Set("pre:parse", "post:parse", "pre:resolve")
  }

  it should "start a fresh accumulator when none is present" in {
    val out = new PostExecuteProbe().run(ctx(meta = Map("unrelated" -> 1)))
    out.meta(HookFiringAuditKeys.StampsKey) shouldBe Set("post:execute")
  }

  it should "ignore a malformed accumulator written under the namespaced key" in {
    // Defense in depth: a non-String Set under the namespaced key
    // must not poison the stamping (no ClassCastException).
    val poisoned = ctx(meta = Map(HookFiringAuditKeys.StampsKey -> Set(1, 2, 3)))
    val out = new PreResolveProbe().run(poisoned)
    out.meta(HookFiringAuditKeys.StampsKey) shouldBe Set("pre:resolve")
  }

  // ---- terminal-reporter selection ----

  "StageReporter" should "not write the report when it is not terminal" in {
    val stamped = new PostParseProbe().run(new PreParseProbe().run(ctx()))
    val out = new StageReporter(HookStage.PostParse).run(stamped)
    out.meta.get(HookFiringAuditKeys.ReportKey) shouldBe None
    // It still stamped its own post point.
    out.meta(HookFiringAuditKeys.StampsKey) shouldBe Set("pre:parse", "post:parse")
  }

  it should "write the report when it is at post:format (completed pipeline)" in {
    val stamped = ctx().copy(meta =
      Map(HookFiringAuditKeys.StampsKey ->
        Set("pre:parse", "post:parse", "pre:resolve", "post:resolve",
            "pre:execute", "post:execute", "pre:format", "post:format")))
    val out = new StageReporter(HookStage.PostFormat).run(stamped)
    out.meta.contains(HookFiringAuditKeys.ReportKey) shouldBe true
  }

  it should "write the report when stop is set, regardless of its own stage" in {
    val stamped = ctx(stop = true).copy(meta =
      Map(HookFiringAuditKeys.StampsKey -> Set("pre:parse", "post:parse")))
    val out = new StageReporter(HookStage.PostParse).run(stamped)
    out.meta.contains(HookFiringAuditKeys.ReportKey) shouldBe true
  }

  // ---- report content ----

  "StageReporter report" should "report all 8 fired and no anomaly when all slots stamped" in {
    val stamped = ctx().copy(meta =
      Map(HookFiringAuditKeys.StampsKey -> AllWireNames.toSet))
    val report = new StageReporter(HookStage.PostFormat).run(stamped)

    val reportMap = report.meta(HookFiringAuditKeys.ReportKey).asInstanceOf[Map[String, Any]]
    reportMap("fired") shouldBe AllWireNames.sorted
    reportMap("skipped") shouldBe List.empty
    reportMap("missing") shouldBe List.empty
    reportMap("stopped") shouldBe false
    report.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }

  it should "classify slots after the deepest fired slot as skipped on a stopped context" in {
    // Halt at Resolve: parse pre+post and resolve pre+post fired; the
    // terminal reporter runs at post:resolve (stop = true).
    val stamped = ctx(stage = PipelineStage.Resolve, stop = true).copy(meta =
      Map(HookFiringAuditKeys.StampsKey ->
        Set("pre:parse", "post:parse", "pre:resolve", "post:resolve")))
    val report = new StageReporter(HookStage.PostResolve).run(stamped)

    val reportMap = report.meta(HookFiringAuditKeys.ReportKey).asInstanceOf[Map[String, Any]]
    reportMap("stopped") shouldBe true
    reportMap("fired") shouldBe
      List("post:parse", "post:resolve", "pre:parse", "pre:resolve") // sorted
    reportMap("skipped") shouldBe
      List("post:execute", "post:format", "pre:execute", "pre:format") // sorted (ASCII 'o' < 'r')
    reportMap("missing") shouldBe List.empty
    report.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }

  it should "surface a typed anomaly when a reached slot never stamps" in {
    // The dispatcher-inertness shape this plugin exists to detect:
    // slots the pipeline reached but whose probe never stamped.
    val incomplete = ctx().copy(
      meta = Map(HookFiringAuditKeys.StampsKey ->
        Set("pre:parse", "post:parse", "pre:resolve", "post:resolve", "pre:format")
      )
    )
    val report = new StageReporter(HookStage.PostFormat).run(incomplete)

    report.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe Some(
      EngineError.UnsupportedCapability(
        engine     = "hook-firing-audit-plugin",
        capability = "HookNotFired",
        message    = "hooks registered but never fired: post:execute, pre:execute"
      )
    )
    val reportMap = report.meta(HookFiringAuditKeys.ReportKey).asInstanceOf[Map[String, Any]]
    reportMap("missing") shouldBe List("post:execute", "pre:execute")
  }

  // ---- Observer semantics ----

  "StageReporter" should "have runsOnStop = true (Observer semantics by default)" in {
    new StageReporter(HookStage.PostFormat).runsOnStop shouldBe true
  }

  // ---- rank helper ----

  "PipelineStageRank" should "rank wireNames on the 0-7 axis in pipeline order" in {
    PipelineStageRank.rankOfWireName("pre:parse") shouldBe 0
    PipelineStageRank.rankOfWireName("post:format") shouldBe 7
    PipelineStageRank.rankOfWireName("pre:execute") should be >
      PipelineStageRank.rankOfWireName("post:resolve")
  }

  it should "map each wireName to twice its stage rank plus the pre/post offset" in {
    PipelineStageRank.rankOfWireName("pre:resolve") shouldBe 2 * PipelineStageRank.rank(PipelineStage.Resolve)
    PipelineStageRank.rankOfWireName("post:resolve") shouldBe 2 * PipelineStageRank.rank(PipelineStage.Resolve) + 1
    PipelineStageRank.rankOfWireName("pre:execute") shouldBe 2 * PipelineStageRank.rank(PipelineStage.Execute)
  }
}
