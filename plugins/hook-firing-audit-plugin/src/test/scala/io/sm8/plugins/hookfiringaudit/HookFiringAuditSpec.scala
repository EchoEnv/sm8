/*
 * Unit spec for the hook-firing-audit plugin: probe stamping behavior,
 * reporter diff logic (fired / legitimately-skipped / missing), and
 * the end-to-end inertness-detection scenario.
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

  /** All 8 probes chained in pipeline order over a starting context. */
  private def runAllProbes(c: Context): Context =
    List(
      (new PreParseProbe: PreHook), (new PostParseProbe: PostHook),
      (new PreResolveProbe: PreHook), (new PostResolveProbe: PostHook),
      (new PreExecuteProbe: PreHook), (new PostExecuteProbe: PostHook),
      (new PreFormatProbe: PreHook)
    ).foldLeft(c) {
      case (acc, pre: PreHook)  => pre.run(acc)
      case (acc, post: PostHook) => post.run(acc)
      case (acc, _)             => acc
    }

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

  // ---- reporter: full pipeline ----

  "PostFormatReporter" should "report all 8 fired and no anomaly on a complete traversal" in {
    val stamped = runAllProbes(ctx())
    val report = new PostFormatReporter().run(stamped)

    report.meta(HookFiringAuditKeys.ReportKey) shouldBe Map(
      "fired"   -> AllWireNames.sorted,
      "skipped" -> List.empty,
      "missing" -> List.empty,
      "stopped" -> false
    )
    report.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }

  // ---- reporter: short-circuit path (legitimate skips) ----

  it should "classify at-or-after-stop stamps as skipped (not missing) on short-circuit" in {
    // Pipeline short-circuited at Resolve: parse's pre+post and
    // resolve's pre+post all fired (the orchestrator fires the
    // stopping stage's post-hooks); everything after resolve's post
    // slot was legitimately skipped.
    val firedBeforeStop = Set("pre:parse", "post:parse", "pre:resolve", "post:resolve")

    val seeded = ctx(stage = PipelineStage.Resolve, stop = true)
      .copy(meta = Map(HookFiringAuditKeys.StampsKey -> firedBeforeStop))
    val report = new PostFormatReporter().run(seeded)

    val reportMap = report.meta(HookFiringAuditKeys.ReportKey).asInstanceOf[Map[String, Any]]
    reportMap("missing") shouldBe List.empty
    // post:format reports as fired even on the stop path: the
    // reporter itself is a post:format observer (runsOnStop = true),
    // so the attachment point genuinely fired.
    reportMap("skipped") shouldBe
      List("post:execute", "pre:execute", "pre:format")
    report.meta.get(HookFiringAuditKeys.AnomalyKey) shouldBe None
  }

  // ---- reporter: inertness anomaly (the defect class this plugin detects) ----

  it should "surface a typed anomaly when a reached-stage probe did not fire" in {
    // Simulates the dispatcher-inertness defect shape this plugin
    // exists to detect: the dispatcher never dispatches a stage's
    // hooks, so that stage's probe never stamps, even though the
    // pipeline traversed the stage.
    val incomplete = ctx().copy(
      meta = Map(HookFiringAuditKeys.StampsKey ->
        Set("pre:parse", "post:parse", "pre:resolve", "post:resolve",
            "pre:format") // pre:execute + post:execute missing
      )
    )
    val report = new PostFormatReporter().run(incomplete)

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

  // ---- reporter runs on the stop path (Observer semantics) ----

  it should "fire on the short-circuit path (runsOnStop observer semantics)" in {
    // A PostHook defaults to runsOnStop = true; assert it explicitly.
    new PostFormatReporter().runsOnStop shouldBe true
  }

  // ---- PipelineStageRank ----

  "PipelineStageRank" should "rank wireNames on the shared 0-7 axis in pipeline order" in {
    PipelineStageRank.rankOfWireName("pre:parse") shouldBe 0
    PipelineStageRank.rankOfWireName("post:format") shouldBe 7
    PipelineStageRank.rankOfWireName("pre:execute") should be >
      PipelineStageRank.rankOfWireName("post:resolve")
  }

  it should "rank PipelineStage values consistently with wireName prefixes" in {
    // A short-circuit at stage S skips every attachment point after
    // S's own post slot: rank(wireName) = 2 * rank(stage) + offset.
    PipelineStageRank.rankOfWireName("pre:resolve") shouldBe 2 * PipelineStageRank.rank(PipelineStage.Resolve)
    PipelineStageRank.rankOfWireName("post:resolve") shouldBe 2 * PipelineStageRank.rank(PipelineStage.Resolve) + 1
    PipelineStageRank.rankOfWireName("pre:execute") shouldBe 2 * PipelineStageRank.rank(PipelineStage.Execute)
  }
}
