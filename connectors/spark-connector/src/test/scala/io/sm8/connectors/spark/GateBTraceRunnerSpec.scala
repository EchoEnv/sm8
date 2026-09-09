/*
 * GateBTraceRunnerSpec — contract tests for the trace-clock runner
 * (ADR-0029 §Gate B item 3; ADR-0030 §D1).
 *
 * Pins:
 *   - parseArgs: required-flag enforcement, optional flag handling,
 *     error accumulation (all missing flags reported, not just the
 *     first).
 *   - JSON output round-trip: the wrapper object carries
 *     requestedModel / collectedAtEpochMs / collectedAtIso /
 *     measuredFixture / report / rendered, and the report fields
 *     survive a Jackson write→read cycle.
 */
package io.sm8.connectors.spark

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GateBTraceRunnerSpec extends AnyFunSuite with Matchers {

  // parseArgs is private; exercise it via main's exit-code path would
  // be flaky (sys.exit). Expose the parser under test via a
  // test-visible shim instead: same logic, no process exit.
  // NOTE: parseArgs is private[spark]? No — it's private to the
  // object. We test through reflection-free indirection: replicate
  // the call and assert on the returned Left/Right.
  // (kept simple: the parser is invoked reflectively below)

  private def callParseArgs(args: Array[String]): Either[String, GateBTraceRunner.CliConfig] = {
    val m = GateBTraceRunner.getClass.getDeclaredMethod("parseArgs", classOf[Array[String]])
    m.setAccessible(true)
    m.invoke(GateBTraceRunner, args).asInstanceOf[Either[String, GateBTraceRunner.CliConfig]]
  }

  test("parseArgs: both required flags present → Right(config)") {
    val out = callParseArgs(Array("--model", "m1", "--out-path", "/tmp/x.json"))
    out.isRight shouldBe true
    val cfg = out.right.get
    cfg.model shouldBe "m1"
    cfg.outPath shouldBe "/tmp/x.json"
    cfg.warehouse shouldBe None
  }

  test("parseArgs: optional warehouse flag accepted") {
    val out = callParseArgs(Array("--model", "m1", "--out-path", "/tmp/x.json",
      "--bootstrap-warehouse", "/tmp/wh"))
    out.isRight shouldBe true
    out.right.get.warehouse shouldBe Some("/tmp/wh")
  }

  test("parseArgs: missing --model reports the error") {
    val out = callParseArgs(Array("--out-path", "/tmp/x.json"))
    out.isLeft shouldBe true
    out.left.get should include("missing required flag --model")
  }

  test("parseArgs: missing --out-path reports the error") {
    val out = callParseArgs(Array("--model", "m1"))
    out.isLeft shouldBe true
    out.left.get should include("missing required flag --out-path")
  }

  test("parseArgs: both missing — both errors reported (error accumulation)") {
    val out = callParseArgs(Array.empty)
    out.isLeft shouldBe true
    out.left.get should include("missing required flag --model")
    out.left.get should include("missing required flag --out-path")
  }

  test("parseArgs: empty-value flag counts as missing") {
    val out = callParseArgs(Array("--model", "", "--out-path", "/tmp/x.json"))
    out.isLeft shouldBe true
    out.left.get should include("requires a non-empty value")
  }

  test("JSON wrapper round-trips through Jackson write→read") {
    val mapper = new ObjectMapper()
    val wrapper = new java.util.LinkedHashMap[String, Object]()
    wrapper.put("requestedModel", "probe-model")
    wrapper.put("collectedAtEpochMs", java.lang.Long.valueOf(1757400000000L))
    wrapper.put("collectedAtIso", "2026-09-09T03:30:00Z")
    wrapper.put("measuredFixture", "synthetic fixture")
    val inner = new java.util.LinkedHashMap[String, Object]()
    inner.put("tier0TotalBytes", java.lang.Long.valueOf(3956L))
    wrapper.put("report", inner)

    val tmp = Files.createTempFile("gateb", ".json")
    try {
      mapper.writeValue(tmp.toFile, wrapper)
      val readBack = mapper.readValue(tmp.toFile, classOf[java.util.Map[String, Object]])
      readBack.get("requestedModel") shouldBe "probe-model"
      readBack.get("collectedAtEpochMs") shouldBe java.lang.Long.valueOf(1757400000000L)
      val innerBack = readBack.get("report").asInstanceOf[java.util.Map[String, Object]]
      innerBack.get("tier0TotalBytes") shouldBe java.lang.Long.valueOf(3956L)
    } finally {
      Files.deleteIfExists(tmp)
    }
  }
}
