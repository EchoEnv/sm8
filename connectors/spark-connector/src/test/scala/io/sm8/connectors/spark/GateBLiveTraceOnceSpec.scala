package io.sm8.connectors.spark

import org.scalatest.funsuite.AnyFunSuite

/** ONE-SHOT live-model Gate B trace against the staged representative
  * workload. Opt-in only: runs when -Dgateb.live.trace=true is set.
  * Writes the JSON to /var/lib/sm8/gate-b/traces/<date>.json via the
  * runner's own out-path handling (called through GateBTraceRunner.main
  * so the exact production CLI path is exercised). */
class GateBLiveTraceOnceSpec extends AnyFunSuite {
  test("live-model trace (opt-in via -Dgateb.live.trace=true)") {
    assume(sys.env.get("GATEB_LIVE_TRACE").contains("true"),
      "opt-in only: set GATEB_LIVE_TRACE=true")
    // Arrange: stage the workload (embedded warehouse under /var/lib).
    val warehouse = "/var/lib/sm8/gate-b/warehouse"
    GateBStageWorkload.main(Array(warehouse))
    // Act: the production CLI entrypoint, same process (inherits the
    // Maven argLine add-opens — the full JVM-17 set that avoids the
    // netty SIGSEGV seen under bare java).
    val out = s"/var/lib/sm8/gate-b/traces/${java.time.LocalDate.now()}.json"
    GateBTraceRunner.main(Array(
      "--model", "gateb_representative",
      "--out-path", out,
      "--bootstrap-warehouse", warehouse,
      "--model-path", "/var/lib/sm8/gate-b/workload/gateb-representative.yaml",
      "--rollup", "by_day_region",
      "--scope-date", java.time.LocalDate.now().minusDays(1).toString
    ))
    // Assert: the runner exits normally only when the report wrote; the
    // file's existence + isLiveModel marker is the contract.
    val source = scala.io.Source.fromFile(out)
    val content =
      try source.mkString
      finally source.close()
    assert(content.contains("\"isLiveModel\" : true"),
      s"trace must carry isLiveModel=true; got: ${content.take(200)}")
  }
}
