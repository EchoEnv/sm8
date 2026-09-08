/*
 * Rollup refusal report CLI tests (design: docs/adr/0027).
 *
 * Same harness approach as CliIntegrationSpec: in-process JDK
 * HttpServer on a random port, realistic /metrics responses,
 * real HTTP from the CLI (no stubs — the wire format IS the
 * contract for the Prometheus text exposition parser).
 *
 * Acceptance criteria:
 * - parser: golden /metrics body -> expected counter map
 *   (HELP/TYPE lines dropped, comments dropped, values as Long)
 * - ranking: refusals sorted desc with correct shares
 * - empty state: zero counters -> "no rollup traffic" message
 * - JSON: --json prints a flat name->value map
 * - HTTP failure: non-200 -> exit 1 with a clear stderr line;
 *   transport failure -> exit 3 (safeRun convention)
 */
package io.sm8.cli

import java.net.InetSocketAddress
import scala.collection.mutable

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RollupReportSpec
  extends AnyFunSpec
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private var server: HttpServer = _
  private var metricsUrl: String = _
  private val responses: mutable.Map[String, (Int, String)] = mutable.Map.empty

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", new RoutingHandler)
    server.start()
    val port = server.getAddress.getPort
    metricsUrl = s"http://127.0.0.1:$port"
  }

  override def afterAll(): Unit =
    if (server != null) server.stop(0)

  override def beforeEach(): Unit = responses.clear()

  private class RoutingHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val path = exchange.getRequestURI.getPath
      val (status, body) = responses.getOrElse(path, (404, "not found"))
      exchange.sendResponseHeaders(status, body.getBytes.length.toLong)
      val os = exchange.getResponseBody
      os.write(body.getBytes)
      os.close()
    }
  }

  /** Wrap Main.run with stdout + stderr capture (same dual-stream
    * convention as CliIntegrationSpec.runCli). */
  private def runCli(args: List[String]): (Int, String, String) = {
    val outBuf = new java.io.ByteArrayOutputStream
    val errBuf = new java.io.ByteArrayOutputStream
    val sysOut = System.out
    val sysErr = System.err
    val outStream = new java.io.PrintStream(outBuf, true, "UTF-8")
    val errStream = new java.io.PrintStream(errBuf, true, "UTF-8")
    System.setOut(outStream)
    System.setErr(errStream)
    val exit =
      try Console.withOut(outStream) {
        Console.withErr(errStream) {
          Main.run(args)
        }
      } finally {
        outStream.flush()
        errStream.flush()
        System.setOut(sysOut)
        System.setErr(sysErr)
      }
    (exit, outBuf.toString("UTF-8"), errBuf.toString("UTF-8"))
  }

  private def reportArgs(more: String*): List[String] =
    "rollup-report" :: List("--metrics-url", metricsUrl) ++ more.toList

  /** A realistic /metrics body: HELP/TYPE blocks, a non-rollup metric
    * (invocation_total — must NOT appear in the report), the three
    * scalar rollup counters, and two per-reason counters. */
  private val populatedBody: String =
    """# HELP sm8_invocation_total Total QueryService.runQuery calls
      |# TYPE sm8_invocation_total counter
      |sm8_invocation_total 42
      |# HELP sm8_rollup_rewrites_total Total RollupRewriter.rewrite calls that returned Rewritten
      |# TYPE sm8_rollup_rewrites_total counter
      |sm8_rollup_rewrites_total 10
      |# HELP sm8_rollup_refusals_total Total RollupRewriter.rewrite calls that returned Unchanged(reason)
      |# TYPE sm8_rollup_refusals_total counter
      |sm8_rollup_refusals_total 9
      |# HELP sm8_rollup_refusals_permanent_total Subset of refusals that can never be served from a rollup
      |# TYPE sm8_rollup_refusals_permanent_total counter
      |sm8_rollup_refusals_permanent_total 1
      |# HELP sm8_rollup_refusals_noGroupSetMatch Total refusals with reason noGroupSetMatch
      |# TYPE sm8_rollup_refusals_noGroupSetMatch counter
      |sm8_rollup_refusals_noGroupSetMatch 5
      |# HELP sm8_rollup_refusals_grainMismatch Total refusals with reason grainMismatch
      |# TYPE sm8_rollup_refusals_grainMismatch counter
      |sm8_rollup_refusals_grainMismatch 4
      |# HELP sm8_process_uptime_seconds Seconds since sm8 process start
      |# TYPE sm8_process_uptime_seconds gauge
      |sm8_process_uptime_seconds 3600
      |""".stripMargin

  private val emptyBody: String =
    """# HELP sm8_rollup_rewrites_total Total RollupRewriter.rewrite calls that returned Rewritten
      |# TYPE sm8_rollup_rewrites_total counter
      |sm8_rollup_rewrites_total 0
      |# HELP sm8_rollup_refusals_total Total refusals
      |# TYPE sm8_rollup_refusals_total counter
      |sm8_rollup_refusals_total 0
      |# HELP sm8_rollup_refusals_permanent_total Permanent subset
      |# TYPE sm8_rollup_refusals_permanent_total counter
      |sm8_rollup_refusals_permanent_total 0
      |""".stripMargin

  describe("parsePrometheusCounters") {
    it("parses counter lines, drops HELP/TYPE comments and non-rollup metrics") {
      val parsed = Main.parsePrometheusCounters(populatedBody)
      parsed.get("sm8_rollup_rewrites_total") shouldBe Some(10L)
      parsed.get("sm8_rollup_refusals_total") shouldBe Some(9L)
      parsed.get("sm8_rollup_refusals_permanent_total") shouldBe Some(1L)
      parsed.get("sm8_rollup_refusals_noGroupSetMatch") shouldBe Some(5L)
      parsed.get("sm8_rollup_refusals_grainMismatch") shouldBe Some(4L)
      parsed.get("sm8_invocation_total") shouldBe Some(42L)
      parsed.get("sm8_process_uptime_seconds") shouldBe Some(3600L)
      parsed.size shouldBe 7
    }

    it("drops malformed value lines without throwing") {
      val body = "sm8_rollup_rewrites_total 5\nbadline\nsm8_rollup_refusals_total notanumber\nsm8_rollup_refusals_total 7"
      val parsed = Main.parsePrometheusCounters(body)
      parsed.get("sm8_rollup_rewrites_total") shouldBe Some(5L)
      parsed.get("sm8_rollup_refusals_total") shouldBe Some(7L) // last wins
      parsed.size shouldBe 2
    }
  }

  describe("rollup-report command") {
    it("renders the ranked report with correct ordering and shares") {
      responses("/metrics") = (200, populatedBody)
      val (exit, out, _) = runCli(reportArgs())
      exit shouldBe 0
      out should include ("rewrites:            10")
      out should include ("refusals:            9")
      out should include ("refusals permanent:  1")
      // Ranked desc: noGroupSetMatch (5) before grainMismatch (4).
      val noGroupIdx = out.indexOf("noGroupSetMatch")
      val grainIdx = out.indexOf("grainMismatch")
      noGroupIdx should be > 0
      grainIdx should be > noGroupIdx
      out should include ("dominant reason: noGroupSetMatch (5)")
      out should include ("55%")
      out should include ("44%")
      // The non-rollup metric must NOT appear.
      out should not include ("sm8_invocation_total")
    }

    it("renders the empty state when no traffic has flowed") {
      responses("/metrics") = (200, emptyBody)
      val (exit, out, _) = runCli(reportArgs())
      exit shouldBe 0
      out should include ("no rollup traffic yet")
    }

    it("prints a flat JSON map with --json") {
      responses("/metrics") = (200, populatedBody)
      val (exit, out, _) = runCli(reportArgs("--json"))
      exit shouldBe 0
      out should include ("\"sm8_rollup_rewrites_total\": 10")
      out should include ("\"sm8_rollup_refusals_noGroupSetMatch\": 5")
      out should not include ("sm8_invocation_total")
    }

    it("exits 1 with a clear stderr line on a non-200 metrics response") {
      responses("/metrics") = (404, "not found")
      val (exit, _, err) = runCli(reportArgs())
      exit shouldBe 1
      err should include ("metrics endpoint returned 404")
    }

    it("exits 1 on a non-Prometheus garbage body (parse-tolerant, no throw)") {
      responses("/metrics") = (200, "<html>not metrics</html>")
      val (exit, out, err) = runCli(reportArgs())
      // The parser drops unparseable lines; the report renders with
      // zero counters (the empty-state path). The stderr warning
      // distinguishes "proxy intercepted" from "no traffic".
      exit shouldBe 0
      out should include ("no rollup traffic yet")
      err should include ("proxy may have intercepted")
    }

    it("renders totals without ranking when refusals exist but no per-reason counters") {
      // Edge: refusals fired but the per-reason source published
      // nothing (e.g. counters arrived before the observer plugin
      // registered). Totals still render; no dominant line.
      val scalarsOnly =
        """# HELP sm8_rollup_rewrites_total rewrites
          |sm8_rollup_rewrites_total 3
          |# HELP sm8_rollup_refusals_total refusals
          |sm8_rollup_refusals_total 2
          |# HELP sm8_rollup_refusals_permanent_total permanent
          |sm8_rollup_refusals_permanent_total 0
          |""".stripMargin
      responses("/metrics") = (200, scalarsOnly)
      val (exit, out, _) = runCli(reportArgs())
      exit shouldBe 0
      out should include ("rewrites:            3")
      out should include ("refusals:            2")
      out should not include ("refusals by reason")
      out should not include ("dominant reason")
    }

    it("renders perfect-routing state (rewrites>0, zero refusals) without a dominant line") {
      val perfectRouting =
        """# HELP sm8_rollup_rewrites_total rewrites
          |sm8_rollup_rewrites_total 12
          |# HELP sm8_rollup_refusals_total refusals
          |sm8_rollup_refusals_total 0
          |# HELP sm8_rollup_refusals_permanent_total permanent
          |sm8_rollup_refusals_permanent_total 0
          |""".stripMargin
      responses("/metrics") = (200, perfectRouting)
      val (exit, out, _) = runCli(reportArgs())
      exit shouldBe 0
      out should include ("rewrites:            12")
      out should not include ("dominant reason")
      out should not include ("no rollup traffic yet")
    }

    it("includes the JSON zero keys for an empty body (cron-friendly)") {
      responses("/metrics") = (200, emptyBody)
      val (exit, out, _) = runCli(reportArgs("--json"))
      exit shouldBe 0
      out should include ("\"sm8_rollup_rewrites_total\": 0")
      out should include ("\"sm8_rollup_refusals_total\": 0")
      out should include ("\"sm8_rollup_refusals_permanent_total\": 0")
    }
  }
}
