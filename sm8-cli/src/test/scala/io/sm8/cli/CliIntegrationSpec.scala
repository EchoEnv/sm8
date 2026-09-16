package io.sm8.cli

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Integration tests for `sm8` — the CLI's only test surface.
  *
  * Approach: spin up an in-process `com.sun.net.httpserver.HttpServer` on a
  * random port, register handlers that respond with realistic MCP server JSON,
  * point `Main.run` at it via `--url`, capture stdout/stderr via `Console`,
  * and assert on exit code + rendered output.
  *
  * Why real HTTP, not mocks: `sm8` is documented as the REST contract's
  * regression witness. Stubbing the HTTP layer would prove nothing about
  * the wire format. JDK's `HttpServer` is in the `jdk.httpserver` module
  * — zero extra deps.
  *
  * Fixture JSON shapes mirror what `RestServer` (the actual MCP server)
  * produces; if the server shape drifts, these tests catch it.
  */
class CliIntegrationSpec
  extends AnyFunSpec
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private var server: HttpServer = _
  private var baseUrl: String = _
  /** Path -> response (status, body). The handler reads this on each call. */
  private val responses: mutable.Map[String, (Int, String)] = mutable.Map.empty
  /** Path -> last received body (so tests can assert on what the CLI sent). */
  private val received: mutable.Map[String, String] = mutable.Map.empty
  /** Path -> last received request headers. Per RFC, header names are
    * case-insensitive — we lower-case keys for stable assertions. */
  private val receivedHeaders: mutable.Map[String, java.util.Map[String, java.util.List[String]]] =
    mutable.Map.empty
  /** Path -> last received HTTP method. */
  private val receivedMethods: mutable.Map[String, String] = mutable.Map.empty

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", new RoutingHandler)
    server.start()
    val port = server.getAddress.getPort
    baseUrl = s"http://127.0.0.1:$port"
    // Print to the underlying System.err so we see it even with Console.withErr wrapping.
    java.lang.System.err.println(s"[CliIntegrationSpec] server bound at $baseUrl")
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
  }

  override def beforeEach(): Unit = resetFixture()

  /** Jackson mapper used by the audit-tail fixtures to produce a
    * well-formed JSON array. The previous hand-rolled serializer
    * produced invalid JSON when fields contained embedded quotes (the
    * `payload` field is a JSON blob). */
  private val fixtureMapper = new com.fasterxml.jackson.databind.ObjectMapper()

  /** Reset the fixture state between tests. */
  private def resetFixture(): Unit = {
    responses.clear()
    received.clear()
    receivedHeaders.clear()
    receivedMethods.clear()
  }

  /** Program the mock server: path -> (status, JSON body). */
  private def respondWith(path: String, status: Int, body: String): Unit = {
    responses(path) = (status, body)
  }

  /** Wrap `Main.run` with stdout + stderr capture, return (exit, out, err).
    *
    * Scala 2.13's `println` goes to `Console.out` (NOT `System.out`), and
    * `Console.withOut` only swaps `Console.out`. So we must:
    *   - `System.setOut`/`System.setErr` for the CLI's `System.err.println`
    *     calls (lifecycle warnings, error envelopes).
    *   - `Console.withOut`/`Console.withErr` for the CLI's `println` calls
    *     (table rendering, status line, etc.).
    * Both routes write into the same ByteArrayOutputStream so the
    * returned `out`/`err` strings aggregate everything. */
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

  private def args(cmd: String, more: String*): List[String] =
    cmd :: List("--url", baseUrl) ++ more.toList

  /** Build args with `--restate-url` pointing at the same in-process
    * fixture (different scheme: `--url` for MCP REST, `--restate-url`
    * for the Restate ingress). */
  private def auditArgs(more: String*): List[String] =
    "audit-tail" :: List("--restate-url", baseUrl) ++ more.toList

  /** Path that matches what `RestateClient.call` constructs. The CLI's
    * call path is `/AuditService/{tenant}/queryRecent` (synchronous —
    * no `/send` suffix; the tenant is the VirtualObject key). `default`
    * is the platform's default tenant (matches `AuditArgs.parse`'s
    * default). */
  private def auditPath(tenant: String = "default"): String =
    s"/AuditService/$tenant/queryRecent"

  // ============================================================================
  // Routing handler — wires each request to its programmed response
  // ============================================================================

  private class RoutingHandler extends HttpHandler {
    override def handle(exch: HttpExchange): Unit = {
      val path = exch.getRequestURI.getPath
      receivedMethods(path) = exch.getRequestMethod
      received(path) =
        Option(exch.getRequestBody).map { in =>
          Source.fromInputStream(in, "UTF-8").getLines.mkString
        }.getOrElse("")
      receivedHeaders(path) = exch.getRequestHeaders
      responses.get(path) match {
        case Some((status, body)) =>
          val bytes = body.getBytes(StandardCharsets.UTF_8)
          exch.sendResponseHeaders(status, bytes.length.toLong)
          exch.getResponseBody.write(bytes)
        case None =>
          // No response programmed — return empty 404. The CLI treats
          // empty body as "no models loaded" or similar; tests that
          // expect a 404 body should program a response.
          exch.sendResponseHeaders(404, -1L)
      }
      exch.close()
    }
  }

  // ============================================================================
  // 1. `list` command
  // ============================================================================

  describe("`list` command") {

    it("renders a table with MODEL, STATUS, DESCRIPTION columns") {
      val dbg = s"DEBUG: baseUrl=$baseUrl\nDEBUG: args=${args("list")}\nDEBUG: server.port=${server.getAddress.getPort}"
      respondWith("/models", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "models": [
        |      {"name": "flights",  "description": "Flight facts",     "status": "published"},
        |      {"name": "carriers", "description": "Carrier lookup",   "status": "deprecated"}
        |    ]
        |  },
        |  "warnings": [],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("list"))
      exit shouldBe 0
      err shouldBe ""
      out should include("MODEL")
      out should include("STATUS")
      out should include("DESCRIPTION")
      out should include("flights")
      out should include("carriers")
      out should include("published")
      out should include("deprecated")
    }

    it("prints WARN: lines for each non-Published model") {
      respondWith("/models", 200, """{
        |  "status": "ok",
        |  "data": {"models": [
        |    {"name": "flights",  "description": "Flight facts",   "status": "published"},
        |    {"name": "legacy",   "description": "Legacy stuff",   "status": "deprecated"},
        |    {"name": "draft_m",  "description": "In progress",    "status": "draft"}
        |  ]},
        |  "warnings": [
        |    "model 'legacy' is deprecated",
        |    "model 'draft_m' is in draft; shape may change"
        |  ],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("list"))
      exit shouldBe 0
      out should include("flights")
      err should include("WARN: model 'legacy' is deprecated")
      err should include("WARN: model 'draft_m' is in draft; shape may change")
    }

    it("prints `(no models loaded)` for an empty registry") {
      respondWith("/models", 200, """{
        |  "status": "ok", "data": {"models": []}, "warnings": [], "meta": {}
        |}""".stripMargin)

      val (exit, out, _) = runCli(args("list"))
      exit shouldBe 0
      out should include("(no models loaded)")
    }

    it("`--json` prints the raw envelope to stdout, no WARN: lines on stderr") {
      respondWith("/models", 200, """{
        |  "status": "ok", "data": {"models": [{"name": "x", "status": "published"}]},
        |  "warnings": ["model 'x' is deprecated"], "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("list", "--json"))
      exit shouldBe 0
      // --json emits the raw envelope to stdout. Test for substring presence
      // without embedded double-quotes (those are awkward to escape).
      out should include("deprecated")  // comes from warnings[0]
      out should include("\"status\"")
      // --json does NOT emit WARN: lines (warnings are for the human-readable
      // path; the raw JSON output is what consumers parse).
      err shouldBe ""
    }
  }

  // ============================================================================
  // 2. `describe` command
  // ============================================================================

  describe("`describe` command") {

    it("renders Model / Version / Status lines + sections") {
      respondWith("/models/flights", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "model": "flights", "version": 3, "status": "published",
        |    "source_table": "flights_csv",
        |    "filters": [],
        |    "dimensions": [{"name": "carrier", "expr": "carrier"}],
        |    "measures": [{"name": "flight_count", "kind": "base", "expr": "count(1)"}],
        |    "joins": []
        |  },
        |  "warnings": [],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("describe", "flights"))
      exit shouldBe 0
      err shouldBe ""
      out should include("Model:        flights")
      out should include("Version:      3")
      out should include("Status:       published")
      out should include("Source table: flights_csv")
      out should include("carrier")
      out should include("flight_count")
    }

    it("prints WARN: line on stderr when model is Deprecated, Status line shows deprecated") {
      respondWith("/models/legacy_flights", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "model": "legacy_flights", "version": 1, "status": "deprecated",
        |    "source_table": "legacy_csv",
        |    "filters": [], "dimensions": [], "measures": [], "joins": []
        |  },
        |  "warnings": ["model 'legacy_flights' is deprecated"],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("describe", "legacy_flights"))
      exit shouldBe 0
      err should include("WARN: model 'legacy_flights' is deprecated")
      out should include("Status:       deprecated")
    }

    it("returns exit 1 + error on stderr for MODEL_NOT_FOUND") {
      respondWith("/models/ghost", 404, """{
        |  "status": "error",
        |  "error": {"code": "MODEL_NOT_FOUND", "message": "no model named 'ghost'"}
        |}""".stripMargin)

      val (exit, _, err) = runCli(args("describe", "ghost"))
      exit shouldBe 1
      err should include("MODEL_NOT_FOUND")
      err should include("ghost")
    }

    it("`describe` with no argument returns exit 2 (usage error)") {
      val (exit, _, err) = runCli(args("describe"))
      exit shouldBe 2
      err should include("Usage")
    }
  }

  // ============================================================================
  // 3. `query` and `explain` commands
  // ============================================================================

  describe("`query` and `explain` commands") {

    it("query: renders a table and row_count footer") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {
        |    "columns": [{"name": "carrier"}, {"name": "flight_count"}],
        |    "rows": [
        |      ["AA", 100], ["UA", 200], ["DL", 300]
        |    ],
        |    "row_count": 3,
        |    "truncated": false
        |  },
        |  "warnings": [],
        |  "meta": {"elapsed_ms": 42, "model": "flights"}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("query", "flights",
        "-m", "flight_count", "-d", "carrier", "--limit", "10"))
      exit shouldBe 0
      err shouldBe ""
      out should include("carrier")
      out should include("flight_count")
      out should include("AA")
      out should include("UA")
      out should include("DL")
      out should include("3 rows")
    }

    it("query: POSTs a body with model/dimensions/measures/limit to /query") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("query", "flights",
        "-d", "carrier", "-m", "flight_count", "-o", "flight_count:desc",
        "--limit", "5"))
      val sent = received("/query")
      sent should include("\"model\":\"flights\"")
      sent should include("\"dimensions\":[\"carrier\"]")
      sent should include("\"measures\":[\"flight_count\"]")
      sent should include("\"limit\":5")
      sent should include("\"direction\":\"desc\"")
    }

    it("query: surfaces WARN: line on stderr when model is Deprecated") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": ["model 'legacy' is deprecated"],
        |  "meta": {}
        |}""".stripMargin)

      val (exit, _, err) = runCli(args("query", "legacy", "-m", "flight_count"))
      exit shouldBe 0
      err should include("WARN: model 'legacy' is deprecated")
    }

    it("query: returns exit 1 for RESULT_TOO_LARGE") {
      respondWith("/query", 400, """{
        |  "status": "error",
        |  "error": {
        |    "code": "RESULT_TOO_LARGE",
        |    "message": "result exceeds maxRows=100 (got 250); add a `limit` parameter",
        |    "details": {"row_count": "250", "max_rows": "100", "suggested_limit": "100"}
        |  }
        |}""".stripMargin)

      val (exit, _, err) = runCli(args("query", "flights", "-m", "flight_count"))
      exit shouldBe 1
      err should include("RESULT_TOO_LARGE")
    }

    it("explain: prints the plan text + WARN: line on stderr") {
      respondWith("/explain", 200, """{
        |  "status": "ok",
        |  "data": "PLAN SUMMARY\n  table: flights\n  group by: carrier\n  compute: flight_count\n",
        |  "warnings": ["model 'flights' is deprecated"],
        |  "meta": {"model": "flights"}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("explain", "flights",
        "-d", "carrier", "-m", "flight_count"))
      exit shouldBe 0
      out should include("PLAN SUMMARY")
      out should include("group by: carrier")
      err should include("WARN: model 'flights' is deprecated")
    }

    it("query with no model returns exit 2 (usage error)") {
      // Note: a query with no `-m` is actually allowed (the CLI builds an
      // empty-measures query). The missing-model case is the real usage
      // error.
      val (exit, _, err) = runCli(args("query"))
      exit shouldBe 2
      err should include("Usage")
    }
  }

  // ============================================================================
  // 2b. `--engine` flag on query/explain (PR #432 — v0.3.1 Step 2)
  // ============================================================================
  //
  // Exposes the MCP server's engine-routing field (added server-side in
  // PR #431). The CLI omits the field when not passed (backward compat);
  // when present, server routes through EngineRegistry if configured.
  describe("`--engine` flag on query/explain") {

    it("query: --engine spark sends \"engine\":\"spark\" in the request body") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("query", "flights", "--engine", "spark", "-m", "flight_count"))
      val sent = received("/query")
      sent should include("\"engine\":\"spark\"")
      sent should include("\"model\":\"flights\"")
    }

    it("query: no --engine flag OMITS the engine field (backward compat)") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("query", "flights", "-m", "flight_count"))
      val sent = received("/query")
      sent should not include "\"engine\""
      sent should include("\"model\":\"flights\"")
    }

    it("query: --engine \"\" (explicit empty) also OMITS the engine field") {
      // Per the design doc (PR #432): omitting the field is the same as
      // sending empty — both signal "server decides routing". A user who
      // really needs to force the legacy path should use a non-empty
      // sentinel (e.g. `--engine legacy`).
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("query", "flights", "--engine", "", "-m", "flight_count"))
      val sent = received("/query")
      sent should not include "\"engine\""
    }

    it("query: --engine (no value) returns exit 2 with typed error message") {
      // No server response registered — should fail at the CLI before any HTTP.
      val (exit, _, err) = runCli(args("query", "flights", "--engine"))
      exit shouldBe 2
      err should include("--engine requires a value")
    }

    it("explain: --engine spark also sends the engine field") {
      respondWith("/explain", 200, """{
        |  "status": "ok",
        |  "data": {"plan": "SELECT 1", "warnings": []},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("explain", "flights", "--engine", "spark"))
      val sent = received("/explain")
      sent should include("\"engine\":\"spark\"")
    }

    it("query: --engine parses anywhere in the arg list (not just before flags)") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      runCli(args("query", "flights", "-m", "flight_count", "--engine", "trino"))
      val sent = received("/query")
      sent should include("\"engine\":\"trino\"")
    }
  }

  // ==========================================================================
  // 3b. Flag-value guards on query/explain (issue #422 — mirrors the
  // validate verb's guard; a value starting with '-' is the next flag,
  // not this flag's value)
  // ==========================================================================

  describe("flag-value guards on query/explain") {

    it("query: -d followed by -m is a parse error naming both flags") {
      val (exit, _, err) = runCli(args("query", "flights", "-d", "-m", "cnt"))
      exit shouldBe 2
      err should include("--dim requires a value")
      err should include("-m")
    }

    it("query: -m followed by -d is a parse error naming both flags") {
      val (exit, _, err) = runCli(args("query", "flights", "-m", "-d", "carrier"))
      exit shouldBe 2
      err should include("--measure requires a value")
      err should include("-d")
    }

    it("query: --dim followed by --measure is a parse error (long forms)") {
      val (exit, _, err) = runCli(args("query", "flights", "--dim", "--measure", "cnt"))
      exit shouldBe 2
      err should include("--dim requires a value")
      err should include("--measure")
    }

    it("query: --limit followed by a flag is a parse error") {
      val (exit, _, err) = runCli(args("query", "flights", "-m", "cnt", "--limit", "-d"))
      exit shouldBe 2
      err should include("--limit requires a value")
    }

    it("query: --engine followed by a flag is a parse error") {
      val (exit, _, err) = runCli(args("query", "flights", "--engine", "-m"))
      exit shouldBe 2
      err should include("--engine requires a value")
    }

    it("query: -o followed by a flag is a parse error") {
      val (exit, _, err) = runCli(args("query", "flights", "-o", "-d"))
      exit shouldBe 2
      err should include("--order requires a value")
    }

    it("explain: -d followed by a flag is a parse error too") {
      val (exit, _, err) = runCli(args("explain", "flights", "-d", "-m"))
      exit shouldBe 2
      err should include("--dim requires a value")
    }

    it("query: still parses normally when values are legit (no regression)") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": 0, "truncated": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)
      val (exit, _, err) = runCli(args("query", "flights",
        "-d", "carrier", "-m", "flight_count", "--limit", "5",
        "-o", "carrier:desc", "--engine", "spark"))
      exit shouldBe 0
      err shouldBe ""
      val sent = received("/query")
      sent should include("\"dimensions\":[\"carrier\"]")
      sent should include("\"measures\":[\"flight_count\"]")
      sent should include("\"limit\":5")
      sent should include("\"order_by\":[{\"field\":\"carrier\",\"direction\":\"desc\"}]")
      sent should include("\"engine\":\"spark\"")
    }
  }

  // ============================================================================
  // 4. Exit codes and global flags
  // ============================================================================

  describe("exit codes and global flags") {

    it("`--help` returns exit 0") {
      val (exit, _, _) = runCli(args("--help"))
      exit shouldBe 0
    }

    it("unknown subcommand returns exit 2") {
      val (exit, _, err) = runCli(args("nope"))
      exit shouldBe 2
      err.toLowerCase should include("unknown")
    }

    it("`--url` (no value) returns exit 2 with typed CliParseError.MissingFlagValue message") {
      // PR #434 (v0.3.1): extractGlobals now returns Either[CliParseError, _]
      // (was Either[String, _]). The single failure mode is the
      // existing `CliParseError.MissingFlagValue(flag = "--url")` case.
      val (exit, _, err) = runCli(args("query", "flights", "--url"))
      exit shouldBe 2
      err should include("--url requires a value")
    }

    it("transport error (server down) returns exit 3") {
      // Point at a port nothing is listening on. `--url` must come AFTER
      // the subcommand for the CLI's existing dispatch to extract it.
      val (exit, _, _) = runCli(args("list", "--url", "http://127.0.0.1:1"))
      exit shouldBe 3
    }

    it("`--json` for query prints the raw envelope") {
      respondWith("/query", 200, """{
        |  "status": "ok",
        |  "data": {"columns": [], "rows": [], "row_count": false},
        |  "warnings": [], "meta": {}
        |}""".stripMargin)

      val (exit, out, err) = runCli(args("query", "flights",
        "-m", "flight_count", "--json"))
      exit shouldBe 0
      out should include("\"status\": \"ok\"")
      // --json does not print WARN lines (raw envelope is the source of truth)
      err shouldBe ""
    }
  }

  // ============================================================================
  // 5. `audit-tail` command (Phase 3) — talks to Restate ingress directly
  // ============================================================================

  describe("`audit-tail` command") {

    /** Build a Restate-shaped success response: the raw JSON array the
      * `AuditService.queryRecent` handler returns. NOT wrapped in an
      * `{ "status": "ok", "output": [...] }` envelope — that's the MCP
      * REST shape, not Restate's.
      *
      * Uses Jackson's `writeValueAsString` so string values containing
      * inner quotes (e.g. the `payload` field, which is a JSON blob) are
      * properly escaped. The earlier hand-rolled serializer produced
      * invalid JSON when fields contained embedded quotes. */
    def restateOk(events: List[Map[String, Any]]): String = {
      val arr = new com.fasterxml.jackson.databind.node.ArrayNode(
        com.fasterxml.jackson.databind.node.JsonNodeFactory.instance)
      events.foreach { e =>
        val obj = arr.addObject()
        e.foreach { case (k, v) =>
          v match {
            case s: String => obj.put(k, s)
            case n: java.math.BigDecimal => obj.put(k, n)
            case n: java.math.BigInteger => obj.put(k, n)
            case n: Int    => obj.put(k, n)
            case n: Long   => obj.put(k, n)
            case n: Double => obj.put(k, n)
            case n: Float  => obj.put(k, n)
            case n: Boolean => obj.put(k, n)
            case null      => obj.putNull(k)
            case other     => obj.put(k, other.toString)
          }
        }
      }
      fixtureMapper.writeValueAsString(arr)
    }

    it("renders TS/TENANT/EVENT/DEDUP/PAYLOAD columns from a Restate response") {
      respondWith(auditPath(), 200, restateOk(List(
        Map(
          "tenant" -> "default", "eventType" -> "QUERY",
          "ts" -> "2026-08-11T15:42:00Z", "dedupHash" -> "abc123",
          "payload" -> "{\"model\":\"flights\",\"rowCount\":3,\"elapsedMs\":412}"
        ),
        Map(
          "tenant" -> "default", "eventType" -> "QUERY",
          "ts" -> "2026-08-11T15:41:30Z", "dedupHash" -> "def456",
          "payload" -> "{\"model\":\"orders\",\"rowCount\":7,\"elapsedMs\":88}"
        ),
      )))

      val (exit, out, err) = runCli(auditArgs("--limit", "5"))
      exit shouldBe 0
      out should include("TS")
      out should include("TENANT")
      out should include("EVENT")
      out should include("DEDUP")
      out should include("PAYLOAD")
      out should include("abc123")
      out should include("def456")
      // payload is truncated to 40 chars; the underlying model name from
      // the JSON blob shows up because the truncate prefix is rendered.
      out should include("flights")
      out should include("orders")
      err shouldBe ""  // no warnings
    }

    it("prints `(no audit events)` for an empty Restate response") {
      respondWith(auditPath(), 200, """[]""")
      val (exit, out, _) = runCli(auditArgs())
      exit shouldBe 0
      out should include("(no audit events)")
    }

    it("POSTs to /AuditService/{tenant}/queryRecent with a JSON body carrying tenant + limit") {
      // The tenant is the VirtualObject key (per AuditService.java:14 —
      // "Key: tenant"). Programming the response at the tenant-scoped path
      // proves the CLI built the URL with the tenant baked in.
      respondWith(auditPath("acme"), 200, """[]""")
      val (_, _, _) = runCli(auditArgs("--tenant", "acme", "--limit", "5"))
      receivedMethods(auditPath("acme")) shouldBe "POST"
      val body = received(auditPath("acme"))
      body should include("\"tenant\":\"acme\"")
      body should include("\"limit\":5")
    }

    it("defaults tenant to `default` when --tenant is not passed") {
      respondWith(auditPath(), 200, """[]""")
      val (_, _, _) = runCli(auditArgs())
      received(auditPath()) should include("\"tenant\":\"default\"")
    }

    it("`--since garbage` returns exit 2 with typed CliParseError.InvalidSince message") {
      val (exit, _, err) = runCli(auditArgs("--since", "not-a-date"))
      exit shouldBe 2
      err should include("ISO-8601")
    }

    it("`--until garbage` returns exit 2 with typed CliParseError.InvalidUntil message") {
      val (exit, _, err) = runCli(auditArgs("--until", "2026-13-99"))
      exit shouldBe 2
      err should include("ISO-8601")
    }

    it("`--tenant <invalid chars>` returns exit 2 with typed CliParseError.InvalidTenant message") {
      val (exit, _, err) = runCli(auditArgs("--tenant", "acme corp!"))  // space + !
      exit shouldBe 2
      err should include("--tenant")
    }

    it("`--limit -1` returns exit 2 (reuses InvalidLimit message)") {
      val (exit, _, err) = runCli(auditArgs("--limit", "-1"))
      exit shouldBe 2
      err should include("--limit must be a non-negative integer")
    }

    it("Restate 5xx envelope returns exit 1 with RestateHttpError on stderr") {
      respondWith(auditPath(), 500, """{"status":"error","error":{"code":"INTERNAL","message":"kaboom"}}""")
      val (exit, _, err) = runCli(auditArgs())
      exit shouldBe 1
      err should include("Restate")
      err should include("INTERNAL")
    }

    it("positional arg is rejected (audit-tail takes no model)") {
      val (exit, _, err) = runCli(auditArgs("flights"))
      exit shouldBe 2
      err should include("unexpected argument")
    }

    it("without --restate-url AND no $RESTATE_URL, exits 2 with a clear message") {
      // Point --url somewhere so the dispatcher accepts the command,
      // but don't set --restate-url and don't have RESTATE_URL.
      val (exit, _, err) = runCli("audit-tail" :: "--url" :: baseUrl :: Nil)
      exit shouldBe 2
      err should include("--restate-url")
    }

    it("`--json` for audit-tail prints the raw Restate response (no MCP envelope wrapping)") {
      respondWith(auditPath(), 200, """[{"eventType":"QUERY","dedupHash":"xyz"}]""")
      val (exit, out, err) = runCli(auditArgs("--json"))
      exit shouldBe 0
      // Raw response is a JSON array — check for the array open + an event
      // field. The MCP `{ "status": "ok", "output": [...] }` envelope is
      // NOT present (that's MCP REST, not Restate).
      out should include("\"eventType\":\"QUERY\"")
      out should include("\"dedupHash\":\"xyz\"")
      out should not include "\"status\":\"ok\""
      err shouldBe ""
    }
  }

  // ============================================================================
  // 6. Auth scaffolding — --token-file + $SDF_TOKEN
  // ============================================================================

  describe("auth: --token-file + $SDF_TOKEN") {

    /** Write a token file in a temp dir and return its path. */
    def writeToken(content: String): String = {
      val f = java.nio.file.Files.createTempFile("sm8-token-", ".tok")
      f.toFile.deleteOnExit()
      java.nio.file.Files.writeString(f, content)
      f.toString
    }

    it("no token configured → no Authorization header sent") {
      respondWith(auditPath(), 200, """[]""")
      runCli(auditArgs())
      receivedHeaders(auditPath()).get("Authorization") shouldBe null
    }

    it("--token-file <tmp> → Bearer s3cret header sent") {
      val tokPath = writeToken("s3cret")
      respondWith(auditPath(), 200, """[]""")
      runCli(auditArgs("--token-file", tokPath))
      val auth = receivedHeaders(auditPath()).get("Authorization")
      auth should not be null
      auth.asScala.headOption.get shouldBe "Bearer s3cret"
    }

    it("token file with trailing newline → header has NO trailing whitespace (the .trim regression)") {
      val tokPath = writeToken("s3cret\n")
      respondWith(auditPath(), 200, """[]""")
      runCli(auditArgs("--token-file", tokPath))
      val auth = receivedHeaders(auditPath()).get("Authorization").asScala.headOption.get
      auth shouldBe "Bearer s3cret"
      auth should not include("\n")
    }

    it("--token-file /nonexistent returns exit 2") {
      val (exit, _, err) = runCli(auditArgs("--token-file", "/nonexistent/path/abcdef"))
      exit shouldBe 2
      err should include("--token-file")
      err should include("/nonexistent/path/abcdef")
    }

    it("token also applied to MCP REST requests (e.g. /models)") {
      val tokPath = writeToken("mcp-tok")
      respondWith("/models", 200, """{"status":"ok","data":{"models":[]}}""")
      runCli(args("list", "--token-file", tokPath))
      val auth = receivedHeaders("/models").get("Authorization").asScala.headOption.get
      auth shouldBe "Bearer mcp-tok"
    }

    // Note: $SDF_TOKEN precedence is NOT unit-testable — `sys.env` is
    // immutable in-JVM and the test fixture spawns a fresh JVM. This is
    // covered by manual smoke after deployment. The shape is verified by
    // resolveToken() reading from sys.env in priority order.
  }

  // `sm8 inspect <key>` posts to the generic transport-layer
  // meta-inspector (`POST /MetaInspectorService/getMeta` with
  // `{"key": <key>}` body). The CLI is generic over the key
  // string; it does not know the value schema.
  describe("`inspect` command") {
    it("POSTs the key to /MetaInspectorService/getMeta and prints the value") {
      val key = "io.sm8.plugins.semanticgraph:graph-snapshot"
      respondWith(
        "/MetaInspectorService/getMeta",
        200,
        s"""{"status":"ok","data":{"key":"$key","present":true,"value":{"vertices":[],"hasCycle":false}}}"""
      )
      val (exit, out, _) = runCli(args("inspect", key))
      exit shouldBe 0
      out should include(s"Key:   $key")
      out should include("Value:")
    }

    it("returns exit 1 + stderr when the key is absent (present=false)") {
      val key = "io.sm8.plugins.unknown:foo"
      respondWith(
        "/MetaInspectorService/getMeta",
        200,
        s"""{"status":"ok","data":{"key":"$key","present":false,"value":null}}"""
      )
      val (exit, _, err) = runCli(args("inspect", key))
      // Issue #425 audit: key-not-present is a domain answer ("no"),
      // not a separate exit code. Same exit 1 as any other domain
      // failure (see cmdValidate + cmdQuery's errorPath path).
      exit shouldBe 1
      err should include(key)
      err should include("not set")
    }

    it("returns exit 2 (usage) when no key is given") {
      val (exit, _, err) = runCli(args("inspect"))
      exit shouldBe 2
      err should include("missing <key>")
    }

    it("--json prints the raw envelope to stdout") {
      val key = "io.sm8.plugins.semanticgraph:graph-snapshot"
      val body =
        s"""{"status":"ok","data":{"key":"$key","present":true,"value":{"vertices":[]}}}"""
      respondWith("/MetaInspectorService/getMeta", 200, body)
      val (exit, out, _) = runCli(args("inspect", key, "--json"))
      exit shouldBe 0
      out should include("\"present\":true")
    }

    it("--json with present:false exits 1 (contract holds in machine mode)") {
      val key = "io.sm8.plugins.unknown:foo"
      val body =
        s"""{"status":"ok","data":{"key":"$key","present":false,"value":null}}"""
      respondWith("/MetaInspectorService/getMeta", 200, body)
      val (exit, out, _) = runCli(args("inspect", key, "--json"))
      exit shouldBe 1
      // Raw envelope still printed (scripts may want the body for
      // context) but the exit code signals the domain failure.
      out should include("\"present\":false")
    }

    it("key not present: exit 1 (domain failure — exit-code contract)") {
      val key = "io.sm8.plugins.semanticgraph:graph-snapshot"
      val body =
        s"""{"status":"ok","data":{"key":"$key","present":false,"value":null}}"""
      respondWith("/MetaInspectorService/getMeta", 200, body)
      val (exit, _, err) = runCli(args("inspect", key))
      exit shouldBe 1
      err should include("not set on the most recent request")
    }

    it("key not present with --json: exit 1 (contract holds in machine mode)") {
      val key = "io.sm8.plugins.semanticgraph:graph-snapshot"
      val body =
        s"""{"status":"ok","data":{"key":"$key","present":false,"value":null}}"""
      respondWith("/MetaInspectorService/getMeta", 200, body)
      val (exit, out, _) = runCli(args("inspect", key, "--json"))
      exit shouldBe 1
      out should include("\"present\":false")
    }
  }

  // -------------------------------------------------------------------------
  // rollup-status (per-rollup freshness + refusal view, issue #425 PR 2)
  // -------------------------------------------------------------------------

  describe("rollup-status") {
    val metricsBody = """# HELP sm8_rollup_freshness 1 if every bucket row is_final=true
        |# TYPE sm8_rollup_freshness gauge
        |sm8_rollup_freshness{rollup="by_day"} 1
        |sm8_rollup_freshness{rollup="by_month"} 0
        |# HELP sm8_rollup_freshness_age_seconds Seconds since the most recent bucket refresh (NaN if never)
        |# TYPE sm8_rollup_freshness_age_seconds gauge
        |sm8_rollup_freshness_age_seconds{rollup="by_day"} 1800
        |sm8_rollup_freshness_age_seconds{rollup="by_month"} NaN
        |# HELP sm8_rollup_freshness_buckets Number of bucket rows in the watermark table
        |# TYPE sm8_rollup_freshness_buckets gauge
        |sm8_rollup_freshness_buckets{rollup="by_day"} 24
        |sm8_rollup_freshness_buckets{rollup="by_month"} 3
        |# HELP sm8_rollup_rewrites_total Total rewrites
        |# TYPE sm8_rollup_rewrites_total counter
        |sm8_rollup_rewrites_total 12
        |# HELP sm8_rollup_refusals_total Total refusals
        |# TYPE sm8_rollup_refusals_total counter
        |sm8_rollup_refusals_total 4""".stripMargin

    it("renders the per-rollup FRESH/STALE table") {
      respondWith("/metrics", 200, metricsBody)
      val (exit, out, err) = runCli(args("rollup-status", "--metrics-url", baseUrl))
      exit shouldBe 0
      err shouldBe ""
      out should include("by_day")
      out should include("FRESH")
      out should include("by_month")
      out should include("STALE")
      out should include("1800s")
      out should include("never")
      out should include("24")
    }

    it("--json emits machine-readable keyed output") {
      respondWith("/metrics", 200, metricsBody)
      val (exit, out, _) = runCli(args("rollup-status", "--metrics-url", baseUrl, "--json"))
      exit shouldBe 0
      out should include("\"by_day\": {\"fresh\": 1")
      out should include("\"age_seconds\": 1800")
      out should include("\"age_seconds\": null")  // NaN → null in machine mode
      out should include("\"_aggregates\"")
    }

    it("probe failure surfaces a stderr warning") {
      respondWith("/metrics", 200,
        """sm8_rollup_freshness_probe_failed 1
          |sm8_rollup_rewrites_total 0""".stripMargin)
      val (exit, out, err) = runCli(args("rollup-status", "--metrics-url", baseUrl))
      exit shouldBe 0
      err should include("freshness probe failed")
    }

    it("no freshness gauges at all: honest empty state, exit 0") {
      respondWith("/metrics", 200, "sm8_invocation_total 5\n")
      val (exit, out, _) = runCli(args("rollup-status", "--metrics-url", baseUrl))
      exit shouldBe 0
      out should include("No rollup freshness data")
      out should include("no rollups are declared")
    }

    it("--json with no freshness gauges: still valid JSON (M1 regression pin)") {
      respondWith("/metrics", 200, "sm8_invocation_total 5\n")
      val (exit, out, _) = runCli(args("rollup-status", "--metrics-url", baseUrl, "--json"))
      exit shouldBe 0
      out should include("\"_aggregates\"")
      // The bug this pins: a leading comma ({,"_aggregates"...) is
      // invalid JSON. Parse the output to prove validity.
      scala.util.Try(new com.fasterxml.jackson.databind.ObjectMapper()
        .readTree(out)).isSuccess shouldBe true
    }

    it("metrics endpoint 404: exit 3 (transport — same as rollup-report)") {
      respondWith("/metrics", 404, "not found")
      val (exit, _, err) = runCli(args("rollup-status", "--metrics-url", baseUrl))
      exit shouldBe 3
      err should include("returned 404")
    }

    it("metrics endpoint 500: exit 3") {
      respondWith("/metrics", 500, "boom")
      val (exit, _, _) = runCli(args("rollup-status", "--metrics-url", baseUrl))
      exit shouldBe 3
    }

    it("extra positional: exit 2 (usage)") {
      val (exit, _, err) = runCli(args("rollup-status", "--metrics-url", baseUrl, "flights"))
      exit shouldBe 2
      err should include("unexpected arguments")
    }
  }

  // -------------------------------------------------------------------------
  // exit-code contract audit (issue #425): the two misclassified sites
  // -------------------------------------------------------------------------

  describe("exit-code contract") {

    it("rollup-report: metrics 404 → exit 3 (transport), not 1") {
      respondWith("/metrics", 404, "not found")
      val (exit, _, err) = runCli(args("rollup-report", "--metrics-url", baseUrl))
      exit shouldBe 3
      err should include("returned 404")
    }

    it("rollup-report: metrics 200 with no rollup counters → exit 0 + warning") {
      respondWith("/metrics", 200, "sm8_invocation_total 5\n")
      val (exit, out, err) = runCli(args("rollup-report", "--metrics-url", baseUrl))
      exit shouldBe 0
      err should include("no recognizable sm8_rollup_* counters")
    }
  }

  // -------------------------------------------------------------------------
  // validate (execute-free validation + plan preview)
  // -------------------------------------------------------------------------

  describe("validate") {
    val validOutcome =
      """{
        |  "status": "ok",
        |  "data": {
        |    "modelVersion": 1,
        |    "rollupDecision": {"rewriteApplied": true, "reason": "covered"},
        |    "decisionHints": null,
        |    "engineSelection": "in-memory",
        |    "tablesTouched": ["events_rollup_day"],
        |    "compiledSql": "Project [region, sum(amount) AS total]"
        |  },
        |  "warnings": []
        |}""".stripMargin

    it("VALID outcome: exit 0, summary block on stdout") {
      respondWith("/QueryValidationService/validate", 200, validOutcome)
      val (exit, out, err) = runCli(args("validate", "flights", "-d", "day", "-m", "cnt"))
      exit shouldBe 0
      err shouldBe ""
      out should include("VALID: flights (v1)")
      out should include("engine:   in-memory")
      out should include("Rewritten")
      out should include("events_rollup_day")
      // Plan hidden without --plan
      out should not include ("== compiled plan preview ==")
    }

    it("--plan shows the compiled preview") {
      respondWith("/QueryValidationService/validate", 200, validOutcome)
      val (exit, out, _) = runCli(args("validate", "flights", "--plan"))
      exit shouldBe 0
      out should include("== compiled plan preview ==")
      out should include("Project [region, sum(amount) AS total]")
    }

    it("sends the same body shape as query (model + dimensions + measures)") {
      respondWith("/QueryValidationService/validate", 200, validOutcome)
      runCli(args("validate", "flights", "-d", "day", "-m", "cnt"))
      val sent = received("/QueryValidationService/validate")
      sent should include("\"model\":\"flights\"")
      sent should include("\"dimensions\":[\"day\"]")
      sent should include("\"measures\":[\"cnt\"]")
    }

    it("error envelope (typed ValidationFailure via TerminalException): exit 1") {
      respondWith("/QueryValidationService/validate", 400,
        """{"status":"error","error":{"code":"TERMINAL","message":"[request] unknown dimension: 'dy'"}}""")
      val (exit, _, err) = runCli(args("validate", "flights", "-d", "dy"))
      exit shouldBe 1
      err should include("unknown dimension")
    }

    it("--json passthrough prints the raw envelope, exits 1 on 4xx") {
      respondWith("/QueryValidationService/validate", 400,
        """{"status":"error","error":{"code":"TERMINAL","message":"boom"}}""")
      val (exit, out, _) = runCli(args("validate", "flights", "--json"))
      exit shouldBe 1
      out should include("\"status\":\"error\"")
    }

    it("--json passthrough exits 0 on a valid outcome") {
      respondWith("/QueryValidationService/validate", 200, validOutcome)
      val (exit, out, _) = runCli(args("validate", "flights", "--json"))
      exit shouldBe 0
      out should include("ok")
      out should include("modelVersion")
    }

    it("flag-shaped value after -d is a parse error (not a silent dim)") {
      val (exit, _, err) = runCli(args("validate", "flights", "-d", "-m", "cnt"))
      exit shouldBe 2
      err should include("--dim requires a value")
    }

    it("missing model: exit 2 with usage") {
      val (exit, _, err) = runCli(args("validate"))
      exit shouldBe 2
      err should include("missing <model>")
    }

    it("unknown flag: exit 2") {
      val (exit, _, err) = runCli(args("validate", "flights", "--bogus"))
      exit shouldBe 2
      err should include("unknown flag: --bogus")
    }

    it("missing flag value: exit 2") {
      val (exit, _, err) = runCli(args("validate", "flights", "-d"))
      exit shouldBe 2
      err should include("--dim requires a value")
    }

    it("--plan with compiledSql null: prints the honest v1-semantics message") {
      respondWith("/QueryValidationService/validate", 200,
        """{"status":"ok","data":{"modelVersion":1,"rollupDecision":{"rewriteApplied":false,"reason":"none"},"engineSelection":"x","tablesTouched":["t"],"compiledSql":null},"warnings":[]}""")
      val (exit, out, _) = runCli(args("validate", "flights", "--plan"))
      exit shouldBe 0
      out should include ("no compiled plan: deployment did not supply a compiledSqlFn")
    }
  }
}