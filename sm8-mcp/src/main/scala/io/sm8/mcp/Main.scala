/*
 * SM8 MCP — Main entry point.
 *
 * Per ADR-013 (PR-259, merged): the Anthropic MCP server for sm8.
 * Stdio transport; spawns as a subprocess (e.g. by Claude Desktop
 * or the smoke-mcp.sh shell script). Reads JSON-RPC on stdin, writes
 * on stdout. Tool calls become HTTP POSTs to the sm8-server Restate
 * ingress at `--ingress-url` (default http://127.0.0.1:8080).
 *
 * ==Layer discipline (RULE#1 / RFC §3 / RFC §11a)==
 *
 * Per [[scala-jvm-safety-mindset]] + RFC §11a: this `main` lives in
 * sm8-mcp (the adapter layer), NOT in sm8-platform (the transport
 * library). The MCP SDK is itself a transport; adding it here keeps
 * deployment concerns out of the transport library.
 *
 * ==Why a separate binary (vs a --mcp-transport flag on sm8-server)==
 *
 * Per ADR-013 §"Why a SEPARATE binary": stdio MCP requires exclusive
 * process-stdout ownership. sm8-server prints startup lines to
 * stdout ("server listening on port ...", "metrics endpoint
 * listening on port ...") — running both in one JVM would corrupt
 * the JSON-RPC stream. Two processes is the smallest correct
 * change; the alternative (redirect all sm8-server stdout writes to
 * stderr) is logged as ADR-014.
 *
 * ==Lifecycle==
 *
 * 1. Parse `--ingress-url` + `--request-timeout` (typed parse, same
 *    pattern as sm8-server's Main).
 * 2. Build a `StdioServerTransportProvider` with the Jackson 3 mapper.
 * 3. Build the typed `McpServer.sync(...)` with the 5 tools.
 * 4. Call `.build()` — the SDK's stdio provider begins the stdin
 *    read loop on `.build()` (verified: SDK's `McpServer.sync`
 *    returns `SingleSessionSyncSpecification.build()` → `McpSyncServer`;
 *    `McpSyncServer` calls `transport.setSessionFactory(...)` on
 *    build, which the SDK's stdio provider uses to spawn the read
 *    loop + write loop threads).
 * 5. JVM shutdown hook: `mcpServer.closeGracefully()` blocks up to
 *    5s waiting for the SDK to drain in-flight JSON-RPC responses.
 * 6. Main thread: `Thread.currentThread().join()` — block forever
 *    until SIGTERM / stdin EOF.
 *
 * ==Skill: building-restate-services==
 *
 * The `/metrics` path is plain HTTP, not a Restate ingress handler.
 * Per the building-restate-services skill: `Instant.now()` is
 * correct here even though it's a no-no inside Restate handler
 * closures (the MCP server is OUTSIDE Restate's journal pipeline).
 */
package io.sm8.mcp

import io.modelcontextprotocol.server.McpServer
import io.sm8.platform.mcp.{Sm8ToolHandlers, HttpIngressClient}
import io.modelcontextprotocol.server.McpServerFeatures
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper
import tools.jackson.databind.json.JsonMapper

import java.time.Duration
import scala.util.control.NonFatal

object Main {

  /** Typed CLI args. Matches the sm8-server pattern
    * (sm8-server/src/main/scala/io/sm8/server/Main.scala): typed
    * parse, typed errors, default values. */
  final case class CliArgs(
      ingressUrl:     String = "http://127.0.0.1:8080",
      requestTimeout: Duration = Duration.ofSeconds(30)
  )

  /** Typed CLI parse failure (mirrors sm8-server's CliError). */
  sealed trait CliError { def reason: String }
  object CliError {
    final case class MissingValue(flag: String) extends CliError {
      val reason = s"sm8-mcp: flag $flag expects a value"
    }
    final case class BadInt(flag: String, value: String) extends CliError {
      val reason = s"sm8-mcp: $flag expects an integer, got '$value'"
    }
    final case class UnknownFlag(flag: String) extends CliError {
      val reason = s"sm8-mcp: unknown flag '$flag' (run with --help)"
    }
  }

  private val Usage: String =
    """sm8-mcp — Anthropic MCP server for the SM8 Restate ingress.
      |
      |Usage: sm8-mcp [--ingress-url <u>] [--request-timeout <secs>]
      |
      |  --ingress-url <u>        Restate ingress base URL
      |                           (default http://127.0.0.1:8080)
      |  --request-timeout <n>    Per-tool-call HTTP timeout (seconds,
      |                           default 30)
      |
      |Reads JSON-RPC on stdin, writes on stdout (stdio MCP transport).
      |Tool calls become HTTP POSTs to --ingress-url.
      |
      |Exposed tools (per ADR-013):
      |  query            -> POST /QueryService/runQuery
      |  list_models      -> POST /ModelService/listModels
      |  describe_model   -> POST /ModelService/describe
      |  list_engines     -> POST /EngineService/listEngines
      |  get_metrics      -> POST /MetricsService/snapshot""".stripMargin

  /** Pure arg parser — fully unit-testable, no IO. */
  def parseArgs(args: List[String]): Either[CliError, CliArgs] = {
    def loop(remaining: List[String], acc: CliArgs): Either[CliError, CliArgs] =
      remaining match {
        case Nil => Right(acc)
        case "--ingress-url" :: value :: rest =>
          loop(rest, acc.copy(ingressUrl = value))
        case "--ingress-url" :: Nil => Left(CliError.MissingValue("--ingress-url"))
        case "--request-timeout" :: value :: rest =>
          try loop(rest, acc.copy(requestTimeout = Duration.ofSeconds(value.toLong)))
          catch { case _: NumberFormatException => Left(CliError.BadInt("--request-timeout", value)) }
        case "--request-timeout" :: Nil => Left(CliError.MissingValue("--request-timeout"))
        case other :: _ => Left(CliError.UnknownFlag(other))
      }
    loop(args, CliArgs())
  }

  /** The entry point. Parses CLI, builds the SDK server, registers
    * the shutdown hook, blocks on `Thread.currentThread().join()`
    * so the process stays alive until SIGTERM or stdin EOF. */
  def main(args: Array[String]): Unit = {
    if (args.contains("--help") || args.contains("-h") || args.isEmpty) {
      println(Usage); return
    }
    parseArgs(args.toList) match {
      case Left(err) =>
        System.err.println(err.reason); sys.exit(2)
      case Right(cli) =>
        runServer(cli)
    }
  }

  /** Build + run. Public for tests. */
  def runServer(cli: CliArgs): McpSyncServer = {
    val transport: StdioServerTransportProvider =
      new StdioServerTransportProvider(buildJsonMapper())
    val client = new HttpIngressClient.Impl(cli.ingressUrl, cli.requestTimeout)
    val handlers = Sm8ToolHandlers.build(client)

    val server = McpServer.sync(transport)
      .serverInfo("sm8-mcp", "0.1.0-SNAPSHOT")
      .capabilities(
        io.modelcontextprotocol.spec.McpSchema.ServerCapabilities.builder()
          .tools(true)
          .build()
      )
      .tools(handlers: _*)
      .build()

    // Per [[scala-jvm-safety-mindset]] (resource lifecycle):
    // JVM shutdown hook closes both layers in the SDK, with API
    // signatures javap-verified on McpSyncServer (r1 fix per
    // PR-258 review pattern):
    //   - `McpSyncServer.closeGracefully(): void` — synchronous
    //     shutdown of the in-process server, no drain wait.
    //   - `StdioServerTransportProvider.closeGracefully(): Mono<Void>`
    //     — reactive shutdown that drains in-flight JSON-RPC
    //     responses. We `.toFuture.get(5s, SECONDS)` on the Mono
    //     so the JVM exit blocks until the SDK finishes writing
    //     pending responses (or 5s, whichever comes first). Without
    //     this, a SIGTERM mid-tool-call leaves the JSON-RPC stream
    //     truncated.
    Runtime.getRuntime().addShutdownHook(new Thread(
      new Runnable {
        def run(): Unit = {
          try server.closeGracefully()
          catch { case NonFatal(_) => () }
          try {
            val drainMono = transport.closeGracefully()
            try drainMono.toFuture.get(5L, java.util.concurrent.TimeUnit.SECONDS)
            catch { case NonFatal(_) => () }
          } catch { case NonFatal(_) => () }
        }
      },
      "sm8-mcp-shutdown"
    ))

    server
  }

  /** Build the Jackson 3 JsonMapper used by the MCP SDK's stdio
    * transport. The SDK requires a `tools.jackson.databind.json.JsonMapper`
    * (Jackson 3) wrapped in `JacksonMcpJsonMapper`. This is SEPARATE
    * from the Jackson 2.15.2 used by sm8-platform's Restate Serde.
    *
    * Verified API surface via javap on
    * `~/.m2/repository/io/modelcontextprotocol/sdk/mcp-json-jackson3/2.0.1/`
    * (PR-259 r2 catch pattern: never fabricate; always javap). */
  def buildJsonMapper(): McpJsonMapper = {
    val jackson3 = JsonMapper.builder().build()
    new JacksonMcpJsonMapper(jackson3)
  }
}