package io.sm8.cli

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

/** `sm8` — a command-line client for the semanticdf REST API.
  *
  * A pure HTTP+JSON client. No Spark, no model loading — it talks to a
  * running semanticdf REST server (started via
  * `mvn exec:java -Dexec.mainClass=io.sm8.mcp.Main -- --transport rest ...`
  * on the `semanticdf-mcp` module).
  *
  * == Subcommands ==
  *
  * {{{
  *   sm8 list                            list available models
  *   "sm8 describe <model>                show a model's dimensions/measures/filters
  *   sm8 query <model> [options]         run a semantic query, print a table
  *   sm8 explain <model> [options]       show the semantic plan (no execution)
  *   sm8 inspect <key>                   read a context.meta key (generic)
  * }}}
  *
  * == Global options ==
  *
  *   --url <base>     server base URL (default $SDF_URL or http://localhost:8080)
  *   --json           print the raw JSON response instead of pretty output
  *   -h, --help       show usage
  *
  * == Query/explain options ==
  *
  *   -d, --dim <name>        dimension (repeatable)
  *   -m, --measure <name>    measure (repeatable)
  *   -o, --order <f:dir>     order by field, dir = asc|desc (repeatable)
  *   --limit <n>             row limit
  *
  * Run via the bin/sm8 wrapper, or directly:
  *   mvn -q exec:java -Dexec.mainClass=io.sm8.cli.Main -Dexec.args="list --url http://localhost:8080"
  */
object Main {

  def main(args: Array[String]): Unit = {
    val exit = run(args.toList)
    sys.exit(exit)
  }

  /** Pure (testable) entry point — returns an exit code instead of calling sys.exit.
    *
    * Wraps each subcommand call in a TransportFailure catch so transport
    * errors (connection refused, timeout) return exit code 3 cleanly,
    * even when the JVM is being driven by a test harness. */
  def run(args: List[String]): Int = args match {
    case Nil | ("-h" :: _) | ("--help" :: _) | ("help" :: _) =>
      printUsage(); 0
    case ("-v" :: _) | ("--version" :: _) =>
      println("sm8 0.1.0-SNAPSHOT (SM8 CLI client)"); 0
    case ("list" :: rest)       => withGlobalConfig(rest) { (cfg, rem) => safeRun { cmdList(cfg); 0 } }
    case ("describe" :: rest)   => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdDescribe(cfg, rem)) }
    case ("query" :: rest)      => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdQuery(cfg, rem, explain = false)) }
    case ("explain" :: rest)    => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdQuery(cfg, rem, explain = true)) }
    case ("audit-tail" :: rest) => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdAuditTail(cfg, rem)) }
    case ("inspect" :: rest)    => withGlobalConfig(rest) { (cfg, rem) => safeRun(cmdInspect(cfg, rem)) }
    case other :: _ =>
      System.err.println(s"sm8: unknown command '$other'. Run 'sm8 --help'."); 2
  }

  /** Run a subcommand with TransportFailure caught — returns 3 on transport
    * error. The detail message is already printed to stderr by [[Client.send]],
    * so this is silent on the success path. */
  private def safeRun(f: => Int): Int =
    try f
    catch { case _: Client.TransportFailure => 3 }

  // ---------------------------------------------------------------------------
  // Global config: --url and --json can appear anywhere; strip them first so
  // each subcommand handler only sees its own flags.
  // ---------------------------------------------------------------------------

  /** Global CLI config. `restateUrl` is set when the user passes
    * `--restate-url` or `$RESTATE_URL` (PRIMARY for `audit-tail` — durable
    * audit lives in the platform's Restate service, not MCP). `token`
    * comes from `--token-file`, then `$SDF_TOKEN`, and is attached as
    * `Authorization: Bearer ...` on every request to both surfaces. */
  private case class Config(
      baseUrl:    String,
      json:       Boolean,
      restateUrl: Option[String] = None,
      token:      Option[String] = None,
  )

  // -------------------------------------------------------------------
  // Typed CLI parse errors (per docs/design/error-handling-style.md).
  //
  // Per the standard's hard bans:
  //   - No `Either[String, X]` in any new code path.
  //   - All sealed error ADTs use SPECIFIC cases (no generic `ParseError`).
  //
  // Each case carries the data needed to format a stable, programmatic
  // human-readable message via `.message`. We don't format eagerly (no
  // s-strings at construction) because the caller may want to log or
  // surface the case structurally before showing it.
  // -------------------------------------------------------------------
  private sealed trait CliParseError extends Product with Serializable {
    /** Stable human-readable message for stderr / logs. */
    def message: String
  }
  private object CliParseError {
    final case class MissingFlagValue(flag: String) extends CliParseError {
      val message: String = s"$flag requires a value"
    }
    final case class MissingModel(usage: String) extends CliParseError {
      val message: String = s"missing <model>. $usage"
    }
    final case class InvalidOrderFormat(value: String) extends CliParseError {
      val message: String = s"--order must be <field:asc|desc>, got '$value'"
    }
    final case class InvalidLimit(value: String) extends CliParseError {
      val message: String = s"--limit must be a non-negative integer, got '$value'"
    }
    final case class UnknownFlag(flag: String) extends CliParseError {
      val message: String = s"unknown flag: $flag"
    }
    final case class UnexpectedPositional(value: String, existingModel: String)
        extends CliParseError {
      val message: String =
        s"unexpected argument: $value (model already given as $existingModel)"
    }
    // -- audit-tail parse errors (Phase 3) --
    final case class InvalidSince(value: String) extends CliParseError {
      val message: String =
        s"--since must be an ISO-8601 instant (e.g. 2026-01-01T00:00:00Z), got '$value'"
    }
    final case class InvalidUntil(value: String) extends CliParseError {
      val message: String =
        s"--until must be an ISO-8601 instant (e.g. 2026-01-01T00:00:00Z), got '$value'"
    }
    final case class InvalidTenant(value: String) extends CliParseError {
      val message: String =
        s"--tenant must match [a-zA-Z0-9_-]{1,64}, got '$value'"
    }
    final case class UnexpectedArgument(value: String) extends CliParseError {
      val message: String =
        s"unexpected argument: $value (audit-tail takes no positional args)"
    }
    final case class TokenFileUnreadable(path: String, reason: String)
        extends CliParseError {
      val message: String =
        s"--token-file '$path' could not be read: $reason"
    }
  }

  /** Pull `--url` / `--json` out of the arg list (they can appear anywhere),
    * then hand the remaining args (and the resolved config) to the
    * subcommand handler. Implemented as a single two-pass walk in
    * [[extractGlobals]]. */
  private def withGlobalConfig(args: List[String])(f: (Config, List[String]) => Int): Int =
    extractGlobals(args) match {
      case Left(err) => System.err.println(s"sm8: ${err.message}"); 2
      case Right((cfg, rem)) => f(cfg, rem)
    }

  /** Two-pass extraction: walk the whole arg list, pulling out --url/--json
    * wherever they appear, leaving everything else in declaration order.
    *
    * Per docs/design/error-handling-style.md hard ban #1: returns
    * `Either[CliParseError, _]` (the existing ADT from PR #433) — no
    * `Either[String, X]`. The single failure mode (`--url` without a
    * value) maps to `CliParseError.MissingFlagValue(flag = "--url")`,
    * reusing the case the `QueryArgs` parser already uses. */
  private def extractGlobals(args: List[String]): Either[CliParseError, (Config, List[String])] = {
    // Single-pass walker. Token resolution happens after the loop
    // because the tailrec can't carry the token string cleanly without
    // growing the accumulator by another field. Two args-list walks
    // on 50-element lists is negligible cost.
    @tailrec def loop(
        in: List[String],
        url: Option[String],
        json: Boolean,
        restateUrl: Option[String],
        kept: List[String],
    ): Either[CliParseError, (Config, List[String])] = in match {
      case Nil =>
        resolveToken(args).right.map { tok =>
          (
            Config(
              baseUrl    = url.getOrElse(defaultUrl),
              json       = json,
              restateUrl = restateUrl.orElse(sys.env.get("RESTATE_URL")),
              token      = tok,
            ),
            kept.reverse,
          )
        }
      case ("--url" :: u :: rest)         => loop(rest, Some(u), json, restateUrl, kept)
      case ("--url" :: Nil)               => Left(CliParseError.MissingFlagValue(flag = "--url"))
      case ("--json" :: rest)             => loop(rest, url, json = true, restateUrl, kept)
      case ("--restate-url" :: u :: rest) => loop(rest, url, json, Some(u), kept)
      case ("--restate-url" :: Nil)       => Left(CliParseError.MissingFlagValue(flag = "--restate-url"))
      // Token file is consumed but the actual read happens in resolveToken
      // (below) — same precedence rule, but we already validated the flag
      // shape here.
      case ("--token-file" :: p :: rest)  => loop(rest, url, json, restateUrl, kept)
      case ("--token-file" :: Nil)        => Left(CliParseError.MissingFlagValue(flag = "--token-file"))
      case other :: rest                  => loop(rest, url, json, restateUrl, other :: kept)
    }
    loop(args, None, json = false, None, Nil)
  }

  /** Resolve the bearer token. Precedence: `--token-file <path>` &
    * gt; `$SDF_TOKEN` &gt; none.
    *
    * Returns `Either` so a missing/unreadable token file is a typed error
    * (printed with exit 2) instead of being silently swallowed. Per
    * `scala-error-handling §1`: errors are data — falling back to
    * `$SDF_TOKEN` when the file is unreadable would mask the user's
    * `--token-file` typo and let the request go out unauthenticated,
    * surfacing later as a confusing 401/404 instead of a clear exit 2 at
    * the CLI. */
  private def resolveToken(args: List[String]): Either[CliParseError, Option[String]] = {
    val tokenFile = args.sliding(2).collectFirst {
      case Seq("--token-file", p) => p
    }
    tokenFile match {
      case Some(p) => loadTokenFromFile(p).map(Some(_))
      case None    => Right(sys.env.get("SDF_TOKEN"))
    }
  }

  /** Load a bearer token from a file. Trims trailing whitespace (e.g. the
    * `\n` from `echo "$TOK" > tok`) — untrimmed tokens corrupt the
    * `Authorization` header. Catches `IOException` specifically per
    * `docs/design/error-handling-style.md:205-207` — no catch-all. */
  private def loadTokenFromFile(path: String): Either[CliParseError, String] =
    try {
      val raw = java.nio.file.Files.readString(java.nio.file.Paths.get(path))
      Right(raw.trim)
    } catch {
      case e: java.io.IOException =>
        Left(CliParseError.TokenFileUnreadable(path, e.getClass.getSimpleName + ": " + e.getMessage))
    }

  /** Plain Jackson mapper (no Scala module) — the CLI only reads JSON via
    * the tree model and writes small string/int values, so a vanilla
    * ObjectMapper is all it needs. Keeps the client dependency-free beyond
    * jackson-databind. */
  private val mapper = new ObjectMapper()

  private def defaultUrl: String =
    sys.env.getOrElse("SDF_URL", "http://localhost:8080")

  // ---------------------------------------------------------------------------
  // Lifecycle warnings — rendered to stderr so they don't pollute --json
  // ---------------------------------------------------------------------------

  /** Print lifecycle warnings to stderr. Wire-stable strings from the MCP
    * server (lifecycle surfacing contract). Format: one `WARN: <message>` line per
    * warning. Printed before the command's main output so the human
    * reader sees them in context. */
  private def printWarnings(warnings: List[String]): Unit = warnings.foreach { w =>
    System.err.println(s"WARN: $w")
  }

  // ---------------------------------------------------------------------------
  // Commands
  // ---------------------------------------------------------------------------

  private def cmdList(cfg: Config): Unit = {
    val resp = Client.get(cfg, "/models")
    if (cfg.json) { println(resp.body); return }
    val root = resp.parseJson
    if (root.errorPath(cfg)) return
    printWarnings(root.warningsPath)
    val models = root.dataPath.field("models").elemList
    if (models.isEmpty) { println("(no models loaded)"); return }
    val rows = models.map { m =>
      val name = m.field("name").text
      val desc = m.field("description").text
      val status = m.field("status").text
      List(name, desc, status)
    }
    println(Table.render(List("MODEL", "STATUS", "DESCRIPTION"), rows))
  }

  private def cmdDescribe(cfg: Config, args: List[String]): Int = args match {
    case Nil =>
      System.err.println("sm8 describe: missing <model>. Usage: sm8 describe <model>"); 2
    case model :: Nil =>
      val resp = Client.get(cfg, s"/models/$model")
      if (cfg.json) { println(resp.body); return 0 }
      val root = resp.parseJson
      if (root.errorPath(cfg)) return 1
      printWarnings(root.warningsPath)
      printDescribe(root.dataPath)
      0
    case _ =>
      System.err.println("sm8 describe: too many arguments. Usage: sm8 describe <model>"); 2
  }

  // `sm8 inspect <key>` reads a `context.meta` value via the
  // generic transport-layer meta-inspector. The CLI knows only
  // the key string (e.g. `io.sm8.plugins.semanticgraph:graph-
  // snapshot`); it does not know the value schema. The transport
  // layer's meta-inspector is the seam — the CLI consumes it as
  // an opaque JSON payload.
  //
  // Uses `Client.postJson` with the Restate wire path
  // `/MetaInspectorService/getMeta` and a `{"key": <key>}` body.
  private def cmdInspect(cfg: Config, args: List[String]): Int = args match {
    case Nil =>
      System.err.println("sm8 inspect: missing <key>. Usage: sm8 inspect <key>"); 2
    case key :: Nil =>
      val body = s"""{"key":${mapper.writeValueAsString(key)}}"""
      val resp = Client.postJson(cfg, "/MetaInspectorService/getMeta", body)
      if (cfg.json) { println(resp.body); return 0 }
      val root = resp.parseJson
      if (root.errorPath(cfg)) return 1
      val data = root.dataPath
      val present = data.field("present").booleanValue()
      if (!present) {
        System.err.println(s"sm8 inspect: key '$key' not set on the most recent request")
        return 4
      }
      val value = data.field("value")
      println(s"Key:   $key")
      println(s"Value: ${mapper.writeValueAsString(value)}")
      0
    case _ =>
      System.err.println("sm8 inspect: too many arguments. Usage: sm8 inspect <key>"); 2
  }
  private def printDescribe(d: JsonNode): Unit = {
    println(s"Model:        ${d.field("model").text}")
    println(s"Version:      ${d.field("version").text}")
    val status = d.field("status").textOption
    status.foreach(s => println(s"Status:       $s"))
    val src = d.field("source_table").textOption
    src.foreach(s => println(s"Source table: $s"))
    println()

    def section(title: String, field: String, cols: List[String], extract: JsonNode => List[String]): Unit = {
      val items = d.field(field).elemList
      if (items.nonEmpty) {
        println(s"$title:")
        val rows = items.map(extract)
        println(Table.render(cols, rows))
        println()
      }
    }

    section("Filters",     "filters",     List("NAME", "EXPR"),
      m => List(m.field("name").text, maskExpr(m.field("expr").text)))
    section("Dimensions",  "dimensions",  List("NAME", "EXPR"),
      m => List(m.field("name").text, maskExpr(m.field("expr").text)))
    section("Measures",    "measures",    List("NAME", "KIND", "EXPR"),
      m => List(m.field("name").text, m.field("kind").text, maskExpr(m.field("expr").text)))

    val joins = d.field("joins").elemList
    if (joins.nonEmpty) {
      println("Joins:")
      val rows = joins.map { j =>
        List(j.field("name").text, j.field("left").text, j.field("right").text,
          j.field("keys").elemList.map(_.text).mkString(", "))
      }
      println(Table.render(List("NAME", "LEFT", "RIGHT", "KEYS"), rows))
    }
  }

  private def cmdQuery(cfg: Config, args: List[String], explain: Boolean): Int = {
    QueryArgs.parse(args) match {
      case Left(err) => System.err.println(s"sm8: ${err.message}"); 2
      case Right(qa) =>
        val body = qa.toJson
        val endpoint = if (explain) "/explain" else "/query"
        val resp = Client.postJson(cfg, endpoint, body)
        if (cfg.json) { println(resp.body); return 0 }
        val root = resp.parseJson
        if (root.errorPath(cfg)) return 1
        printWarnings(root.warningsPath)
        if (explain) {
          // /explain returns Envelope[String] — the plan text is `data`.
          println(root.dataPath.text)
        } else {
          printQueryResult(root.dataPath)
        }
        0
    }
  }

  private def printQueryResult(d: JsonNode): Unit = {
    val cols = d.field("columns").elemList.map(_.field("name").text)
    val rows = d.field("rows").elemList.map { r =>
      r.elemList.map(cellToString).toList
    }
    val count = d.field("row_count").text.toInt
    val truncated = d.field("truncated").text.toBoolean
    println(Table.render(cols, rows))
    println(s"\n$count row${if (count == 1) "" else "s"}${if (truncated) " (TRUNCATED)" else ""}")
  }

  private def cellToString(n: JsonNode): String =
    if (n.isNull) "NULL"
    else if (n.isNumber) n.asText()
    else if (n.isBoolean) n.asText()
    else n.asText()

  /** Mask opaque lambda `toString` addresses for graceful degradation against
    * older server versions that don't carry the `exprString` field. Newer
    * servers (≥ PR feat/describe-model-expr-string) emit the original YAML
    * expression string verbatim, so this is a no-op in the common case —
    * we keep it for safety. e.g. `io.sm8.adapters.YamlLoader$$$Lambda$...`
    * is human-unreadable; masking it keeps the table legible when run
    * against a pre-PR server. */
  private def maskExpr(s: String): String =
    if (s != null && (s.contains("$") && s.contains("Lambda"))) "<inline fn>"
    else if (s != null && s.contains("@") && s.matches(".*@[0-9a-fA-F]+")) "<inline fn>"
    else s

  // ---------------------------------------------------------------------------
  // Query/explain flag parsing
  // ---------------------------------------------------------------------------

  private case class QueryArgs(
      model: String,
      dims: List[String],
      measures: List[String],
      order: List[(String, String)],
      limit: Option[Int],
      /**
       * Engine routing hint (per docs/design/multi-engine-design.md §6.4,
       * landed in PR #431). The server interprets this:
       *   - absent   -> server decides routing (default, backward compat)
       *   - empty    -> legacy `Models` + `SemanticTable` path
       *   - non-empty -> route through the `EngineRegistry`
       *
       * The CLI omits the field when empty (per karpathy §2: minimum code).
       * If a user passes `--engine ""` literally, they get the same
       * behavior as omitting the field — which is the right default.
       * Users who need to *force* the legacy path with the field present
       * can pass a non-empty sentinel (e.g. `--engine legacy`).
       */
      engine: String = "",
  ) {
    /** Build the JSON request body for /query and /explain. */
    def toJson: String = {
      val sb = new StringBuilder
      sb.append('{').append("\"model\":").append(mapper.writeValueAsString(model))
      if (engine.nonEmpty) sb.append(",\"engine\":").append(mapper.writeValueAsString(engine))
      if (measures.nonEmpty) sb.append(",\"measures\":").append(mapper.writeValueAsString(measures.toArray))
      if (dims.nonEmpty)     sb.append(",\"dimensions\":").append(mapper.writeValueAsString(dims.toArray))
      if (order.nonEmpty) {
        val arr = order.map { case (f, d) => s"""{"field":${mapper.writeValueAsString(f)},"direction":"$d"}""" }
        sb.append(",\"order_by\":[").append(arr.mkString(",")).append("]")
      }
      limit.foreach(n => sb.append(",\"limit\":").append(n))
      sb.append('}').toString
    }
  }

  private object QueryArgs {
    def parse(args: List[String]): Either[CliParseError, QueryArgs] = {
      @tailrec def loop(
          in: List[String],
          model: Option[String],
          dims: List[String],
          measures: List[String],
          order: List[(String, String)],
          limit: Option[Int],
          engine: String,
      ): Either[CliParseError, QueryArgs] = in match {
        case Nil =>
          model match {
            case Some(m) =>
              Right(QueryArgs(
                model    = m,
                dims     = dims.reverse,
                measures = measures.reverse,
                order    = order.reverse,
                limit    = limit,
                engine   = engine,
              ))
            case None =>
              Left(CliParseError.MissingModel(
                usage = "Usage: sm8 query <model> -d <dim> -m <measure>"
              ))
          }
        case ("-d" | "--dim") :: v :: rest => loop(rest, model, v :: dims, measures, order, limit, engine)
        case ("-d" | "--dim") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--dim"))
        case ("-m" | "--measure") :: v :: rest => loop(rest, model, dims, v :: measures, order, limit, engine)
        case ("-m" | "--measure") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--measure"))
        case ("-o" | "--order") :: v :: rest =>
          v.split(":", 2) match {
            case Array(f, d) if d == "asc" || d == "desc" =>
              loop(rest, model, dims, measures, (f, d) :: order, limit, engine)
            case Array(f) =>
              loop(rest, model, dims, measures, (f, "asc") :: order, limit, engine)
            case _ => Left(CliParseError.InvalidOrderFormat(value = v))
          }
        case ("-o" | "--order") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--order"))
        case "--limit" :: v :: rest =>
          v.toIntOption match {
            case Some(n) if n >= 0 => loop(rest, model, dims, measures, order, Some(n), engine)
            case _ => Left(CliParseError.InvalidLimit(value = v))
          }
        case "--limit" :: Nil => Left(CliParseError.MissingFlagValue(flag = "--limit"))
        // PR #432 (v0.3.1 Step 2): expose the MCP server's engine-routing
        // field on the CLI. Omitted by default (server decides routing per
        // PR #431); when present, server routes through EngineRegistry
        // if configured.
        case ("--engine") :: v :: rest => loop(rest, model, dims, measures, order, limit, engine = v)
        case ("--engine") :: Nil => Left(CliParseError.MissingFlagValue(flag = "--engine"))
        case flag :: _ if flag.startsWith("-") => Left(CliParseError.UnknownFlag(flag = flag))
        case v :: rest => model match {
          case Some(existing) =>
            Left(CliParseError.UnexpectedPositional(value = v, existingModel = existing))
          case None => loop(rest, Some(v), dims, measures, order, limit, engine)
        }
      }
      loop(args, None, Nil, Nil, Nil, None, engine = "")
    }
  }

  // ---------------------------------------------------------------------------
  // audit-tail flag parsing (Phase 3)
  // ---------------------------------------------------------------------------

  /** Args for `sm8 audit-tail [--limit N] [--since T] [--until T] [--tenant T]`.
    * Tenant regex per Restate's documented constraints; since/until
    * validated client-side so a typo costs no round-trip. */
  private case class AuditArgs(
      tenant: String,
      limit:  Option[Int],
      since:  Option[String],
      until:  Option[String],
  )
  private object AuditArgs {
    private val TenantRe = "^[a-zA-Z0-9_-]{1,64}$".r
    def parse(args: List[String]): Either[CliParseError, AuditArgs] = {
      @tailrec def loop(
          in: List[String],
          tenant: String,           // default applied below
          limit: Option[Int],
          since: Option[String],
          until: Option[String],
      ): Either[CliParseError, AuditArgs] = in match {
        case Nil => Right(AuditArgs(tenant, limit, since, until))
        case "--tenant" :: v :: rest =>
          if (TenantRe.findFirstIn(v).isDefined) loop(rest, v, limit, since, until)
          else Left(CliParseError.InvalidTenant(value = v))
        case "--tenant" :: Nil => Left(CliParseError.MissingFlagValue(flag = "--tenant"))
        case "--limit" :: v :: rest =>
          v.toIntOption match {
            case Some(n) if n >= 0 => loop(rest, tenant, Some(n), since, until)
            case _ => Left(CliParseError.InvalidLimit(value = v))
          }
        case "--limit" :: Nil => Left(CliParseError.MissingFlagValue(flag = "--limit"))
        case "--since" :: v :: rest =>
          if (parseInstant(v).isDefined) loop(rest, tenant, limit, Some(v), until)
          else Left(CliParseError.InvalidSince(value = v))
        case "--since" :: Nil => Left(CliParseError.MissingFlagValue(flag = "--since"))
        case "--until" :: v :: rest =>
          if (parseInstant(v).isDefined) loop(rest, tenant, limit, since, Some(v))
          else Left(CliParseError.InvalidUntil(value = v))
        case "--until" :: Nil => Left(CliParseError.MissingFlagValue(flag = "--until"))
        case flag :: _ if flag.startsWith("-") => Left(CliParseError.UnknownFlag(flag = flag))
        case v :: _ => Left(CliParseError.UnexpectedArgument(value = v))
      }
      // Default tenant: matches the platform's AuditService default.
      loop(args, tenant = "default", None, None, None)
    }
    private def parseInstant(s: String): Option[String] =
      try { java.time.Instant.parse(s); Some(s) }
      catch { case _: java.time.format.DateTimeParseException => None }
  }

  // ---------------------------------------------------------------------------
  // Restate ingress HTTP client (Phase 3)
  //
  // Talks the Restate HTTP ingress protocol directly. Wire shape:
  //   POST /<ServiceName>[/<key>]/<handlerName>/send
  //   Content-Type: application/json
  //   Authorization: Bearer <token>   (if cfg.token)
  //   { ...request body... }
  //
  // Response (Restate native):
  //   { "status": "ok"|"error", "output"|"error": ... }
  //
  // We keep the surface minimal — one `call` method — because every
  // additional method is another ripple point per scala-impact-analysis.
  // ---------------------------------------------------------------------------

  private object RestateClient {
    private val http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build()

    /** Typed errors per `scala-error-handling §1`. */
    sealed trait RestateError
    case class RestateHttpError(uri: String, status: Int, code: String, message: String)
        extends RestateError
    case class RestateDecodeError(uri: String, reason: String) extends RestateError
    case class RestateConnectError(uri: String, cause: Throwable) extends RestateError

    /** Call a Restate handler synchronously (returns the handler's output).
      *
      * Wire shape (verified live against `restate_dev`):
      *   `POST /<Service>[/<key>]/<handler>` — `application/json` body, raw
      *   JSON response (the handler's return value). NO `/send` suffix
      *   (`/send` is Restate's fire-and-forget variant — it returns 200 OK
      *   with no body, which is wrong for `audit-tail`).
      *
      * @param base the Restate ingress base URL (e.g. `http://localhost:8080`)
      * @param service the `@Service`/`@Workflow`/`@VirtualObject` name
      * @param key the object key for VirtualObject calls; `None` produces
      *            a path of `/<Service>/<handler>` (only valid for plain
      *            `@Service` types — VirtualObjects REQUIRE a key).
      * @param handler the handler method name
      * @param token optional bearer token
      * @param body the request body (will be JSON-serialized)
      * @return parsed JSON tree (caller validates shape — success returns
      *         the handler's raw output; Restate's HTTP error envelope
      *         is unwrapped to `RestateHttpError` on 4xx/5xx).
      */
    def call(
        base:    String,
        service: String,
        key:     Option[String],
        handler: String,
        token:   Option[String],
        body:    Any,
    ): Either[RestateError, JsonNode] = {
      val path = key.fold(s"/$service/$handler")(k => s"/$service/$k/$handler")
      val url  = base.replaceAll("/+$", "") + path
      val reqB = HttpRequest.newBuilder(java.net.URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
      token.foreach(t => reqB.header("Authorization", s"Bearer $t"))
      val req = reqB.build()
      try {
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        if (resp.statusCode() / 100 != 2) {
          // Restate returns 4xx/5xx with a JSON envelope explaining why.
          val parsed = try mapper.readTree(resp.body()) catch {
            case _: com.fasterxml.jackson.core.JsonProcessingException =>
              mapper.createObjectNode()
          }
          val code = parsed.path("error").path("code").asText("UNKNOWN")
          val msg  = parsed.path("error").path("message").asText(resp.body())
          Left(RestateHttpError(url, resp.statusCode(), code, msg))
        } else {
          try Right(mapper.readTree(resp.body()))
          catch {
            case e: com.fasterxml.jackson.core.JsonProcessingException =>
              Left(RestateDecodeError(url, e.getClass.getSimpleName + ": " + e.getMessage))
          }
        }
      } catch {
        case e: java.net.ConnectException =>
          System.err.println(s"sm8: could not connect to $url (is Restate running?)")
          Left(RestateConnectError(url, e))
        case e: java.net.http.HttpTimeoutException =>
          // PR-200: honor PR-176 NonFatal discipline — HttpTimeoutException
          // is an IOException subtype, but treat it as a distinct typed
          // shape so the user sees "timed out" instead of the JVM-default
          // `HttpTimeoutException: <msg>` rendering (pre-PR-200 the
          // `case e: Exception` catch surfaced the full qualified name).
          System.err.println(s"sm8: request to $url timed out")
          Left(RestateConnectError(url, e))
        case e: java.io.IOException =>
          // PR-200 (audit follow-up M1): narrow from `case e: Exception`
          // to `IOException` — Restate's HTTP-client contract throws
          // IOException for transport errors (HttpTimeoutException,
          // ConnectException, malformed-URI, etc.). The prior
          // `case e: Exception` catch also caught
          // `InterruptedException` (extends `Exception`) and dropped
          // the interrupt flag. The new arm below re-sets the flag
          // so any subsequent `wait`/`sleep`/`blocking IO` exits
          // promptly. Matches `EngineService.executeEngine:248-249`
          // (PR-176 reference).
          System.err.println(s"sm8: request failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
          Left(RestateConnectError(url, e))
        case e: InterruptedException =>
          // PR-200: a `SIGINT`/cancel during a Restate call must NOT
          // be silently swallowed (it would corrupt future cancel
          // detection — the JVM needs the interrupt flag set to
          // break out of `Thread.sleep`, `wait`, blocking IO, etc.).
          // Re-set the interrupt flag and surface as a connect error
          // with the interrupt signal preserved. Matches the
          // `EngineService.executeEngine` pattern (sm8-platform
          // query path; PR-176 reference).
          Thread.currentThread().interrupt()
          System.err.println(s"sm8: request to $url cancelled")
          Left(RestateConnectError(url, e))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // audit-tail command (Phase 3)
  // ---------------------------------------------------------------------------

  private def cmdAuditTail(cfg: Config, args: List[String]): Int = {
    AuditArgs.parse(args) match {
      case Left(err) =>
        System.err.println(s"sm8: ${err.message}"); 2
      case Right(aa) =>
        val base = cfg.restateUrl.getOrElse {
          System.err.println(
            "sm8: audit-tail requires --restate-url or $RESTATE_URL " +
            "(audit data lives in the Platform Restate service, not in MCP)")
          return 2
        }
        // Use java.util.LinkedHashMap (not Scala Map) so Jackson treats
        // it as a JSON object. Without the Scala module, Jackson would
        // introspect Scala's Map4 inner fields and serialize them as
        // `scala$collection$immutable$Map$Map4$key1` etc. Only emit
        // non-null fields so the payload is minimal.
        val body = new java.util.LinkedHashMap[String, AnyRef]()
        body.put("tenant", aa.tenant)
        aa.since.foreach(s => body.put("since", s))
        aa.until.foreach(s => body.put("until", s))
        aa.limit.foreach(n => body.put("limit", Int.box(n)))
        RestateClient.call(
          base    = base,
          service = "AuditService",
          // key = tenant: VirtualObject key. AuditService journal state
          // is partitioned per tenant (see AuditService.java:14 — "Key:
          // tenant") so the tenant-as-key mapping is what the platform
          // expects. Without a key, Restate rejects VirtualObject calls
          // with "bad path, expected /:object-name/:object-key/:handler".
          key     = Some(aa.tenant),
          handler = "queryRecent",
          token   = cfg.token,
          body    = body,
        ) match {
          case Left(RestateClient.RestateHttpError(_, status, code, msg)) =>
            System.err.println(s"sm8: Restate $code ($status): $msg"); 1
          case Left(RestateClient.RestateDecodeError(_, reason)) =>
            System.err.println(s"sm8: bad response from Restate: $reason"); 1
          case Left(RestateClient.RestateConnectError(_, _)) =>
            // Connect error already printed by the client. 3 = transport.
            3
          case Right(node) =>
            // Synchronous Restate call returns the handler's raw JSON
            // output directly — for `queryRecent` that's a JSON array of
            // AuditEventRow records (NOT wrapped in a { status, output }
            // envelope — that's the MCP REST shape, not Restate's).
            if (cfg.json) { println(node.toString); return 0 }
            printAuditTable(node)
            0
        }
    }
  }

  private def printAuditTable(events: JsonNode): Unit = {
    val arr = if (events != null && events.isArray) events.iterator.asScala.toList else Nil
    if (arr.isEmpty) {
      println("(no audit events)"); return
    }
    // Columns mirror the platform's AuditEventRow record
    // (semanticdf-platform/.../audit/AuditEventStore.java:88-93):
    // tenant, eventType, ts, dedupHash, payload. The original MCP-side
    // AuditEvent had model/status/rowCount/elapsedMs as top-level fields;
    // when emitted via Restate their summary lives in the opaque `payload`
    // JSON blob. We truncate payload for the table; `--json` shows the
    // full structure for scripts.
    val payloadMax = 40
    val rows = arr.map { e =>
      val raw = e.field("payload").text
      val payload = if (raw.length > payloadMax) raw.take(payloadMax - 1) + "…" else raw
      List(
        e.field("ts").text,
        e.field("tenant").text,
        e.field("eventType").text,
        e.field("dedupHash").text,
        payload,
      )
    }
    println(Table.render(
      List("TS", "TENANT", "EVENT", "DEDUP", "PAYLOAD"),
      rows,
    ))
    println(s"\n${arr.size} event${if (arr.size == 1) "" else "s"}")
  }

  // ---------------------------------------------------------------------------
  // HTTP client + JSON response wrapper
  // ---------------------------------------------------------------------------

  private object Client {
    private val http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build()

    case class Response(status: Int, body: String) {
      /** Parse the body as a JSON tree. Throws on malformed JSON — callers
        * should have already validated via errorPath or know the shape. */
      def parseJson: JsonRoot = {
        val node = mapper.readTree(body)
        JsonRoot(node)
      }
    }

    def get(cfg: Config, path: String): Response = {
      val b = HttpRequest.newBuilder(uri(cfg, path))
        .timeout(Duration.ofSeconds(30))
        .GET()
      cfg.token.foreach(t => b.header("Authorization", s"Bearer $t"))
      send(b.build())
    }

    def postJson(cfg: Config, path: String, body: String): Response = {
      val b = HttpRequest.newBuilder(uri(cfg, path))
        .timeout(Duration.ofSeconds(30))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
      cfg.token.foreach(t => b.header("Authorization", s"Bearer $t"))
      send(b.build())
    }

    private def uri(cfg: Config, path: String): URI =
      URI.create(cfg.baseUrl.replaceAll("/+$", "") + path)

    private def send(req: HttpRequest): Response =
      try {
        val resp = http.send(req, HttpResponse.BodyHandlers.ofString())
        Response(resp.statusCode(), resp.body())
      } catch {
        case e: java.net.ConnectException =>
          // Print to stderr and rethrow as a typed exception. The caller
          // (Main.run) catches it and returns exit code 3. We don't call
          // sys.exit here because that would kill the test JVM when
          // CliIntegrationSpec exercises Main.run directly.
          System.err.println(s"sm8: could not connect to ${req.uri} (is the server running?)")
          throw new TransportFailure(req.uri.toString, e)
        case e: java.net.http.HttpTimeoutException =>
          // PR-200: honor PR-176 NonFatal discipline — distinct typed
          // exception so the user sees "timed out" not the generic
          // "request failed" line. Matches the rest of the codebase.
          System.err.println(s"sm8: request to ${req.uri} timed out")
          throw new TransportFailure(req.uri.toString, e)
        case e: java.io.IOException =>
          // PR-200 (audit follow-up M1): narrow from `case e: Exception`
          // to `IOException` — `java.net.http.HttpClient.send` only
          // throws `IOException` and its subtypes for transport
          // errors. The prior `case e: Exception` catch also caught
          // `InterruptedException` (which extends `Exception`) and
          // dropped the interrupt flag. The new arm below re-sets
          // the flag so any subsequent `wait`/`sleep`/`blocking IO`
          // exits promptly. Matches `EngineService.executeEngine:248-249`
          // (PR-176 reference).
          System.err.println(s"sm8: request failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
          throw new TransportFailure(req.uri.toString, e)
        case e: InterruptedException =>
          // PR-200: preserve the cancel signal — re-set the interrupt
          // flag so any subsequent `wait`/`sleep`/`blocking IO` exits
          // promptly, then surface as the typed `TransportFailure`.
          // Matches the `EngineService.executeEngine` pattern (PR-176).
          Thread.currentThread().interrupt()
          System.err.println(s"sm8: request to ${req.uri} cancelled")
          throw new TransportFailure(req.uri.toString, e)
      }

    /** Raised by [[send]] on any HTTP transport error (connect refused,
      * timeout, malformed response). [[Main.run]] catches this and returns
      * exit code 3 so the CLI's behaviour is testable end-to-end without
      * the JVM dying via `sys.exit`. */
    class TransportFailure(uri: String, cause: Throwable)
      extends RuntimeException(s"transport failure for $uri", cause)
  }

  /** Thin wrapper over a Jackson JsonNode tree to make the response-walking
    * code readable. All accessors are lenient — missing fields render as
    * empty rather than throwing, since the server is the source of truth
    * for the shape and a partial render beats a crash. */
  private final case class JsonRoot(node: JsonNode) {
    def dataPath: JsonNode =
      if (node.has("data") && !node.get("data").isNull) node.get("data")
      else com.fasterxml.jackson.databind.node.NullNode.getInstance()

    /** If the envelope is an error, print it and return true. */
    def errorPath(cfg: Config): Boolean = {
      val status = node.path("status").asText("")
      if (status == "error" || node.has("error")) {
        val err = node.get("error")
        val code = err.path("code").asText("UNKNOWN")
        val msg = err.path("message").asText("(no message)")
        if (cfg.json) println(body) else System.err.println(s"sm8: $code: $msg")
        true
      } else false
    }

    /** Lifecycle warnings carried on the envelope. The MCP server emits
      * these when a tool touched a Deprecated or Draft model;
      * the field is additive for tolerant JSON clients and absent on older
      * server versions, in which case this returns Nil. */
    def warningsPath: List[String] = {
      val arr = node.path("warnings")
      if (arr == null || !arr.isArray) Nil
      else arr.iterator.asScala.toList.map(_.asText(""))
    }

    def body: String = node.toString
  }

  // ---------------------------------------------------------------------------
  // JSON node helpers (implicit-class style, kept local to avoid polluting
  // the Jackson namespace project-wide)
  // ---------------------------------------------------------------------------

  private implicit class JsonNodeOps(private val node: JsonNode) extends AnyVal {
    def field(name: String): JsonNode =
      if (node != null && node.has(name) && !node.get(name).isNull) node.get(name)
      else com.fasterxml.jackson.databind.node.NullNode.getInstance()
    def elemList: List[JsonNode] =
      if (node != null && node.isArray) node.iterator.asScala.toList else Nil
    def text: String = if (node == null || node.isNull) "" else node.asText("")
    def textOption: Option[String] =
      if (node == null || node.isNull) None else Some(node.asText(""))
  }

  // ---------------------------------------------------------------------------
  // Minimal table renderer (no deps)
  // ---------------------------------------------------------------------------

  private object Table {
    def render(headers: List[String], rows: List[List[String]]): String = {
      val all = headers :: rows
      val widths = headers.indices.map { i =>
        all.map(row => if (i < row.length) row(i).length else 0).max
      }
      val fmt = widths.map(w => s"%-${w}s").mkString("  ")
      val sep = widths.map("-" * _).mkString("  ")
      val headerLine = fmt.format(headers: _*)
      val dataLines = rows.map(r => fmt.format(padTo(r, headers.size): _*))
      (headerLine :: sep :: dataLines).mkString("\n")
    }
    private def padTo(row: List[String], n: Int): List[String] =
      row ++ List.fill(n - row.length)("")
  }

  // ---------------------------------------------------------------------------
  // Usage
  // ---------------------------------------------------------------------------

  private def printUsage(): Unit = {
    println(
      """sm8 — a command-line client for SM8 REST + Restate APIs.
        |
        |usage: sm8 <command> [options]
        |
        |commands:
        |  list                            list available models
        |  describe <model>                show a model's dimensions / measures / filters / joins
        |  query <model> [opts]            run a semantic query, print a table
        |  explain <model> [opts]          show the semantic plan (no execution)
        |  audit-tail [opts]               show recent audit events (Restate, durable)
        |  inspect <key>                   read a context.meta key (generic meta-inspector)
        |
        |query/explain options:
        |  -d, --dim <name>                dimension (repeatable)
        |  -m, --measure <name>            measure (repeatable)
        |  -o, --order <field:asc|desc>    order by field (repeatable; asc default)
        |  --limit <n>                     row limit
        |
        |audit-tail options:
        |  --tenant <id>                   tenant ID (default: default; matches [a-zA-Z0-9_-]{1,64})
        |  --limit <n>                     row limit
        |  --since <iso8601>               start of time window (e.g. 2026-01-01T00:00:00Z)
        |  --until <iso8601>               end of time window
        |
        |global options:
        |  --url <base>                    MCP REST URL (default $SDF_URL or http://localhost:8080)
        |  --restate-url <base>            Restate ingress URL for audit-tail (default $RESTATE_URL)
        |  --token-file <path>             bearer token file (default $SDF_TOKEN); chmod 600 it
        |  --json                          print raw JSON response
        |  -h, --help                      show this help
        |  -v, --version                   print version
        |
        |examples:
        |  sm8 list
        |  sm8 describe flights
        |  sm8 query flights -d carrier -m flight_count -o carrier:asc --limit 10
        |  sm8 explain flights -d carrier -m flight_count
        |  sm8 audit-tail --limit 5 --restate-url http://localhost:9080
        |  sm8 inspect io.sm8.plugins.semanticgraph:graph-snapshot
        |""".stripMargin)
  }
}
