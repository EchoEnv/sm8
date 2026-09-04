/*
 * SM8 Platform — Main (Step 11 production entry point, per the deployment-shape design).
 *
 * The runnable process that wires the SM8 MCP server:
 *
 *   1. parse CLI args (typed, per [[scala-error-handling-mindset]])
 *   2. load the Model from YAML (PlatformModelLoader — schema
 *      validation + semantic parse, both typed)
 *   3. discover EngineProviders via Java ServiceLoader
 *      (`META-INF/services/io.sm8.core.engine.EngineProvider`)
 *   4. build EngineRegistry (fail-loud at boot per design §4.1)
 *   5. install the JVM shutdown hook (a prior PR, audit 2026-08-30
 *      L4: BEFORE transport.start so SIGTERM during start() or
 *      before this point still triggers cleanup)
 *   6. start HttpTransport (binds the actual socket)
 *
 * ==Per karpathy-guidelines-mindset "smallest correct change"==
 *
 * Pure composition. Every piece already exists: PlatformModelLoader,
 * EngineRegistry, HttpTransport. Main adds NO engine logic.
 *
 * ==Per `semantic-layer-engine-architecture.md` §3 Core Boundary==
 *
 * Entry point lives in `sm8-platform` (NOT core). Core stays frozen.
 * Main knows no data source — engines arrive via ServiceLoader from
 * the connector JARs on the deployment classpath (e.g.
 * `connectors/spark-connector` ships `SparkEngineProvider`).
 *
 * ==Per `plugins.md` Rule 4==
 *
 * Main is NOT a Plugin — it does not register via `Plugin.setup`.
 * It is deployment wiring, the outermost layer.
 *
 * ==Per [[scala-jvm-safety-mindset]]==
 *
 * - Shutdown hook: `transport.stop()` on JVM exit (idempotent —
 *   HttpTransport.stop() is a no-op when never started OR already
 *   stopped). Registered BEFORE `transport.start()` (a prior PR) so
 *   SIGTERM during start() or before this point still triggers
 *   cleanup; otherwise the socket stays bound (TIME_WAIT ~60s) and
 *   the SparkSession is orphaned.
 * - Fail loud: every Left/throwable maps to a typed exit code +
 *   stderr message. No silent degradation.
 *
 * ==Per [[scala-spark-batch-bugs-mindset]] (per user directive)==
 *
 * - mantra #1 (closure-safety): the wired `EngineRegistry` is
 *   `Serializable` (verified by MainSpec round-trip + upstream
 *   `EngineServiceSpec` serializable-safe contract). Providers are
 *   instantiated on the DRIVER at boot — never shipped to executors.
 * - mantra #5 (driver/executor): Main runs in the driver process.
 *   A discovered `SparkEngineProvider` compiles + collects in the
 *   driver. No executor-side resources leak through Main.
 * - mantras #2/#3/#4: N/A — Main executes nothing itself; it is
 *   wiring only.
 *
 * ==Per [[scala-perf-testing-mindset]]==
 *
 * Boot-time work (model load, provider discovery, bind) happens
 * once. Per-request dispatch is the existing `QueryService` path.
 *
 * ==Serializable / Spark (direct + indirect)==
 *
 * Direct: zero `org.apache.spark.*` imports (layer boundary holds).
 * Indirect: the registry may hold `SparkEngineProvider` when the
 * spark-connector JAR is on the classpath; its Serializable
 * contract is enforced by `SparkEngineProviderSpec` upstream.
 */
package io.sm8.server

import io.sm8.core.engine.{EngineError, EngineIdentity, EngineProvider, EngineRegistry, QueryRequest, PortableQueryResult}
import io.sm8.core.model.Model

import io.sm8.platform.query.{HttpTransport, MetricsHttpRoute, PlatformModelLoader}

import java.nio.file.{Path, Paths}
import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
 * Production entry point for the SM8 MCP server.
 *
 * Usage:
 * {{{
 * java -cp ... io.sm8.server.Main \
 *   --model /path/to/model.yaml \
 *   [--port 8080] \
 *   [--engine spark-3.5]
 * }}}
 *
 * Exit codes (typed, per [[scala-error-handling-mindset]]):
 *  - 0 — clean shutdown (SIGINT/SIGTERM)
 *  - 1 — model load failure (typed PlatformModelError, printed)
 *  - 2 — CLI usage error (missing/bad args; unknown flag)
 *  - 3 — boot failure (no providers discovered, default engine
 *        unavailable, or bind failure — all fail loud per §4.1)
 */
object Main {

  /** Stdio transport drain delay. After `awaitClose` returns, give
    * the SDK's outbound scheduler (single-thread executor) time to
    * drain the last JSON-RPC frame before `System.exit(0)`. Belt-
    * and-suspenders in production; important for the smoke tests
    * where the subprocess reads stdout before EOF on its stdin.
    *
    * Per C5-de-L2: previously a magic `Thread.sleep(500)` inline.
    */
  private val StdioDrainDelayMs: Long = 500L

  /** CLI shape. Parsed once; pure data (scala-data-drivenrefactor).
    *
    * `modelPath` is `None` when `--model` was absent — the empty
    * string is NOT a legal Path (POSIX resolves it to the working
    * directory, which made `Files.exists("")` silently return true
    * and produced a misleading "Is a directory" parse failure
    * downstream). `run()` rejects `None` with a typed
    * `CliError.MissingFlag` so the operator sees the right message.
    */
  final case class CliArgs(
      modelPath:     Option[Path],
      port:          Int    = 8080,
      metricsPort:   Int    = 9090,
      engine:        Option[String]        = None,
      connectorUrl:  Option[String]        = None,
      mcpHttpPort:   Int                   = 0,  // 0 = disabled; set to enable per the design
      mcpHttpEndpoint: String              = "/mcp",
      mcpHttpDisallowDelete: Boolean       = false,
      mcpTransport:  Option[String]        = None, // None = disabled; "stdio" = in-process stdio MCP per the stdio design
      // For --mcp-transport stdio: where to forward the 5 tool calls.
      // Default points at a local sm8-server ingress at 8080 (the
      // standard deployment shape; sm8-stdio + sm8-server may share
      // the same process or be on different hosts).
      ingressUrl:     String               = "http://127.0.0.1:8080",
      requestTimeout: java.time.Duration    = java.time.Duration.ofSeconds(30),
      // Skip the startup ingress reachability probe (stdio boot
      // path only — see ADR-015 §Open questions; the --mcp-http-port
      // path uses --ingress-url pointing at the same process's --port
      // where misconfiguration is less likely). Defaults to false
      // (probe runs) so operators get the diagnostic at boot;
      // operators prioritizing fast cold-start over misconfiguration
      // detection can pass --skip-ingress-probe.
      skipIngressProbe: Boolean            = false
  )

  /** Typed CLI parse failure — `reason` goes to stderr. */
  sealed trait CliError { def reason: String }
  object CliError {
    final case class MissingFlag(flag: String) extends CliError {
      val reason = s"sm8: missing required flag $flag (run 'sm8-server --help')"
    }
    final case class BadInt(flag: String, value: String) extends CliError {
      val reason = s"sm8: $flag expects an integer, got '$value'"
    }
    final case class UnknownFlag(flag: String) extends CliError {
      val reason = s"sm8: unknown flag '$flag' (run 'sm8-server --help')"
    }
    final case class MissingValue(flag: String) extends CliError {
      val reason = s"sm8: flag $flag expects a value"
    }
    final case class MutuallyExclusive(flag1: String, flag2: String) extends CliError {
      val reason = s"sm8: $flag1 and $flag2 are mutually exclusive (set one or the other, not both)"
    }
    final case class BadUrl(flag: String, value: String) extends CliError {
      val reason = s"sm8: $flag expects a URL, got '$value'"
    }
    // Per C5-de-L4: typed error for flag values that must be one of
    // a known enum (e.g., --mcp-transport must be "stdio" today).
    // Distinguishes from BadInt (numeric) and BadUrl (URL).
    final case class BadValue(flag: String, value: String, hint: String) extends CliError {
      val reason = s"sm8: $flag expects $hint, got '$value'"
    }
  }

  private val Usage: String =
    """sm8-server — SM8 MCP server (Step 11, the design)
      |
      |Usage: sm8-server --model <yaml-path> [--port <n>] [--engine <name>]
      |
      |  --model <path>   model manifest (YAML, schema-validated)
      |  --port <n>       TCP port (default 8080; 0 = ephemeral)
      |  --metrics-port <n>  TCP port for Prometheus /metrics (default 9090)
      |  --mcp-http-port <n>   TCP port for Streamable HTTP MCP transport
      |                         (default 0 = disabled; per the HTTP-MCP design a prior PR)
      |  --mcp-http-endpoint <path>  MCP endpoint path (default /mcp)
      |  --mcp-http-disallow-delete  DELETE /mcp returns 405 instead of 200
      |  --mcp-transport <mode>  in-process MCP transport (per the stdio design):
      |                         "stdio" = JSON-RPC over stdin/stdout.
      |                         Mutually exclusive with --mcp-http-port
      |                         (stdio rejected if both set; exit 2).
      |  --ingress-url <url>   Restate ingress base URL for the 5 MCP tools
      |                        (default http://127.0.0.1:8080; http or https).
      |                        Used only when --mcp-transport stdio is set.
      |  --request-timeout <secs>  Per-tool-call HTTP timeout in SECONDS
      |                            (default 30). MUST match the unit of
      |                            sm8-mcp's --request-timeout (also seconds).
      |  --skip-ingress-probe    Skip the startup ingress reachability probe
      |                            (--mcp-transport stdio only). Faster cold-
      |                            start, no startup misconfig warning.
      |  --engine <name>     default engine (default: first discovered
      |                      EngineProvider on the classpath)
      |  --connector-url <u> optional connector URL (e.g.
      |                      'spark://host:7077', 'spark-connect://host:15002',
      |                      'local[*]'). When set, the platform asks
      |                      the discovered connector descriptor to
      |                      realize against the URL via its (String) ctor
      |                      (no spark types in the platform).
      |
      |Engines are discovered via META-INF/services/
      |io.sm8.core.engine.EngineProvider (Java ServiceLoader).""".stripMargin

  /** Pure arg parser — fully unit-testable, no IO.
    *
    * `modelPath` is `None` when `--model` was absent. Callers must
    * surface `Left(CliError.MissingFlag("--model"))` via `run()` —
    * the parser does not emit that error itself so it stays
    * composable in tests that construct `CliArgs(...)` directly.
    */
  def parseArgs(args: List[String]): Either[CliError, CliArgs] = {
    def loop(remaining: List[String], acc: CliArgs): Either[CliError, CliArgs] =
      remaining match {
        case Nil => Right(acc)
        case "--help" :: _ | "-h" :: _ => Left(CliError.MissingFlag("--model")) // handled by run() before parse
        case "--model" :: value :: rest if value.startsWith("-") || value.isEmpty =>
          // Empty value: `Paths.get("")` resolves to the working
          // directory on POSIX (a directory exists), which would
          // silently pass `Files.exists` and surface as a misleading
          // "Is a directory" parse failure downstream. Reject at the
          // CLI boundary so the operator sees a typed MissingValue.
          Left(CliError.MissingValue("--model"))
        case "--model" :: value :: rest =>
          loop(rest, acc.copy(modelPath = Some(Paths.get(value))))
        case "--model" :: Nil => Left(CliError.MissingValue("--model"))
        case "--port" :: value :: rest =>
          try loop(rest, acc.copy(port = value.toInt))
          catch { case _: NumberFormatException => Left(CliError.BadInt("--port", value)) }
        case "--port" :: Nil => Left(CliError.MissingValue("--port"))
        case "--metrics-port" :: value :: rest =>
          try loop(rest, acc.copy(metricsPort = value.toInt))
          catch { case _: NumberFormatException => Left(CliError.BadInt("--metrics-port", value)) }
        case "--metrics-port" :: Nil => Left(CliError.MissingValue("--metrics-port"))
        case "--engine" :: value :: rest =>
          loop(rest, acc.copy(engine = Some(value)))
        case "--engine" :: Nil => Left(CliError.MissingValue("--engine"))
        case "--connector-url" :: value :: rest =>
          loop(rest, acc.copy(connectorUrl = Some(value)))
        case "--connector-url" :: Nil => Left(CliError.MissingValue("--connector-url"))
        case "--mcp-http-port" :: value :: rest =>
          try loop(rest, acc.copy(mcpHttpPort = value.toInt))
          catch { case _: NumberFormatException => Left(CliError.BadInt("--mcp-http-port", value)) }
        case "--mcp-http-port" :: Nil => Left(CliError.MissingValue("--mcp-http-port"))
        case "--mcp-http-endpoint" :: value :: rest =>
          loop(rest, acc.copy(mcpHttpEndpoint = value))
        case "--mcp-http-endpoint" :: Nil => Left(CliError.MissingValue("--mcp-http-endpoint"))
        case "--mcp-http-disallow-delete" :: Nil =>
          loop(Nil, acc.copy(mcpHttpDisallowDelete = true))
        case "--mcp-transport" :: value :: rest =>
          // Per C5-de-L4: validate the value at parseArgs time so a
          // bad value (e.g., `--mcp-transport http` when the operator
          // meant the Streamable HTTP transport via --mcp-http-port)
          // fails fast instead of running the full boot (model load,
          // plugin discovery, transport.start, metrics bind) before
          // the unknown-value error.
          if (value != "stdio")
            Left(CliError.BadValue("--mcp-transport", value, "expected 'stdio'"))
          else {
            // The mutex check is performed post-loop (see below) so
            // the rule is symmetric regardless of argv order.
            loop(rest, acc.copy(mcpTransport = Some(value)))
          }
        case "--mcp-transport" :: Nil => Left(CliError.MissingValue("--mcp-transport"))
        case "--ingress-url" :: value :: rest =>
          // Validate URL parse eagerly so the operator sees a typed
          // CLI error at boot, not a deferred IllegalArgumentException
          // on the first tool call. Per the stdio design r2 Q4:
          // BadUrl(flag, value) is the typed surface.
          //
          // Per pig's PR-265 de-review H1: `new URI(value).toURL`
          // throws java.net.URISyntaxException (a checked Exception,
          // NOT a RuntimeException subclass) for malformed schemes
          // such as `--ingress-url 127.0.0.1:8080` (no scheme). The
          // previous catch listed only MalformedURLException +
          // IllegalArgumentException, missing URISyntaxException
          // entirely — the exception propagated out of parseArgs
          // (which declares Either, no throws clause) and crashed
          // run() with a stack trace instead of the typed
          // Left(CliError.BadUrl(...)). The Non-fatal catch wraps
          // all three + any future URL/URI Exception types under
          // the same typed-error contract.
          try {
            val u = new java.net.URI(value).toURL
            // PR-265 de-review L2: restrict to http(s) so the
            // --ingress-url flag cannot silently accept ftp://, file://,
            // jar://, etc. The HTTP ingress client only knows HTTP.
            // The protocol whitelist is the CLI-side complement to
            // the runtime check that would otherwise surface as a
            // confusing tool-call failure much later.
            val proto = u.getProtocol
            if (proto == null || proto.isEmpty || (proto != "http" && proto != "https"))
              Left(CliError.BadUrl("--ingress-url", value))
            else loop(rest, acc.copy(ingressUrl = value))
          } catch { case NonFatal(_) =>
            Left(CliError.BadUrl("--ingress-url", value))
          }
        case "--ingress-url" :: Nil => Left(CliError.MissingValue("--ingress-url"))
        case "--request-timeout" :: value :: rest =>
          // Per C5-arch-H1: this previously parsed as ofMillis while the
          // default (line 125) is ofSeconds(30) and sm8-mcp's equivalent
          // (sm8-mcp/.../Main.scala:117) uses ofSeconds. Operator passing
          // "--request-timeout 30" would get a 30-MILLISECOND HTTP timeout
          // and every tool call would fail. Units MUST match the default.
          try loop(rest, acc.copy(requestTimeout = java.time.Duration.ofSeconds(value.toLong)))
          catch { case _: NumberFormatException => Left(CliError.BadInt("--request-timeout", value)) }
        case "--request-timeout" :: Nil => Left(CliError.MissingValue("--request-timeout"))
        case "--skip-ingress-probe" :: Nil =>
          loop(Nil, acc.copy(skipIngressProbe = true))
        case "--skip-ingress-probe" :: _ =>
          Left(CliError.UnknownFlag("--skip-ingress-probe (takes no value)"))
        case other :: _ => Left(CliError.UnknownFlag(other))
      }
    // Per the stdio design §Mutex precedence (symmetric): reject
    // when BOTH --mcp-transport stdio AND --mcp-http-port > 0 are
    // set, regardless of argv order. The check is at end-of-parse
    // (post-loop) so it sees both flags no matter which the user
    // wrote first.
    loop(args, CliArgs(modelPath = None)).flatMap { cli =>
      if (cli.mcpTransport.isDefined && cli.mcpHttpPort > 0)
        Left(CliError.MutuallyExclusive("--mcp-transport stdio", "--mcp-http-port"))
      else Right(cli)
    }
  }

  /** Discover providers via ServiceLoader. Driver-side, once at boot.
    *
    * Per scala-impact-analysismindset: additive mechanism, local
    * to this entry point — does NOT touch the SDK Portal (which
    * discovers Plugins, a different extension type per plugins.md).
    */
  def discoverProviders(classLoader: ClassLoader): List[EngineProvider] = {
    import scala.jdk.CollectionConverters._
    ServiceLoader.load(classOf[EngineProvider], classLoader)
      .iterator().asScala.toList
  }

  /** Wire model + registry + transport WITHOUT starting the server.
    * Pure construction — unit-testable without binding a socket. */
  /** Realize a discovered provider against a connector URL. Legacy
    * 2-arg signature preserved for backward compat with MainSpec.
    *
    * Per the layering RFC §3 + the deployment-shape design: the deployment holds only the URL string.
    * For each discovered provider, `realize(url)` is invoked;
    * `Some(p)` replaces the stub, `None` keeps the stub as-is.
    * Available providers are kept as-is (already realized).
    */
  def realize(
      providers:    List[EngineProvider],
      connectorUrl: Option[String]
  ): List[EngineProvider] = providers.map { p =>
    if (p.available) p
    else connectorUrl match {
      case Some(url) => p.realize(url).getOrElse(p)
      case None      => p
    }
  }

  /** the typed-error realize. Returns `List[Either[EngineError,
    * EngineProvider]]` per provider.
    *
    * Per [[scala-error-handling-mindset]] §1 + the error-handling design §C1: every
    * provider gets a typed result. Replaces the legacy 2-arg
    * `realize(providers, connectorUrl)` (which silently downgraded
    * to stubs when a connector URL was given); see audit findings
    * C1 (audit 2026-08-27 @ becaaec) for the drift that motivated
    * this typed contract.
    */
  def realize(
      classLoader: ClassLoader,
      providers:   List[EngineProvider],
      engineName:  String,
      rawUrl:      Option[String]
  ): List[Either[EngineError, EngineProvider]] = rawUrl match {
    case None        => providers.map(Right(_))
    case Some(_) if engineName.isEmpty =>
      providers.map(p => if (p.available) Right(p) else Left(EngineError.ConnectionFailed(
        engine = p.identity.name,
        reason = "URL provided without --engine",
        message = s"sm8: engine '${p.identity.name}': --connector-url requires --engine <name>"
      )))
    case Some(url)   => EngineLoader.discoverAndRealize(classLoader, engineName, Some(url))
  }

  def wire(
      model:        Model,
      providers:    List[EngineProvider],
      engineName:   Option[String],
      connectorUrl: Option[String] = None,
      plugins:      Seq[io.sm8.sdk.Plugin] = Nil,
      metaInspectorEngineFn: Option[() => Map[String, Any]] = None,
      registryInspectorFn: Option[io.sm8.platform.query.RegistrySources] = None,
  ): Either[String, (EngineRegistry, HttpTransport, List[EngineProvider])] = {
    // Per the audit (2026-08-27 [C1]): use the TYPED 5-arg realize so
    // engine-realization failures surface as `EngineError.ConnectionFailed`
    // or `EngineError.EngineUnavailable` at boot (URL parser failures map
    // to `EngineUnavailable` — see `EngineUrlParser.lookup`), NOT as the
    // silent generic "no EngineProvider discovered."
    // The classloader is passed in (instead of `Thread.currentThread`)
    // so MainSpec can inject a deterministic loader.
    val realizedEither: List[Either[EngineError, EngineProvider]] =
      realize(
        classLoader = Thread.currentThread().getContextClassLoader,
        providers   = providers,
        engineName  = engineName.getOrElse(""),
        rawUrl      = connectorUrl
      )
    val realized: List[EngineProvider] = realizedEither.collect { case Right(p) => p }
    val typedErrors: List[EngineError] = realizedEither.collect { case Left(e) => e }

    if (realized.filter(_.available).isEmpty) {
      // Surface the FIRST typed error if realization produced any;
      // fall back to the legacy "no EngineProvider discovered" message
      // only if no providers were even attempted (empty providers list
      // from the caller). This restores the typed-error story that
      // the error-handling design §C1 promised.
      typedErrors.headOption match {
        case Some(err) =>
          Left(s"sm8: engine realization failed: ${err.message}")
        case None =>
          Left("sm8: no EngineProvider discovered (add a connector JAR " +
            "with META-INF/services/io.sm8.core.engine.EngineProvider)")
      }
    } else {
      val available = realized.filter(_.available)
      val engines: Map[String, EngineProvider] =
        available.map(p => p.identity.name -> p).toMap
      val default = engineName.getOrElse(available.map(_.identity.name).sorted.head)
      if (!engines.contains(default))
        Left(s"sm8: engine '$default' not discovered (available: ${engines.keys.toList.sorted.mkString(", ")})")
      else
        try {
          val registry = EngineRegistry(engines, default)
          Right((registry, HttpTransport(
            model,
            registry,
            io.sm8.core.cache.ResultCache.NoOp,
            plugins,
            metaInspectorEngineFn,
            registryInspectorFn
          ), realized))
        } catch {
          case e: IllegalArgumentException => Left(e.getMessage)
        }
    }
  }

  /**
   * Probe the Restate ingress URL with a short-timeout HEAD request
   * and print a warning to stderr if it is not reachable. Without
   * this, an operator who misconfigures `--ingress-url` would only
   * learn via silent `isError=true` on every `tools/call` — a
   * failure mode that can take hours to diagnose.
   *
   * The probe is best-effort: a failure does NOT abort startup,
   * because in some deployment shapes the ingress is co-located with
   * sm8-server and may bind a fraction of a second later.
   *
   * Note on response classification: the Restate ingress replies to
   * a bare `HEAD /` with a non-2xx status (typically 404 — the
   * EndpointRequestHandler only matches `/invoke/Service/Handler`,
   * `/discover`, or `/health`; a proxy may answer 405). So any HTTP
   * response at all — 200, 404, 405, 500 — is treated as "server
   * accepted the TCP connection and an HTTP-speaking process
   * answered". We warn ONLY on connect-level failures (refused,
   * DNS, timeout). This means the probe does NOT detect "Restate up
   * but misrouted", but it also does NOT false-positive on every
   * healthy boot (which a stricter "warn on 4xx/5xx" rule would do).
   * The trade-off is documented in the ADR §Deployment shape section.
   *
   * @param ingressUrl the URL to probe; the path is irrelevant — we
   *                   hit the host root
   */
  // Package-private (private[server]) so MainSpec can exercise the
  // probe directly without spawning a subprocess (per pr267-arch-002:
  // unit-test coverage of the warning paths). Callers within the same
  // file (the stdio boot path) invoke it unchanged.
  private[server] def probeIngressOrWarn(ingressUrl: String): Unit = {
    val probeTimeout = java.time.Duration.ofSeconds(3)
    val probeClient = java.net.http.HttpClient.newBuilder()
      .connectTimeout(probeTimeout)
      .build()
    val probeReq = java.net.http.HttpRequest.newBuilder(
      java.net.URI.create(ingressUrl)
    )
      .timeout(probeTimeout)
      .method("HEAD", java.net.http.HttpRequest.BodyPublishers.noBody())
      .build()
    try {
      val resp = probeClient.send(probeReq, java.net.http.HttpResponse.BodyHandlers.discarding())
      // A 4xx response is the EXPECTED status from a healthy Restate
      // ingress (bare HEAD / matches no handler path). We warn only on
      // 5xx (server unhealthy); 2xx/3xx/4xx are treated as "reachable".
      val status = resp.statusCode()
      if (status >= 500) {
        System.err.println(s"sm8: WARNING — ingress reachable but returned $status: $ingressUrl (tools/call may fail)")
      }
    } catch {
      case NonFatal(e) =>
        System.err.println(s"sm8: WARNING — ingress unreachable: $ingressUrl (${e.getClass.getSimpleName}: ${e.getMessage}); tools/call will fail until the ingress is up")
    }
  }

  /** Entry point. `main` delegates here (pattern from sm8-cli Main:
    * pure + testable, returns an exit code instead of sys.exit). */
  def run(args: List[String]): Int = {
    if (args.contains("--help") || args.contains("-h") || args.isEmpty) {
      // Per C5-arch-L4: --help must go to stderr so launching
      // `sm8-server --help` under `--mcp-transport stdio` (e.g.
      // Claude Desktop spawning us to discover the tool list) does
      // NOT corrupt the stdio JSON-RPC stream. The 4 startup banners
      // were correctly redirected in PR-264; --help was missed.
      System.err.println(Usage); return if (args.isEmpty) 2 else 0
    }
    parseArgs(args) match {
      case Left(err) =>
        System.err.println(err.reason); 2
      case Right(cli) if cli.modelPath.isEmpty =>
        // --model was absent from the arg list; parseArgs leaves
        // modelPath = None and run() surfaces the typed MissingFlag.
        // This is a CLI usage error (exit 2), not a model-load failure
        // (exit 1) — the operator forgot a flag, not a malformed
        // manifest.
        System.err.println(CliError.MissingFlag("--model").reason); 2
      case Right(cli) =>
        PlatformModelLoader.fromPath(cli.modelPath.get) match {
          case Left(modelErr) =>
            System.err.println(s"sm8: model load failed: ${modelErr.toString}"); 1
          case Right(model) =>
            // Composite root (the layering RFC §11a): load plugins via the ServiceLoader
            // portal + append the deployment-local MetaCaptureObserver so
            // the MetaInspectorService serves the most recent request's
            // Context.meta. The plugins are threaded into HttpTransport,
            // which registers them on QueryService.definition's dispatcher.
            val latestMeta: java.util.concurrent.atomic.AtomicReference[Map[String, Any]] =
              new java.util.concurrent.atomic.AtomicReference[Map[String, Any]](Map.empty)
            // Per the audit (2026-08-27 [C2]): use the
            // `PluginDiscovery` factory (sm8-core) for plugin
            // discovery. sm8-server is the deployment layer; depending
            // on the concrete `EngineImpl` class violates the layer
            // discipline of `semantic-layer-engine-architecture.md` §3
            // (Core Boundary). The factory is the inward-facing seam
            // that insulates this deployment wiring from future
            // refactors of `EngineImpl`. Post-PR-272 the Engine is
            // constructed via `EngineFactory.create(plugins)`
            // (sm8-platform); this site only does the discovery.
            // Post-PR-273 / #286 sm8-core is filesystem-IO-free; the
            // `discoverFromConfig()` path reads the `sm8.plugins.allowed`
            // resource via JDK `BufferedReader`, not `scala.io.Source`.
            // Per the metrics-shim (= a prior PR): wire the MetricsSink
            // BEFORE PluginDiscovery so the cache plugin's hit/miss
            // handlers see the registered QueryMetrics (and tick the
            // counters). If not registered, the trait defaults to NoOp
            // so this is safe in tests and other deployments.
            io.sm8.core.cache.MetricsRegistry.register(
              io.sm8.platform.query.QueryMetrics
            )
            val discovered: List[io.sm8.sdk.Plugin] =
              io.sm8.core.PluginDiscovery.discoverFromConfig()
            val plugins: List[io.sm8.sdk.Plugin] =
              discovered :+ new MetaCaptureObserver(latestMeta)
            // ADDITIVE in C10-PR-C: build a 2nd engine instance from
            // the same plugins list, for the registry inspector
            // (`listPlugins` / `listHooks`). `EngineFactory.create`
            // registers every plugin via `use(plugin)` at construction,
            // so this parallel engine's HookManager mirrors the live
            // engine's registry at boot (same plugin instances, same
            // setup calls). Read-only diagnostic surface.
            val registryEngine: io.sm8.sdk.Engine =
              io.sm8.core.EngineFactory.create(plugins)
            val registrySources: io.sm8.platform.query.RegistrySources =
              io.sm8.platform.query.RegistrySources(
                hooksFn = () => registryEngine.hooks.listAllHooks(),
                pluginsFn = () =>
                  discovered.map { p =>
                    p -> io.sm8.sdk.SetupStatus.Registered(p.name)
                  }
              )
            wire(
              model,
              discoverProviders(Thread.currentThread().getContextClassLoader),
              cli.engine,
              cli.connectorUrl,
              plugins,
              Some(() => latestMeta.get()),
              Some(registrySources)
            ) match {
              case Left(bootErr) =>
                System.err.println(bootErr); 3
              case Right((_, transport, realized)) =>
                // a prior PR (audit 2026-08-30 L4): install the JVM
                // shutdown hook BEFORE transport.start() so SIGTERM
                // arriving during start() or before this point still
                // triggers cleanup. Previously the hook was installed
                // AFTER start(), leaving a race window where the
                // socket stayed bound (TIME_WAIT ~60s) and the
                // SparkSession was orphaned (no close() invocation).
                // Idempotency: HttpTransport.stop() no-ops when
                // `server.isDefined == false`; EngineProvider.close()
                // is a no-op default for non-Spark connectors.
                installShutdownHook(transport, realized)
                // Per C5-de-M2: when --mcp-transport stdio is set,
                // the stdio MCP path does NOT need the Restate HTTP
                // ingress. Tool calls go via a separate plain JDK
                // HttpClient (HttpIngressClient.Impl). Starting the
                // HTTP transport here would bind an unnecessary port
                // (which could conflict with `--port` or `--metrics-port`)
                // and a port-bind failure would kill the stdio MCP
                // before it even starts. Skip transport.start when
                // stdio mode is enabled; the transport still gets
                // cleaned up via installShutdownHook on JVM exit.
                // C5-r2-de-L1: `.contains("stdio")` is intentional
                // rather than `.isDefined` — parseArgs rejects every
                // other value today, but the explicit comparison
                // keeps the invariant LOCAL to this check (a future
                // parseArgs relaxation cannot silently route a new
                // transport value through the stdio boot path).
                val stdioMode = cli.mcpTransport.contains("stdio")
                val boundPort: Int =
                  if (stdioMode) {
                    System.err.println(s"sm8: stdio MCP transport mode (model=${model.name}, version=${model.version}); skipping HTTP ingress bind")
                    // C5-r2-de-L4: no "server listening on port"
                    // banner in stdio mode — nothing is listening on
                    // a port; the skip banner above is the truth. The
                    // smoke asserts on 'sm8:.*listening on port' in
                    // stderr, which the metrics banner still provides
                    // (metrics stays bound when metricsPort > 0).
                    -1
                  } else {
                    try transport.start(cli.port)
                    catch {
                      case e: IllegalStateException =>
                        System.err.println(s"sm8: ${e.getMessage}"); return 3
                    }
                  }
                if (!stdioMode) {
                  System.err.println(s"sm8: server listening on port $boundPort " +
                    s"(model=${model.name}, version=${model.version})")
                }
                // Standalone Prometheus metrics HttpServer on a
                // SEPARATE port (`--metrics-port`, default 9090),
                // per `docs/adr/0012-b-export-prometheus-metrics.md`.
                // Per [[scala-jvm-safety-mindset]], install a SECOND
                // shutdown hook BEFORE MetricsHttpRoute.start() so a
                // SIGTERM arriving during bind() still closes the
                // socket — same ordering invariant as the a prior PR
                // hook (the pre-existing hook for transport +
                // providers is preserved unchanged). The slot is an
                // AtomicReference so the hook (installed first) can
                // observe the server handle (assigned after bind).
                //
                // Bind failure is fail-LOUD on stderr but does NOT
                // abort the process: the Restate ingress is already
                // up and useful without observability.
                //
                // The startedAt Instant is shared with MetricsService
                // so the /metrics body's uptime matches the
                // MetricsService/snapshot handler exactly.
                val metricsSlot =
                  new java.util.concurrent.atomic.AtomicReference[io.vertx.core.http.HttpServer]()
                val metricsHook = new Thread(
                  new Runnable {
                    def run(): Unit = {
                      val ms = metricsSlot.get()
                      if (ms != null) MetricsHttpRoute.stop(ms)
                    }
                  },
                  "sm8-metrics-shutdown-hook"
                )
                Runtime.getRuntime().addShutdownHook(metricsHook)
                try {
                  metricsSlot.set(MetricsHttpRoute.start(cli.metricsPort,
                    io.sm8.platform.query.MetricsService.startedAtInstant))
                  System.err.println(s"sm8: metrics endpoint listening on port ${cli.metricsPort}")
                } catch {
                  case e: IllegalStateException =>
                    System.err.println(s"sm8: ${e.getMessage} — continuing without metrics")
                }

                // a prior PR: Streamable HTTP MCP transport (per the HTTP-MCP design).
                // Bound only if --mcp-http-port is non-zero. The same
                // shutdown-hook-before-bind + AtomicReference slot
                // pattern as MetricsHttpRoute. The 5 MCP tools
                // (a prior PR) are wired via the in-process stdio
                // transport (see below) and ALSO via this Streamable
                // HTTP transport (the stdio transport follow-up); both share the
                // same `Sm8ToolHandlers` factory in sm8-platform.
                if (cli.mcpHttpPort > 0) {
                  try {
                    val tools = io.sm8.platform.mcp.Sm8ToolHandlers.build(
                      new io.sm8.platform.mcp.HttpIngressClient.Impl(
                        cli.ingressUrl, cli.requestTimeout
                      )
                    )
                    val (mcpHttpServer, mcpSyncServer, _) = io.sm8.platform.query.McpHttpRoute.start(
                      cli.mcpHttpPort,
                      io.sm8.platform.query.McpHttpRoute.Config(
                        endpointPath   = cli.mcpHttpEndpoint,
                        disallowDelete = cli.mcpHttpDisallowDelete
                      ),
                      "sm8", "0.1.0-SNAPSHOT", tools
                    )
                    System.err.println(s"sm8: MCP Streamable HTTP endpoint listening on port ${cli.mcpHttpPort} (path ${cli.mcpHttpEndpoint})")
                    // Stop hook (separate from metrics/Restate hooks;
                    // JVM hook order is not guaranteed, but each hook
                    // is independent).
                    Runtime.getRuntime().addShutdownHook(new Thread(
                      new Runnable {
                        def run(): Unit =
                          io.sm8.platform.query.McpHttpRoute.stop(mcpHttpServer, mcpSyncServer)
                      },
                      "sm8-mcp-http-shutdown"
                    ))
                  } catch {
                    case e: IllegalStateException =>
                      System.err.println(s"sm8: ${e.getMessage} — continuing without MCP HTTP")
                  }
                }

                // In-process stdio MCP transport (per the stdio design).
                // Bound only if --mcp-transport is Some (mutually
                // exclusive with --mcp-http-port > 0; parseArgs
                // already rejects that combination). The stdio
                // transport reuses the same Sm8ToolHandlers factory
                // (5 a prior PR tools). The main thread blocks on
                // a CountDownLatch that the SDK's
                // `closeGracefully()` completes on EOF (per the
                // stdio design §Wiring — Thread.join() never wakes
                // on EOF because EOF doesn't invoke JVM shutdown
                // hooks). The Process exits naturally when the MCP
                // session closes.
                cli.mcpTransport match {
                  case Some("stdio") =>
                    try {
                      val client = new io.sm8.platform.mcp.HttpIngressClient.Impl(
                        cli.ingressUrl, cli.requestTimeout
                      )
                      // Probe the ingress with a short-timeout HEAD so
                      // operators learn about misconfiguration at boot
                      // rather than on the first tools/call. Failures
                      // are a warning, not a fatal — stdio MCP itself
                      // works; the ingress may be co-located with
                      // sm8-server and start a beat later in some
                      // deployment shapes.
                      // Per --skip-ingress-probe:
                      // operators prioritizing fast cold-start disable the
                      // probe entirely; default false preserves the
                      // diagnostic on every healthy boot.
                      if (!cli.skipIngressProbe)
                        probeIngressOrWarn(cli.ingressUrl)
                      val tools = io.sm8.platform.mcp.Sm8ToolHandlers.build(client)
                      val stdio = io.sm8.platform.mcp.McpStdioRoute(
                        "sm8", "0.1.0-SNAPSHOT", tools
                      )
                      stdio.buildServer()
                      // Per C5-de-H1: install a JVM shutdown hook that
                      // wakes the stdio close latch so SIGTERM during
                      // `awaitClose(timeoutSeconds=30)` exits within
                      // milliseconds instead of blocking the full 30s.
                      // CountDownLatch.await is NOT interruptible, and
                      // without this hook the JVM would wait for the
                      // timeout before exiting. The HTTP transport path
                      // got its equivalent hook at line ~688; this
                      // mirrors the pattern for the stdio path.
                      Runtime.getRuntime().addShutdownHook(new Thread(
                        new Runnable {
                          def run(): Unit = {
                            // Wake the latch so awaitClose returns;
                            // then dispose the SDK executors.
                            stdio.signalClose()
                            try stdio.stop()
                            catch { case NonFatal(_) => () }
                          }
                        },
                        "sm8-stdio-shutdown"
                      ))
                      // The SDK's inbound loop calls
                      // `session.close()` on stdin EOF, which counts
                      // down our latch. After awaitClose returns we
                      // give the SDK's outbound scheduler (running on
                      // a dedicated executor) time to drain the last
                      // JSON-RPC frames, then exit the JVM — non-daemon
                      // threads (Vert.x / Restate ingress) would keep
                      // the JVM alive forever otherwise.
                      val ok = stdio.awaitClose(timeoutSeconds = 30)
                      if (!ok) {
                        System.err.println("sm8: MCP stdio transport did not close within 30s; forcing exit")
                      }
                      // Per C5-de-L2: extract magic 500ms to a named
                      // constant. The intent is to give the SDK's
                      // outbound scheduler (single-thread executor)
                      // time to drain the last JSON-RPC frame before
                      // System.exit(0). Belt-and-suspenders in
                      // production but important for tests where the
                      // subprocess reads stdout before EOF.
                      Thread.sleep(StdioDrainDelayMs)
                      System.exit(0)
                      0
                    } catch {
                      case NonFatal(e) =>
                        System.err.println(s"sm8: ${e.getClass.getSimpleName}: ${e.getMessage}")
                        return 1
                    }
                  case Some(other) =>
                    System.err.println(s"sm8: unknown --mcp-transport value: $other (expected 'stdio')")
                    return 2
                  case None =>
                    // No in-process stdio MCP; just block the main thread.
                }

                // Block the main thread; the shutdown hooks stop the servers.
                Thread.currentThread().join()
                0
            }
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val exit = run(args.toList)
    if (exit != 0) sys.exit(exit)
    // exit 0 path: main thread was interrupted (JVM shutting down) —
    // let the JVM exit naturally so the shutdown hook completes.
  }

  /**
   * Install the JVM shutdown hook that releases the HTTP transport
   * + closes all realized engine providers on JVM exit.
   *
   * Per the shutdown-hook design: the hook releases BOTH the socket AND any
   * realized engine providers. A SIGTERM without this hook leaves
   * the cluster's executor processes orphaned.
   *
   * Per a prior PR (audit 2026-08-30 L4): this is registered BEFORE
   * `transport.start()` from `run()` so SIGTERM during start() or
   * before this point still triggers cleanup. The pre-a prior PR
   * ordering (`transport.start → sys.addShutdownHook`) exposed a
   * race window where a SIGTERM arriving between the two calls
   * orphaned the resources.
   *
   * Exception propagation: `run()` catches `IllegalStateException`
   * from `transport.start` only. A `TimeoutException` from the
   * 30 s bind future (see [[io.sm8.platform.query.HttpTransport.start]])
   * or an NPE from `RestateHttpServer.fromEndpoint` propagates to
   * `main()`. This is intentional: the hook is registered, so JVM-exit
   * cleanup still fires even when the exception escapes `run()`.
   * `transport.stop()` is a no-op on an unstarted transport (see
   * [[io.sm8.platform.query.HttpTransport.stop]]), so the hook body
   * does not throw on a partial boot.
   *
   * Idempotency guarantees:
   *  - `HttpTransport.stop()` no-ops when `server.isDefined == false`
   *    (never started or already stopped); safe to call even if
   *    `start()` never ran.
   *  - `EngineProvider.close()` is a no-op default for non-Spark
   *    connectors (in-memory, trino, etc.); safe to call on every
   *    realized provider regardless of realized-state.
   *
   * Exposed (not `private`) so MainSpec can verify the hook is
   * registered with the JVM regardless of `transport.start` outcome.
   *
   * @return the registered hook thread (caller may pass it to
   *         `Runtime.getRuntime.removeShutdownHook` for test cleanup)
   */
  def installShutdownHook(
      transport: HttpTransport,
      realized: List[EngineProvider]
  ): Thread = {
    val hook = new Thread(
      new Runnable {
        def run(): Unit = {
          transport.stop()
          realized.foreach(_.close())
        }
      },
      "sm8-shutdown-hook"
    )
    Runtime.getRuntime().addShutdownHook(hook)
    hook
  }
}
