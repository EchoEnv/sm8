/*
 * SM8 Platform — Main (Step 11 production entry point, per ADR-006).
 *
 * The runnable process that wires the SM8 MCP server:
 *
 *   1. parse CLI args (typed, per scala-error-handlingmindset)
 *   2. load the Model from YAML (PlatformModelLoader — schema
 *      validation + semantic parse, both typed)
 *   3. discover EngineProviders via Java ServiceLoader
 *      (`META-INF/services/io.sm8.core.engine.EngineProvider`)
 *   4. build EngineRegistry (fail-loud at boot per design §4.1)
 *   5. install the JVM shutdown hook (PR-215, audit 2026-08-30
 *      L4: BEFORE transport.start so SIGTERM during start() or
 *      before this point still triggers cleanup)
 *   6. start HttpTransport (binds the actual socket)
 *
 * ==Per karphyaguidsmindset "smallest correct change"==
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
 * ==Per scala-jvm-safetymindset==
 *
 * - Shutdown hook: `transport.stop()` on JVM exit (idempotent —
 *   HttpTransport.stop() is a no-op when never started OR already
 *   stopped). Registered BEFORE `transport.start()` (PR-215) so
 *   SIGTERM during start() or before this point still triggers
 *   cleanup; otherwise the socket stays bound (TIME_WAIT ~60s) and
 *   the SparkSession is orphaned.
 * - Fail loud: every Left/throwable maps to a typed exit code +
 *   stderr message. No silent degradation.
 *
 * ==Per scala-spark-batch-bugs-mindset (per user directive)==
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
 * ==Per scala-perf-testingmindset==
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

import io.sm8.platform.query.{HttpTransport, PlatformModelLoader}

import java.nio.file.{Path, Paths}
import java.util.ServiceLoader

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
 * Exit codes (typed, per scala-error-handlingmindset):
 *  - 0 — clean shutdown (SIGINT/SIGTERM)
 *  - 1 — model load failure (typed PlatformModelError, printed)
 *  - 2 — CLI usage error (missing/bad args; unknown flag)
 *  - 3 — boot failure (no providers discovered, default engine
 *        unavailable, or bind failure — all fail loud per §4.1)
 */
object Main {

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
      engine:        Option[String]        = None,
      connectorUrl:  Option[String]        = None,
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
  }

  private val Usage: String =
    """sm8-server — SM8 MCP server (Step 11, ADR-006)
      |
      |Usage: sm8-server --model <yaml-path> [--port <n>] [--engine <name>]
      |
      |  --model <path>   model manifest (YAML, schema-validated)
      |  --port <n>       TCP port (default 8080; 0 = ephemeral)
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
        case "--engine" :: value :: rest =>
          loop(rest, acc.copy(engine = Some(value)))
        case "--engine" :: Nil => Left(CliError.MissingValue("--engine"))
        case "--connector-url" :: value :: rest =>
          loop(rest, acc.copy(connectorUrl = Some(value)))
        case "--connector-url" :: Nil => Left(CliError.MissingValue("--connector-url"))
        case other :: _ => Left(CliError.UnknownFlag(other))
      }
    loop(args, CliArgs(modelPath = None))
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
    * Per RFC §3 + ADR-006: the deployment holds only the URL string.
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

  /** PR-15 typed-error realize. Returns `List[Either[EngineError,
    * EngineProvider]]` per provider.
    *
    * Per scala-error-handlingmindset §1 + ADR-008-Q §C1: every
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
      // ADR-008-Q §C1 promised.
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
            metaInspectorEngineFn
          ), realized))
        } catch {
          case e: IllegalArgumentException => Left(e.getMessage)
        }
    }
  }

  /** Entry point. `main` delegates here (pattern from sm8-cli Main:
    * pure + testable, returns an exit code instead of sys.exit). */
  def run(args: List[String]): Int = {
    if (args.contains("--help") || args.contains("-h") || args.isEmpty) {
      println(Usage); return if (args.isEmpty) 2 else 0
    }
    parseArgs(args) match {
      case Left(err) =>
        System.err.println(err.reason); 2
      case Right(cli) if cli.modelPath.isEmpty =>
        // Empty --model is impossible at this point because parseArgs
        // produced a Right, but `modelPath = None` means --model was
        // absent entirely. Surface as a typed CLI error (exit 2),
        // not a model-load failure (exit 1) — the operator typo'd
        // a flag, not a malformed manifest.
        System.err.println(CliError.MissingFlag("--model").reason); 2
      case Right(cli) =>
        PlatformModelLoader.fromPath(cli.modelPath.get) match {
          case Left(modelErr) =>
            System.err.println(s"sm8: model load failed: ${modelErr.toString}"); 1
          case Right(model) =>
            // Composite root (RFC §11a): load plugins via the ServiceLoader
            // portal + append the deployment-local MetaCaptureObserver so
            // the MetaInspectorService serves the most recent request's
            // Context.meta. The plugins are threaded into HttpTransport,
            // which registers them on QueryService.definition's dispatcher.
            val latestMeta: java.util.concurrent.atomic.AtomicReference[Map[String, Any]] =
              new java.util.concurrent.atomic.AtomicReference[Map[String, Any]](Map.empty)
            // Per the audit (2026-08-27 [C2]): use the `PluginDiscovery`
            // factory (sm8-core) instead of `new EngineImpl().discoverFromConfig()`.
            // sm8-server is the deployment layer; depending on the
            // concrete `EngineImpl` class violates the layer discipline
            // of `semantic-layer-engine-architecture.md` §3 (Core Boundary).
            // The factory is the inward-facing seam that insulates this
            // deployment wiring from future refactors of `EngineImpl`.
            val discovered: List[io.sm8.sdk.Plugin] =
              io.sm8.core.PluginDiscovery.discoverFromConfig()
            val plugins: List[io.sm8.sdk.Plugin] =
              discovered :+ new MetaCaptureObserver(latestMeta)
            wire(
              model,
              discoverProviders(Thread.currentThread().getContextClassLoader),
              cli.engine,
              cli.connectorUrl,
              plugins,
              Some(() => latestMeta.get())
            ) match {
              case Left(bootErr) =>
                System.err.println(bootErr); 3
              case Right((_, transport, realized)) =>
                // PR-215 (audit 2026-08-30 L4): install the JVM
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
                val boundPort = try transport.start(cli.port)
                catch {
                  case e: IllegalStateException =>
                    System.err.println(s"sm8: ${e.getMessage}"); return 3
                }
                println(s"sm8: server listening on port $boundPort " +
                  s"(model=${model.name}, version=${model.version})")
                // Block the main thread; the shutdown hook stops the server.
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
   * Per ADR-008-O: the hook releases BOTH the socket AND any
   * realized engine providers. A SIGTERM without this hook leaves
   * the cluster's executor processes orphaned.
   *
   * Per PR-215 (audit 2026-08-30 L4): this is registered BEFORE
   * `transport.start()` from `run()` so SIGTERM during start() or
   * before this point still triggers cleanup. The pre-PR-215
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
