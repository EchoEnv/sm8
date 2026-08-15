/*
 * SM8 Platform — Main (Step 11 production entry point, per ADR-006).
 *
 * The runnable process that wires the SM8 MCP server:
 *
 *   1. parse CLI args (typed, per scala-error-handlingmindset)
 *   2. load the Model from YAML (PlatformModelLoader — schema
 *      validation + semantic parse, both typed)
 *   3. discover MCPEngineProviders via Java ServiceLoader
 *      (`META-INF/services/io.sm8.core.engine.MCPEngineProvider`)
 *   4. build MCPEngineRegistry (fail-loud at boot per design §4.1)
 *   5. start HttpTransport (binds the actual socket)
 *   6. install a JVM shutdown hook (scala-jvm-safetymindset:
 *      release the socket on SIGTERM/SIGINT)
 *
 * ==Per [[karphyaguidsmindset]] "smallest correct change"==
 *
 * Pure composition. Every piece already exists: PlatformModelLoader,
 * MCPEngineRegistry, HttpTransport. Main adds NO engine logic.
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
 * ==Per [[scala-jvm-safetymindset]]==
 *
 * - Shutdown hook: `transport.stop()` on JVM exit (idempotent —
 *   HttpTransport.stop() is a no-op when already stopped).
 * - Fail loud: every Left/throwable maps to a typed exit code +
 *   stderr message. No silent degradation.
 *
 * ==Per [[scala-spark-batch-bugs-mindset]] (per user directive)==
 *
 * - mantra #1 (closure-safety): the wired `MCPEngineRegistry` is
 *   `Serializable` (verified by MainSpec round-trip + upstream
 *   `EngineServiceSpec` serializable-safe contract). Providers are
 *   instantiated on the DRIVER at boot — never shipped to executors.
 * - mantra #5 (driver/executor): Main runs in the driver process.
 *   A discovered `SparkEngineProvider` compiles + collects in the
 *   driver. No executor-side resources leak through Main.
 * - mantras #2/#3/#4: N/A — Main executes nothing itself; it is
 *   wiring only.
 *
 * ==Per [[scala-perf-testingmindset]]==
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
package io.sm8.platform.query

import io.sm8.core.engine.{EngineError, EngineIdentity, MCPEngineProvider, MCPEngineRegistry, MCPQueryRequest, PortableQueryResult}
import io.sm8.core.model.Model

import java.nio.file.{Path, Paths}
import java.util.ServiceLoader

/**
 * Production entry point for the SM8 MCP server.
 *
 * Usage:
 * {{{
 * java -cp ... io.sm8.platform.query.Main \
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

  /** CLI shape. Parsed once; pure data (scala-data-drivenrefactor). */
  final case class CliArgs(
      modelPath:     Path,
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
      |                      MCPEngineProvider on the classpath)
      |  --connector-url <u> optional connector URL (e.g.
      |                      'spark://host:7077', 'spark-connect://host:15002',
      |                      'local[*]'). When set, the platform asks
      |                      the discovered connector descriptor to
      |                      realize against the URL via its (String) ctor
      |                      (no spark types in the platform).
      |
      |Engines are discovered via META-INF/services/
      |io.sm8.core.engine.MCPEngineProvider (Java ServiceLoader).""".stripMargin

  /** Pure arg parser — fully unit-testable, no IO. */
  def parseArgs(args: List[String]): Either[CliError, CliArgs] = {
    def loop(remaining: List[String], acc: CliArgs): Either[CliError, CliArgs] =
      remaining match {
        case Nil => Right(acc)
        case "--help" :: _ | "-h" :: _ => Left(CliError.MissingFlag("--model")) // handled by run() before parse
        case "--model" :: value :: rest if value.startsWith("-") =>
          Left(CliError.MissingValue("--model"))
        case "--model" :: value :: rest =>
          loop(rest, acc.copy(modelPath = Paths.get(value)))
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
    loop(args, CliArgs(modelPath = Paths.get("")))
  }

  /** Discover providers via ServiceLoader. Driver-side, once at boot.
    *
    * Per [[scala-impact-analysismindset]]: additive mechanism, local
    * to this entry point — does NOT touch the SDK Portal (which
    * discovers Plugins, a different extension type per plugins.md).
    */
  def discoverProviders(classLoader: ClassLoader): List[MCPEngineProvider] = {
    import scala.jdk.CollectionConverters._
    ServiceLoader.load(classOf[MCPEngineProvider], classLoader)
      .iterator().asScala.toList
  }

  /** Wire model + registry + transport WITHOUT starting the server.
    * Pure construction — unit-testable without binding a socket. */
  /**
    * Realize a discovered provider against a URL by reflection.
    *
    * Per RFC §3 + the user's "no spark types in the platform"
    * directive: the platform holds ONLY a string. For each
    * discovered provider that is not available (i.e. the
    * contract-gap stub from the connector's no-arg ctor), look
    * for a `(String) ctor` on the class. If found, instantiate
    * with the URL. The connector's (String) ctor builds the
    * real SparkSession (or TrinoClient, etc.) — the platform
    * never imports the connector class directly.
    *
    * Future connectors that support a URL realization (Trino URL,
    * DuckDB path, etc.) just need a `(String) ctor` — no platform
    * change.
    */
  def realize(
      providers:    List[MCPEngineProvider],
      connectorUrl: Option[String]
  ): List[MCPEngineProvider] = connectorUrl match {
    case None => providers
    case Some(url) =>
      providers.map { p =>
        if (p.available) p
        else {
          val cls = p.getClass
          try {
            val ctor = cls.getConstructor(classOf[String])
            val instance = ctor.newInstance(url).asInstanceOf[MCPEngineProvider]
            instance
          } catch {
            case _: NoSuchMethodException =>
              // Connector has no (String) ctor — keep the stub.
              // The platform will fail loud at wire() if no available
              // provider is left.
              p
          }
        }
      }
  }

  def wire(
      model:        Model,
      providers:    List[MCPEngineProvider],
      engineName:   Option[String],
      connectorUrl: Option[String] = None,
  ): Either[String, (MCPEngineRegistry, HttpTransport)] = {
    val realized = realize(providers, connectorUrl)
    val available = realized.filter(_.available)
    if (available.isEmpty)
      Left("sm8: no MCPEngineProvider discovered (add a connector JAR " +
        "with META-INF/services/io.sm8.core.engine.MCPEngineProvider)")
    else {
      val engines: Map[String, MCPEngineProvider] =
        available.map(p => p.identity.name -> p).toMap
      val default = engineName.getOrElse(available.map(_.identity.name).sorted.head)
      if (!engines.contains(default))
        Left(s"sm8: engine '$default' not discovered (available: ${engines.keys.toList.sorted.mkString(", ")})")
      else
        try {
          val registry = MCPEngineRegistry(engines, default)
          Right((registry, HttpTransport(model, registry)))
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
      case Right(cli) =>
        PlatformModelLoader.fromPath(cli.modelPath) match {
          case Left(modelErr) =>
            System.err.println(s"sm8: model load failed: ${modelErr.toString}"); 1
          case Right(model) =>
            wire(model, discoverProviders(Thread.currentThread().getContextClassLoader), cli.engine, cli.connectorUrl) match {
              case Left(bootErr) =>
                System.err.println(bootErr); 3
              case Right((_, transport)) =>
                val boundPort = try transport.start(cli.port)
                catch {
                  case e: IllegalStateException =>
                    System.err.println(s"sm8: ${e.getMessage}"); return 3
                }
                println(s"sm8: server listening on port $boundPort " +
                  s"(model=${model.name}, version=${model.version})")
                // scala-jvm-safetymindset: release the socket on exit.
                sys.addShutdownHook { transport.stop() }
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
}
