/*
 * SM8 Spark Connector — GateBTraceRunner (ADR-0029 §Gate B item 3,
 * amended 2026-09-09: operator enablement instrumentation; ADR-0030 §D1
 * experiment harness).
 *
 * A thin cron-friendly wrapper around RollupRefreshCostProbe.run that:
 *   - parses --model (accepted and recorded in the output for
 *     forward-compat; HONEST LIMITATION: RollupRefreshCostProbe.run
 *     currently measures its DOCUMENTED SYNTHETIC FIXTURE, not the
 *     named production model — wiring the probe against a live
 *     registered model is future work, see ADR-0029 §Gate B item 3's
 *     "representative model" clause. The instrumentation shape and
 *     the metric definitions are production-ready; the data source
 *     is not yet)
 *   - parses --out-path (file path for the JSON report; required)
 *   - builds a fresh SparkSession with an embedded HadoopCatalog
 *     (the ADR-0028 minimum viable catalog; production swaps this for
 *     a real catalog via cluster-level config)
 *   - invokes RollupRefreshCostProbe.run
 *   - persists the GateBReport as JSON to --out-path (so the trace
 *     accumulation is durable and cron-attestable, not just printed)
 *   - exits 0 on success, non-zero on probe failure (machine-parseable)
 *
 * Why hand-rolled JSON and not play-json/circe/macros:
 *   - GateBReport fields are flat scalars + nested case classes
 *   - Jackson is already on the classpath (sm8-core transitive dep)
 *   - one small ObjectMapper beats a whole JSON library for 8 fields
 *
 * Closure safety: same contract as RollupRefreshCostProbe — driver-side
 * only, no user-code ships to executors.
 *
 * ==Manifest validation boundary (deliberate bypass)==
 *
 * Live-model mode loads the YAML via
 * `io.sm8.core.manifest.ModelLoader.fromStream` DIRECTLY (see the
 * `case Some(yamlPath)` branch of the run dispatch — the only
 * loader call site in this file) — it does NOT go through
 * `io.sm8.core.manifest.ManifestValidator`, and therefore skips the
 * JSON-Schema gate that the production server path enforces
 * (`sm8-platform` `PlatformModelLoader.validateAndLoad` runs
 * validator-first, then the loader).
 *
 * This is deliberate, for a standalone-CLI research tool:
 *   - The runner is an operator-driven one-shot CLI (cron-attestable
 *     trace collection), not a long-lived service. The manifest is
 *     the operator's own input file; a malformed one fails loudly at
 *     the loader's typed `ManifestError` (parse/shape errors still
 *     surface — the schema gate is what is skipped).
 *   - The schema evolves independently (it is a classpath resource
 *     amended in place — see the v2 schema description); pinning this
 *     CLI to schema-validity would break older staged manifests
 *     mid-experiment.
 *
 * Consequence: a manifest that the schema would REJECT (unknown
 * top-level block, unknown dimension `type` label, bad join `kind`
 * casing) can still load here if the loader accepts it. The
 * production server (sm8-server Main → PlatformModelLoader) WILL
 * reject the same file. `GateBTraceRunnerSpec` pins this asymmetry
 * with a schema-invalid-but-loader-valid manifest.
 */
package io.sm8.connectors.spark

import java.nio.file.{Files, Path, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature

import scala.collection.JavaConverters._

import scala.util.control.NonFatal

object GateBTraceRunner {

  /** Run a single trace collection cycle.
    *
    * @param args "--model <name> --out-path <file>" (both required).
    *             "--bootstrap-warehouse <dir>" optional (default: a
    *             temp dir cleaned up at end; useful for the test, not
    *             for production where the catalog is already
    *             configured cluster-side).
    */
  /** Run the trace collection. Returns the exit code (0 success,
    * 1 probe failure, 2 bad arguments, 3 memory guard abort) — the
    * JVM exits AFTER main returns, so Spark's lifecycle cleanup runs
    * to completion before process termination. (R1 scorpion HIGH:
    * the previous sys.exit-inside-try pattern threw ControlThrowable,
    * racing spark.stop() in finally against Netty/listener-bus threads
    * that may still be mid-stop. The probe's own main avoids this
    * pattern — we align.)
    *
    * @param args the CLI arguments (see [[parseArgs]])
    * @return the process exit code
    */
  def run(args: Array[String]): Int = {
    val parsed = parseArgs(args) match {
      case Right(c) => c
      case Left(usage) =>
        System.err.println(usage)
        return 2
    }

    // Memory guard (same pattern as RollupObservationHarness; R1
    // zebra HIGH: the guard existed in the harness but not here —
    // two overlapping local[1] Spark sessions on the 7.5 GiB box
    // will OOM; "staggering" is not a guard).
    val memLines = java.nio.file.Files
      .readAllLines(java.nio.file.Paths.get("/proc/meminfo")).asScala
    /** Read a `/proc/meminfo` size value (e.g. `MemTotal: 7730 kB`).
      *
      * @param key the line prefix (e.g. `"MemTotal:"`)
      * @return the value in kB, or 0 if the line is missing
      */
    def kb(key: String): Long =
      memLines.find(_.startsWith(key)).map(_.trim.split("\\s+")(1).toLong).getOrElse(0L)
    val memTotal = kb("MemTotal:")
    val memUsedPct =
      if (memTotal == 0L) 0L
      else ((memTotal - kb("MemAvailable:")) * 100) / memTotal
    if (memUsedPct >= 85) {
      System.err.println(s"[gate-b-trace] ABORT: RAM at $memUsedPct% (>= 85% guard). " +
        "Free memory or delay the cron; the probe needs ~1-1.5 GB for the local Spark session.")
      return 3
    }

    val warehouse = parsed.warehouse.getOrElse(
      Files.createTempDirectory("gateb-trace-clock").toString)

    val spark = RollupRefreshCostProbe.buildSpark(warehouse)
    try {
      // Dispatch: live-model mode (modelPath set) vs synthetic fixture.
      val (report, measuredWhat) = parsed.modelPath match {
        case Some(yamlPath) =>
          // Live-model mode: load the YAML manifest, resolve the named
          // rollup, build the scope, run runModel. Non-destructive: the
          // Tier 0 overwrite is the same operation the production
          // refresh path performs; no table is dropped.
          val stream = new java.io.FileInputStream(yamlPath)
          val loaded = try {
            io.sm8.core.manifest.ModelLoader.fromStream(stream, yamlPath)
          } finally stream.close()
          loaded match {
            case Left(err) =>
              // M1 (R1 ibis): surface the parse error to the operator
              // (the rollup-not-found branch below already does this).
              System.err.println(s"[gate-b-trace] failed to load model from " +
                s"'$yamlPath': $err")
              return 1
            case Right(model) =>
              val spec = model.rollups.find(_.name == parsed.rollup.get)
              spec match {
                case None =>
                  System.err.println(s"[gate-b-trace] rollup '${parsed.rollup.get}' " +
                    s"not found on model '${model.name}' — available: " +
                    model.rollups.map(_.name).mkString(", "))
                  return 1
                case Some(s) => // s bound here; used below via `s` (L1: no spec.get)
                  // C1 (R1 poodle CRITICAL): derive the scope key from the
                  // rollup's actual grainDimension — the previous hardcoded
                  // "order_date" silently built a no-op scope (0 bytes touched)
                  // for any model whose grainDimension is something else.
                  val grainDim = s.grainDimension.getOrElse("")
                  val scopeDate = parsed.scopeDate.getOrElse("")
                  if (grainDim.isEmpty) {
                    System.err.println(s"[gate-b-trace] rollup '${s.name}' has no " +
                      "grainDimension — cannot build a scoped refresh without one. " +
                      "Add timeGrain + grainDimension to the rollup declaration.")
                    return 1
                  }
                  val scope = RollupMaterializer.RefreshScope.Partitions(
                    List(Map(grainDim -> scopeDate)))
                  val rep = RollupRefreshCostProbe.runModel(spark, model, s, scope)
                  val what = s"live model '${model.name}' rollup '${s.name}' " +
                    s"scope ${parsed.scopeDate.getOrElse("")}"
                  (rep, what)
              }
          }
        case None =>
          // Synthetic fixture (procedure exercise; NOT decision-grade).
          val rep = RollupRefreshCostProbe.run(spark, warehouse)
          (rep, "RollupRefreshCostProbe synthetic fixture (single partition, scoped Tier 0 + Tier 1)")
      }
      val mapper = new ObjectMapper()
        .registerModule(DefaultScalaModule)
        .enable(SerializationFeature.INDENT_OUTPUT)
      val outPath = Paths.get(parsed.outPath)
      if (outPath.getParent != null) Files.createDirectories(outPath.getParent)
      val isLive = parsed.modelPath.isDefined
      val wrapper = new java.util.LinkedHashMap[String, Object]()
      wrapper.put("requestedModel", parsed.model)
      // Structured mode marker (R1 loon L2 / dragon final gate): a
      // boolean downstream tooling can branch on without string
      // parsing the banner or measuredSource.
      wrapper.put("isLiveModel", java.lang.Boolean.valueOf(isLive))
      wrapper.put("collectedAtEpochMs", java.lang.Long.valueOf(System.currentTimeMillis()))
      wrapper.put("collectedAtIso",
        java.time.Instant.ofEpochMilli(System.currentTimeMillis()).toString)
      // H2 (R1 poodle): the field name measuredFixture was misleading
      // once live-model mode landed — for live runs the value names
      // a live model, not a fixture. Renamed to measuredSource.
      wrapper.put("measuredSource", measuredWhat)
      // Structured mode flag (R1 dragon #10): downstream tooling
      // needs a grep-friendly boolean, not a banner-string parse.
      wrapper.put("isLiveModel", java.lang.Boolean.valueOf(parsed.modelPath.isDefined))
      // Serialize the structured case-class report as a flat JSON
      // object (not a {"class": "...", "...": ...} discriminator map).
      // Casts are safe: every field is primitive/Option-primitive.
      @SuppressWarnings(Array("unchecked"))
      val reportMap = mapper.convertValue(report, classOf[java.util.Map[String, Any]])
      wrapper.put("report", reportMap)
      // Banner prefix (R1 zebra LOW + live-model distinction): log-greps
      // on `rendered` must surface WHICH mode ran, not just the metrics.
      val banner = if (parsed.modelPath.isDefined) "[LIVE MODEL]" else
        "[SYNTHETIC FIXTURE — not production model]"
      wrapper.put("rendered", banner + "\n" + report.render)
      mapper.writeValue(outPath.toFile, wrapper)
      System.out.println(s"[gate-b-trace] model=${parsed.model} → ${parsed.outPath}")
      System.out.println(report.render)
      0
    } catch {
      case NonFatal(e) =>
        System.err.println(s"[gate-b-trace] FAILED: " +
          s"${e.getClass.getSimpleName}: ${e.getMessage}")
        1
    } finally {
      spark.stop()
      parsed.warehouse match {
        case None =>
          val wh = new java.io.File(warehouse)
          if (wh.exists()) recursiveDelete(wh)
        case Some(_) => ()
      }
    }
  }

  /** JVM entry point: must return Unit (JVM requirement).
    * Calls [[run]] (which returns the exit code) and propagates it
    * via [[sys.exit]]. The runbook's spark-submit/java invocations
    * target this name.
    *
    * @param args CLI flags; see [[parseArgs]] for the accepted set
    */
  def main(args: Array[String]): Unit = sys.exit(run(args))

  /** CLI argument parsing — minimal two-flag parser (no deps on
    * scopt/case-app). Returns an error message with usage on any
    * failure (printed to stderr; process exits 2). */
  final case class CliConfig(
    model: String,
    outPath: String,
    warehouse: Option[String],
    /** Live-model mode (ADR-0029 §Gate B item 3 "representative
      * model"): path to the model YAML manifest. When set, the probe
      * measures a real scoped refresh of the named rollup on the
      * loaded model. When empty, the synthetic fixture runs. */
    modelPath: Option[String] = None,
    /** Live-model mode: the rollup name to measure (required when
      * modelPath is set). */
    rollup: Option[String] = None,
    /** Live-model mode: the scope partition date (yyyy-MM-dd local).
      * Required when modelPath is set and the rollup is time-grained. */
    scopeDate: Option[String] = None)

  /** Recognized flags — unknown-flag detection (R1 scorpion LOW F3). */
  private val recognizedFlags: Set[String] =
    Set("--model", "--out-path", "--bootstrap-warehouse",
        "--model-path", "--rollup", "--scope-date")

  private def parseArgs(args: Array[String]): Either[String, CliConfig] = {
    // R1 scorpion HIGH F2: args.sliding(2,2) silently swallows a
    // dangling trailing flag. Detect first.
    if (args.length % 2 != 0)
      return Left(s"odd number of arguments (${args.length}): " +
        s"every flag requires a value. Got: ${args.mkString(" ")}")
    val byKey = args.sliding(2, 2).filter(_.length == 2).map {
      case Array(k, v) => k -> v
      case _          => sys.error("unreachable")
    }.toMap
    val errors = scala.collection.mutable.ListBuffer[String]()
    /** Look up a required flag; appends an error to the buffer when
      * absent or empty. (Local helper — see parseArgs docblock.)
      *
      * @param flag the flag name to look up (including the leading `--`)
      * @return Some(value) if present and non-empty; None otherwise
      */
    def req(flag: String): Option[String] =
      byKey.get(flag) match {
        case Some(v) if v.nonEmpty => Some(v)
        case Some(_) => errors += s"$flag requires a non-empty value"; None
        case None    => errors += s"missing required flag $flag"; None
      }
    /** Look up an optional flag.
      *
      * @param flag the flag name to look up (including the leading `--`)
      * @return Some(value) if present and non-empty; None otherwise
      */
    def opt(flag: String): Option[String] = byKey.get(flag).filter(_.nonEmpty)

    val model = req("--model")
    val outPath = req("--out-path")
    val warehouse = opt("--bootstrap-warehouse")
    val modelPath = opt("--model-path")
    val rollup = opt("--rollup")
    val scopeDateOpt = opt("--scope-date")
    val scopeDate = scopeDateOpt.filter(_.nonEmpty).flatMap { s =>
      // M2 (R1 poodle): validate yyyy-MM-dd format; reject anything else.
      val ok = s.length == 10 &&
        s(4) == '-' && s(7) == '-' &&
        s.substring(0, 4).forall(_.isDigit) &&
        s.substring(5, 7).forall(_.isDigit) &&
        s.substring(8, 10).forall(_.isDigit)
      if (ok) Some(s)
      else {
        errors += s"--scope-date must match yyyy-MM-dd (got: '$s')"
        None
      }
    }

    // Cross-flag validation (ADR-0031 §D1 / ADR-0030 §D1):
    // --model-path requires --rollup. --scope-date requires
    // --model-path (the synthetic fixture has no scope parameter).
    if (modelPath.isDefined && rollup.isEmpty)
      errors += "--model-path requires --rollup <name>"
    if (scopeDate.isDefined && modelPath.isEmpty)
      errors += "--scope-date requires --model-path (the synthetic fixture has no scope)"
    if (rollup.isDefined && modelPath.isEmpty)
      errors += "--rollup requires --model-path"
    // H2 (R1 ibis): live-model mode without --scope-date was silently
    // passing an empty scope (the runner's getOrElse("") substituted a
    // blank partition value, which the materializer refuses typed as
    // RefuseScopeUncovered — but the operator got exit 1 with no
    // useful message). Reject the missing scope-date here.
    if (modelPath.isDefined && rollup.isDefined && scopeDate.isEmpty)
      errors += "--model-path + --rollup requires --scope-date <yyyy-MM-dd>"

    (model, outPath, warehouse, modelPath, rollup, scopeDate) match {
      case (Some(m), Some(p), w, mp, r, sd)
          if errors.isEmpty =>
        Right(CliConfig(m, p, w, mp, r, sd))
      case _ =>
        Left(
          s"""usage: GateBTraceRunner --model <name> --out-path <file>
             |                    [--bootstrap-warehouse <dir>]
             |                    [--model-path <yaml> --rollup <name> [--scope-date <yyyy-MM-dd>]]
             |  --model                 label recorded in the output (required)
             |  --out-path              output JSON file (required)
             |  --bootstrap-warehouse   optional: directory for the probe's
             |                          embedded HadoopCatalog; defaults to a
             |                          temp dir cleaned at end
             |  --model-path            live-model mode: path to the model YAML
             |                          manifest (measures a real scoped refresh)
             |  --rollup                live-model mode: rollup name to measure
             |  --scope-date            live-model mode: scope partition date
             |                          (yyyy-MM-dd local)
             |${errors.mkString("", "\n  error: ", "")}""".stripMargin)
    }
  }

  /** Recursively delete a directory tree (warehouse hygiene). */
  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }
}
