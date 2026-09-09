/*
 * SM8 Spark Connector — GateBTraceRunner (ADR-0029 §Gate B item 3 +
 * ADR-0030 §D1 evidence collection).
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
 */
package io.sm8.connectors.spark

import java.nio.file.{Files, Path, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
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
  def main(args: Array[String]): Int = {
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
      val report = RollupRefreshCostProbe.run(spark, warehouse)
      val mapper = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
      val outPath = Paths.get(parsed.outPath)
      if (outPath.getParent != null) Files.createDirectories(outPath.getParent)
      // Wrap the report with trace metadata so the JSON is
      // self-describing: which model was REQUESTED (even though the
      // probe currently measures the synthetic fixture — see the
      // class-level HONEST LIMITATION), when the run happened, and
      // the probe's own rendered text for grep-ability in logs.
      val wrapper = new java.util.LinkedHashMap[String, Object]()
      wrapper.put("requestedModel", parsed.model)
      wrapper.put("collectedAtEpochMs", java.lang.Long.valueOf(System.currentTimeMillis()))
      wrapper.put("collectedAtIso",
        java.time.Instant.ofEpochMilli(System.currentTimeMillis()).toString)
      wrapper.put("measuredFixture",
        "RollupRefreshCostProbe synthetic fixture (single partition, scoped Tier 0 + Tier 1)")
      wrapper.put("report", report)
      // Banner prefix (R1 zebra LOW): log-greps on `rendered` must
      // surface the synthetic-fixture warning, not just the metrics.
      wrapper.put("rendered",
        "[SYNTHETIC FIXTURE — not production model]\n" + report.render)
      mapper.writeValue(outPath.toFile, wrapper)
      // Also print to stdout — the cron's log file captures it without
      // needing to read the JSON.
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
      // Clean only if WE created the warehouse (cron-supplied warehouses
      // are operator-owned; we never auto-delete them).
      parsed.warehouse match {
        case None =>
          val wh = new java.io.File(warehouse)
          if (wh.exists()) recursiveDelete(wh)
        case Some(_) => () // operator-owned; leave alone
      }
    }
  }

  /** Scala-main entry — JVM exits AFTER main returns so Spark
    * lifecycle completes cleanly.
    *
    * @param args the CLI arguments (see [[parseArgs]])
    */
  def mainEntry(args: Array[String]): Unit = sys.exit(main(args))

  /** CLI argument parsing — minimal two-flag parser (no deps on
    * scopt/case-app). Returns an error message with usage on any
    * failure (printed to stderr; process exits 2). */
  final case class CliConfig(
    model: String,
    outPath: String,
    warehouse: Option[String])

  /** Recognized flags — unknown-flag detection (R1 scorpion LOW F3). */
  private val recognizedFlags: Set[String] =
    Set("--model", "--out-path", "--bootstrap-warehouse")

  private def parseArgs(args: Array[String]): Either[String, CliConfig] = {
    // R1 scorpion HIGH F2: args.sliding(2,2) silently swallows a
    // dangling trailing flag (`--out-path /tmp/x.json --model`
    // forgets the value and gets the misleading "missing required
    // flag --model" instead of a structural error). Detect first.
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
    val model = req("--model")
    val outPath = req("--out-path")
    val warehouse = byKey.get("--bootstrap-warehouse")
    (model, outPath, warehouse) match {
      case (Some(m), Some(p), w) => Right(CliConfig(m, p, w))
      case _ => Left(
        s"""usage: GateBTraceRunner --model <name> --out-path <file> [--bootstrap-warehouse <dir>]
           |  --model              registered model name (required)
           |  --out-path           output JSON file (required)
           |  --bootstrap-warehouse  optional: directory for the probe's
           |                         embedded HadoopCatalog; defaults to a
           |                         temp dir cleaned at end. Production
           |                         uses a cluster-side catalog config;
           |                         this flag is for offline fixtures.
           |${errors.mkString("", "\n  error: ", "")}""".stripMargin)
    }
  }

  /** Recursively delete a directory tree (warehouse hygiene). */
  private def recursiveDelete(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(recursiveDelete))
    f.delete()
  }
}
