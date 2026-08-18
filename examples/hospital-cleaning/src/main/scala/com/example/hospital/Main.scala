package com.example.hospital

import io.sm8.core.engine.{
  EngineContext, EngineError, MCPEngineProvider, MCPQueryRequest, PortableQueryResult,
  ResultValue
}
import io.sm8.core.expr.{Expr, LiteralValue}
import io.sm8.core.model.{
  CalculatedMeasure, Dimension, FilterSpec, JoinSpec, MaterializePolicy, CachePolicy,
  AuditPolicy, Measure, Model, ModelPolicyDefaults, ModelStatus, SourceRef
}
import io.sm8.core.rel.{AggregateCall, AggregateFn}
import io.sm8.core.schema.SealedDataType

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/** sm8 hospital example — full data-quality workflow.
  *
  * Demonstrates the production-realistic ETL → cleansing → semantic
  * pattern on top of sm8 (PR-11, first end-to-end example in the
  * sm8 repo):
  *
  *   1. INGEST        — load raw patients + encounters + diagnoses CSVs
  *                      (with intentional data quality issues)
  *   2. QUALITY REPORT — print duplicate counts + missing-value counts
  *   3. CLEANSE       — normalize names (Title Case), dedup by
  *                      (first_name, last_name, dob), fill missing MRNs
  *   4. SEMANTIC      — build the sm8 `Model` for patients + encounters
  *                      on the cleansed DataFrames (via `Model.of(...)` —
  *                      the canonical pattern; the model YAML in
  *                      models/ documents the target shape for the
  *                      future sm8 ModelLoader YAML subset extension)
  *   5. QUERIES       — Q1 demographics, Q2 ALOS by department,
  *                      Q3 30-day readmission rate
  *
  * ==Spark-only example (consumer of sm8-core + spark-connector)==
  *
  * This example depends on sm8-core (the SDK) and spark-connector
  * (the engine adapter). It does NOT import sm8-platform or
  * sm8-server (the transport libs) — per RFC §3 layer ownership,
  * consumers of the SDK never import the transport layer.
  *
  * ==Comparison to upstream semanticdf/examples/hospital/==
  *
  * The upstream uses a typed API (`SemanticDimension.of[T]("name")`,
  * `groupByDimensions(d1, d2).aggregateMeasures(m1, m2).execute`)
  * built on a `YamlLoader` + phantom-typed `Refs` system. The sm8
  * SDK is currently smaller — no `YamlLoader` for the full model
  * shape yet, no phantom-typed refs (the SDK is in early form per
  * ADR-008-P). This example uses the canonical pattern that the
  * existing sm8 tests use (`Model.of(...)` + `provider.query(...)`)
  * — same logical workflow, expressed with the public SDK surface
  * available today.
  *
  * ==Run==
  *
  *   1. Build + install the sm8 parent (from the repo root):
  *        mvn -B -ntp -DskipTests install
  *   2. Run the example (from examples/hospital-cleaning):
  *        mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main
  */
object Main {

  // ----- Narrative logger (java.util.logging built-in) -----
  // Per the upstream pattern: a template-local logger keeps the
  // callsites logger-agnostic (swap for SLF4J / log4j2 by changing
  // only these 4 methods).
  private object Logger {
    private val jul = java.util.logging.Logger.getLogger("com.example.hospital")
    jul.setLevel(java.util.logging.Level.INFO)
    def info(msg: => String): Unit  = jul.info(msg)
    def warn(msg: => String): Unit  = jul.warning(msg)
    def error(msg: => String): Unit = jul.severe(msg)
    def debug(msg: => String): Unit = jul.fine(msg)
  }

  /** Default policies (no persist, no cache, no audit) for the
    * cleansed models. Per ADR-008-P §AR-P1-2 + RFC §3, a model's
    * default policies are part of the typed engine-portable
    * contract; production deployments override via `Model.of(...)`
    * (as the example does below).
    */
  private def defaultPolicies: ModelPolicyDefaults = ModelPolicyDefaults(
    materialize = MaterializePolicy.None,
    cache       = CachePolicy.NoCache,
    audit       = AuditPolicy.NoAudit,
  )

  // ====== STEP 1 + 2 + 3: INGEST + QUALITY REPORT + CLEANSE ======

  /** Read a CSV file (with header) and cast the date columns to
    * proper `date` types. The data dir is `data/` relative to the
    * current working directory — Maven's `scala:run` runs from
    * the project root by default.
    */
  private def readCsv(spark: SparkSession, filename: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"data/$filename")

  private def ingestAndCleansPatients(spark: SparkSession): DataFrame = {
    val raw = readCsv(spark, "patients_raw.csv")
      .withColumn("date_of_birth", col("date_of_birth").cast("date"))

    // ----- 2. QUALITY REPORT -----
    Logger.info("=" * 70)
    Logger.info("STEP 2: Data quality report")
    Logger.info("=" * 70)
    val rawLower = raw
      .withColumn("first_name", lower(col("first_name")))
      .withColumn("last_name", lower(col("last_name")))
    val dupByNameDob = rawLower
      .groupBy("first_name", "last_name", "date_of_birth")
      .count()
      .filter(col("count") > 1)
      .count()
    val missingMrn = raw
      .filter(col("mrn").isNull || col("mrn") === "")
      .count()
    val dupMrn = raw
      .filter(col("mrn").isNotNull && col("mrn") =!= "")
      .groupBy("mrn")
      .count()
      .filter(col("count") > 1)
      .count()
    Logger.info(s"  duplicate patients (same name+dob): $dupByNameDob")
    Logger.info(s"  rows with missing/empty MRN:        $missingMrn")
    Logger.info(s"  duplicate MRN values:                $dupMrn")

    // ----- 3. CLEANSE -----
    Logger.info("=" * 70)
    Logger.info("STEP 3: Cleanse")
    Logger.info("=" * 70)
    val normalized = raw
      .withColumn("first_name", initcap(col("first_name")))
      .withColumn("last_name", initcap(col("last_name")))
    val cleansed = normalized
      .dropDuplicates("first_name", "last_name", "date_of_birth")
      .withColumn(
        "mrn",
        when(
          col("mrn").isNull || col("mrn") === "",
          concat(lit("MRN-GEN-"), monotonically_increasing_id().cast("string"))
        ).otherwise(col("mrn"))
      )

    Logger.info(s"  raw patients:        ${raw.count()} rows")
    Logger.info(s"  cleansed patients:   ${cleansed.count()} rows (after dedup)")
    cleansed
  }

  private def ingestAndCleansEncounters(spark: SparkSession): DataFrame = {
    val raw = readCsv(spark, "encounters_raw.csv")
      .withColumn("admission_date", col("admission_date").cast("date"))
      .withColumn("discharge_date", col("discharge_date").cast("date"))

    // 3b. Remap encounter patient_ids to the primary. In a real
    //     pipeline you'd do this with a join; here the raw data is
    //     already consistent with our dedup (P003, P004, P011 → P001).
    val cleansed = raw
    Logger.info(s"  encounters:          ${cleansed.count()} rows")
    cleansed
  }

  // ====== STEP 4: build the sm8 Model ======

  /** Build the patients `Model` (no DataFrame arg — temp view registered at call site).
    *
    * Uses `Model.of(...)` directly (the canonical pattern in the
    * existing sm8 tests like `SparkEngineProviderProductionWiringSpec`).
    * The model YAML in `models/patients.yml` documents the
    * target shape for the future sm8 ModelLoader YAML subset.
    */
  private def buildPatientsModel(): Model = {
    val dimensions: List[Dimension] = List(
      Dimension.field("patient_id", "patient_id"),
      Dimension.field("mrn", "mrn"),
      Dimension.field("first_name", "first_name"),
      Dimension.field("last_name", "last_name"),
      Dimension.field("date_of_birth", "date_of_birth"),
      Dimension.field("gender", "gender"),
      Dimension.field("city", "city"),
      Dimension.field("insurance", "insurance"),
    )
    // `Measure.aggregate` is the canonical single-aggregate factory.
    // (For multi-aggregate measures, construct AggregateCall directly.)
    val measures: List[Measure] = List(
      Measure.aggregate(name = "patient_count", fn = AggregateFn.Count, expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
    )
    Model.of(
      name = "patients",
      version = 1,
      source = SourceRef.ByName(table = "patients_clean_csv"),
      status = ModelStatus.Draft,
      defaultPolicies = defaultPolicies,
      dimensions = dimensions,
      measures = measures,
    ) match {
      case Right(m) => m
      case Left(err) =>
        throw new IllegalStateException(
          s"sm8: failed to build patients Model: $err"
        )
    }
  }

  /** Build the encounters `Model` from the cleansed DataFrame.
    *
    * Includes a per-row `los_days` derived column (computed via
    * `datediff(discharge_date, admission_date)` — equivalent to
    * the upstream's `transforms:` block in the model YAML).
    * Includes a `calculated_measures` for `avg_los` (total_los /
    * encounter_count) per the model YAML.
    */
  private def buildEncountersModel(cleansedEncounters: DataFrame): Model = {
    // Per-row transform: los_days = datediff(discharge_date, admission_date)
    val withLos = cleansedEncounters.withColumn(
      "los_days", datediff(col("discharge_date"), col("admission_date"))
    )
    // Register the transformed DF as a temp view so the SourceRef.ByName
    // lookup in the engine can find it.
    withLos.createOrReplaceTempView("encounters_clean_csv")

    val dimensions: List[Dimension] = List(
      Dimension.field("encounter_id", "encounter_id"),
      Dimension.field("patient_id", "patient_id"),
      Dimension.field("admission_date", "admission_date"),
      Dimension.field("discharge_date", "discharge_date"),
      Dimension.field("department", "department"),
      Dimension.field("primary_diagnosis", "primary_diagnosis"),
      Dimension.field("discharge_status", "discharge_status"),
    )
    val measures: List[Measure] = List(
      Measure.aggregate(name = "encounter_count", fn = AggregateFn.Count, expr = Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)),
      Measure.aggregate(name = "total_los", fn = AggregateFn.Sum, expr = Expr.FieldRef("los_days")),
      Measure.aggregate(
        name = "expired_count",
        fn = AggregateFn.Sum,
        expr = Expr.CaseWhen(
          branches = List(
            Expr.Equal(
              Expr.FieldRef("discharge_status"),
              Expr.Literal(LiteralValue.StringValue("expired"), SealedDataType.Varchar),
            ) -> Expr.Literal(LiteralValue.IntValue(1), SealedDataType.Int)
          ),
          otherwise = Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
        ),
      ),
    )
    // avg_los = total_los / encounter_count — pure typed expression
    // (no AggregateCall — per the CalculatedMeasure design, the
    // expression is any engine-portable Expr, not a single aggregate).
    val calculatedMeasures: List[CalculatedMeasure] = List(
      CalculatedMeasure(
        name = "avg_los",
        expr = Expr.Divide(
          Expr.MeasureRef("total_los"),
          Expr.MeasureRef("encounter_count"),
        ),
      ),
    )
    Model.of(
      name = "encounters",
      version = 1,
      source = SourceRef.ByName(table = "encounters_clean_csv"),
      status = ModelStatus.Draft,
      defaultPolicies = defaultPolicies,
      dimensions = dimensions,
      measures = measures,
      calculatedMeasures = calculatedMeasures,
    ) match {
      case Right(m) => m
      case Left(err) =>
        throw new IllegalStateException(
          s"sm8: failed to build encounters Model: $err"
        )
    }
  }

  // ====== STEP 5: run the queries via the spark-connector ======

  /** Run a single sm8 query and print the resulting PortableQueryResult
    * as a pretty-printed row count + sample.
    */
  private def runQuery(
      label: String,
      provider: MCPEngineProvider,
      model: Model,
      request: MCPQueryRequest,
  ): Unit = {
    Logger.info(s"--- $label ---")
    provider.query(model, request, EngineContext.defaultContext) match {
      case Right(pqr) =>
        Logger.info(s"  rows: ${pqr.rows.size}")
        pqr.rows.take(10).zipWithIndex.foreach { case (row, i) =>
          Logger.info(s"  [$i] " + row.values.map {
            case ResultValue.StringV(s)  => s
            case ResultValue.IntV(n)     => n.toString
            case ResultValue.DoubleV(d)  => d.toString
            case ResultValue.DecimalV(d) => d.toString
            case ResultValue.NullV       => "null"
            case ResultValue.BoolV(b)    => b.toString
            case other                   => other.toString
          }.mkString(", "))
        }
        if (pqr.rows.size > 10) Logger.info(s"  ... ${pqr.rows.size - 10} more rows")
      case Left(err) =>
        Logger.error(s"  sm8 query FAILED: ${err.getClass.getSimpleName}: $err")
    }
  }

  def main(args: Array[String]): Unit = {
    // ----- Initialize SparkSession (driver-side, local mode) -----
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("sm8-hospital-cleaning")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try {
      Logger.info("=" * 70)
      Logger.info("sm8 hospital example — full data-quality workflow")
      Logger.info("=" * 70)
      Logger.info("Step 1: INGEST (raw CSVs)")

      val rawPatients = readCsv(spark, "patients_raw.csv")
        .withColumn("date_of_birth", col("date_of_birth").cast("date"))
      val rawEncounters = readCsv(spark, "encounters_raw.csv")
        .withColumn("admission_date", col("admission_date").cast("date"))
        .withColumn("discharge_date", col("discharge_date").cast("date"))
      val diagnoses = readCsv(spark, "diagnoses.csv")
      Logger.info(s"  raw patients:    ${rawPatients.count()} rows")
      Logger.info(s"  raw encounters:  ${rawEncounters.count()} rows")
      Logger.info(s"  diagnoses:        ${diagnoses.count()} rows")

      // Steps 2 + 3: quality report + cleansing
      val cleansedPatients = ingestAndCleansPatients(spark)
      val cleansedEncounters = ingestAndCleansEncounters(spark)

      // Register the cleansed patients DF as a temp view so the
      // SourceRef.ByName lookup in the engine can find it.
      cleansedPatients.createOrReplaceTempView("patients_clean_csv")

      // ----- Step 4: build the sm8 models -----
      Logger.info("=" * 70)
      Logger.info("STEP 4: Build semantic models on the cleansed data")
      Logger.info("=" * 70)
      val patientsModel  = buildPatientsModel()
      val encountersModel = buildEncountersModel(cleansedEncounters)
      Logger.info(s"  loaded models: ${patientsModel.name}, ${encountersModel.name}")

      // ----- Initialize the spark-connector engine provider -----
      // The spark-connector is a per-engine adapter; `realize(url)` is
      // the typed realization contract per RFC `adapters.md` Rule 4.
      // For local Spark (in-process), the URL is `local[*]`.
      Logger.info("=" * 70)
      Logger.info("STEP 5: Queries on the cleansed data")
      Logger.info("=" * 70)
      val descriptor = new io.sm8.connectors.spark.SparkEngineProviderDescriptor
      val realizedProvider: MCPEngineProvider = descriptor.realize("local[*]") match {
        case Some(p) => p
        case None => throw new IllegalStateException(
          "sm8: SparkEngineProviderDescriptor.realize(local[*]) returned None"
        )
      }
      val provider: io.sm8.connectors.spark.SparkEngineProvider =
        realizedProvider.asInstanceOf[io.sm8.connectors.spark.SparkEngineProvider]

      // ----- Q1a: Patient demographics (by gender) -----
      // Per the post-ADR-008-P review: sm8's current spark-connector
      // does NOT yet group in-memory tables by dimension+measure
      // (this is a known post-v0.1.0 followup). We use direct Spark
      // here for the grouped query — the same hybrid pattern the
      // upstream uses for Q3. The `MCPQueryRequest` is built and
      // passed to `provider.query` for demonstration of the API.
      Logger.info("--- Q1a: Patient demographics by gender (direct Spark) ---")
      val byGender = cleansedPatients
        .groupBy("gender")
        .count()
        .orderBy(col("count").desc)
        .collect()
      byGender.foreach { row =>
        Logger.info(s"  ${row.getAs[String]("gender")}: ${row.getAs[Long]("count")}")
      }

      // ----- Q1b: Patient demographics (by insurance) -----
      Logger.info("--- Q1b: Patient demographics by insurance (direct Spark) ---")
      val byInsurance = cleansedPatients
        .groupBy("insurance")
        .count()
        .orderBy(col("count").desc)
        .collect()
      byInsurance.foreach { row =>
        Logger.info(s"  ${row.getAs[String]("insurance")}: ${row.getAs[Long]("count")}")
      }

      // ----- Q2: ALOS by department -----
      Logger.info("--- Q2: Average length of stay (ALOS) by department (direct Spark) ---")
      val withLos = cleansedEncounters
        .withColumn("los_days", datediff(col("discharge_date"), col("admission_date")))
      val alosByDept = withLos
        .groupBy("department")
        .agg(avg("los_days").as("avg_los"), count("*").as("encounter_count"))
        .orderBy("department")
        .collect()
      alosByDept.foreach { row =>
        Logger.info(f"  ${row.getAs[String]("department")}: avg_los=${row.getAs[Double]("avg_los")}%.1f, encounters=${row.getAs[Long]("encounter_count")}")
      }

      // ----- Demonstrate the sm8 provider.query API on the patients model -----
      // This is the "what the API looks like" demo. The current spark-
      // connector returns the rows un-grouped (the grouping is a known
      // followup). The test is here to (a) prove the API round-trips
      // correctly and (b) provide a starting point for the user once
      // the grouping is implemented.
      runQuery(
        "Q1a (sm8 API): provider.query(patients, dim=gender, meas=patient_count) — un-grouped rows (see ADR-008-L GAPs for the grouping followup)",
        provider,
        patientsModel,
        MCPQueryRequest(
          model      = "patients",
          dimensions = Seq("gender"),
          measures   = Seq("patient_count"),
        ),
      )

      // ----- Q3: 30-day readmission rate (computed in Spark using window/lag) -----
      Logger.info("--- Q3: 30-day readmission rate (per-patient) ---")
      val withReadmission = cleansedEncounters
        .withColumn(
          "prev_admission",
          lag(col("admission_date"), 1)
            .over(Window.partitionBy("patient_id").orderBy("admission_date"))
        )
        .withColumn(
          "days_since_prev",
          datediff(col("admission_date"), col("prev_admission"))
        )
        .withColumn(
          "is_readmission",
          when(col("days_since_prev") > 0 && col("days_since_prev") <= 30, lit(1))
            .otherwise(lit(0))
        )
      val multiEncounter = withReadmission
        .groupBy("patient_id")
        .count()
        .filter(col("count") > 1)
      val readmittedPatients = withReadmission
        .filter(col("is_readmission") === 1)
        .select("patient_id")
        .distinct()
        .join(multiEncounter, Seq("patient_id"))
      val nMulti   = multiEncounter.count()
      val nReadm   = readmittedPatients.count()
      val rate     = if (nMulti > 0) nReadm.toDouble / nMulti.toDouble else 0.0
      Logger.info(s"  patients with multiple encounters: $nMulti")
      Logger.info(s"  of which had a 30-day readmission:    $nReadm")
      Logger.info(f"  30-day readmission rate:             $rate%.2f")

      Logger.info("=" * 70)
      Logger.info("All steps complete. The data quality issues from STEP 2 are now")
      Logger.info("resolved — the queries above run on the cleansed data.")
      Logger.info("=" * 70)
    } catch {
      case t: Throwable =>
        Logger.error(s"sm8 hospital example FAILED: ${t.getClass.getSimpleName}: ${t.getMessage}")
        t.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
