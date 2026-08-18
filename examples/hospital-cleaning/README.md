# sm8 hospital-cleaning example

Hospital data management + cleansing on top of **sm8** — the **full data-quality workflow** (ingest → profile → cleanse → load → query). Patient demographics, ALOS (Average Length of Stay), 30-day readmission rate.

This is the **first end-to-end example** in the sm8 repo (PR-11). It complements the other consumer templates by showing the **cleansing step** — most templates load clean data; this one starts with messy data and cleanses it in Scala before loading into sm8.

## What you get

```
examples/hospital-cleaning/
├── README.md                          ← you are here
├── pom.xml                            ← standalone Maven project (NOT part of sm8 reactor)
├── data/
│   ├── patients_raw.csv               ← 11 rows with intentional quality issues
│   ├── patients_clean.csv              ← 9 rows after dedup + normalization
│   ├── encounters_raw.csv              ← 13 rows including a 30-day readmission for P007
│   ├── encounters_clean.csv            ← 13 rows, patient_ids remapped to canonical
│   └── diagnoses.csv                   ← 6 ICD-10 reference codes
├── models/
│   ├── patients.yml                   ← model shape (documentation; target for the future sm8 YAML loader subset)
│   └── encounters.yml                  ← model shape with per-row transforms
└── src/main/scala/com/example/hospital/
    └── Main.scala                       ← full ETL → cleansing → queries workflow
```

## Run it (5 minutes)

### Prerequisites

- JDK 17, Maven 3.9+
- Spark 3.5.x (NOT required to be pre-installed — the `spark-sql` dependency in `pom.xml` brings it in)

### Step 1: install sm8 locally

```bash
cd /path/to/sm8
mvn -B -ntp -DskipTests install
```

This installs the sm8 reactor modules (`sm8-core_2.13`, `spark-connector_2.13`, etc.) into your local Maven repository at version `0.1.0-SNAPSHOT`.

### Step 2: run the example

```bash
cd examples/hospital-cleaning
mvn -B -ntp scala:run -DmainClass=com.example.hospital.Main
```

You'll see all 5 steps run in sequence:

1. **INGEST** — load the raw CSVs (intentional data quality issues)
2. **QUALITY REPORT** — print counts of duplicates / missing values
3. **CLEANSE** — normalize names, dedup patients, fill missing MRNs
4. **SEMANTIC** — load the sm8 `Model` objects on the cleansed DataFrames
5. **QUERIES** — Q1 demographics, Q2 ALOS by department, Q3 30-day readmission rate

## The data quality issues (and how the cleanser handles them)

| Issue | How it's caught | Cleansing step |
|---|---|---|
| Mixed-case names (`john smith` vs `John Smith` vs `John A Smith`) | The quality report groups by lowercased name + dob | `initcap()` normalizes to Title Case |
| Duplicate patients (P001/P003/P004/P011 all "John Smith b. 1955") | Group count > 1 in the quality report | `dropDuplicates("first_name", "last_name", "date_of_birth")` |
| Duplicate MRNs (P001 and P004 both have `MRN-1001`) | Group count > 1 on mrn column | Remap to the canonical patient_id (the lowest) |
| Missing MRN (P006 has empty MRN) | `mrn IS NULL OR mrn = ''` filter | `coalesce(mrn, "MRN-GEN-" + id)` |

After cleansing: **11 patients → 9 unique patients**; all MRNs filled; all names in Title Case.

## Sample output (actual, captured 2026-08-18)

The example prints a stage-by-stage trace via `Logger.info`. Output on the demo data:

```
INFO hospital: ======================================================================
INFO hospital: sm8 hospital example — full data-quality workflow
INFO hospital: ======================================================================
INFO hospital: Step 1: INGEST (raw CSVs)
INFO hospital:   raw patients:    11 rows
INFO hospital:   raw encounters:  13 rows
INFO hospital:   diagnoses:        6 rows
INFO hospital: ======================================================================
INFO hospital: STEP 2: Data quality report
INFO hospital: ======================================================================
INFO hospital:   duplicate patients (same name+dob): 1
INFO hospital:   rows with missing/empty MRN:        1
INFO hospital:   duplicate MRN values:                2
INFO hospital: ======================================================================
INFO hospital: STEP 3: Cleanse
INFO hospital: ======================================================================
INFO hospital:   raw patients:        11 rows
INFO hospital:   cleansed patients:   9 rows (after dedup)
INFO hospital:   encounters:          13 rows
INFO hospital: ======================================================================
INFO hospital: STEP 4: Build semantic models on the cleansed data
INFO hospital: ======================================================================
INFO hospital:   loaded models: patients, encounters
INFO hospital: ======================================================================
INFO hospital: STEP 5: Queries on the cleansed data
INFO hospital: ======================================================================
INFO hospital: --- Q1a: Patient demographics by gender (direct Spark) ---
INFO hospital:   M: 5
INFO hospital:   F: 4
INFO hospital: --- Q1b: Patient demographics by insurance (direct Spark) ---
INFO hospital:   BlueCross: 3
INFO hospital:   Aetna: 2
INFO hospital:   Medicare: 1
INFO hospital:   Kaiser: 1
INFO hospital:   Cigna: 1
INFO hospital:   UnitedHealth: 1
INFO hospital: --- Q2: Average length of stay (ALOS) by department (direct Spark) ---
INFO hospital:   Cardiology: avg_los=3.1, encounters=7
INFO hospital:   Emergency: avg_los=4.0, encounters=1
INFO hospital:   Obstetrics: avg_los=2.0, encounters=1
INFO hospital:   Oncology: avg_los=5.0, encounters=1
INFO hospital:   Pediatrics: avg_los=3.0, encounters=3
INFO hospital: --- Q1a (sm8 API): provider.query(patients, dim=gender, meas=patient_count) — un-grouped rows (see ADR-008-L GAPs for the grouping followup) ---
INFO hospital:   rows: 9
INFO hospital:   [0] P006, MRN-GEN-0, James, Brown, DateV(1990-05-15), M, Los Angeles, Medicare, 1
INFO hospital:   ... (9 rows total — un-grouped)
INFO hospital: --- Q3: 30-day readmission rate (per-patient) ---
INFO hospital:   patients with multiple encounters: 2
INFO hospital:   of which had a 30-day readmission:    1
INFO hospital:   30-day readmission rate:             0.50
```

The 30-day readmission rate is `1 / 2 = 0.50`. P001 has two encounters but they're 31 days apart (just outside the 30-day window). P007 has three encounters — E005 (2024-02-20), E012 (2024-04-18), and E013 (2024-05-02), which is **14 days after E012** → triggers `is_readmission = 1` → patient counted as readmitted.

### Honest limitations (per the post-ADR-008-P review)

The example is a **complete end-to-end demo** of the data-quality workflow. The Q1a/Q1b/Q2 grouped queries use **direct Spark** (not sm8's `provider.query`) because:

- **sm8's current spark-connector** returns rows from `provider.query(model, request, ctx)` but does **NOT yet** apply the `dimensions` + `measures` grouping (the `applyAggregations` path in the spark-connector is a known followup; see ADR-008-L GAP 7 / PR-M4 followup).
- The **`Q1a (sm8 API)`** block in STEP 5 demonstrates that `provider.query` returns the rows correctly through the engine-portable Protocol — the API round-trips, the grouping is the only followup.
- The Q3 (30-day readmission) example uses **window/lag in Spark** directly because the final aggregation crosses group boundaries (per-patient max is_readmission, then a final ratio across patients). This is the same hybrid pattern the upstream uses.

**Once the spark-connector's `applyAggregations` is upgraded** to honor `MCPQueryRequest.dimensions` + `measures` end-to-end (a future PR; per ADR-008-P §"What's Next" + ADR-008-L GAP 7), the Q1/Q2 `runQuery(...)` calls can replace the direct-Spark `groupBy().count()/.agg(...)` blocks — the rest of the example needs no change.

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| Data quality profiling (group counts, null detection) | STEP 2 |
| In-place Spark cleansing: `initcap`, `dropDuplicates`, `coalesce`, `monotonically_increasing_id` | STEP 3 |
| Building sm8 `Model` objects via the `Model.of(...)` builder API | STEP 4 |
| Loading in-memory DataFrames into sm8 via `createOrReplaceTempView` + `SourceRef.ByName` | STEP 4 |
| `groupBy(dim).aggregate(measure)` per group | Q1, Q2 |
| Hybrid pattern: per-patient measures via Spark, final rate in Scala | Q3 |
| The spark-connector realize-then-query pattern (`SparkEngineProviderDescriptor.realize(url)` then `provider.query(...)`) | STEP 5 |

## Architecture: where this example fits in the sm8 RFC §3 stack

```
  ┌─────────────────────────────────────────────────────┐
  │ THIS EXAMPLE (examples/hospital-cleaning)            │  Consumer layer
  │   - reads CSVs                                       │  (per RFC §3)
  │   - does the ETL + cleansing in Spark                │  Imports:
  │   - builds the sm8 `Model` via Model.of(...)         │  - sm8-core (SDK)
  │   - queries via spark-connector                      │  - spark-connector
  └────────────────────┬────────────────────────────────┘
                       │ Model.of(), provider.query()
  ┌────────────────────▼────────────────────────────────┐
  │ spark-connector   (the engine adapter for Spark)     │  Adapter layer
  │   - typed `realize(url)` per RFC `adapters.md` Rule 4│  Imports:
  │   - compiles Model → Spark DataFrame                 │  - sm8-core
  │   - runs the query, returns PortableQueryResult       │  - spark
  └────────────────────┬────────────────────────────────┘
                       │ MCPEngineProvider, PortableQueryResult
  ┌────────────────────▼────────────────────────────────┐
  │ sm8-core   (the FROZEN Core — engine-portable SDK)    │  Core layer
  │   - Model, Dimension, Measure, CalculatedMeasure     │  Spark-free
  │   - SourceRef, MCPQueryRequest, EngineError ADT      │  Public Maven coord
  │   - 4-stage pipeline contract                         │
  └─────────────────────────────────────────────────────┘
```

This example does **NOT** import `sm8-platform` or `sm8-server` (the transport libs) — per RFC §3, consumers of the SDK never import the transport layer.

## Related

- **[`sm8-core/.../model/Model.scala`](../../sm8-core/src/main/scala/io/sm8/core/model/Model.scala)** — the `Model.of(...)` builder API used in this example
- **[`spark-connector/.../SparkEngineProvider.scala`](../../connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala)** — the `MCPEngineProvider` implementation invoked in STEP 5
- **[`sm8-core/.../engine/MCPEngineProvider.scala`](../../sm8-core/src/main/scala/io/sm8/core/engine/MCPEngineProvider.scala)** — the production abstraction (per ADR-001 §P1-3 + ADR-006 Post-#65)
- **[`docs/adr/0001-0004-engine-portable-architecture.md`](../../docs/adr/0001-0004-engine-portable-architecture.md)** — the architectural foundation
- **[`docs/adr/0008-p-post-review-followup.md`](../../docs/adr/0008-p-post-review-followup.md)** — the post-review followup plan; §"What's Next" lists this example as the highest-leverage adoption unlock
- **[`docs/adr/0008-l-querybuilder.md`](../../docs/adr/0008-l-querybuilder.md)** — the QueryBuilder + 8 GAPs appendix (this example exercises the resolved GAPs)

## Comparison to the upstream semanticdf hospital template

The upstream `/tmp/semanticdf/examples/hospital/` uses a typed API (`SemanticDimension.of[T]("name")`, `groupByDimensions(d1, d2).aggregateMeasures(m1, m2).execute`) built on a `YamlLoader` + phantom-typed `Refs` system. The sm8 SDK is currently smaller (per ADR-008-P — many of the upstream's typed wrappers are post-v0.1.0 followups). This example uses the canonical pattern that the existing sm8 tests use (`Model.of(...)` + `provider.query(...)`) — same logical workflow, expressed with the public SDK surface available today.

The model YAMLs in `models/` document the target shape for the future sm8 ModelLoader YAML subset extension (per ADR-008-P §AR-P1-3 post-v0.1.0). The transforms / is_time_dimension / smallest_time_grain fields shown there are not yet honored by the current sm8 ModelLoader; for this example we express those concerns in Scala (e.g. `withColumn("los_days", datediff(...))` for the upstream's `transforms: los_days:` block).

## License

This example is part of the sm8 project. See the top-level [LICENSE](../../LICENSE) for terms.
