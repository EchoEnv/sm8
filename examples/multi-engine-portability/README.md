# sm8 multi-engine-portability example

One `Model`, two engines, one identical `QueryRequest` shape, two `PortableQueryResult`s. This is the sm8 **engine-portability claim made concrete** — the third axis the other two examples don't cover (hospital = cleansing + query on one engine; flight-delays = rollup freshness lifecycle on one engine; this one = **the same semantic model running against two different engines through the same wire format**).

## What you get

```
examples/multi-engine-portability/
├── README.md                          ← you are here
├── pom.xml                            ← standalone Maven (sm8-core + spark-connector + duckdb-connector)
├── data/
│   └── sales.csv                      ← 8 rows: product/region/units/unit_price
└── src/main/scala/com/example/multiengine/
    └── Main.scala                     ← ingest into both engines -> same DSL Model -> query both -> diff
```

## Run it (5 minutes)

### Prerequisites

- JDK 17, Maven 3.9+
- Spark 3.5.x (not required to be pre-installed — the `spark-sql` dep brings it in)
- DuckDB JDBC driver 1.5.5.1 (transitive via `duckdb-connector_2.13`; bundles native libs for linux_amd64, linux_arm64, osx_universal, windows_amd64)

### Step 1: install sm8 locally

```bash
cd /path/to/sm8
mvn -B -ntp -DskipTests install
```

This installs `sm8-core_2.13`, `spark-connector_2.13`, and `duckdb-connector_2.13` into `~/.m2/repository` at version `0.1.0-SNAPSHOT`.

### Step 2: run the example

```bash
cd examples/multi-engine-portability
mvn -B -ntp scala:run -DmainClass=com.example.multiengine.Main
```

You'll see all 5 steps run in sequence:

1. **INGEST** — the same `sales.csv` lands in both engines: a Spark temp view (`sales_spark`) and a DuckDB table (`sales_duckdb`) over a file-backed JDBC connection.
2. **DECLARE** — two `Model`s built via the SAME `ModelBuilder` DSL chain, differing only in `SourceRef.ByName` (each engine resolves its own physical table).
3. **REALIZE** — `SparkEngineProviderDescriptor.realize("local[*]")` + `DuckdbEngineProviderDescriptor.realize(jdbc:duckdb:...)` — two `EngineProvider`s behind one interface.
4. **QUERY** — both engines get the same `QueryRequest` shape.
5. **DIFF** — both results render via the portable `ResultValue` ADT and are compared as normalized row multisets.
The closing block then prints the `Model -> EngineProvider.query -> PortableQueryResult` contract summary with both engine identities (no `STEP 6` prefix — it is the run's closing banner, not a numbered step).

## Sample output (actual, captured 2026-09-17 from this example)

```
======================================================================
sm8 multi-engine-portability example — one Model, two engines
======================================================================
STEP 1: INGEST sales.csv into Spark (temp view) AND DuckDB (JDBC table)
  spark view 'sales_spark' rows: 8
  duckdb table 'sales_duckdb' seeded from data/sales.csv
STEP 2: DECLARE two Models (same DSL chain, per-engine SourceRef)
  spark model: sales -> ByName(None,None,sales_spark)
  duckdb model: sales -> ByName(None,None,sales_duckdb)
STEP 3: REALIZE engines (SparkEngineProviderDescriptor + DuckdbEngineProviderDescriptor)
  spark provider: spark
  duckdb provider: duckdb
STEP 4: QUERY both engines with the same QueryRequest shape
--- Q-spark: sales by product/region ---
  rows: 8
--- Q-duckdb: sales by product/region ---
  rows: 8
STEP 5: DIFF the two results (normalized row multiset)
  spark rows (normalized): 8
  duckdb rows (normalized): 8
  note: DuckDB v1 runs SELECT * (no semantic projection yet);
        Spark runs the full semantic query. The demonstrated
        portability contract is Model-in/PortableQueryResult-out.
  spark sample: gadget,north,42 | gadget,south,67 | gadget,west,91
  duckdb sample: gadget,north,42,49.5 | gadget,south,67,49.5 | gadget,west,91,49.5
  base rows: 8; spark aggregated rows: 8; duckdb raw rows: 8
STEP 6: WIRE FORMAT — the EngineProvider.portable contract
Engine-portable wire format demonstrated:
======================================================================
Engine-portable wire format demonstrated:
  Model (semantic layer)  -> EngineProvider.query -> PortableQueryResult
  spark provider:   spark 3.5.8
  duckdb provider:  duckdb duckdb-jdbc-1.5.5.1
Both consumed the SAME Model DSL shape and the SAME QueryRequest
shape; both returned the SAME PortableQueryResult wire type.
======================================================================
Multi-engine portability complete.
```

## What this exercises (a checklist for the reader)

| Concept | Where it shows up |
|---|---|
| Two engine connectors behind one `EngineProvider` interface | STEP 3 — `SparkEngineProviderDescriptor` + `DuckdbEngineProviderDescriptor` |
| The SAME `ModelBuilder` DSL chain produces a Model for either engine | STEP 2 — only `SourceRef.ByName` differs |
| The SAME `QueryRequest` shape flows into both engines | STEP 4 |
| Both engines return the SAME `PortableQueryResult` wire type | STEP 4/5 |
| Portable `ResultValue` ADT rendering (engine-agnostic value decode) | STEP 5 — `normalizedRows` |
| DuckDB file-backed JDBC seeding (raw-INSERT across the wire; the provider's lazy re-derive path exists in the connector but is not exercised here) | `seedDuckDb` |

## Architecture: where this example fits in the sm8 RFC §3 stack

```
┌──────────────────────────────────────────────────────┐
│ THIS EXAMPLE (examples/multi-engine-portability)     │  Consumer layer
│   - reads CSV into Spark + DuckDB                    │  (per RFC §3)
│   - builds the SAME Model via ModelBuilder DSL       │  Imports:
│   - queries BOTH engines through EngineProvider      │  - sm8-core
│   - diffs PortableQueryResults                       │  - spark-connector
│                                                      │  - duckdb-connector
└───────────────────┬──────────────────────────────────┘
                    │ Model, QueryRequest, PortableQueryResult
┌───────────────────▼──────────────────────────────────┐
│ spark-connector  │  duckdb-connector                 │  Adapter layer
│  (JVM distributed)│  (embedded columnar)              │  Each imports:
│                   │                                   │  - sm8-core only
└───────────────────┬──────────────────────────────────┘
                    │ EngineProvider, PortableQueryResult
┌───────────────────▼──────────────────────────────────┐
│ sm8-core   (the FROZEN Core — engine-portable SDK)    │  Core layer
│   - Model, SourceRef, QueryRequest                   │  Spark-free
│   - PortableQueryResult, ResultValue ADT             │  DuckDB-free
│   - EngineProvider contract                          │
└──────────────────────────────────────────────────────┘
```

The key property on display: **`sm8-core` knows nothing about Spark or DuckDB**; each connector knows nothing about the other; the example (a consumer) can hold both engines at once and switch between them without changing its semantic-layer code.

## Honest limitations

- **DuckDB's v1 provider runs `SELECT * FROM table`** — semantic-layer column projection (dimensions/measures) lands with the DuckDB semantic-bridge follow-up. The Spark provider DOES apply the semantic query (dimensions + measures + aggregation), so the two result SHAPES differ: Spark returns aggregated `(product, region, units_sold)` tuples; DuckDB returns the raw base rows. The portability contract being demonstrated is the WIRE FORMAT (`Model` in → `PortableQueryResult` out, same `EngineProvider` interface, same `ResultValue` decode path), not result-shape equality across engines at different maturity levels. The example's STEP 5 log states this explicitly.
- **Both engines happen to return 8 rows here** — Spark's un-grouped path returns one row per base row (a documented ADR-008-L GAP), so the counts coincide with DuckDB's raw scan. The comparison in the example is on normalized row multisets (order-independent), with a sanity bound (Spark aggregated rows ≤ base rows).
- **DuckDB seeding is wire-level** — the example INSERTs the CSV rows itself because DuckDB has no Spark reader. A production deployment would use DuckDB's own CSV reader (`read_csv_auto`) or an external table; the example keeps the seeding explicit so both engines demonstrably start from the same data.
- **File-backed DuckDB URL** (not `:memory:`) so a failed run leaves a readable artifact and the provider's lazy re-derive path (close + reopen) observes the same table.

## Related

- **[`sm8-core/.../engine/EngineProvider.scala`](../../sm8-core/src/main/scala/io/sm8/core/engine/EngineProvider.scala)** — the `EngineProvider` contract + `QueryRequest` + `PortableQueryResult`
- **[`connectors/spark-connector/.../SparkEngineProviderDescriptor.scala`](../../connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProviderDescriptor.scala)** — the Spark adapter
- **[`connectors/duckdb-connector/.../DuckdbEngineProviderDescriptor.scala`](../../connectors/duckdb-connector/src/main/scala/io/sm8/connectors/duckdb/DuckdbEngineProviderDescriptor.scala)** — the DuckDB adapter
- **`examples/hospital-cleaning`** — single-engine (Spark) cleansing + query example
- **`examples/flight-delays`** — single-engine (Spark) rollup freshness lifecycle example
