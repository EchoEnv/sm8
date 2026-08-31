# Cross-engine conformance matrix

> **Status**: this matrix documents the conformance surface of every engine connector in the v1 architecture spec
> (RFC `docs/rfcs/2026-08-12_v1_architecture-spec/`). The shared base
> defines 5 mechanical checks and 9 abstract members that the 4 concrete
> connector specs + 4 identity-invariant specs each specialize. If you
> add a 5th reference engine, copy the bottom row's column shape verbatim.

## 1. The shared base

The shared abstract base lives in `sm8-core` (test-jar) at
`sm8-core/src/test/scala/io/sm8/sdk/contract/AdapterConformanceSpec.scala`.
It defines **5 mechanical checks** every connector inherits by
structural override:

| # | Check | What it asserts |
|---|---|---|
| 1 | `should "carry the wire-stable engine name on the realized provider"` | `realize(validUrl).get.identity.name == wireName` |
| 2 | `should "reject blank and null URLs"` | `realize("")`, `realize("   ")`, `realize(null)` all return `None` (skipped when `hasUrlGrammar = false`) |
| 3 | `should "reject grammar-invalid URLs with None"` | every URL in `invalidUrls` returns `None` |
| 4 | `should "reject a foreign EngineUrl case via typed realization"` | `realizeTyped(foreignEngineUrl)` returns `Left(EngineError.ConnectionFailed)` named with the engine label (never a silent `None`) |
| 5 | `should "honor the query contract deterministically (well-formed result, or typed deferred error for stub engines)"` | two calls of `provider.query(model, request, ctx)` agree on `(schema, metadata)` (schema + metadata equality — NOT full row equality, because `collect()` has no `ORDER BY`); if `querySucceeds = true`, the result is a well-formed `Right(PortableQueryResult)`; if `querySucceeds = false`, the result is a typed `Left(EngineError.FeatureDeferred)` |

### Abstract members each connector must supply

| Member | Type | Default |
|---|---|---|
| `descriptor` | `TypedRealizationProvider` | required (no default) |
| `wireName` | `String` | required (the engine's wire-stable name) |
| `validUrl` | `String` | required (a grammar-valid connection URL) |
| `invalidUrls` | `Seq[String]` | required (URLs the grammar must reject) |
| `foreignEngineUrl` | `EngineUrl` | required (a URL of a DIFFERENT engine — must be rejected) |
| `hasUrlGrammar` | `Boolean` | `true` |
| `wellFormedQuery` | `(Model, QueryRequest)` | required (a query the engine accepts) |
| `querySucceeds` | `Boolean` | `true` |
| `queryContext` | `EngineContext` | `EngineContext.defaultContext` |

## 2. Per-connector conformance

Each connector ships one `*AdapterConformanceSpec` extending the
shared base. This table is the source of truth — when a connector
diverges, document it here.

| Connector | `wireName` | `validUrl` | `hasUrlGrammar` | `querySucceeds` | `wellFormedQuery` fixture | `invalidUrls` (representative) |
|---|---|---|---|---|---|---|
| `in-memory-connector` | `in-memory` | `"in-memory"` (any non-null) | `false` (no grammar) | `true` | `emptyModel("in-memory", "conformance")` (no DB) | (empty — no grammar to reject; `InMemoryEngineUrlParser` is registered as SPI but accepts any non-null string) |
| `trino-connector` | `trino` | `"jdbc:trino://localhost:8080"` | `true` | `false` (stub returns `FeatureDeferred`) | `emptyModel("trino", "conformance")` | `"http://not-a-jdbc-url"`, `"jdbc:mysql://wrong-engine"`, `"jdbc:trino:"` (no authority) |
| `spark-connector` | `spark` | `"local[*]"` | `true` (master URIs are free-form; only blank/null rejected) | `true` | Live `local[*]` session + registered `conformance_people` temp view | (empty — no grammar reject besides blank/null) |
| `duckdb-connector` | `duckdb` | `"jdbc:duckdb:<tmpfile>"` (shared file-backed, see note 1) | `true` | `true` | File-backed DuckDB + `CREATE OR REPLACE TABLE conformance_people` | `"http://not-a-jdbc-url"`, `"jdbc:mysql://wrong-engine"`, `"jdbc:trino://localhost:8080"`, `"jdbc:duckdb"` (no colon) |

**Note 1 (DuckDB shared-file design)**: the conformance base calls `descriptor.realize(validUrl).get` in its determinism test, which creates a *fresh* in-memory DuckDB per realization. The DuckDB spec uses a SHARED file-backed URL (`jdbc:duckdb:<tmpfile>`) so the base's separate realization re-opens the same file and the conformance table seeded by `wellFormedQuery` is visible to it. `afterAll` closes every realized provider (tracked in a `ListBuffer`) and deletes the temp file.

**Note 2 (in-memory `hasUrlGrammar = false`)**: the in-memory engine has no URL grammar. It accepts any non-null string and always realizes. The blank/null branch is skipped (per the `if (hasUrlGrammar)` guard in the base).

**Note 3 (trino `querySucceeds = false`)**:
the trino connector is currently a stub pre-cluster-provisioning
and returns `Left(EngineError.FeatureDeferred)`; the conformance
base's `if/else` branch flips the assertion from well-formedness
to typed-error contract. The in-memory + spark + duckdb providers
execute real queries and return `Right(PortableQueryResult)`.

## 3. `EngineUrl` sealed cases

`sm8-core/.../EngineUrl.scala` is sealed. External connectors do NOT
extend the trait — they use the `EngineUrlParser` SPI to validate
their grammar and realize against the existing 4 cases. The
DuckDB engine is the documented exception: a 4th case `DuckDb`
because its URL must carry the wire-stable name `duckdb` on the
typed carrier (the `InMemory(seed)` slot would have lied about the
engine name `in-memory` on every cross-engine audit event).

| Case | Field | `engineName` | Conveyed URL examples |
|---|---|---|---|
| `EngineUrl.Spark(master: String)` | Spark master URL | `"spark"` | `local[*]`, `spark://host:7077`, `spark-connect://host:15002` |
| `EngineUrl.Trino(jdbcUrl: String)` | JDBC-style URL | `"trino"` | `jdbc:trino://host:8080/catalog/schema` |
| `EngineUrl.InMemory(seed: Option[String])` | optional seed (legacy / test) | `"in-memory"` | `"in-memory"`, or `"seed-string"` |
| `EngineUrl.DuckDb(jdbcUrl: String)` | DuckDB JDBC URL | `"duckdb"` | `jdbc:duckdb:`, `jdbc:duckdb:/path/to/db.duckdb` |

## 4. Per-connector provider + parser + identity-invariant shapes

Every reference engine has the same five-piece shape. A new engine
that follows this template slots into the conformance base with no
core change (unless the URL needs a new wire-stable name on the
typed carrier — see §3 above for when that's required).

| Connector | `EngineProvider` | `EngineProviderDescriptor` | `EngineUrlParser` | `EngineIdentityInvariantSpec` |
|---|---|---|---|---|
| `in-memory-connector` | `InMemoryEngineProvider` | `InMemoryEngineProviderDescriptor` | `InMemoryEngineUrlParser` (grammar-free — accepts any non-null) | `InMemoryEngineIdentityInvariantSpec` |
| `trino-connector` | `TrinoEngineProvider` | `TrinoEngineProviderDescriptor` | `TrinoEngineUrlParser` | `TrinoEngineIdentityInvariantSpec` |
| `spark-connector` | `SparkEngineProvider` | `SparkEngineProviderDescriptor` | `SparkEngineUrlParser` | `SparkEngineIdentityInvariantSpec` |
| `duckdb-connector` | `DuckdbEngineProvider` | `DuckdbEngineProviderDescriptor` | `DuckdbEngineUrlParser` | `DuckdbEngineIdentityInvariantSpec` |

Every `*EngineIdentityInvariantSpec` pins three things:
1. The realized provider's `identity.name` equals `EngineXxxConstants.WireName`.
2. The URL carrier's `engineName` matches.
3. The descriptor's identity carries the `UnrealizedNativeVersion` sentinel for engines with that constant (spark / trino / duckdb). The in-memory engine has no `UnrealizedNativeVersion` constant — its `nativeVersion` is the single literal `"embedded"` because there's no remote to realize against; the invariant spec asserts the realized provider's `identity.name` only, not a UnrealizedNativeVersion invariant.

## 5. Deep specs (per-engine, NOT in the conformance base)

Beyond the 5 shared checks, each connector ships deep specs for
engine-specific behavior. These are NOT mechanically inherited —
a new connector must port the relevant subset for its engine.

| Connector | Total specs | Conformance | IdentityInvariant | Other deep specs (purpose) |
|---|---|---|---|---|
| `in-memory-connector` | 6 | 1 | 1 | 4 (Descriptor, Realize, Provider, ReplaySafety) |
| `trino-connector` | 6 | 1 | 1 | 4 (Descriptor, Realize, Provider, ReplaySafety) |
| `spark-connector` | 32 | 1 | 1 | 30 (PortableQueryCompiler family, SparkFilter, SortBy, Window, Aggregate, TypeBridge, SourceResolver, etc.) |
| `duckdb-connector` | 3 | 1 | 1 | 1 (Provider — covers SQL round-trip, typed-row decode, determinism, missing-table, non-ByName, close idempotence, Java-serialization round-trip, parser) |

The 5-shared-checks conformance is the floor. Spark has 30 deep
specs because the engine has 30 specific things to verify
(pushdown, broadcast, windowing, etc.). DuckDB has 1 because the
v1 surface is a single SELECT path. The conformance base gives every
connector the same minimum; the deep-spec surface is engine-specific.

## 6. How to add a 5th reference engine (template)

1. **Copy the bottom row's column shape** in §2 verbatim — pick your
   `wireName`, `validUrl`, `hasUrlGrammar`, `querySucceeds`,
   `wellFormedQuery` fixture shape.
2. **Ship the 7-file connector** (mirrors the other 4):
   - `pom.xml` with `enforce-no-spark` (blocks `org.apache.spark:*`
     at the reactor level) + the JDBC/native driver dep + `sm8-core`
     test-jar dep for the conformance base
   - `XxxEngineConstants.scala` (single source of truth for
     `WireName`, `AdapterVersion`, `UnrealizedNativeVersion` sentinels)
   - `XxxEngineUrlParser.scala` extending `EngineUrlParser` (skip
     this if your engine has no URL grammar, like in-memory)
   - `XxxEngineProviderDescriptor.scala` extending
     `TypedRealizationProvider` (the ServiceLoader-discoverable entry)
   - `XxxEngineProvider.scala` extending `TypedRealizationProvider`
     (the realized provider with a `connectionOrNull`-style lazy
     resource initializer; `@transient` on captured non-serializable
     resources; double-checked locking for first-use race safety)
   - `XxxAdapterConformanceSpec.scala` extending the shared base
   - `XxxEngineIdentityInvariantSpec.scala` mirroring the 4
     existing ones
3. **SPI registrations**: 2 files in
   `src/main/resources/META-INF/services/` —
   `io.sm8.core.engine.EngineProvider` and (if your engine has a
   grammar) `io.sm8.core.engine.EngineUrlParser`.
4. **Root pom**: add `<module>connectors/xxx-connector</module>` in
   the right order (after the existing connectors, before plugins).
5. **Core change (only if you need a wire-stable name on the
   carrier)**: if `InMemory(seed)` or any other existing case can
   carry your URL without losing the `engineName`, you don't need
   this. If you do, add a new `final case class Xxx(url: String)
   extends EngineUrl` to `EngineUrl.scala`. 1-arg final case class
   shape = JVM classfile ABI stable (mirrors the existing `Trino` /
   `DuckDb` cases).
6. **Run**: `mvn -pl connectors/xxx-connector -am test`. The
   conformance + invariant specs run together; 5 conformance + 4
   to 6 invariant tests per connector (in-memory 4, trino 5, spark
   5, duckdb 6 — variance reflects how many invariants each
   engine has to express, not a template floor).

## 7. What the matrix does NOT guarantee (and why)

- **Schema-only shape check, NOT per-cell type fidelity.** The
  conformance base (`AdapterConformanceSpec.scala`) checks
  `isWellFormed` (row value count == schema field count) and
  `(schema, metadata)` equality across two calls. It does NOT
  verify that a column typed `SealedDataType.Int` decodes to
  `ResultValue.IntV` — the value decoder is engine-specific. The
  DuckDB v1 decoder (`DuckdbEngineProvider.query` line ~224) maps
  every JDBC value to `ResultValue.StringV(String.valueOf(raw))`
  — schema carries the typed `SealedDataType` (via `sqlTypeToSealed`)
  but values are strings, so a typed consumer will hit a runtime
  mismatch if it relies on the cell type. A typed-decoder
  (returning `IntV` / `DecimalV` / `BoolV` etc.) lands on a
  follow-up. The matrix documents the SHAPE contract, not the
  DECODER contract.
- **DuckDB in-memory mode is all-on-heap.** The
  `DuckdbEngineProvider` header warns explicitly that
  `jdbc:duckdb:` (the default in-memory form) loads ALL data
  into the JVM heap. For production-scale datasets use a
  file-backed URL (`jdbc:duckdb:/path/to/db.duckdb`) so DuckDB
  can page to disk. The conformance spec uses a file-backed URL
  to share the `conformance_people` table across realizations
  (see §2 Note 1).
- **Plugin / hook layer is out of scope.** The adapter conformance
  is for `EngineProvider` + `EngineUrl`; hook/plugin conformance
  uses a different shared base (`HookContractSpec` +
  `PluginContractSpec`, the plugin-side unification).
- **DuckDB performance at scale is not in the conformance base.**
  Tests run on 2-row inserts; 10M-row inserts need the
  file-backed variant and a separate scale spec (the spark-connector
  has `SparkConnectorBigDataScaleSpec` for that role; equivalent
  for DuckDB is a future addition).

## 8. Cross-references

- `docs/rfcs/2026-08-12_v1_architecture-spec/semantic-layer-engine-architecture.md`
  — the v1 architecture spec, §3 (core boundary), §12 (adapter
  conformance testing)
- `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md` — Rule 4
  (per-connector URL grammar) and the realization contract
- `sm8-core/src/test/scala/io/sm8/sdk/contract/AdapterConformanceSpec.scala`
  — the shared base (5 mechanical checks, 9 abstract members)
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineUrl.scala`
  the 4 sealed cases
- `sm8-core/src/main/scala/io/sm8/core/engine/TypedRealizationProvider.scala`
  + `sm8-core/src/main/scala/io/sm8/core/engine/EngineProvider.scala`
  — the typed-realization + provider contracts
- The conformance base + 4 concrete specs (in-memory, trino, spark, duckdb) and the 4th sealed `EngineUrl.DuckDb` case were added together to the v1 conformance surface; this matrix is the consolidating doc.
