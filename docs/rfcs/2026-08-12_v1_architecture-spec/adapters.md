# Adapters

Companion doc to `semantic-layer-engine-architecture.md` (Sections 3, 7, 7a, 12). Read that first for the pipeline and core boundary.

## What an Adapter Is

An adapter connects the engine to one specific data source. It's the only piece of the system allowed to know about connection strings, drivers, API endpoints, or query dialects for that source.

```
Adapter:
  name: string
  connect(config) -> void
  query(semantic_query) -> raw_result
  schema() -> schema_object
```

An adapter never:
- knows about hooks, plugins, or the pipeline stages — it only implements its own contract
- imports another adapter
- performs caching, auth, or logging (those are hook responsibilities)

## Types of Adapters, by Source Category

| Type | Connects to | Example |
|---|---|---|
| **Database** | SQL/NoSQL databases | `PostgresAdapter`, `MongoAdapter` |
| **API / remote service** | REST, GraphQL, gRPC endpoints | `RestApiAdapter`, `GraphQLAdapter` |
| **File-based** | Local or object-storage files | `CsvAdapter`, `ParquetAdapter`, `S3Adapter` |
| **Streaming** | Message queues / event streams | `KafkaAdapter` |
| **In-memory / mock** | No external system — data held in process | `InMemoryAdapter` (used for tests and the reference example) |

Every adapter, regardless of category, implements the exact same contract — the category is a description of what's behind `connect()`/`query()`, not a different interface.

## Types of Adapters, by Capability

Orthogonal to the source category above — worth tracking so a coding agent doesn't assume every adapter can do everything:

| Capability | Meaning | Notes |
|---|---|---|
| **Read-only** | implements `query()`/`schema()` only | most analytical sources (data warehouses) |
| **Read-write** | also exposes a write path | only needed if the semantic layer supports writes; not in the base `Adapter` contract — extend it deliberately if required |
| **Sync** | `query()` blocks and returns directly | default assumption in the base contract |
| **Async** | `query()` returns a future/promise | language-dependent; the base contract above is written sync-style — translate per language idiom |

If a new capability type is needed (e.g. streaming results instead of one-shot `query()`), that's a contract change, which — per the architecture doc's Core Boundary rule — should be proposed and reviewed deliberately rather than added ad hoc by one adapter.

## Example

```
adapter InMemoryAdapter implements Adapter:
  name = "in_memory"
  data = {}

  connect(config):
    self.data = config.seed_data or {}

  query(semantic_query):
    table = semantic_query.table
    if table not in self.data:
      raise Error("unknown table: " + table)
    return self.data[table]

  schema():
    return { table_name: list(row_keys) for table_name, rows in self.data }
```

## Rules

1. **Errors propagate, never get swallowed.** An adapter's `query()` failing should raise, not return `null`/an empty result silently.
2. **`connect()` fails loud on bad config.** Silent no-ops on invalid config are explicitly disallowed by the conformance suite (architecture doc Section 12).
3. **`schema()` must reflect what `query()` can actually return** — it's the contract consumers use to know what's queryable, and conformance tests check the two stay consistent.
4. **An adapter that supports URL-based connection (Spark master URL, Trino JDBC URL, DuckDB path, HTTP endpoint, etc.) MUST expose a typed `realize(url: String): Option[MCPEngineProvider]` method on the concrete `MCPEngineProvider`** (added 2026-08-15, per ADR-006 Post-#65 Refinement). The deployment module calls this typed method — it does **not** reflect over the class to find a `(String)` ctor. The reflection pattern is a transitional workaround (PR #65); the durable shape is the typed `realize(url)` contract. Per-connector `realize()` validates its own URL grammar; the deployment module does NOT validate.

## Conformance

Every adapter — built-in or community — must pass the shared contract test suite in `/tests/contract` before it can be merged or trusted: valid connect succeeds, invalid connect raises clearly, query output matches schema, malformed queries raise rather than returning partial data. Full detail in the architecture doc, Section 12.

## Where Adapters Live

`/adapters` for built-in reference adapters; community/first-party adapters ship inside their own plugin package — see `plugins.md` and architecture doc Section 11 (Repo Structure).
