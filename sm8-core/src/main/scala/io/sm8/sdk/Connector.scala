/*
 * SM8 SDK — Connector.
 *
 * A Connector connects the engine to one specific data source. It is the
 * only piece of the system allowed to know about connection strings,
 * drivers, API endpoints, or query dialects for that source.
 *
 * Vocabulary note: this trait was named `Adapter` in RFC §7 + adapters.md
 * (2026-07). SM8 locked the rename to `Connector` (vocabulary Option Y)
 * because:
 *   1. Every Connector in SM8 is an external integration (industry-
 *      standard "Connector" term, e.g. Mulesoft / Airbyte / Kafka Connect).
 *   2. Consistency between folder name (`connectors/`), Maven coords
 *      (`io.sm8:*-connector_2.13`), RFC doc (`connectors.md`), and the
 *      Protocol trait name itself.
 *
 * Per RFC §12 conformance (also enforced by `ConnectorContractSpec` in
 * Step 2): every Connector must pass four assertions —
 *   1. connect() with valid config succeeds (does NOT return silently)
 *   2. connect() with invalid config raises a clear error
 *   3. query() returns data matching schema()
 *   4. query() on a malformed request raises (does NOT return garbage)
 *
 * Frozen after Step 1. Any change to the contract methods is a breaking
 * SDK change.
 */
package io.sm8.sdk

/**
 * A Connector connects the engine to one external data source.
 *
 * @deprecated This trait is the legacy Connector abstraction from the
 * initial vocabulary (pre-PR-O3). The current production abstraction is
 * `MCPEngineProvider` (per ADR-001 §P1-3 + ADR-006 Post-#65). The
 * spark-connector still uses `Connector` for its `SparkConnector`
 * skeleton (PR-A), but the real production wiring goes through
 * `MCPEngineProvider`. Per ADR-008-P §AR-P1-7, this trait is
 * deprecated and will be removed in v1.0.0. Plugin authors should
 * implement `MCPEngineProvider` instead; `EngineHookDispatcher`
 * wiring is unchanged.
 *
 * Connector authors should:
 *   - never import another Connector (RFC adapters.md Rule — only Plugin
 *     can register a Connector);
 *   - never swallow errors in query() (RFC adapters.md Rule 1);
 *   - keep schema() honest — it must reflect what query() can return
 *     (RFC adapters.md Rule 3, conformance-enforced);
 *   - never mutate Context (that's a Hook's job).
 */
@deprecated("Use MCPEngineProvider instead (ADR-008-P §AR-P1-7); this trait is retained for SparkConnector (PR-A) only and will be removed in v1.0.0", "0.1.0")
trait Connector {

  /**
   * Unique name for this Connector instance within the engine's
   * ConnectorRegistry. Convention: `<source>-<flavor>`, e.g.
   * `spark-3.5`, `trino`, `duckdb`. RFC adapters.md.
   */
  def name: String

  /**
   * Establish a connection to the data source. Called once at engine
   * startup (or lazily on first query, per Connector implementation).
   *
   * MUST raise on invalid config (no silent no-op). MUST NOT return
   * until the connection is usable (or the raise has happened).
   *
   * Per Step-3 plan: signature will tighten to
   * `Either[EngineError, Unit]` once EngineError moves in Step 0. For
   * Step 1 we keep the RFC shape (Unit return, raise on failure).
   */
  def connect(config: ConnectorConfig): Unit

  /**
   * Execute a semantic query against the connected source.
   *
   * MUST raise on malformed input. MUST return data that matches
   * `schema()`. MUST NOT return partial or garbage data on failure.
   *
   * Per Step-3 plan: signature will tighten to
   * `Either[EngineError, ResultRows]` once EngineError moves in.
   */
  def query(request: SemanticQuery): ResultRows

  /**
   * Describe what this Connector can answer. The engine uses this to
   * validate that a query against a Connector is well-formed before
   * `query()` runs. The schema must stay in sync with what `query()`
   * can actually return (conformance-enforced).
   */
  def schema(): ConnectorSchema
}

/**
 * Opaque Connector configuration. The full config shape (host, port,
 * credentials, transport options) lands in Step 3 when the Engine
 * skeleton is built. Concrete Connector test specs define their own
 * `case class` config subtypes (e.g. `InMemoryConfig`, `TrinoConfig`).
 *
 * NOT `sealed` — anyone (test stubs, Connectors, third-party Plugins)
 * may define a concrete subtype. Marker types are open by design.
 */
trait ConnectorConfig

/**
 * Opaque semantic query. The full shape (model name, measures,
 * dimensions, filters, etc.) lands in Step 3. Concrete Connector
 * test specs define their own `case class` query subtypes (e.g.
 * `ListTables`, `AggregateByCarrier`). Not sealed.
 */
trait SemanticQuery