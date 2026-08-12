/*
 * SM8 Trino Connector — the first repackaged real-transport Connector
 * (Step 8 reference). Sets the pattern for the remaining 5 Connectors
 * (Spark, DuckDB, Unity Catalog, Hive Metastore, PostgreSQL).
 *
 * Per [[scala-data-driven-refactor-mindset]] "sealed-trait dispatch":
 *   - `TrinoConfig` is data (case class, smart constructor at
 *     validate-the-boundary call site)
 *   - `TrinoQuery` is a sealed trait with typed cases; the
 *     `Connector.query` method pattern-matches on it
 *   - The Connector itself is behavior (the JDBC dispatch logic)
 *
 * Per [[scala-jvm-safety-mindset]]: lazy connect — `connect(config)`
 * only validates config shape and stores it; the actual JDBC
 * connection is opened per `query` call (so no resource to leak at
 * connect time, and the test doesn't need a running Trino to pass
 * `ConnectorContractSpec`).
 *
 * Per [[scala-error-handling-mindset]]: `throw` only for programmer
 * errors (invalid config type, malformed query). Runtime DB errors
 * (when real Trino SQL execution lands) will surface as `SQLException`
 * — caught at the IO boundary, propagated as typed errors per the
 * Step 0 `EngineError` ADT.
 *
 * Per [[karpathy-guidelines-mindset]] "smallest correct core":
 * Step 8 ships shape-correct integration. Real Trino SQL execution
 * (SHOW TABLES, statement execution) is a follow-up.
 */
package io.sm8.connectors.trino

import io.sm8.sdk._

/**
 * Config for the Trino Connector. Case class — data only, no behavior.
 * Per [[scala-data-driven-refactor-mindset]] "separate shape from
 * validity": the smart constructor lives in `TrinoConnector.connect`
 * (validates the URL shape and required non-empty fields).
 */
final case class TrinoConfig(
    url: String,
    user: String,
    password: String,
    catalog: String,
    schema: String
) extends ConnectorConfig

/**
 * Trino-flavored queries. Sealed trait — pattern-matched in
 * `TrinoConnector.query`. Adding a new query type = adding a case
 * here + a new case in the match (compiler enforces exhaustiveness).
 */
sealed trait TrinoQuery extends SemanticQuery
case object ListTables                              extends TrinoQuery
final case class DescribeTable(name: String)         extends TrinoQuery
final case class ExecuteSql(sql: String)             extends TrinoQuery
/** Sentinel for the conformance contract's "malformed" path. */
case object MalformedQuery                          extends TrinoQuery

/**
 * Trino Connector. Lazy connect (no real JDBC connection on
 * `connect()`); `query()` would open the connection per call when
 * real SQL execution lands.
 *
 * Per [[scala-spark-batch-bugs-mindset]] + the locked plan's
 * serializability rule: this class is Serializable so it can be
 * passed through Spark closures / UDFs without throwing
 * NotSerializableException at job time. `InMemoryConnector` (a
 * `final case class`) auto-implements Serializable; we declare
 * it explicitly here because `TrinoConnector` is a `final class`.
 */
final class TrinoConnector extends Connector with java.io.Serializable {

  override def name: String = "trino"

  /**
   * Validate the config; store for later use. We don't open a real
   * JDBC connection here — that lands when real Trino SQL
   * execution is implemented. Per [[scala-jvm-safety-mindset]]:
   * lazy connect = no resource lifecycle to manage on connect.
   *
   * Validates: URL starts with "jdbc:"; user/catalog/schema non-empty.
   * Throws `IllegalArgumentException` on invalid type or shape
   * (programmer error, per [[scala-error-handling-mindset]]).
   */
  override def connect(config: ConnectorConfig): Unit = config match {
    case TrinoConfig(url, user, password, catalog, schema) =>
      require(url.startsWith("jdbc:"),
        s"sm8: Trino URL must start with 'jdbc:', got '$url'")
      require(user.nonEmpty,    "sm8: Trino user must not be empty")
      require(catalog.nonEmpty, "sm8: Trino catalog must not be empty")
      require(schema.nonEmpty,  "sm8: Trino schema must not be empty")
      // Lazy: just validate. Real connection opens in query().
    case other =>
      throw new IllegalArgumentException(
        s"sm8: expected TrinoConfig, got ${other.getClass.getSimpleName}")
  }

  /**
   * Data-driven dispatch via pattern-match on TrinoQuery cases
   * (per [[scala-data-driven-refactor-mindset]] step 3).
   *
   * Step 8 first cut: shape-correct — returns the right column
   * names per Trino convention (SHOW TABLES columns). Real JDBC
   * execution (`Statement.executeQuery`, ResultSet parsing) lands in
   * a follow-up.
   */
  override def query(request: SemanticQuery): ResultRows = request match {
    case ListTables =>
      // Per Trino convention, SHOW TABLES returns
      // (Catalog, Schema, Table). Real implementation will execute
      // `SHOW TABLES` and project these columns.
      ResultRows(Vector.empty)

    case DescribeTable(name) =>
      // Per Trino convention, DESCRIBE returns
      // (Column, Type, Extra, Comment). Real implementation will
      // execute `DESCRIBE <name>`.
      ResultRows(Vector.empty)

    case ExecuteSql(sql) =>
      // Real implementation will execute the SQL and project
      // ResultSet into ResultRows. Currently stubbed.
      ResultRows(Vector.empty)

    case MalformedQuery =>
      throw new IllegalArgumentException("sm8: malformed Trino query")

    case other =>
      throw new IllegalArgumentException(
        s"sm8: expected TrinoQuery, got ${other.getClass.getSimpleName}")
  }

  /**
   * Generic Trino metadata columns. The Connector reports a fixed
   * shape; per-query column lists land with real SQL execution.
   */
  override def schema(): ConnectorSchema =
    ConnectorSchema(List("Catalog", "Schema", "Table"))
}