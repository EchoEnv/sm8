/*
 * SM8 Trino Connector — conformance test.
 *
 * Extends `ConnectorContractSpec` and supplies the 5 abstract
 * methods. This is the canonical pattern a real Connector follows;
 * the next 5 Connectors (Spark, DuckDB, Unity Catalog, HMS,
 * PostgreSQL) will copy this shape.
 */
package io.sm8.connectors.trino

import io.sm8.sdk._
import io.sm8.sdk.contract.ConnectorContractSpec

class TrinoConnectorSpec extends ConnectorContractSpec {

  private val trino = new TrinoConnector

  override def connector: Connector = trino

  /** Valid: a real-shape Trino URL. `connect()` only validates shape
   *  (lazy), so no actual JDBC connection is opened here — the test
   *  passes without a running Trino. */
  override def validConfig: ConnectorConfig =
    TrinoConfig(
      url      = "jdbc:trino://localhost:8080",
      user     = "sm8",
      password = "",
      catalog  = "hive",
      schema   = "default"
    )

  /** Invalid: a non-TrinoConfig type. `connect()` must raise. */
  override def invalidConfig: ConnectorConfig = new ConnectorConfig {}

  /** Valid: ListTables is a well-formed TrinoQuery. */
  override def validRequest: SemanticQuery = ListTables

  /** Malformed: the sentinel. `query()` must raise. */
  override def malformedRequest: SemanticQuery = MalformedQuery
}