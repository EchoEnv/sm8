/*
 * SM8 Core — ConnectorContractSpec.
 *
 * Abstract base class for testing any `io.sm8.sdk.Connector`
 * implementation. Concrete Connector tests extend this and supply
 * test data (configs, queries, expected results).
 *
 * The 4 assertions below implement RFC §12 (the conformance contract):
 *   1. connect() with valid config succeeds (does NOT return silently
 *      on bad config — it raises; it does NOT silently no-op).
 *   2. connect() with invalid config raises a clear error.
 *   3. query() returns data matching schema() — the returned rows
 *      have no columns outside the schema and (if non-empty) all
 *      schema columns are present.
 *   4. query() on a malformed request raises — does NOT return
 *      partial or garbage data.
 *
 * Why abstract: per karpathy-app-design §5.3 ("The SDK contains only
 * types, not behavior"), contract tests must exist as types the
 * Connector author extends — they cannot be hidden inside the
 * Core. Each Connector ships its own test class that extends this
 * base.
 *
 * Pattern (used by every Connector's spec):
 *
 *   class InMemoryConnectorSpec extends ConnectorContractSpec {
 *     def connector: Connector = new InMemoryConnector
 *     def validConfig: ConnectorConfig = InMemoryConfig(seed = Map.empty)
 *     def invalidConfig: ConnectorConfig = InMemoryConfig(seed = null)
 *     def validRequest: SemanticQuery = ListTables
 *     def malformedRequest: SemanticQuery = BadQuery
 *     def rowsMatchSchema(rows: ResultRows, schema: ConnectorSchema): Boolean = ...
 *   }
 */
package io.sm8.sdk.contract

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import io.sm8.sdk.{Connector, ConnectorConfig, ConnectorSchema, ResultRows, SemanticQuery}

abstract class ConnectorContractSpec extends AnyFlatSpec with Matchers {

  // ---- Abstract test data — every concrete spec MUST supply these ----

  /** The Connector under test. */
  def connector: Connector

  /** A config that should succeed when passed to `connector.connect`. */
  def validConfig: ConnectorConfig

  /** A config that should raise when passed to `connector.connect`. */
  def invalidConfig: ConnectorConfig

  /** A query that should succeed when passed to `connector.query`. */
  def validRequest: SemanticQuery

  /** A query that should raise when passed to `connector.query`. */
  def malformedRequest: SemanticQuery

  /**
   * True iff `rows` are well-formed against `schema` per this
   * Connector's semantics. Default: every row's keys are a subset of
   * `schema.columns` AND (if `rows` is non-empty) all schema columns
   * appear in at least one row. Override if a Connector has a richer
   * notion of "matches".
   */
  def rowsMatchSchema(rows: ResultRows, schema: ConnectorSchema): Boolean = {
    val rowKeys: Set[String] = rows.rows.flatMap(_.keys.toSet).toSet
    val schemaCols: Set[String] = schema.columns.toSet
    rowKeys.subsetOf(schemaCols) &&
      (rows.rows.isEmpty || rows.rows.forall(_.keys.toSet == schemaCols))
  }

  // ---- RFC §12 conformance assertions ----

  "Connector (RFC §12)" should "connect with valid config (no exception, no silent no-op)" in {
    noException should be thrownBy connector.connect(validConfig)
  }

  it should "raise on connect with invalid config (no silent no-op)" in {
    an [Exception] should be thrownBy connector.connect(invalidConfig)
  }

  it should "return data matching schema() on a valid query" in {
    val schema = connector.schema()
    val rows   = connector.query(validRequest)
    withClue(s"rows did not match schema ${schema}: ") {
      rowsMatchSchema(rows, schema) shouldBe true
    }
  }

  it should "raise on a malformed query (no partial / garbage data)" in {
    an [Exception] should be thrownBy connector.query(malformedRequest)
  }
}