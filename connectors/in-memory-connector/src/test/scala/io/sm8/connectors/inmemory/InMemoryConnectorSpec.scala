/*
 * SM8 in-memory Connector — conformance test.
 *
 * Extends `ConnectorContractSpec` and supplies the 5 abstract test
 * methods. This is the canonical pattern a real Connector follows.
 *
 * Per [[scala-data-driven-refactor-mindset]]: the seed data is a
 * typed `Map[String, GenericTable]`, not a `Map[String, Any]`.
 * Table shape is a sealed trait; GenericTable is the case class.
 */
package io.sm8.connectors.inmemory

import io.sm8.sdk._
import io.sm8.sdk.contract.ConnectorContractSpec

class InMemoryConnectorSpec extends ConnectorContractSpec {

  /** Seed: one table with two rows of typed Map data. */
  private val seedTable: GenericTable = GenericTable(
    columns = List("carrier", "flight_count"),
    rows    = Vector(
      Map("carrier" -> "AA", "flight_count" -> 5),
      Map("carrier" -> "UA", "flight_count" -> 3)
    )
  )

  /** The Connector under test — constructed with the seed. */
  override def connector: Connector =
    InMemoryConnector(tables = Map("flights" -> seedTable))

  /** Valid config — the seed matches. */
  override def validConfig: ConnectorConfig =
    InMemoryConfig(seed = Map("flights" -> seedTable))

  /** Invalid config — wrong type. */
  override def invalidConfig: ConnectorConfig =
    new ConnectorConfig {}

  /** Valid request — select a real table. */
  override def validRequest: SemanticQuery =
    SelectTable("flights")

  /** Malformed request — the sentinel case in the InMemoryQuery sealed trait. */
  override def malformedRequest: SemanticQuery =
    MalformedQuery
}