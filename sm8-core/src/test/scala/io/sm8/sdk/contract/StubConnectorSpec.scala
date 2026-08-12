/*
 * SM8 Core — StubConnectorSpec.
 *
 * Concrete test that extends `ConnectorContractSpec` and proves the
 * 4 RFC §12 conformance assertions work end-to-end against a
 * minimal `Connector` implementation. Acts as the reference pattern
 * for real Connectors (`InMemoryConnector` in Step 3, `SparkConnector`
 * in Step 8, etc.).
 *
 * If this test fails, the conformance contract is broken — fix the
 * contract base, not this stub.
 */
package io.sm8.sdk.contract

import io.sm8.sdk.{Connector, ConnectorConfig, ConnectorSchema, ResultRows, SemanticQuery}

// ---- Test data — local to this spec ----

/** Local config type — distinguishes "should succeed" from "should raise". */
final case class StubConfig(valid: Boolean) extends ConnectorConfig

/** Local query type — distinguishes "well-formed" from "malformed". */
final case class StubQuery(malformed: Boolean) extends SemanticQuery

// ---- The stub Connector ----

/**
 * Minimal `Connector` with predictable, controllable behavior. Used
 * only to prove the contract base's assertions fire correctly.
 *
 * Real Connectors replace this with a real implementation
 * (`InMemoryConnector`, `SparkConnector`, etc.).
 */
final class StubConnector extends Connector {
  override def name: String = "stub"

  override def connect(config: ConnectorConfig): Unit = config match {
    case StubConfig(true)  => ()                 // success
    case StubConfig(false) => throw new IllegalArgumentException("stub: invalid config")
    case other             => throw new IllegalArgumentException(
      s"stub: unexpected config type ${other.getClass.getSimpleName}")
  }

  override def query(request: SemanticQuery): ResultRows = request match {
    case StubQuery(false) => ResultRows(Vector(Map("id" -> 1, "value" -> "ok")))
    case StubQuery(true)  => throw new IllegalArgumentException("stub: malformed query")
    case other            => throw new IllegalArgumentException(
      s"stub: unexpected query type ${other.getClass.getSimpleName}")
  }

  override def schema(): ConnectorSchema =
    ConnectorSchema(List("id", "value"))
}

// ---- The contract test ----

/**
 * Extends `ConnectorContractSpec` and supplies the 5 abstract
 * methods. Each Connector's real test class follows this same shape.
 */
class StubConnectorSpec extends ConnectorContractSpec {

  override def connector: Connector = new StubConnector

  override def validConfig: ConnectorConfig     = StubConfig(valid = true)
  override def invalidConfig: ConnectorConfig   = StubConfig(valid = false)
  override def validRequest: SemanticQuery      = StubQuery(malformed = false)
  override def malformedRequest: SemanticQuery  = StubQuery(malformed = true)
}