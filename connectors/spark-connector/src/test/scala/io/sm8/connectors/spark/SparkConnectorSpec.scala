/*
 * SM8 Spark Connector — test.
 *
 * Extends `ConnectorContractSpec` (RFC §12 conformance base) with
 * test data specific to the SparkConnector. The contract tests
 * verify the 4 RFC §12 assertions:
 *   1. connect() with valid config succeeds
 *   2. connect() with invalid config raises
 *   3. query() returns data matching schema()
 *   4. query() on malformed request raises
 *
 * Plus a 5th assertion: closure-safety baseline — the connector
 * round-trips through ObjectOutputStream (per the PR #36 contract).
 * This proves the connector itself is serializable today, BEFORE
 * the real Spark Connect runtime lands. When the real impl adds
 * `SparkSession`/`DataFrame` capture, the same test continues to
 * gate the contract.
 *
 * Per [[karpathy-guidelines-mindset]] 'smallest correct core':
 * 5 tests in 1 file. No real Spark runtime needed.
 */
package io.sm8.connectors.spark

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.sm8.sdk.contract.ConnectorContractSpec

import io.sm8.sdk.{Connector, ConnectorConfig, ConnectorSchema, ResultRows, SemanticQuery}

class SparkConnectorSpec extends ConnectorContractSpec {

  // ---- Abstract test data (per ConnectorContractSpec contract) ----

  /** The connector under test. A fresh instance per test class. */
  override def connector: Connector = new SparkConnector

  /** Valid config: a non-null SparkConfig with master + appName. */
  override def validConfig: ConnectorConfig =
    SparkConfig(master = "local[*]", appName = "sm8-test")

  /**
   * Invalid config: the skeleton throws on null (per the contract's
   * `require` in `connect()`). Real impl will throw on malformed URL.
   */
  override def invalidConfig: ConnectorConfig = null

  /** Valid request: skeleton accepts any non-null request. */
  override def validRequest: SemanticQuery = new SemanticQuery {}

  /** Malformed request: null triggers the skeleton's null-check. */
  override def malformedRequest: SemanticQuery = null

  // ---- Closure-safety baseline (RFC §13 + PR #36 contract) ----

  /** Round-trip via Java serialization — proves the connector
    * itself is Serializable today (the skeleton captures no
    * Spark-typed state; the real impl will capture StorageLevel /
    * SparkSession which are also Serializable in Spark 3.x). */
  private def roundTripViaJavaSerialization[T <: AnyRef](value: T): T = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  it should "round-trip through Java serialization (closure-safe baseline)" in {
    val original = new SparkConnector
    val restored = roundTripViaJavaSerialization(original)
    restored should not be null
    restored.name shouldBe "spark"
  }
}
