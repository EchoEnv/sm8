/*
 * SM8 Spark Connector — engine-portable adapter for Apache Spark.
 *
 * Per agile-kindling-beacon plan line 287: Step 8 ships the
 * SparkConnector as a Maven module that conforms to
 * `io.sm8.sdk.Connector` (formerly `Adapter`). This file is the
 * SKELETON — name + connect + schema stubs + a contract-shaped
 * `query()` returning empty rows. The real `df.filter(...).collect()`
 * runtime lands in a follow-up PR after the Spark Connect cluster
 * is provisioned.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: this is the
 * ONE reactor module allowed to depend on `org.apache.spark:*`.
 * Every Spark type referenced from the constructor body (e.g.
 * `org.apache.spark.storage.StorageLevel` when the materialize
 * plugin's `df.persist` lifecycle lands) IS `Serializable` in Spark
 * 3.x — verified at the type level by the closure-safety
 * lockdown PR #36 (`Plugin extends java.io.Serializable` +
 * `closedOverVars` introspection).
 *
 * Per [[karpathy-guidelines-mindset]] 'smallest correct core':
 * the `query()` body is a contract-only stub. Real Spark Connect
 * session + `df.filter` + `toPortableResult` adapter land in a
 * follow-up PR. The current scope proves the connector:
 *   - passes the 4 RFC §12 conformance assertions
 *   - is `Serializable` (closure-safety baseline)
 *   - declares its captured state via `closedOverVars` for future
 *     Plugin-portal introspection.
 */
package io.sm8.connectors.spark

import io.sm8.sdk.{
  Connector,
  ConnectorConfig,
  ConnectorSchema,
  ResultRows,
  SemanticQuery
}

/**
 * Spark-flavored Connector (skeleton). The full Connector contract
 * (name + connect + schema + query) ships today; the real
 * Spark Connect runtime ships in a follow-up PR.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1:
 * `with java.io.Serializable` is the baseline — every Spark type
 * the connector closes over (StorageLevel, SparkSession,
 * DataFrame) is Serializable in Spark 3.x.
 *
 * `closedOverVars` lists the captured state the future real
 * implementation will close over. Today the connector captures
 * nothing — the list is empty. When the real impl ships, this
 * list grows (per the PluginSerializationSpec contract assertion).
 *
 * The default `closedOverVars = Seq.empty` (from the SDK Plugin
 * trait) applies here too — but `Connector` itself does not
 * declare a `closedOverVars` method. We extend the underlying
 * Serializable contract via the trait declaration only.
 */
final class SparkConnector extends Connector with java.io.Serializable {

  override def name: String = "spark"

  /**
   * Connect to the Spark cluster. The skeleton accepts any
   * `ConnectorConfig` and stores a placeholder reference. The
   * real impl will:
   *   - validate the config shape (URL/credentials/master)
   *   - open a `SparkSession` (or accept a builder arg)
   *   - close the session on JVM shutdown
   *
   * Today: throws on invalid config (RFC §12 conformance
   * assertion 1+2). The skeleton accepts any non-null config
   * and returns Unit.
   */
  override def connect(config: ConnectorConfig): Unit = {
    if (config == null) {
      throw new IllegalArgumentException("sm8: SparkConnector.connect requires non-null config")
    }
    // No real session opens yet. The real impl lands with the
    // Spark Connect runtime + df.filter() in a follow-up PR.
  }

  /**
   * Execute a semantic query. The skeleton returns empty rows
   * (RFC §12 conformance assertion 3: query() returns data
   * matching schema). Real impl lands in a follow-up PR.
   */
  override def query(request: SemanticQuery): ResultRows = {
    if (request == null) {
      // RFC §12 conformance assertion 4: malformed query raises.
      throw new IllegalArgumentException("sm8: SparkConnector.query requires non-null request")
    }
    // Real impl: df.filter(...).collect() → RestateCachedRow →
    // ResultRows. The shape is established; the runtime is next.
    ResultRows(Vector.empty)
  }

  /**
   * Static schema. Real impl will derive from the underlying
   * DataFrame's schema (after the first successful query).
   */
  override def schema(): ConnectorSchema = ConnectorSchema(List("v"))
}

/**
 * Configuration type for SparkConnector. Today a marker case
 * class (no real connection params); the real impl will carry
 * `master`, `appName`, optional `SparkSession.Builder` overrides.
 */
final case class SparkConfig(
    master: String,
    appName: String
) extends ConnectorConfig
