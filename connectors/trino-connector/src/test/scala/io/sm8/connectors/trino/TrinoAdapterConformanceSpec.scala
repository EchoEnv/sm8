/*
 * SM8 Trino connector — shared adapter conformance suite.
 *
 * Extends the unified `AdapterConformanceSpec` from sm8-core's
 * test-jar. Trino has a real URL grammar (`jdbc:trino://...`); the
 * spec exercises blank/null rejection, grammar-invalid rejection
 * (non-`jdbc:trino://` URLs), foreign-EngineUrl typed rejection
 * (`EngineUrl.Spark` → typed `EngineError.ConnectionFailed`), and
 * the determinism contract on a stub-stub query that returns
 * `EngineError.FeatureDeferred` (the connector's documented
 * behavior at this stage of provisioning).
 */
package io.sm8.connectors.trino

import io.sm8.core.engine.{EngineUrl, QueryRequest, TypedRealizationProvider}
import io.sm8.sdk.contract.AdapterConformanceSpec

class TrinoAdapterConformanceSpec extends AdapterConformanceSpec {

  /** The ServiceLoader descriptor under test (URL-grammar validating).
    *
    * @return the trino descriptor
    */
  override def descriptor: TypedRealizationProvider = new TrinoEngineProviderDescriptor()

  /** Wire-stable name matching [[TrinoEngineConstants]].
    *
    * @return "trino"
    */
  override def wireName: String = "trino"

  /** A grammar-valid JDBC URL.
    *
    * @return a well-formed jdbc:trino URL
    */
  override def validUrl: String = "jdbc:trino://localhost:8080"

  /** Non-`jdbc:trino://` URLs and a scheme-only URL the grammar rejects.
    *
    * @return the rejection corpus for the trino grammar
    */
  override def invalidUrls: Seq[String] = Seq(
    "http://not-a-jdbc-url",
    "jdbc:mysql://wrong-engine",
    "jdbc:trino:"           // scheme prefix without authority
  )

  /** Spark URL is foreign to this engine.
    *
    * @return a Spark EngineUrl the trino descriptor must reject
    */
  override def foreignEngineUrl: EngineUrl = EngineUrl.Spark("local[*]")

  /** Trino is a stub until the cluster is provisioned: query returns
    * `Left(FeatureDeferred)`. The base asserts the typed-error +
    * determinism contract in that case instead of well-formedness.
    *
    * @return false — the stub surfaces FeatureDeferred
    */
  override def querySucceeds: Boolean = false

  /** Empty model + matching request; the stub answers with a typed
    * deferred error, deterministically. */
  override def wellFormedQuery = {
    val m = emptyModel("trino", "conformance")
    (m, QueryRequest(model = m.name))
  }
}
