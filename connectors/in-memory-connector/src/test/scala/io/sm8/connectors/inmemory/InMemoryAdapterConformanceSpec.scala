/*
 * SM8 in-memory connector — shared adapter conformance suite.
 *
 * Extends the unified `AdapterConformanceSpec` from sm8-core's
 * test-jar. The in-memory engine has no URL grammar (it's always
 * realized), so `hasUrlGrammar = false` and the blank/null assertion
 * branch is skipped. All other §12 checks apply normally.
 *
 * The "determinism" check uses the in-memory engine's `emptyModel`
 * fixture + a `QueryRequest` matching that model; `query` returns
 * the canonical empty `PortableQueryResult` with `engine = "in-memory"`
 * metadata. Deterministic by construction.
 */
package io.sm8.connectors.inmemory

import io.sm8.core.engine.{EngineUrl, QueryRequest, TypedRealizationProvider}
import io.sm8.sdk.contract.AdapterConformanceSpec

class InMemoryAdapterConformanceSpec extends AdapterConformanceSpec {

  /** The ServiceLoader descriptor under test (always-realized).
    *
    * @return the in-memory descriptor
    */
  override def descriptor: TypedRealizationProvider = new InMemoryEngineProviderDescriptor()

  /** Wire-stable name matching [[InMemoryEngineConstants]].
    *
    * @return "in-memory"
    */
  override def wireName: String = "in-memory"

  /** Any non-null string realizes; the value is opaque.
    *
    * @return a placeholder URL (never parsed)
    */
  override def validUrl: String = "in-memory"

  /** No grammar → nothing to reject.
    *
    * @return empty — nothing to reject
    */
  override def invalidUrls: Seq[String] = Seq.empty

  /** Spark URL is foreign to this engine.
    *
    * @return a Spark EngineUrl the in-memory descriptor must reject
    */
  override def foreignEngineUrl: EngineUrl = EngineUrl.Spark("local[*]")

  /** Always-realized engine: no blank/null grammar to check.
    *
    * @return false — skip the blank/null rejection assertions
    */
  override def hasUrlGrammar: Boolean = false

  /** Empty model + matching request; the engine answers with the
    * canonical empty result, deterministically. */
  override def wellFormedQuery = {
    val m = emptyModel("in-memory", "conformance")
    (m, QueryRequest(model = m.name))
  }
}
