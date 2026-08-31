/*
 * SM8 Core — shared adapter conformance suite (base class).
 *
 * Every connector — built-in or community — extends this base from
 * its own test sources and supplies its descriptor + the per-engine
 * conformance expectations. The assertions here are the mechanical
 * checks from the v1 architecture spec §12 ("Adapter Conformance
 * Testing"), adapted to the EngineProvider contract:
 *
 *   1. Routing invariant: the realized provider's identity.name is
 *      the wire-stable engine name (the registry routing key).
 *   2. URL grammar: blank and null URLs never realize a provider;
 *      a grammar-valid URL does.
 *   3. Typed realization: realizeTyped rejects foreign EngineUrl
 *      cases with a typed EngineError (never a silent None).
 *   4. Determinism + malformed-input: the same well-formed query
 *      twice yields the same result (replay/journal safety), and a
 *      query the engine cannot serve surfaces a typed EngineError —
 *      never partial or garbage data (spec §12 bullet 4).
 *
 * (§12's four bullets map onto these four checks; the spec's
 * "malformed semantic query raises" is covered by the typed-error
 * branch of check 4 for stub engines and by each connector's
 * engine-specific deep specs for live engines.)
 *
 * Per-connector expectations that CANNOT be shared live as abstract
 * members: `descriptor`, `wireName`, `validUrl` / `invalidUrls`,
 * `foreignEngineUrl`, and `wellFormedQuery` (a (Model, QueryRequest,
 * EngineContext) triple the connector knows its engine accepts).
 *
 * Mirrors the plugin-side unification (HookContractSpec /
 * PluginContractSpec): structural inheritance from this base is what
 * makes a connector's conformance mechanically checkable, not a
 * code-review judgement call.
 */
package io.sm8.sdk.contract

import io.sm8.core.engine.{EngineContext, EngineError, EngineProvider, EngineUrl, PortableQueryResult, QueryRequest, TypedRealizationProvider}
import io.sm8.core.model.{Model, SourceRef}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class AdapterConformanceSpec extends AnyFlatSpec with Matchers {

  // ---- Abstract members — every concrete connector spec MUST supply these ----

  /** The connector's ServiceLoader descriptor under test. Type is
    * `TypedRealizationProvider` because the conformance base exercises
    * `realizeTyped` (the typed-error path); concrete providers are
    * obtained via `descriptor.realize(validUrl).get`.
   *
   * @return the connector descriptor under test
   */
  def descriptor: TypedRealizationProvider


  /** The wire-stable engine name the realized provider must carry
    * (e.g. "spark", "trino", "in-memory").
    *
    * @return the registry routing key for this engine
    */
  def wireName: String

  /** A URL the connector's grammar accepts (may be null/blank for
    * engines with no URL grammar — override `hasUrlGrammar` to
    * signal that).
    *
    * @return a grammar-valid connection URL
    */
  def validUrl: String

  /** URLs the connector's grammar must reject (realize → None).
    *
    * @return the rejection corpus for the grammar check
    */
  def invalidUrls: Seq[String]

  /** An EngineUrl case belonging to a DIFFERENT engine — realizeTyped
    * must reject it with a typed error.
    *
    * @return a foreign-engine URL the descriptor must reject
    */
  def foreignEngineUrl: EngineUrl

  /** True iff this engine has a URL grammar (realize validates
    * strings). In-memory-style always-realized engines set false;
    * their realize() accepts anything.
    *
    * @return whether the blank/null URL rejection assertions apply
    */
  def hasUrlGrammar: Boolean = true

  /** A well-formed (model, request) pair the engine accepts: query
    * returns Right and the result is well-formed (every row passes
    * `PortableQueryResult.isWellFormed`).
    *
    * @return the (Model, QueryRequest) fixture for the query contract
    */
  def wellFormedQuery: (Model, QueryRequest)

  /** True iff the engine's `query` returns a well-formed `Right` for
    * `wellFormedQuery`. Engines still in provisioning (e.g. the Trino
    * stub pre-cluster) return a typed `EngineError.FeatureDeferred`
    * instead — override to `false` and the base asserts the deferred
    * error contract (typed, named engine, deterministic) instead of
    * the well-formedness contract.
    *
    * @return whether the well-formedness branch or the deferred-error
    *   branch of the query contract applies
    */
  def querySucceeds: Boolean = true

  /** The query context. Default: the engine-default context with no
    * decision hints. Override to inject connector-specific policies.
    *
    * @return the EngineContext passed to the query contract
    */
  def queryContext: EngineContext = EngineContext.defaultContext

  // ---- Shared fixtures ----

  /** Canonical empty model built on the named source, for engines
    * that need a Model to answer `query`. The first argument
    * documents the intended source catalog, but the production
    * smart constructor `SourceRef.byName(name, table)` currently
    * drops `name` — `catalog` lands as `None` (see Model.scala).
    * The parameter records intent for the day the 2-arg form
    * forwards it. */
  protected def emptyModel(catalog: String, sourceTable: String): Model =
    Model.of(
      name    = "conformance-model",
      version = 1,
      source  = SourceRef.byName(catalog, sourceTable)
    ).toOption.get

  // ---- Conformance assertions ----

  "Adapter conformance: routing invariant" should "carry the wire-stable engine name on the realized provider" in {
    val realized: Option[EngineProvider] = descriptor.realize(validUrl)
    withClue(s"realize($validUrl) must yield a provider for this engine") {
      realized shouldBe defined
    }
    realized.get.identity.name shouldBe wireName
  }

  it should "reject blank and null URLs" in {
    val cases: Seq[String] = if (hasUrlGrammar) Seq("", "   ", null) else Seq.empty
    cases.foreach { url =>
      withClue(s"realize($url) must be None (blank/null never realize)") {
        descriptor.realize(url) shouldBe None
      }
    }
  }

  it should "reject grammar-invalid URLs with None" in {
    invalidUrls.foreach { url =>
      withClue(s"realize($url) must be None (grammar mismatch)") {
        descriptor.realize(url) shouldBe None
      }
    }
  }

  it should "reject a foreign EngineUrl case via typed realization" in {
    val out = descriptor.realizeTyped(foreignEngineUrl)
    withClue("realizeTyped(foreign) must be Left with a typed EngineError (never silent None)") {
      out.isLeft shouldBe true
    }
    out.swap.toOption.get shouldBe an[EngineError]
    withClue("the typed error must name the engine that rejected it") {
      out.swap.toOption.get.engine shouldBe wireName
    }
  }

  it should "honor the query contract deterministically (well-formed result, or typed deferred error for stub engines)" in {
    val (model, request) = wellFormedQuery
    // Labelled guard before .get: if realize regresses to None, this
    // test fails with a clue instead of a NoSuchElementException.
    val realized = descriptor.realize(validUrl)
    withClue(s"realize($validUrl) must yield a provider for the query contract") {
      realized shouldBe defined
    }
    val provider: EngineProvider = realized.get

    val first  = provider.query(model, request, queryContext)
    val second = provider.query(model, request, queryContext)

    if (querySucceeds) {
      withClue("query on a well-formed input must succeed") {
        first.isRight shouldBe true
      }
      val result: PortableQueryResult = first.toOption.get
      withClue("every row of the result must be well-formed (schema-conformant)") {
        result.isWellFormed shouldBe true
      }
    } else {
      withClue("a stub engine must surface a typed EngineError, never a silent success") {
        first.isLeft shouldBe true
      }
      val err = first.swap.toOption.get
      withClue("the stub error must name the engine") {
        err.engine shouldBe wireName
      }
    }
    // Determinism is asserted on schema + metadata (engine name),
    // NOT on row-vector equality: `collect()` has no ORDER BY, so a
    // future Spark optimization could legitimately reorder rows and
    // a full-result `shouldBe` would misreport that as a determinism
    // regression. Schema + engine-marker equality is the durable
    // determinism contract.
    withClue("the same query twice must agree on schema and engine metadata (replay/journal safety)") {
      second.map(r => (r.schema, r.metadata)) shouldBe
        first.map(r => (r.schema, r.metadata))
    }
  }
}
