/*
 * SM8 Core — TypedRealizationProvider test.
 *
 * Per ADR-008-Q §C2 / §C7 / DE P1-B:
 *
 *   "TypedRealizationProvider is a SUBTRAIT of EngineProvider. Existing
 *    implementors of EngineProvider are NOT broken. External ServiceLoader
 *    discoverers opt into typed realization by ALSO implementing
 *    TypedRealizationProvider."
 *
 * Per the subtrait design (Architect P0-2 fix): the default `realizeTyped`
 * impl lives on the SUBTRAIT, not on `EngineProvider`. So:
 *
 *   - A `EngineProvider` that DOES NOT extend `TypedRealizationProvider`
 *     has NO `realizeTyped` method at all (backward-compat — proven).
 *   - A `EngineProvider` that DOES extend `TypedRealizationProvider`
 *     gets the default `realizeTyped` impl, which delegates to the
 *     existing `realize(url: String): Option[EngineProvider]`.
 *   - A connector MAY override `realizeTyped` for engine-specific
 *     typed errors (e.g. wrapping a `SparkException` into `ConnectionFailed`).
 *
 * Per [[scala-bug-huntingmindset]] §3: the subtrait relationship is the
 * binary-compat guarantee. Tests verify BOTH paths.
 */
package io.sm8.core.engine

import io.sm8.core.model.Model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TypedRealizationProviderSpec extends AnyFlatSpec with Matchers {

  // -- A stub provider that does NOT implement TypedRealizationProvider.
  //    Per ADR-008-Q §C2: a `instanceOf TypedRealizationProvider` check
  //    on this returns false. The `realizeTyped` method does not exist
  //    on this type (compile-time evidence). --

  private final class StubProviderNoTyped(
      override val identity: EngineIdentity,
      override val available: Boolean,
      private val stubRealize: String => Option[EngineProvider]
  ) extends EngineProvider with java.io.Serializable {

    override def realize(url: String): Option[EngineProvider] = stubRealize(url)

    override def query(
        model: Model, request: QueryRequest, ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = ???

    override def explain(
        model: Model, request: QueryRequest, ctx: EngineContext
    ): Either[EngineError, String] = ???
  }

  // -- A stub provider that DOES implement TypedRealizationProvider.
  //    Proves the default delegate works for any existing implementor
  //    that opts in. --

  private final class StubProviderWithTyped(
      override val identity: EngineIdentity,
      override val available: Boolean,
      private val stubRealize: String => Option[EngineProvider]
  ) extends TypedRealizationProvider with java.io.Serializable {

    override def realize(url: String): Option[EngineProvider] = stubRealize(url)

    override def query(
        model: Model, request: QueryRequest, ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = ???

    override def explain(
        model: Model, request: QueryRequest, ctx: EngineContext
    ): Either[EngineError, String] = ???
  }

  // -- A stub provider that overrides `realizeTyped` (proves the
  //    override path returns connector-specific typed errors). --

  private final class StubProviderWithTypedOverride(
      override val identity: EngineIdentity,
      override val available: Boolean
  ) extends TypedRealizationProvider with java.io.Serializable {

    override def realize(url: String): Option[EngineProvider] = None  // unused

    override def realizeTyped(parsedUrl: EngineUrl): Either[EngineError, EngineProvider] =
      Left(EngineError.ConnectionFailed(
        engine = "custom",
        reason = "SparkSession.builder().getOrCreate() threw SparkException: connection refused",
        message = s"sm8: custom engine: ${parsedUrl.raw} refused"
      ))

    override def query(
        model: Model, request: QueryRequest, ctx: EngineContext
    ): Either[EngineError, PortableQueryResult] = ???

    override def explain(
        model: Model, request: QueryRequest, ctx: EngineContext
    ): Either[EngineError, String] = ???
  }

  // -- Backward-compat proof: a provider that does NOT extend
  //    TypedRealizationProvider has no `realizeTyped` method --
  //    (verified via compile-time: the call below would not compile
  //    if `StubProviderNoTyped` had `realizeTyped`) --

  "StubProviderNoTyped" should "not extend TypedRealizationProvider (backward-compat)" in {
    val provider = new StubProviderNoTyped(
      identity = EngineIdentity("no-typed", "1.0", "0.1.0"),
      available = true,
      stubRealize = _ => None
    )
    provider.isInstanceOf[TypedRealizationProvider] shouldBe false
  }

  // -- Default delegate (proves the subtrait default impl works) --
  // Per ADR-008-Q §C2: the default impl delegates to `realize(url)`.
  // Uses `StubProviderWithTyped` (which DOES extend the subtrait).
  // The companion "should not extend TypedRealizationProvider" test
  // above proves the binary-compat half (existing EngineProvider
  // impls are unaffected).

  "TypedRealizationProvider default impl" should
      "return Right(provider) when realize(url) returns Some" in {
    val realized = new StubProviderWithTyped(
      identity = EngineIdentity("test", "1.0", "0.1.0"),
      available = true,
      stubRealize = _ => Some(new StubProviderWithTyped(
        identity = EngineIdentity("test", "1.0", "0.1.0"),
        available = true,
        stubRealize = _ => None
      ))
    )
    val url = EngineUrl.Spark(master = "local[*]")
    realized.realizeTyped(url) match {
      case Right(p) => p.identity.name shouldBe "test"
      case Left(e)  => fail(s"expected Right, got $e")
    }
  }

  it should "return Left(ConnectionFailed) when realize(url) returns None" in {
    val provider = new StubProviderWithTyped(
      identity = EngineIdentity("test", "1.0", "0.1.0"),
      available = true,
      stubRealize = _ => None
    )
    val url = EngineUrl.Spark(master = "garbage")
    provider.realizeTyped(url) match {
      case Left(EngineError.ConnectionFailed(engine, reason, _)) =>
        engine shouldBe "spark"
        reason should include ("realize(url) returned None")
      case other => fail(s"expected typed error, got $other")
    }
  }

  it should "preserve the engineName from the EngineUrl in the typed error" in {
    val provider = new StubProviderWithTyped(
      identity = EngineIdentity("test", "1.0", "0.1.0"),
      available = true,
      stubRealize = _ => None
    )
    provider.realizeTyped(EngineUrl.Trino(jdbcUrl = "jdbc:trino://x:8080")) match {
      case Left(EngineError.ConnectionFailed(engine, _, _)) =>
        engine shouldBe "trino"
      case other => fail(s"expected typed error, got $other")
    }
  }

  // -- Connector override (proves typed errors work for custom impls) --

  "TypedRealizationProvider connector override" should
      "return connector-specific typed errors" in {
    val provider = new StubProviderWithTypedOverride(
      identity = EngineIdentity("custom", "1.0", "0.1.0"),
      available = true
    )
    provider.realizeTyped(EngineUrl.Spark(master = "spark://invalid:7077")) match {
      case Left(EngineError.ConnectionFailed(engine, reason, _)) =>
        engine shouldBe "custom"
        reason should include ("SparkException")
      case other => fail(s"expected typed error, got $other")
    }
  }
}
