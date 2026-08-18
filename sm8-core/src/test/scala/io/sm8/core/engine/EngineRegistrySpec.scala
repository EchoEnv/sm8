package io.sm8.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.sm8.core.model.Model

/** Tests for [[EngineRegistry]] (added in PR 5 of the 12-PR
  * triage plan). Per the design §6.4: the registry's `select`
  * filters availability, the default must be available at
  * construction, the `availableProviders` list reflects runtime
  * availability. */
class EngineRegistrySpec extends AnyFunSuite with Matchers {

  // -- Test fixture: a minimal EngineProvider impl --



  private final class FakeProvider(
      override val identity: EngineIdentity,
      override val available: Boolean,
  ) extends EngineProvider {
    override def query(
        model: Model, request: QueryRequest, ctx: EngineContext,
    ): Either[EngineError, PortableQueryResult] = ???
    override def explain(
        model: Model, request: QueryRequest, ctx: EngineContext,
    ): Either[EngineError, String] = ???
  }

  /** A provider whose availability can be toggled post-construction.
    * Used to test the runtime-available branch (the default must
    * be available at construction, then can flip off later). */
  private final class FlipFlopProvider(
      override val identity: EngineIdentity,
      initial: Boolean,
  ) extends EngineProvider {
    private var _available: Boolean = initial
    def setAvailable(v: Boolean): Unit = _available = v
    override def available: Boolean = _available
    override def query(
        model: Model, request: QueryRequest, ctx: EngineContext,
    ): Either[EngineError, PortableQueryResult] = ???
    override def explain(
        model: Model, request: QueryRequest, ctx: EngineContext,
    ): Either[EngineError, String] = ???
  }

  // -- Construction invariants --

  test("registry construction fails if default is not in the engines map") {
    val providers = Map(
      "spark" -> new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true),
    )
    intercept[IllegalArgumentException] {
      EngineRegistry(providers, default = "trino")
    }
  }

  test("registry construction fails if default is registered but unavailable") {
    val providers = Map(
      "spark" -> new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = false),
    )
    intercept[IllegalArgumentException] {
      EngineRegistry(providers, default = "spark")
    }
  }

  // -- select --

  test("select returns Right(provider) for a registered + available name") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val registry = EngineRegistry(Map("spark" -> spark), default = "spark")
    registry.select("spark") shouldBe Right(spark)
  }

  test("select returns Left(EngineUnavailable) for an unknown name") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val registry = EngineRegistry(Map("spark" -> spark), default = "spark")
    registry.select("trino") match {
      case Left(EngineError.EngineUnavailable(name, available, wasDefault, message)) =>
        name shouldBe "trino"
        wasDefault shouldBe false
        available should contain ("spark")
      case other => fail(s"expected Left(EngineUnavailable), got $other")
    }
  }

  // Per v0.3.0 pre-tag audit: `select` MUST distinguish "user
  // asked for the default" from "user asked for a non-default
  // name". Previously wasDefault was always `false`.
  test("select wasDefault=true when user asks for the default name (and the provider flips off after startup)") {
    // The default must be available at construction (per the
    // `apply` invariant). To test the wasDefault branch, we
    // construct with a healthy default + a healthy non-default,
    // then flip the default off post-construction.
    val spark = new FlipFlopProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), initial = true)
    val trino = new FakeProvider(EngineIdentity("trino", "0.286", "0.2.4"), available = true)
    val registry = EngineRegistry(Map("spark" -> spark, "trino" -> trino), default = "spark")
    spark.setAvailable(false)
    registry.select("spark") match {
      case Left(EngineError.EngineUnavailable(name, _, wasDefault, _)) =>
        name shouldBe "spark"
        wasDefault shouldBe true
      case other => fail(s"expected Left(EngineUnavailable) with wasDefault=true, got $other")
    }
  }

  test("select wasDefault=false when user asks for a NON-default name") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val trino = new FlipFlopProvider(EngineIdentity("trino", "0.286", "0.2.4"), initial = true)
    val registry = EngineRegistry(Map("spark" -> spark, "trino" -> trino), default = "spark")
    trino.setAvailable(false)
    registry.select("trino") match {
      case Left(EngineError.EngineUnavailable(name, _, wasDefault, _)) =>
        name shouldBe "trino"
        wasDefault shouldBe false
      case other => fail(s"expected Left(EngineUnavailable) with wasDefault=false, got $other")
    }
  }

  test("availableProviders lists only registered + available providers") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val trino = new FakeProvider(EngineIdentity("trino", "0.286", "0.2.4"), available = true)
    val down  = new FakeProvider(EngineIdentity("databricks", "13.3", "0.2.4"), available = false)
    val registry = EngineRegistry(
      Map("spark" -> spark, "trino" -> trino, "databricks" -> down),
      default = "spark",
    )
    registry.availableProviders shouldBe List("spark", "trino") // sorted, no databricks
  }
}
