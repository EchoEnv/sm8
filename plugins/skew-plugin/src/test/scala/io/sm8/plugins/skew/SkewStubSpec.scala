/*
 * SM8 skew Plugin — test.
 */
package io.sm8.plugins.skew

import io.sm8.core.EngineImpl
import io.sm8.core.model.{JoinSpec, Model, SourceRef}
import io.sm8.core.rel.JoinKind
import io.sm8.sdk.HookStage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SkewStubSpec extends AnyFlatSpec with Matchers {

  "SkewStub.setup" should "register a single Pre-hook at PreExecute" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new SkewStub
    engine.use(plugin)
    engine.hooks.preHooksFor(HookStage.PreExecute).map(_._1.name) shouldBe List("skew-stub")
  }

  it should "fire once per engine.run" in {
    val engine: EngineImpl = EngineImpl()
    val plugin = new SkewStub
    engine.use(plugin)

    engine.run(new io.sm8.sdk.Request {})
    plugin.fires.get() shouldBe 1
  }

  private def modelWith(est: Option[Long]): Model =
    Model.of(
      name = "m",
      version = 1,
      source = SourceRef.ByName(table = "t"),
      joins = est.toList.map(e =>
        JoinSpec("j", "right", JoinKind.Inner, List("k" -> "k"), estimatedRows = Some(e))
      )
    ).toOption.get

  it should "decide true when a join estimate is at or above the threshold" in {
    val plugin = new SkewStub
    plugin.consult(modelWith(Some(100000L)), threshold = 100000L) shouldBe true
  }

  it should "decide true when a join estimate is above the threshold" in {
    val plugin = new SkewStub
    plugin.consult(modelWith(Some(200000L)), threshold = 100000L) shouldBe true
  }

  it should "decide false when no join estimate meets the threshold" in {
    val plugin = new SkewStub
    plugin.consult(modelWith(Some(1000L)), threshold = 100000L) shouldBe false
  }

  it should "decide false when no join declares an estimate" in {
    val plugin = new SkewStub
    plugin.consult(modelWith(None), threshold = 100000L) shouldBe false
  }
}
