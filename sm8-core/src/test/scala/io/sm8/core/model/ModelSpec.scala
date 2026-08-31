/*
 * SM8 Core — Model test.
 *
 * Per [[debug-mantra-mindset]] (reproduce → trace → cross-reference →
 * verify): tests prove the smart constructor's validity checks.
 * Per [[karpathy-guidelinesmindset]] "smart constructor for validity-
 * at-boundary" + "match existing style" (ScalaTest flatSpec).
 */
package io.sm8.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ModelSpec extends AnyFlatSpec with Matchers {

  "Model.of" should "build a valid Model with minimal args" in {
    val result = Model.of(
      name     = "flights",
      version  = 1,
      source   = SourceRef.ByName(table = "flights_tbl")
    )
    result.isRight shouldBe true
    val model = result.toOption.get
    model.name shouldBe "flights"
    model.version shouldBe 1
    model.dimensions shouldBe Nil
    model.measures shouldBe Nil
    model.filters shouldBe Nil
    model.status shouldBe ModelStatus.Draft
  }

  it should "reject blank name with InvalidName" in {
    val result = Model.of(name = "", version = 1, source = SourceRef.ByName(table = "y"))
    result shouldBe Left(ModelValidationError.InvalidName("Model name must be non-blank"))
  }

  it should "reject null name with InvalidName" in {
    val result = Model.of(name = null, version = 1, source = SourceRef.ByName(table = "y"))
    result shouldBe Left(ModelValidationError.InvalidName("Model name must be non-blank"))
  }

  it should "reject negative version with InvalidVersion" in {
    val result = Model.of(name = "ok", version = -1, source = SourceRef.ByName(table = "y"))
    result shouldBe Left(ModelValidationError.InvalidVersion(-1))
  }

  it should "allow all optional fields with defaults" in {
    val result = Model.of(name = "minimal", version = 0, source = SourceRef.ByName(table = "y"))
    result.isRight shouldBe true
    val m = result.toOption.get
    m.description shouldBe None
    m.dimensions shouldBe Nil
    m.measures shouldBe Nil
    m.filters shouldBe Nil
  }
}

class ModelStatusSpec extends AnyFlatSpec with Matchers {
  "ModelStatus" should "be a sealed trait with 3 cases" in {
    ModelStatus.Draft shouldBe a [ModelStatus]
    ModelStatus.Published shouldBe a [ModelStatus]
    ModelStatus.Deprecated shouldBe a [ModelStatus]
  }
}

class SourceRefSpec extends AnyFlatSpec with Matchers {
  "SourceRef" should "have 3 sealed cases" in {
    val byName = SourceRef.ByName(table = "tbl1")
    val byPath = SourceRef.ByPath("csv", "/data/x.csv")
    val byProvider = SourceRef.ByProvider("prov1")

    byName shouldBe a [SourceRef]
    byPath shouldBe a [SourceRef]
    byProvider shouldBe a [SourceRef]
  }

  it should "accept catalog + namespace as Options, table as required" in {
    val src = SourceRef.ByName(
      catalog   = Some("default"),
      namespace = Some("analytics"),
      table     = "products",
    )
    src.catalog shouldBe Some("default")
    src.namespace shouldBe Some("analytics")
    src.table shouldBe "products"
  }
  it should "default catalog + namespace to None when omitted (the 1-arg form)" in {
    val src = SourceRef.ByName(table = "people")
    src.catalog shouldBe None
    src.namespace shouldBe None
    src.table shouldBe "people"
  }
  it should "support all-None as the legacy single-cluster default" in {
    val src = SourceRef.ByName(
      catalog   = None,
      namespace = None,
      table     = "events",
    )
    src.table shouldBe "events"
    (src.catalog.isEmpty && src.namespace.isEmpty) shouldBe true
  }

}
