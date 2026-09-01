/*
 * SM8 Core — ModelSummary test.
 *
 * Per [[ADR-012-a]] (`docs/adr/0012-a-modelservice-restate-handler.md`):
 * tests for the read-only DTO projection that backs the new
 * `ModelService` Restate handler.
 *
 * Per [[debug-mantra-mindset]]: tests prove `fromModel` is total —
 * every `ModelStatus` case (Draft/Published/Deprecated) and every
 * `SourceRef` subtype (ByName/ByPath/ProviderRef) is handled.
 *
 * Per [[scala-data-driven-refactor-mindset]]: the projection is
 * single-source — there should be exactly one canonical way to derive
 * a `ModelSummary` from a `Model`, and this spec locks it down.
 */
package io.sm8.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ModelSummarySpec extends AnyFlatSpec with Matchers {

  /** Minimal canonical Model. Source required; everything else default.
    * Returns the typed `Model` produced by `Model.of`'s smart constructor. */
  private def makeModel(
      name: String = "smoke-model",
      version: Int = 1,
      source: SourceRef = SourceRef.ByName(table = "smoke_table"),
      status: ModelStatus = ModelStatus.Draft,
      description: Option[String] = None
  ): Model = Model.of(
    name        = name,
    version     = version,
    source      = source,
    status      = status,
    description = description
  ).toOption.get

  "ModelSummary.fromModel" should "project all 3 ModelStatus cases" in {
    // Per [[ADR-012-a]] §Implementation: fromModel explicitly handles
    // Draft/Published/Deprecated — none of these cases should throw.
    Seq(ModelStatus.Draft, ModelStatus.Published, ModelStatus.Deprecated).foreach { st =>
      val summary = ModelSummary.fromModel(makeModel(status = st))
      summary.status shouldBe st.toString.toLowerCase
    }
  }

  it should "preserve all 3 SourceRef.ByName fields (catalog/namespace/table)" in {
    val model = makeModel(
      source = SourceRef.ByName(
        catalog   = Some("smoke_catalog"),
        namespace = Some("smoke_ns"),
        table     = "smoke_table"
      )
    )
    val summary = ModelSummary.fromModel(model)
    summary.catalog shouldBe Some("smoke_catalog")
    summary.namespace shouldBe Some("smoke_ns")
    summary.table shouldBe "smoke_table"
  }

  it should "leave catalog/namespace as None + table empty for non-ByName source refs" in {
    // ByPath and ProviderRef are the other SourceRef subtypes; they
    // don't carry table info, so table="" + None for the rest.
    val byPathModel = makeModel(source = SourceRef.ByPath("parquet", "/data/x", Map.empty))
    val s = ModelSummary.fromModel(byPathModel)
    s.table shouldBe ""
    s.catalog shouldBe None
    s.namespace shouldBe None
  }

  it should "count dimensions + measures as 0 when none declared" in {
    // The minimal `makeModel` helper produces an empty dimensions /
    // measures seq by default — this is the 0-case assertion.
    val summary = ModelSummary.fromModel(makeModel())
    summary.dimensions shouldBe 0
    summary.measures shouldBe 0
  }

  it should "carry description verbatim (Some / None)" in {
    val summary1 = ModelSummary.fromModel(makeModel(description = Some("hello")))
    summary1.description shouldBe Some("hello")
    val summary2 = ModelSummary.fromModel(makeModel(description = None))
    summary2.description shouldBe None
  }

  it should "preserve name + version verbatim" in {
    val summary = ModelSummary.fromModel(makeModel(name = "specific-name", version = 7))
    summary.name shouldBe "specific-name"
    summary.version shouldBe 7
  }

  "ModelSummary case class" should "be Product with Serializable (journal-safe)" in {
    // Per [[scala-spark-batch-bugs-mindset]] mantra #1 + ADR-006:
    // captured types in Restate handler closures must be journal-safe.
    val summary = ModelSummary.fromModel(makeModel())
    summary.isInstanceOf[Product] shouldBe true
    summary.isInstanceOf[Serializable] shouldBe true
  }
}