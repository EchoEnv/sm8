/*
 * SM8 Core — ModelSummary.
 *
 * Per [[ADR-012-a]] (`docs/adr/0012-a-modelservice-restate-handler.md`):
 * a read-only projection of `Model` surfaced by the new
 * `sm8-platform` `ModelService` Restate handler so the Restate web UI
 * (Services + Invocations pages) can discover what model a
 * deployment is serving. Lives in the `model` package — it's a DTO
 * of `Model`, not a parse/validate artifact (the `manifest` package
 * is for `ManifestError` + `ModelLoader`).
 *
 * Per [[scala-data-driven-refactor-mindset]]: pure data; the
 * `fromModel` projection is the single canonical way to derive a
 * `ModelSummary` from a `Model` (no callers should do this inline).
 *
 * Per [[scala-error-handling-mindset]]: `fromModel` is total — every
 * `SourceRef` subtype and every `ModelStatus` case is handled.
 *
 * Per [[scala-jvm-safety-mindset]] (null is a liar): `catalog` and
 * `namespace` are `Option[String]` (never null). `table` is a plain
 * `String`; empty for non-`ByName` source refs.
 */
package io.sm8.core.model

/**
 * Wire-stable DTO surfaced via the `ModelService` Restate handler.
 * Field shape is part of the registered Restate service description
 * (the JSON schema is auto-published to the Restate admin endpoint).
 *
 * @param name        the model name (e.g. `smoke-e2e-model`)
 * @param version     the model version (positive integer)
 * @param status      the model lifecycle status, lowercased
 *                    (`"draft"` / `"published"` / `"deprecated"`)
 * @param catalog     optional catalog name (`SourceRef.ByName.catalog`),
 *                    `None` if not set
 * @param namespace   optional namespace (`SourceRef.ByName.namespace`),
 *                    `None` if not set
 * @param table       the table name (empty if `SourceRef` is not
 *                    `ByName`)
 * @param dimensions  the count of typed dimensions declared in the model
 * @param measures    the count of typed measures declared in the model
 * @param description human-readable description (`None` if absent)
 */
final case class ModelSummary(
    name:        String,
    version:     Int,
    status:      String,
    catalog:     Option[String],
    namespace:   Option[String],
    table:       String,
    dimensions:  Int,
    measures:    Int,
    description: Option[String]
) extends Product with Serializable

object ModelSummary {

  /**
   * Derive a `ModelSummary` from a `Model`. The single canonical
   * way to construct a summary — do not construct inline.
   *
   * @param model the model to project
   * @return      the wire-stable DTO
   */
  def fromModel(model: Model): ModelSummary = {
    val (catalog, namespace, table) = model.source match {
      case SourceRef.ByName(c, n, t) => (c, n, t)
      case _                         => (None, None, "")
    }
    val status = model.status match {
      case ModelStatus.Draft      => "draft"
      case ModelStatus.Published  => "published"
      case ModelStatus.Deprecated => "deprecated"
    }
    ModelSummary(
      name        = model.name,
      version     = model.version,
      status      = status,
      catalog     = catalog,
      namespace   = namespace,
      table       = table,
      dimensions  = model.dimensions.size,
      measures    = model.measures.size,
      description = model.description
    )
  }
}