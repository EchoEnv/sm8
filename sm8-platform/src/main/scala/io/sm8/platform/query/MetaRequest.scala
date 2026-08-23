/*
 * SM8 Platform — MetaRequest + MetaResponse (PR-150, ADR-008-AI
 * follow-up).
 *
 * Per the architect's 2026-08-23 design review
 * (`docs/review/graph-display-design-review.md`): the transport
 * layer exposes a GENERIC `MetaInspectorService` that reads any
 * `context.meta` key and returns the typed value as JSON. The
 * wire DTOs are minimal — no plugin-specific knowledge.
 */
package io.sm8.platform.query

/**
 * Request payload for the `getMeta` handler.
 *
 * @param key the `context.meta` key to read. Per the plugin
 *        namespacing convention (e.g.
 *        `"io.sm8.plugins.semanticgraph:graph-snapshot"`), keys are
 *        namespaced under the plugin's id.
 */
final case class MetaRequest(key: String)

/**
 * Response payload for the `getMeta` handler.
 *
 * @param key the requested key (echoed for client convenience)
 * @param present `true` if the key was set on the most recent
 *        request; `false` if the key is absent
 * @param value the value at the key (as a `Map[String, Any]`
 *        suitable for Jackson serialization). `None` when `present`
 *        is `false`.
 */
final case class MetaResponse(
    key: String,
    present: Boolean,
    value: Option[Map[String, Any]]
)