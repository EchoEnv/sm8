/*
 * SM8 Platform — MetaRequest + MetaResponse.
 *
 * The transport layer exposes a generic `MetaInspectorService`
 * that reads any `context.meta` key and returns the typed value
 * as JSON. The wire DTOs are minimal — no plugin-specific
 * knowledge.
 */
package io.sm8.platform.query

/**
 * Request payload for the `getMeta` handler.
 *
 * @param key the `context.meta` key to read. Per the plugin
 *        namespacing convention (e.g.
 *        `"io.sm8.plugins.semanticgraph:graph-snapshot"`), keys
 *        are namespaced under the plugin's id.
 */
final case class MetaRequest(key: String)

/**
 * Response payload for the `getMeta` handler.
 *
 * @param key     the requested key (echoed for client convenience)
 * @param present `true` if the key was set on the most recent
 *                request; `false` if the key is absent
 * @param value   the value at the key (as a `Map[String, Any]`
 *                suitable for Jackson serialization). `None` when
 *                `present` is `false`.
 */
final case class MetaResponse(
    key: String,
    present: Boolean,
    value: Option[Map[String, Any]]
)

/**
 * Request payload for the `getMetaByPrefix` handler.
 *
 * @param prefix the prefix to filter keys by (exact string match
 *               at the start of the key). Examples:
 *               - `"sm8.cache"` matches `"sm8.cache.policy"`,
 *                 `"sm8.cache.lastHit"`, etc.
 *               - `""` (empty string) matches ALL keys (useful for
 *                 full introspection).
 *
 *               The prefix is matched verbatim (no glob, no regex)
 *               per the building-restate-services skill rule
 *               "determinism": keep handler logic predictable.
 */
final case class MetaByPrefixRequest(prefix: String)

/**
 * Single key/value entry returned by `getMetaByPrefix`.
 *
 * Same shape as `MetaResponse` minus the explicit `present` field
 * (every entry in the response is, by construction, present — the
 * prefix filter excludes absent keys). This is a deliberate
 * simplification: the wire DTO stays flat.
 *
 * @param key   the `context.meta` key
 * @param value the value at the key, as a `Map[String, Any]`
 *              suitable for Jackson serialization
 */
final case class MetaEntry(key: String, value: Map[String, Any])

/**
 * Response payload for the `getMetaByPrefix` handler.
 *
 * @param prefix the prefix used to filter (echoed for client
 *               convenience)
 * @param count  the number of entries returned (avoids the client
 *               needing to `entries.size` for sanity checks)
 * @param entries the matching entries in stable insertion order
 *                (the underlying `Map[String, Any]` preserves
 *                insertion order per the `scala.collection.Map`
 *                default since 2.13; Jackson round-trip preserves
 *                this)
 */
final case class MetaByPrefixResponse(
    prefix: String,
    count: Int,
    entries: Seq[MetaEntry]
)