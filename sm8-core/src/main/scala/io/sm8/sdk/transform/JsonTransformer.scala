/*
 * SM8 SDK — JsonTransformer (built-in reference).
 *
 * The "default" Transformer that ships with the Core. Plugin authors
 * can swap it for any other `Transformer` via `setActive(name)`.
 *
 * the Transformer's behavior is decided by which case is the active
 * one (json vs markdown vs custom). The Pipeline calls
 * `env.transformers.active.fold(ctx)(_.transform(ctx))` — only ONE
 * transform fires per request (per Q3 = swap).
 *
 * Step 5 ships identity-style behavior: this Transformer doesn't
 * actually serialize (Result is still a marker trait in Step 5).
 * It stamps the Context with a marker so observers can tell it ran.
 * Real serialization lands in Step 0 when the IR moves and Result
 * gets a typed shape.
 *
 * no Jackson dependency added here. A real JSON serializing Transformer
 * is the responsibility of a downstream Connector (or a future
 * built-in in a separate module that depends on Jackson).
 */
package io.sm8.sdk.transform

import io.sm8.sdk.{Context, Transformer}

/**
 * Built-in reference Transformer. Name = "json", priority = 100
 * (first-party range per RFC §8).
 *
 * Construction: `JsonTransformer()` (case class, no args).
 * Auto-activated on first `register(...)` per the TransformerRegistry
 * contract.
 */
final case class JsonTransformer() extends Transformer {

 override def name: String = "json"

 override def priority: Int = 100

 /**
 * Stamps the Context's meta with a marker so observers (tests +
 * future introspection) can tell this Transformer ran. Identity
 * otherwise — the result passes through unchanged. Real JSON
 * serialization lands in Step 0.
 */
 override def transform(context: Context): Context =
 context.copy(meta = context.meta + ("active_transformer" -> name))
}