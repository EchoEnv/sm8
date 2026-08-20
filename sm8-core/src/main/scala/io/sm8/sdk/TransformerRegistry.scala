/*
 * SM8 SDK — TransformerRegistry.
 *
 * New in Step 3 (extends the SDK from 7 to 10 types).
 *
 * Holds Transformers and tracks which one is active (Q3 = Transformer:
 * swap, exactly one active). Plugins register Transformers via
 * `engine.transformers.register(t)`. The Engine has one active
 * Transformer at a time, selected by name (default is the first
 * registered; settable via `setActive(name)`).
 */
package io.sm8.sdk

/**
 * Holds Transformers; exactly one is active at a time.
 */
trait TransformerRegistry {

 /**
 * Register a Transformer. If this is the first Transformer, it
 * becomes active automatically. Re-registering a name throws.
 *
 * @return this registry, for chaining
 * @throws IllegalArgumentException if a Transformer with the same name is already registered
 */
 def register(transformer: Transformer): TransformerRegistry

 /** Set the active Transformer by name. None if not found. */
 def setActive(name: String): Option[Transformer]

 /** Currently active Transformer, if any. */
 def active: Option[Transformer]
}