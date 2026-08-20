/*
 * SM8 SDK — ConnectorRegistry.
 *
 * New in Step 3 (extends the SDK from 7 to 10 types — required by
 * the RFC's `engine.adapters.register(...)` pattern that Plugins
 * call from `setup(engine)`).
 *
 * Holds named Connectors. Plugin authors call `engine.connectors.register(c)`
 * from their `setup(engine)` method.
 *
 * Implementation lives in `io.sm8.core.ConnectorRegistryImpl` (Step 3).
 * This trait is the SDK boundary.
 */
package io.sm8.sdk

trait ConnectorRegistry {

 /**
 * Register a Connector. `name` must be unique across all registered
 * Connectors — re-registering a name throws `IllegalArgumentException`
 * (per karpathy §2 "throw for programmer errors").
 *
 * @return this registry, for chaining
 * @throws IllegalArgumentException if a Connector with the same name is already registered
 */
 def register(connector: Connector): ConnectorRegistry

 /** Lookup by name. None if no Connector is registered with that name. */
 def get(name: String): Option[Connector]

 /** All registered Connectors, in registration order. */
 def all: Seq[Connector]

 /** True iff a Connector with the given name is registered. */
 def contains(name: String): Boolean = get(name).isDefined
}