/*
 * SM8 Core — internal ConnectorRegistry implementation.
 *
 * NOT part of the SDK (lives in `io.sm8.core`, not `io.sm8.sdk`).
 * Plugin authors access via `engine.connectors`.
 */
package io.sm8.core

import io.sm8.sdk.{Connector, ConnectorRegistry}

/**
 * Concrete `ConnectorRegistry`. Holds a `Map` keyed by Connector name;
 * `register` is O(1); duplicate names throw.
 */
final class ConnectorRegistryImpl extends ConnectorRegistry {

 private val byName: scala.collection.mutable.LinkedHashMap[String, Connector] =
 scala.collection.mutable.LinkedHashMap.empty

 override def register(connector: Connector): ConnectorRegistry = {
 if (byName.contains(connector.name)) {
  throw new IllegalArgumentException(
  s"sm8: Connector '${connector.name}' is already registered")
 }
 byName += (connector.name -> connector)
 this
 }

 override def get(name: String): Option[Connector] = byName.get(name)

 override def all: Seq[Connector] = byName.values.toSeq
}