/*
 * SM8 Audit Plugin — phantom-typed witnesses (PR-16, ADR-008-Q §PR-16).
 *
 * Per the ADR §"Plugin Refs + example": the audit plugin's typed
 * dimension witnesses for the canonical audit fields.
 */
package io.sm8.plugins.audit

import io.sm8.core.model.TypedDimension

/**
 * Phantom-typed dimension witnesses for the audit plugin.
 */
object Refs {

  sealed trait EventId
  sealed trait ActorId
  sealed trait ResourceName

  val eventId: TypedDimension[EventId] =
    TypedDimension.of[EventId]("event_id")

  val actorId: TypedDimension[ActorId] =
    TypedDimension.of[ActorId]("actor_id")

  val resourceName: TypedDimension[ResourceName] =
    TypedDimension.of[ResourceName]("resource_name")
}
