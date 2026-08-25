/*
 * SM8 Core — concrete Request and Result shapes for Step 3.
 *
 * NOT part of the SDK (lives in `io.sm8.core`). These are minimal
 * shapes for the Step 3 end-to-end smoke. The full typed
 * `core.Request` / `core.Result` move from `semanticdf-core` in
 * Step 0.
 *
 * Plugin authors in Step 3 send a `ConnectorRequest` to
 * `engine.run(.)`. The engine routes to the named Connector,
 * invokes `query`, and returns a `ConnectorResult` (rows + schema).
 */
package io.sm8.core

import io.sm8.core.engine.EngineError
import io.sm8.sdk.{ConnectorSchema, Request, Result, ResultRows, SemanticQuery}

/**
 * A request to execute `query` against the Connector named
 * `connectorName`. The engine looks up the Connector and routes.
 */
final case class ConnectorRequest(
 connectorName: String,
 query: SemanticQuery
) extends Request

/**
 * The result of executing a `ConnectorRequest`. Carries the
 * Connector's response rows + schema. Wraps both into the engine's
 * portable row + schema shapes.
 */
final case class ConnectorResult(
 connectorName: String,
 schema: ConnectorSchema,
 rows: ResultRows
) extends Result

/**
 * A typed failure surfaced by the pipeline instead of a silent empty
 * success. Replaces the earlier unknown-connector stub
 * (`ConnectorResult(name, empty schema, empty rows)`), which a typo'd
 * connector name turned into an empty, successful result. An
 * unknown connector is `EngineError.EngineUnavailable`; an unknown
 * request type is `EngineError.UnsupportedCapability` — both travel
 * in the Result envelope so callers observe the typed failure
 * (P1-A3-E4).
 */
final case class ConnectorError(
 connectorName: String,
 error: EngineError
) extends Result