/*
 * SM8 Platform — engine-portable hook Context shapes.
 *
 * Bridges the SDK's marker `Request`/`Result` traits (in
 * `io.sm8.sdk`) with the platform's typed engine-portable shapes
 * (`Model`, `QueryRequest`, `PortableQueryResult`).
 *
 * Per scala-data-driven-refactor-mindset "default to typed
 * carriers": the wrappers are case classes (data only). Plugin
 * authors read `request.model` / `request.mcpRequest` to get the
 * typed values; they write `context.meta` for cross-hook state.
 *
 * Per scala-impact-analysis-mindset "frozen SDK surface":
 * these wrappers extend `io.sm8.sdk.Request` / `io.sm8.sdk.Result`
 * but do NOT modify the SDK. The SDK traits are open markers;
 * adding subtypes is non-breaking.
 *
 * Per sm8-implementation-rules "type-class + data-driven": the
 * wrapper carries the typed evidence inline — no implicit
 * evidence lookup, no Map dispatch. Consumers cast on read via
 * `.asInstanceOf[EngineHookRequest]` after constructing the
 * Context themselves (the constructor is the only point where
 * the cast is enforced by type discipline).
 */
package io.sm8.platform.query.hooks

import io.sm8.core.engine.{ QueryRequest, PortableQueryResult }
import io.sm8.core.model.Model
import io.sm8.sdk.{ Request, Result }

/**
 * Typed carrier for the engine-portable request.
 *
 * Carries enough state for any Plugin to:
 *   - read the model (e.g. for a config-validation hook)
 *   - read the MCP request (e.g. for a filter-audit hook)
 *   - compute a cache-key on the model + request identity
 *
 * @param model       the engine-portable model
 * @param mcpRequest  the engine-portable request shape
 * @param cacheKey    pre-computed cache key for this request
 */
final case class EngineHookRequest(
    model:      Model,
    mcpRequest: QueryRequest,
    cacheKey:   String
) extends Request

/**
 * Typed carrier for the engine-portable result.
 *
 * A Plugin on `post:execute` can read `result.pqr` to inspect
 * the rows + schema.
 *
 * @param pqr the engine-portable result
 */
final case class EngineHookResult(
    pqr: PortableQueryResult
) extends Result
