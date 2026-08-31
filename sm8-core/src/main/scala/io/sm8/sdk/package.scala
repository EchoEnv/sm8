/*
 * SM8 SDK -- single import path for extension authors.
 *
 * extension authors do `import io.sm8.sdk._` and get the full SDK
 * surface (Protocols, Context, registries) plus re-exports of the
 * core engine-portable types (ResultCache, EngineHookRequest, etc.)
 * that plugin authors need to read but should not re-import from the
 * core package.
 *
 * existing pattern is block-imports from `io.sm8.sdk.{.}` and
 * `io.sm8.core.{.}`. This package object makes the latter optional
 * (plugin authors use ONLY `io.sm8.sdk._`).
 *
 * Per ADR-008-P §AR-P1-2: the SDK Protocol surface stays frozen
 * (PreHook, PostHook, Transformer, Context, Engine, etc.); the
 * re-exports below are ADDITIVE (no breaking change for existing
 * plugin authors who still import from `io.sm8.core._` directly).
 *
 * adding to a package object is binary-compatible (existing
 * `import io.sm8.core._` users continue to work).
 */
package io.sm8.sdk

/** Re-exports of core types that plugin authors commonly need. */
object `package` {

 // -- Cache (PR-K, PR-N5) --
 val ResultCache = io.sm8.core.cache.ResultCache

 // -- Engine-portable wire shape (PR-O1c) --
 type EngineHookRequest = io.sm8.core.engine.EngineHookRequest
 val EngineHookRequest = io.sm8.core.engine.EngineHookRequest
 type EngineHookResult = io.sm8.core.engine.EngineHookResult
 val EngineHookResult = io.sm8.core.engine.EngineHookResult

 // -- Engine-portable request (PR-K) --
}
