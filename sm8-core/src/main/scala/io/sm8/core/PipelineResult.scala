/*
 * SM8 Core — Pipeline result shapes.
 *
 * Minimal core-local `Result` subtypes for the in-tree fallback
 * Pipeline. Production requests are realized per-URL by the
 * `EngineProvider` family (ServiceLoader discovery) and never touch
 * this Pipeline; the only Result this Pipeline now produces is a
 * typed failure (`PipelineError`) when an unknown request type
 * reaches the dispatch site, or an explicit short-circuit marker
 * (`PipelineSkipped`) when a hook set `Context.stop = true` before
 * any stage produced a result.
 *
 * Lives in `io.sm8.core` (internal) — not part of the SDK. Plugin
 * authors see `Result` in the envelope; the concrete subtype is a
 * pattern-match concern inside core.
 */
package io.sm8.core

import io.sm8.core.engine.EngineError
import io.sm8.sdk.{PipelineStage, Result}

/**
 * Typed failure surfaced by the in-tree Pipeline when it cannot
 * execute a request. An unrecognized request type must never pass
 * through as a silent success (empty Result mistaken for success);
 * it travels in the `Result` envelope so callers pattern-match on
 * the typed `EngineError`.
 *
 * The `engine` field carries the originating engine name (or `"pipeline"`
 * when the failure is dispatch-level) so log lines and tests can
 * attribute the failure correctly.
 */
final case class PipelineError(
  engine: String,
  error: EngineError
) extends Result

/**
 * Explicit short-circuit marker returned by `Pipeline.run` when a
 * hook set `Context.stop = true` before any stage produced a `Result`.
 * Carries the `PipelineStage` where the pipeline halted so observers
 * can attribute the skip (pre/post hooks for that stage still fire per
 * `PostHook.runsOnStop` semantics — see `Pipeline.runPreHooks` /
 * `runPostHooks`).
 *
 * This is an explicit marker, not a silent empty success — the
 * `stage` field names where the pipeline halted. Callers must
 * pattern-match and treat this distinctly from a normal Result.
 */
final case class PipelineSkipped(
  stage: PipelineStage
) extends Result
