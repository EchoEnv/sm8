/*
 * SM8 SDK — PreHook, PostHook, Transformer.
 *
 * Hooks are functions bound to a specific pipeline stage. They receive
 * the shared Context, may read or write `context.result` / `context.meta`,
 * and may set `context.stop = true` to short-circuit the rest of the
 * pipeline.
 *
 * Three hook shapes per the RFC + plan:
 *   - PreHook       — bound to pre:<stage>; runs before the stage body
 *   - PostHook      — bound to post:<stage>; runs after the stage body
 *   - Transformer   — bound to the format stage; exactly one active
 *                     (swap, not accumulate) — converts Core output to
 *                     a delivery format
 *
 * Per RFC §8 priority ranges (enforced at registration time):
 *   - 0-99    : core / built-in
 *   - 100-899 : first-party / official
 *   - 900+    : community / user
 *
 * Per RFC §9 error policy: a hook that throws aborts the pipeline
 * (fail-fast). Plugins that need non-fatal hooks must catch their own
 * exceptions inside the hook function.
 *
 * Per RFC hooks.md Rule 2: hooks MUST NOT mutate `context.request`. Use
 * `context.meta` for derived values.
 */
package io.sm8.sdk

/**
 * A function bound to `pre:<stage>`. Runs in priority order (lower first;
 * ties broken by registration order — RFC §8).
 *
 * Use cases (per RFC hooks.md classification):
 *   - validator        (pre:parse, pre:resolve)
 *   - short-circuit    (pre:execute — e.g. cache-read returning cached result)
 *   - enricher         (rare on pre:; post: more common)
 *
 * PreHook authors should:
 *   - NOT mutate `context.request` (RFC hooks.md Rule 2);
 *   - return quickly — every PreHook runs on every request;
 *   - respect the priority ranges (0-99 core, 100-899 first-party, 900+ community).
 */
trait PreHook {

  /** Unique name for this PreHook. Used in registry + introspection. */
  def name: String

  /**
   * Lower runs first. Tie-breaking: registration order (RFC §8).
   * Range reserved by origin (RFC §8):
   *   - 0-99    core
   *   - 100-899 first-party
   *   - 900+    community
   */
  def priority: Int

  /**
   * Which pipeline stage this PreHook is bound to. The engine rejects
   * registration if `stage` is not in the named set:
   * `pre:parse | pre:resolve | pre:execute | pre:format`.
   */
  def stage: HookStage

  /**
   * Run the hook. Receives the Context; returns a Context (possibly
   * with mutated `meta`, `result`, or `stop`). MUST NOT throw unless
   * the hook wants fail-fast behavior (RFC §9).
   */
  def run(context: Context): Context
}

/**
 * A function bound to `post:<stage>`. Same shape as PreHook, different
 * multiplicity: both PreHook and PostHook are accumulate (all registered
 * instances run, in priority order).
 *
 * Use cases (per RFC hooks.md):
 *   - mutator        (post:execute, post:format — rename fields, unit conversion)
 *   - observer       (any pre/post — logging, metrics, audit)
 *   - enricher       (post:parse, post:resolve)
 */
trait PostHook {

  /** Unique name for this PostHook. */
  def name: String

  /** Lower runs first. Same ranges as PreHook. */
  def priority: Int

  /**
   * Which pipeline stage this PostHook is bound to. Must be one of:
   * `post:parse | post:resolve | post:execute | post:format`.
   */
  def stage: HookStage

  /** Run the hook. Receives Context, returns Context (possibly mutated). */
  def run(context: Context): Context
}

/**
 * A function bound to the `format` stage. Multiplicity is SWAP — exactly
 * one Transformer is active at a time, selected by config. This is the
 * one place SM8 deviates from pure accumulate (per Q3 — Y for Transformer).
 *
 * Use cases:
 *   - JSON serializer (default)
 *   - Markdown renderer (for LLM-readable output)
 *   - CSV / Parquet exporter
 *   - Custom format for a specific consumer (e.g. chart-JSON)
 *
 * Transformer authors should:
 *   - be deterministic — the same `Context.result` must produce the same
 *     `Context.result` (no random IDs, no timestamps);
 *   - not throw — Transformer exceptions are pipeline-fatal and almost
 *     always indicate a bug;
 *   - keep it pure — no IO, no side effects (those belong in PostHooks).
 */
trait Transformer {

  /** Unique name. Selected via config (`transformer = "<name>"`). */
  def name: String

  /**
   * Priority reserved (100+) for first-party Transformers. The engine
   * rejects registration outside the reserved range.
   */
  def priority: Int

  /**
   * Convert `Context.result` into the engine's response format.
   * Returns a Context with the formatted `result`.
   */
  def transform(context: Context): Context
}

/**
 * The 8 named hook attachment points — pre/post for each of the 4
 * pipeline stages. Sealed: the engine cannot add new hook points
 * without a Core change (the pipeline shape is frozen).
 *
 * Exactly 8 values, per RFC hooks.md "Attachment Points":
 *   pre:parse | pre:resolve | pre:execute | pre:format
 *   post:parse | post:resolve | post:execute | post:format
 */
sealed trait HookStage
object HookStage {
  case object PreParse    extends HookStage
  case object PostParse   extends HookStage
  case object PreResolve  extends HookStage
  case object PostResolve extends HookStage
  case object PreExecute  extends HookStage
  case object PostExecute extends HookStage
  case object PreFormat   extends HookStage
  case object PostFormat  extends HookStage

  /** Wire name (e.g. `pre:execute`) for the hook registry. */
  def wireName(stage: HookStage): String = stage match {
    case PreParse    => "pre:parse"
    case PostParse   => "post:parse"
    case PreResolve  => "pre:resolve"
    case PostResolve => "post:resolve"
    case PreExecute  => "pre:execute"
    case PostExecute => "post:execute"
    case PreFormat   => "pre:format"
    case PostFormat  => "post:format"
  }
}