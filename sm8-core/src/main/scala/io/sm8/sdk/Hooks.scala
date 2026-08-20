/*
 * SM8 SDK — PreHook, PostHook, Transformer.
 *
 * Hooks are functions bound to a specific pipeline stage. They receive
 * the shared Context, may read or write `context.result` / `context.meta`,
 * and may set `context.stop = true` to short-circuit the rest of the
 * pipeline.
 *
 * Three hook shapes per the RFC + plan:
 * - PreHook  — bound to pre:<stage>; runs before the stage body
 * - PostHook  — bound to post:<stage>; runs after the stage body
 * - Transformer — bound to the format stage; exactly one active
 *      (swap, not accumulate) — converts Core output to
 *      a delivery format
 *
 * Per RFC §8 priority ranges (enforced at registration time):
 * - 0-99 : core / built-in
 * - 100-899 : first-party / official
 * - 900+ : community / user
 *
 * Per RFC §9 error policy: a hook that throws aborts the pipeline
 * (fail-fast). Plugins that need non-fatal hooks must catch their own
 * exceptions inside the hook function.
 *
 * Per RFC hooks.md Rule 2: hooks MUST NOT mutate `context.request`. Use
 * `context.meta` for derived values.
 */
package io.sm8.sdk

import io.sm8.core.engine.EngineError

/**
 * A function bound to `pre:<stage>`. Runs in priority order (lower first;
 * ties broken by registration order — RFC §8).
 *
 * Use cases (per RFC hooks.md classification):
 * - validator  (pre:parse, pre:resolve)
 * - short-circuit (pre:execute — e.g. cache-read returning cached result)
 * - enricher   (rare on pre:; post: more common)
 *
 * PreHook authors should:
 * - NOT mutate `context.request` (RFC hooks.md Rule 2);
 * - return quickly — every PreHook runs on every request;
 * - respect the priority ranges (0-99 core, 100-899 first-party, 900+ community).
 */
trait PreHook {

 /** Unique name for this PreHook. Used in registry + introspection. */
 def name: String

 /**
 * Lower runs first. Tie-breaking: registration order (RFC §8).
 * Range reserved by origin (RFC §8):
 * - 0-99 core
 * - 100-899 first-party
 * - 900+ community
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
 * Use cases (per RFC `hooks.md` 5 behavioral types):
 * - mutator  (post:execute, post:format — rename fields, unit conversion)
 * - observer  (any pre/post — logging, metrics, audit)
 * - enricher  (post:parse, post:resolve)
 *
 * Per RFC `hooks.md` Rule: "Short-circuit / cache | Checks for a
 * precomputed answer, sets `context.result` and `context.stop = true` to
 * skip remaining stages". Observers MUST see the cache-HIT path so audit
 * trails + metrics reflect real workload. Mutators MUST NOT fire on
 * cache-HIT (re-mutating the result that the cache already stored is
 * both wasted work AND a no-op at best, wrong at worst).
 *
 * The default `runsOnStop = true` matches Observer semantics (always
 * fire). Plugins that want Mutator semantics (skip on `c.stop`) override
 * to `false` — see [[CacheWritePostHook]] in the cache plugin.
 *
 * default method (not a separate trait). One mechanism, two intents —
 * per the RFC's "classification of intent, not a separate mechanism"
 * convention. Per [[scala-bug-hunting-mindset]] §4 "the boundary is
 * where it breaks": the short-circuit flag at the dispatcher boundary
 * is the fault line; this flag lets the dispatcher honor both
 * intents without splitting the trait.
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

 /**
 * Whether this hook runs after a preceding hook (Pre-hook) set
 * `context.stop = true`. Default `true` = Observer semantics (always
 * fire — audit, metrics). Override to `false` for Mutator semantics
 * (skip on stop — cache write, row cap, schema mutation).
 *
 * The dispatcher short-circuits post-hooks with `runsOnStop = false`
 * when `c.stop` is true, preserving the cache-HIT invariant: the
 * `Result` already set by a Pre-hook (e.g. the cache read hook) is
 * the final result, no further mutation allowed.
 */
 def runsOnStop: Boolean = true
}

/**
 * A function bound to the `format` stage. Multiplicity is SWAP — exactly
 * one Transformer is active at a time, selected by config. This is the
 * one place SM8 deviates from pure accumulate (per Q3 — Y for Transformer).
 *
 * Use cases:
 * - JSON serializer (default)
 * - Markdown renderer (for LLM-readable output)
 * - CSV / Parquet exporter
 * - Custom format for a specific consumer (e.g. chart-JSON)
 *
 * Transformer authors should:
 * - be deterministic — the same `Context.result` must produce the same
 *  `Context.result` (no random IDs, no timestamps);
 * - not throw — Transformer exceptions are pipeline-fatal and almost
 *  always indicate a bug;
 * - keep it pure — no IO, no side effects (those belong in PostHooks).
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
 * pre:parse | pre:resolve | pre:execute | pre:format
 * post:parse | post:resolve | post:execute | post:format
 */
sealed trait HookStage
object HookStage {
 case object PreParse extends HookStage
 case object PostParse extends HookStage
 case object PreResolve extends HookStage
 case object PostResolve extends HookStage
 case object PreExecute extends HookStage
 case object PostExecute extends HookStage
 case object PreFormat extends HookStage
 case object PostFormat extends HookStage

 /** Wire name (e.g. `pre:execute`) for the hook registry. */
 def wireName(stage: HookStage): String = stage match {
 case PreParse => "pre:parse"
 case PostParse => "post:parse"
 case PreResolve => "pre:resolve"
 case PostResolve => "post:resolve"
 case PreExecute => "pre:execute"
 case PostExecute => "post:execute"
 case PreFormat => "pre:format"
 case PostFormat => "post:format"
 }
}
/**
 * Per-PR-3b (ADR-008-P §C1): the per-stage hook runner protocol.
 *
 * The Core's 4-stage pipeline (parse -> resolve -> execute -> format, per
 * RFC §5) fires two hook attachment points per stage (pre/post) = 8 hook
 * points total (the HookStage ADT). The runner's contract is "wraps one
 * stage's compile + fire pre-hooks before, fire post-hooks after,
 * short-circuit if any pre-hook sets stop". The spark-connector
 * SparkEngineProvider.query consumes this Protocol via an Option[HookRunner]
 * constructor parameter (default None = no plugin hooks fire, per the
 * bare-deploy shape).
 *
 * this trait is TYPES-ONLY (no behavior, no method bodies). The concrete
 * implementation lives in the platform layer
 * (sm8-platform/.../EngineHookDispatcher extends HookRunner) so the
 * spark-connector depends on the SDK surface, not on the platform layer
 * (preserving RFC §3 layer ownership: connectors do not import the
 * transport library).
 *
 * (cross-module -- SDK type consumed by every engine adapter). It MUST
 * return Either[EngineError, Context] so a hook failure surfaces as a
 * typed error rather than a thrown exception that escapes the function.
 *
 * two parameters. The runner is stateless; the execute thunk is
 * supplied by the engine adapter that owns the stage (the spark-connector
 * owns the execute stage; the platform doesn't know about DataFrames).
 *
 * Typical call site (spark-connector SparkEngineProvider.query):
 * {{{
 * val initialCtx = Context(request = EngineHookRequest(model, request, cacheKey))
 * dispatcher.run(initialCtx, { ctx =>
 *  compileSteps(ctx).map { df => ctx.copy(result = Some(EngineHookResult(pqr))) }
 * })
 * }}}
 */
trait HookRunner {
 /** Run the pre-hooks + execute-thunk + post-hooks for one stage.
 *
 * Pre-hooks fire in priority order; if any pre-hook sets ctx.stop = true,
 * the execute-thunk is skipped and the short-circuit path runs the
 * post-hooks (observability).
 *
 * @param initial the starting Context (must carry request =
 *     EngineHookRequest so pre-hooks can read request.model /
 *     request.mcpRequest / request.cacheKey).
 * @param execute the stage's compile-thunk. Returns
 *     Right(ctx with result populated) on success,
 *     Left(EngineError) on failure. The runner fires
 *     post-hooks only on Right.
 * @return  the final Context (post-hooks mutated it) on success;
 *    the original typed error on failure.
 */
 def run(
  initial: Context,
  execute: Context => Either[EngineError, Context]
 ): Either[EngineError, Context]
}
