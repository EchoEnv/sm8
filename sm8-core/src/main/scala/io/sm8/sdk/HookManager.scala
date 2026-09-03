/*
 * SM8 SDK — HookManager.
 *
 * New in Step 3 (extends the SDK from 7 to 10 types).
 *
 * Holds Hooks (PreHook, PostHook, Transformer) and dispatches them
 * around the pipeline. Step 3 ships a thin skeleton; full priority
 * ordering + fail-fast dispatch lands in Step 4 (HookManager impl).
 *
 * Plugin authors call:
 * engine.hooks.register(stage, hook, priority)
 * engine.transformers.register(t)
 *
 * from their `setup(engine)` method.
 */
package io.sm8.sdk

/**
 * HookManager — owns Pre/PostHook registration and dispatch.
 *
 * Step 3 surface: register-only, no dispatch yet.
 * Step 4 adds: priority-ordered dispatch + fail-fast on throw.
 *
 * No-eviction invariant (per ADR-008-AE v1.0): the manager accumulates
 * hooks per stage in mutable Maps; there is NO automatic eviction.
 * The SDK contract is single-boot, single-reload — a future hot-reload
 * path (e.g. for live plugin updates in long-running services) must
 * explicitly clear + re-register. The accumulated hooks live for
 * the lifetime of the manager.
 */
trait HookManager {

 /**
 * Register a PreHook bound to `stage` with `priority`.
 * Lower priority runs first (RFC §8). Priority range reserved by
 * origin: 0-99 core, 100-899 first-party, 900+ community.
 *
 * @return this manager, for chaining
 * @throws IllegalArgumentException if priority is negative
 *
 * No-eviction invariant (per ADR-008-AE v1.0): the hook is appended
 * to the per-stage buffer; there is NO automatic eviction. See the
 * trait header for the SDK single-boot, single-reload contract.
 */
 def registerPreHook(stage: HookStage, hook: PreHook, priority: Int): HookManager

 /**
 * Origin-aware registration (RFC §8 conformance).
 *
 * Plugin authors who declare their origin (Core / FirstParty /
 * Community) get the engine to enforce the reserved range at
 * registration time — the implementation overrides this overload
 * with the typed range check. Plugin authors using the 3-arg
 * overload get the non-negative check only — the documented SDK
 * contract. The existing 3-arg overload is
 * preserved with identical semantics; downstream Plugins and
 * HookManagerImpl are unaffected.
 */
 def registerPreHook(stage: HookStage, hook: PreHook, priority: Int, origin: HookOrigin): HookManager = registerPreHook(stage, hook, priority)
 /**
 * Register a PostHook bound to `stage` with `priority`.
 * Same priority rules as registerPreHook.
 *
 * No-eviction invariant (per ADR-008-AE v1.0): the hook is appended
 * to the per-stage buffer; there is NO automatic eviction.
 */
 def registerPostHook(stage: HookStage, hook: PostHook, priority: Int): HookManager

 /**
 * Origin-aware registration (RFC §8 conformance). Same
 * semantics as the PreHook variant: declaring an explicit
 * `origin` opts the registration into strict range
 * enforcement at the implementation layer.
 */
 def registerPostHook(stage: HookStage, hook: PostHook, priority: Int, origin: HookOrigin): HookManager = registerPostHook(stage, hook, priority)
 /**
 * All PreHooks for a given stage, in priority order (lower first;
 * ties broken by registration order). Returns the accumulated buffer;
 * no eviction (per ADR-008-AE v1.0). Empty if no PreHooks registered.
 * Used by the Pipeline runner.
 */
 def preHooksFor(stage: HookStage): Seq[(PreHook, Int)]

 /**
 * All PostHooks for a given stage, in priority order. Returns the
 * accumulated buffer; no eviction (per ADR-008-AE v1.0). Empty if no
 * PostHooks registered.
 */
 def postHooksFor(stage: HookStage): Seq[(PostHook, Int)]

 /**
  * ADDITIVE in C10-PR-A: every hook that has been registered on this
  * manager (across all 8 stages), with full registration metadata.
  * Sorted by `(stage, priority ASC, pluginName, name)`. Returns the
  * accumulated buffer (no eviction per ADR-008-AE v1.0); empty if
  * nothing registered.
  *
  * Backs the `list_hooks` transport surface in PR-B.
  * Source-compatible default impl: derived from the existing
  * `preHooksFor` / `postHooksFor` accessors, so non-`HookManagerImpl`
  * implementations need not override.
  */
 def listAllHooks(): Seq[RegisteredHook] = {
  val pre = HookStage.values.toSeq.flatMap { stage =>
   preHooksFor(stage).map { case (hook, p) =>
    RegisteredHook(
     name       = hook.name,
     stage      = stage,
     priority   = p,
     origin     = HookOrigin.FirstParty, // best-effort; override for exactness
     pluginName = "<unknown>"
    )
   }
  }
  val post = HookStage.values.toSeq.flatMap { stage =>
   postHooksFor(stage).map { case (hook, p) =>
    RegisteredHook(
     name       = hook.name,
     stage      = stage,
     priority   = p,
     origin     = HookOrigin.FirstParty, // best-effort; override for exactness
     pluginName = "<unknown>"
    )
   }
  }
  (pre ++ post).sortBy(h => (h.stage, h.priority, h.pluginName, h.name))
 }
}