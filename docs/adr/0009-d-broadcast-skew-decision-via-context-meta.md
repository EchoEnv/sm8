# ADR-009-d: Broadcast + skew decision lives in the plugin's hook; spark connector consumes via `EngineContext.decisionHints`

| Field | Value |
|---|---|
| **Status** | **Implemented — merged as PR-174 (`0161b7b`); the broadcast + skew decision now lives in the plugin's `PreExecute` hook; the cross-boundary transport is the typed `DecisionHints` value on `EngineContext.decisionHints` (zero SDK change, zero string-meta leakage); 3 specs assert the wiring end-to-end (2 in the plugin stubs + DecisionHintsSpec + EngineServiceRunQueryWithHooksSpec + SparkBroadcastSeedSpec) |
| **Date** | 2026-08-25 |
| **Module** | `plugins/broadcast-plugin` + `plugins/skew-plugin` (the stubs' `PreExecute` hook now consults + writes `context.meta("sm8.broadcast.arm")` / `("sm8.skew.arm")`) + `sm8-core/engine/EngineContext` (new `decisionHints: Option[DecisionHints]` field + new `DecisionHints(broadcastArmed, skewArmed)` core type) + `sm8-platform/query/EngineService.runQueryWithHooks` (one-line edit: fold post-hook `Context.meta` into `decisionHints` before `executeEngine`) + `connectors/spark-connector` (seed helpers read `eCtx.decisionHints` with inline fallback) |
| **Implementation evidence** | PR-174 (`0161b7b`) squash-merged on `main`; full reactor green (15 modules, 220 spark-connector tests passing including 4 oracle-armed/disarmed/no-oracle broadcast + 4 oracle-armed/disarmed/no-oracle/disagreement skew tests, plus 86 broadcast/skew stub tests, plus 86 platform tests, plus 3 DecisionHintsSpec tests); architect + data-eng dual review both CORRECT (confidence 0.85); the v0.3 implementation fixes commit `6701244` addressed: (P2) the skew-axis divergence test at the (10M, 1B) window; (P3) `clearQuerySessionTL()` now wrapped in try/finally so a raw `Throwable` clears the seam on every exit; (P3) inline ADR-prefix references stripped from implementation comments; (P3) broadcast/skew arm asymmetry documented in `DecisionHints` scaladoc ("Per-decision oracle semantics" + "Asymmetry rationale" sections); (data-eng P3) `BroadcastStub` now writes a distinct `BroadcastThresholdBytes = 10 MiB` constant to `sm8.broadcast.thresholdBytes` (decoupled from the row-count `BroadcastThresholdRows = 10M` threshold); data-eng post-PR review caught 1 additional spec-assertion oversight at `EngineServiceRunQueryWithHooksSpec:360/395` (still asserting the old row-count constant); fixed in the amended head `6701244`). ADR addendum (this PR): every `EngineProvider` must honor-or-`UnsupportedCapability`; cross-engine spec added in Commit 3. |
| **Supersedes scope** | the design gap from ADR-008-AC + ADR-009-a + ADR-009-b v0.5-r1: the broadcast + skew decision was duplicated between the spark connector's seed helpers (presence-ARM, inline) and the plugin stubs' `consult()` (value-based, not wired). This ADR makes the plugin the single source of truth for the decision while keeping the spark connector as the sole Spark-aware module |

| **Skill alignment** | `karpathy-app-design`, `karpathy-guidelines`, `scala-spark-batch-bugs`, `scala-impact-analysis`, `scala-bug-hunting`, `scala-error-handling`, `scala-jvm-safety`, `scala-perf-testing`, `scala-data-driven-refactor`, `scala2-scaladoc`, `debug-mantra` |
| **Architecture alignment** | RFC `semantic-layer-engine-architecture.md` §3 (Core Boundary: plugin knows about business rules via hooks; hook is "when in the pipeline" not "which system"; adapter never imports a plugin); RFC `plugins.md` Rule 3 (plugins communicate via `context.meta`, never by importing each other); RFC `plugins.md` Rule 1 (setup is idempotent registration only); RFC `hooks.md` (PreExecute hooks read `context.request` / write `context.meta`; hook never knows which adapter is in use); RFC §3 (only the hook layer may carry the decision across the boundary) |

## Decision-at-a-glance

1. **Decision lives in the plugin's hook** (value-consult, not presence-ARM — see §3 below). The `BroadcastStub`'s and `SkewStub`'s `PreExecute` hooks (priority 250, registered in `setup()`) read `context.request.model` (the `EngineHookRequest` already carries the validated `Model`), compute the decision per their existing `consult(model, threshold)` logic with a private default threshold, and write a per-decision-key Boolean into `context.meta`:
   - `context.meta("sm8.broadcast.arm") = true | false` — for broadcast decisions
   - `context.meta("sm8.skew.arm") = true | false` — for skew decisions

2. **The decision crosses the boundary as a typed core value, not a free-form string.** A new non-SDK core type `DecisionHints(broadcastArmed: Option[Boolean], skewArmed: Option[Boolean])` is added to `sm8-core/engine/DecisionHints.scala`. A new field `decisionHints: Option[DecisionHints] = None` is added to the existing `EngineContext` case class (which lives in `sm8-core/engine/EngineContext.scala`, NOT in the SDK — see §4). The spark connector reads `eCtx.decisionHints` (a typed value, not a meta-key string).

3. **The decision diverges from the inline presence rule** (P1-A from the prior review). The plugin's `consult(model, threshold)` uses a **private default threshold** (e.g. `BroadcastThresholdRows = 10_000_000` for broadcast, `SkewThresholdRows = 1_000_000_000` for skew), and the inline fallback in the spark connector's seed helper uses the **presence rule** (`model.joins.exists(_.estimatedRows.isDefined)`). For a model with `estimatedRows > BroadcastThresholdRows`, the inline rule ARMS the broadcast; the plugin DISARMS. The two regimes differ on identical models — the wiring is observable. (Per the design consultation: the deployment semantics change is a real behavior change, not a relocation-only. The no-oracle path stays byte-identical to today; the oracle-wired path is the value-consult rule.)

4. **`EngineService.runQueryWithHooks` folds the post-hook `Context.meta` into `decisionHints`** (one-line edit, see §6). The platform dispatcher (`EngineHookDispatcher`) already fires PreExecute hooks; we add a single line after the dispatcher returns: extract the two meta keys, build a `DecisionHints`, attach to the `EngineContext` passed to `executeEngine`. No new platform code beyond this one-line fold.

5. **The spark connector reads `eCtx.decisionHints` with an inline fallback.** `seedBroadcastThreshold` reads `eCtx.decisionHints.flatMap(_.broadcastArmed)`; if `Some(b)`, the arm Boolean is the decision; if `None`, the inline presence rule runs. Same pattern for `seedSkewFactor` with `skewArmed`.

6. **The bare-deploy shape is unchanged** (`hookRunner: Option[HookRunner] = None` on the spark provider). The platform dispatcher always fires; with `hookRunner = None` on the provider, no connector-level hooks fire (the platform hooks fire independently on the platform Context). The decision still flows through `EngineContext.decisionHints` because `EngineService` does the fold, not the provider. Production deployment with the platform but without the spark provider's hookRunner: the broadcast/skew stubs fire on the platform Context, write meta, the platform fold reads meta, `decisionHints` is populated, the spark provider's seed helpers consume. The bare-deploy shape + no platform dispatcher: `decisionHints = None`, inline fallback fires (byte-identical to today).

7. **Throwing oracle is handled by the existing `EngineHookDispatcher` fail-fast path.** The hook's `run(context)` already has try/catch freedom (the SDK doesn't mandate re-throw). On exception, the hook writes `context.meta("sm8.broadcast.error") = ex.getMessage` + sets `context.stop = true`. The dispatcher's existing fail-fast (ADR-008-AF v1.0) catches the throw, builds `EngineError.HookFailed(engine, name, priority, stage, message)` (5-field, no defaults — see §7), and returns `Left(...)`. **We never construct HookFailed manually in the spark connector** — the dispatcher is the single owner.

8. **The v0.5-r1 invariants are preserved** (per ADR-009-c v0.5-r1): per-query `newSession()`, `querySessionTL` seam (cleared in finally when created), `lastQuerySessionTL` seam, null-spark short-circuit. No early `return` inside the flatMap lambda (P1-D from the prior review). All error paths return `Left` values; the method tail `{ val _r = compiled; if (createdQuerySessionHere) clearQuerySessionTL(); _r }` always runs.

9. **Zero SDK changes.** The SDK's `Context.meta: Map[String, Any]`, `Plugin.setup(engine: Engine)`, `HookManager.registerPreHook`, `PreHook.run(context: Context)`, `HookOrigin`, `HookRunner`, and `Result` are all unchanged. The only core addition (`DecisionHints` + the `EngineContext.decisionHints` field) lives in `sm8-core/engine/`, NOT in `sm8-sdk/`. The SDK freeze per ADR-007 is preserved.

10. **Zero banned-deps lift.** The plugin's hook reads `context.request.model: Model` (a `sm8-core` type) and writes `context.meta: Map[String, Any]` (Boolean). No Spark import. The spark connector remains the SOLE module that imports `org.apache.spark.*`.

11. **Two typed hooks per stub (not one shared signature).** To avoid the cross-wired-semantics defect (the Spark-safety F-2 finding from the prior review: `SkewStub.consult` does `>=` and `BroadcastStub.consult` does `<=` on the same `(Model, Long): Boolean` signature), the two hooks are independent: `BroadcastPreStubHook` writes `sm8.broadcast.arm`; `SkewPreStubHook` writes `sm8.skew.arm`. They never see each other's meta keys.

13. **Every `EngineProvider` MUST either consume every `DecisionHints` field for which its engine has a native config, or reject it with a typed error.** An adapter whose engine can apply a given field must consume it — e.g. the spark connector's `autoBroadcastJoinThreshold` for `broadcastArmed`/`broadcastThresholdBytes` and its skew join config for `skewArmed`. An adapter whose engine has no native config for a field that the fold decided must return `EngineError.UnsupportedCapability(engine, capability, message)` naming the unsupported capability. **Silent drops** (a decision produced but ignored with no error and no log) are a contract violation — the same severity as a fold miss in the platform. The typed `EngineContext.decisionHints` reaches every adapter, so consumption-vs-rejection is each adapter's own responsibility: an engine that cannot honor a decided field must say so rather than return an empty success that hides the dropped decision. The in-memory reference engine acts on this rule directly: it accepts the empty (no-oracle) `decisionHints` unchanged, and any non-`None` field surfaces as an `UnsupportedCapability` naming the first decided field.

---

## Revision history

| Version | Date | Change |
|---|---|---|
| v0.1 (BLOCKED) | 2026-08-25 | First draft. **Blocked by dual review.** Four P1s: (1) hook sample used presence-ARM (identical to inline fallback), contradicting the "value-consult" claims — the deployment semantics change was fictional and the tests non-falsifiable; (2) every production construction site passes `hookRunner = None`, the platform's `Context.meta` is discarded at the provider boundary, so the hook's arm never reaches `seedBroadcastThreshold` in production; (3) `EngineError.HookFailed` is 5-field but the sample only provided 3 (won't compile); (4) the `return Left(...)` from inside the flatMap lambda bypasses `clearQuerySessionTL()` (reintroduces the v0.5-r1 race on the throwing-oracle error path). |
| v0.2 (BLOCKED) | 2026-08-25 | Second draft. **Blocked by dual review.** Two new P1s: (1) the hook's own try/catch swallows every Throwable while the on-disk `EngineHookDispatcher` constructs `HookFailed` ONLY by catching a thrown exception (never inspects meta for `*.error` keys) — so the throwing-oracle path surfaces as anonymous `ProviderInvocationFailed(NoResult)`, losing hook identity; the planned "throwing oracle returns Left(HookFailed) with all 5 fields" test is unimplementable; (2) the fold was placed at "after dispatcher returns and before executeEngine" — an instant that does not exist; `executeEngine` is invoked INSIDE the `engineExecutor` thunk that the dispatcher passes to `dispatcher.run(initialCtx, engineExecutor)`. |
| v0.3 (rebuilt) | 2026-08-25 | Third draft. **Fixes both v0.2 P1s**: (1) the hook's try/catch is REMOVED — a throwing consult propagates to the dispatcher's existing catch which constructs the 5-field `HookFailed` (per ADR-008-AF v1.0) with sanitized message; `runQueryWithHooks` returns `Left(HookFailed(...))`; the spark connector's `query()` is never called; no `querySessionTL` leak (the throwing-oracle test relocates to the platform `EngineServiceRunQueryWithHooksSpec`, not the spark connector's `SparkBroadcastSeedSpec`); (2) the fold is relocated INSIDE the `engineExecutor` thunk — the executor's first action folds the post-PreExecute `Context.meta` into `DecisionHints`; the executor's existing `executeEngine(model, hookReq.mcpRequest, provider)` call is augmented with a 4th `decisionCtx` arg. The v0.2 corrections are kept; the design is now falsifiable (the inline vs value-consult regimes DIVERGE on identical models, so the wiring is observable in tests, not fictional). |
| v0.3 (Implementation, PR-173 + PR-174) | 2026-08-25 | PR-173 (`b38cdf4`) shipped the ADR docs (this file). PR-174 (`0161b7b`) shipped the production wiring: (a) `BroadcastStub.PreExecute.run` writes `sm8.broadcast.arm` (Boolean) + `sm8.broadcast.thresholdBytes` (Long, bytes) at private default thresholds `BroadcastThresholdRows = 10M` (arm consult) + `BroadcastThresholdBytes = 10 MiB` (byte budget); same shape for `SkewStub` with `SkewThresholdRows = 1B`; (b) `sm8-core/engine/DecisionHints.scala` (new) carries the typed `DecisionHints(broadcastArmed, skewArmed, broadcastThresholdBytes)` ADT with full scaladoc (per-decision oracle semantics + asymmetry rationale); (c) `EngineContext` gains `decisionHints: Option[DecisionHints] = None` field; (d) `EngineService.runQueryWithHooks.engineExecutor` thunk folds post-PreExecute `Context.meta` into `DecisionHints` (naturally gated: a throwing oracle short-circuits before the executor fires); (e) `SparkEngineProvider.seedBroadcastThreshold` + `seedSkewFactor` consult `eCtx.decisionHints` first, fall back to inline presence rule. 5 specs + 18 falsifiable tests added (DecisionHintsSpec, BroadcastStubNoOpContractSpec, SkewStubNoOpContractSpec, EngineServiceRunQueryWithHooksSpec, SparkBroadcastSeedSpec with new v0.3 oracle-armed/disarmed/no-oracle/disagreement tests on both axes + the (10M, 1B) window skew-axis divergence). The v0.5-r1 invariants (per-query `newSession()`, `querySessionTL` seam, `lastQuerySessionTL` seam, null-spark short-circuit) are preserved; `clearQuerySessionTL()` is now in try/finally (architect F2 P3 fix at `e7eee1f` in the same wave). |
| Status | 2026-08-25 | Status promoted to **Implemented**. The broadcast + skew decision is live in production: the plugin's `PreExecute` hook is the single source of truth; the typed `DecisionHints` crosses the adapter boundary via `EngineContext.decisionHints`; the spark connector consumes it with an inline fallback. 5 specs, 18 falsifiable tests, dual reviewer CORRECT verdict (architect + data-eng, confidence 0.85). No open follow-ups from this ADR; next wave candidates (ADR-009-e or v0.6 if any) remain out of scope. The `BroadcastStub.consult` + `SkewStub.consult` methods are RETAINED on the plugin (the SDK `Plugin.setup` API still exposes them for plugins that want a synchronous consult call outside the hook path — e.g., a planner that wants to pre-flight the decision without registering a hook) but they are not the production decision path anymore. |
---

## Context

### The duplication (the problem this ADR solves)

Today there are two independent implementations of the broadcast/skew decision:

| Layer | Implementation | Source |
|---|---|---|
| Plugin stubs | `BroadcastStub.consult(model, threshold)` / `SkewStub.consult(model, threshold)` (value-based, threshold-driven) | `plugins/{broadcast,skew}-plugin/src/main/scala/io/sm8/plugins/{broadcast,skew}/*Stub.scala` |
| Spark connector seeds | `seedBroadcastThreshold(ctx, model)` / `seedSkewFactor(ctx, model)` (presence-ARM: `model.joins.exists(_.estimatedRows.isDefined)`) | `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala` (companion, L819, L871) |

The plugin stubs are **not wired into production** (no caller invokes `consult()`). The spark connector's seeds run inline. Both have legitimate roles (plugin = engine-agnostic decision oracle; spark connector = spark-specific seed writer) but the decision itself is currently made in the wrong layer (the connector), with the oracle sitting unused.

### Why the prior attempts didn't wire them

**Attempt 1 (ADR-008-AC, Option A — implement real behavior in the stubs): rejected** by the senior dual review:
1. Plugins are banned from depending on Spark (`<bannedDependencies>org.apache.spark:*</bannedDependencies>` per ADR-008-AD); they can't `import org.apache.spark.sql.SparkSession` to call `conf.set(...)`.
2. Per-request `SparkSession.conf.set(...)` mutates the global session config under concurrent queries — a known correctness anti-pattern.
3. The SDK's `Context.result: Option[Result]` (Result is a marker trait, frozen per ADR-007) doesn't carry a DataFrame, so the stub can't read the join sides.
4. Implementing real behavior would duplicate the spark connector's seed helpers (which are already in the right place).

**Attempt 2 (ADR-009-a, Option B — "plugins own Spark"): rejected** for portability reasons. ADR-009-a explicitly notes: "Option B (plugins own Spark) — rejected (ADR-008-AD, portability)."

**Attempt 3 (this conversation round, Approach C originally proposed `engine.hooks.consultBroadcast(model)`): rejected** by the dual review:
- Spark-safety F-1: the spark provider has no channel to engine.hooks today (`hookRunner = None` in every construction site; production dispatcher fires from OUTSIDE `provider.query`).
- Spark-safety F-2: `BroadcastStub.consult` and `SkewStub.consult` share one signature with opposite semantics (`<=` vs `>=`); an undifferentiated stub-walk would cross-wire decisions.
- RFC F-1: `HookManagerImpl` stores only `HookEntry[PreHook]` (not Plugin instances), so `engine.hooks.consultBroadcast(model)` has no dispatch path.
- RFC F-2: adding `consultBroadcast`/`consultSkew` to `Engine.hooks` violates RFC §4 ("Engine contains no business logic").

**Attempt 4 (this conversation round, ADR-009-d v0.1 first draft): BLOCKED** by the dual review:
- P1-A: hook sample was presence-ARM (identical to inline fallback), contradicting the "value-consult" claims. Deployment semantics change was fictional.
- P1-B: every production construction site passes `hookRunner = None`; the platform's `Context.meta` is discarded at the provider boundary. The hook's arm never reaches `seedBroadcastThreshold` in production.
- P1-C: `EngineError.HookFailed` is 5-field but the sample only provided 3 — wouldn't compile.
- P1-D: the `return Left(...)` from inside the flatMap lambda bypasses `clearQuerySessionTL()` (reintroduces the v0.5-r1 race on the throwing-oracle error path).

### What this v0.2 design does differently

The v0.2 design (the one this document specifies) addresses all four P1s:

1. **The plugin's decision is a value-consult** with a private default threshold (`BroadcastThresholdRows = 10_000_000`, `SkewThresholdRows = 1_000_000_000`). The inline presence rule ARMS any estimated join; the value-consult rule ARMS only when the join's estimated rows cross the threshold. The two regimes differ on identical models — the wiring is observable and the tests are falsifiable.
2. **The decision crosses the boundary as a typed `DecisionHints` field on `EngineContext`** (a non-SDK core type in `sm8-core/engine/`), NOT as a free-form meta-key string the spark connector would re-read. `EngineService.runQueryWithHooks` does the one-line fold from post-hook `Context.meta` into `decisionHints` — this is the production wiring. The spark provider's `hookRunner = None` is irrelevant: the platform dispatcher always fires the hooks; the fold is in the platform, not the provider.
3. **`HookFailed` is never constructed manually in the spark connector.** The existing `EngineHookDispatcher` already constructs the 5-field `EngineError.HookFailed(engine, name, priority, stage, message)` correctly. We rely on the dispatcher's existing fail-fast path. The spark connector just reads `decisionHints` and applies it; it never sees the hook's exception.
4. **No `return` inside any Either chain.** The throwing-oracle path is handled by the dispatcher's fail-fast, which returns `Left(HookFailed(...))` as a value, which flows through `executeEngine`'s `Either` chain, which is bound to the `compiled` val, which the method tail evaluates to apply `clearQuerySessionTL()`. The v0.5-r1 invariant is preserved.

---

## Decision

### 1. The two stub hooks now consult + write `context.meta` (value-consult)

```scala
// plugins/broadcast-plugin/src/main/scala/io/sm8/plugins/broadcast/BroadcastStub.scala
private final class BroadcastPreStubHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {

  // ADR per-query broadcast decision: the plugin's private threshold.
  // The inline rule in the spark connector ARMS any join with
  // estimatedRows; this rule ARMS only when estimatedRows <= threshold.
  // A model with est > threshold is DISARMED here but ARMED inline —
  // the regimes differ, making the wiring observable.
  private val BroadcastThresholdRows: Long = 10_000_000L

  override val name: String = "broadcast-stub"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    // ADR per-query decision: read the validated model from the
    // engine-hook request, compute the value-consult arm with the
    // plugin's private default threshold, write the arm + (when
    // armed) the threshold bytes to context.meta. NO try/catch —
    // a throwing consult must propagate to the platform
    // dispatcher's existing fail-fast (EngineHookDispatcher
    // L121-122 constructs HookFailed from a thrown exception with
    // a sanitized message; the dispatcher never inspects context.meta
    // for *.error keys). Swallowing here would surface as anonymous
    // ProviderInvocationFailed(NoResult) and lose hook identity.
    val model: io.sm8.core.model.Model = context.request match {
      case ehr: io.sm8.core.engine.EngineHookRequest => ehr.model
      case _ => return context
    }
    val arm: Boolean = model.joins.exists(_.estimatedRows.exists(_ <= BroadcastThresholdRows))
    val newMeta: Map[String, Any] = context.meta +
      ("sm8.broadcast.arm" -> arm) +
      ("sm8.broadcast.thresholdBytes" -> BroadcastThresholdBytes)
    context.copy(meta = newMeta)
  }
}
```

```scala
// plugins/skew-plugin/src/main/scala/io/sm8/plugins/skew/SkewStub.scala
private final class SkewPreStubHook(counter: AtomicInteger)
    extends PreHook with java.io.Serializable {

  // ADR per-query skew decision: the plugin's private threshold.
  // The inline rule ARMS any join with estimatedRows; this rule
  // ARMS only when estimatedRows >= threshold (large-row join).
  private val SkewThresholdRows: Long = 1_000_000_000L

  override val name: String = "skew-stub"
  override val priority: Int = 250
  override def stage: HookStage = HookStage.PreExecute

  override def run(context: Context): Context = {
    counter.incrementAndGet()
    // ADR per-query decision: value-consult arm with the plugin's
    // private default threshold; writes arm to context.meta. NO
    // try/catch — see broadcast hook for the throw-vs-swallow
    // contract (a throwing consult propagates to the dispatcher's
    // existing catch which constructs the 5-field HookFailed).
    val model: io.sm8.core.model.Model = context.request match {
      case ehr: io.sm8.core.engine.EngineHookRequest => ehr.model
      case _ => return context
    }
    val arm: Boolean = model.joins.exists(_.estimatedRows.exists(_ >= SkewThresholdRows))
    context.copy(meta = context.meta + ("sm8.skew.arm" -> arm))
  }
}
```
- **No `return` inside flatMap**: the throwing-oracle path is handled by the dispatcher's existing catch — the exception propagates, the dispatcher returns `Left(HookFailed)` as a value, the `runQueryWithHooks` flatMap propagates the `Left`, the spark provider's `query()` is never invoked. The method tail `{ val _r = compiled; if (createdQuerySessionHere) clearQuerySessionTL(); _r }` always evaluates (when reached); the throwing path bypasses the provider entirely.

### 2. The typed decision channel: `DecisionHints` on `EngineContext`

```scala
// NEW FILE: sm8-core/src/main/scala/io/sm8/core/engine/DecisionHints.scala
package io.sm8.core.engine

/**
 * Per-query broadcast + skew arm decisions from any registered
 * plugin's PreExecute hook. Both arms are Optional[Boolean]: None
 * means "no oracle registered for this decision; the adapter may
 * use its inline fallback". Some(true) means "oracle armed". Some(false)
 * means "oracle disarmed" (the adapter must NOT arm, even if its
 * inline rule would).
 *
 * @param broadcastArmed the broadcast arm decision from the plugin
 *                       (None = no oracle; Some(true) = arm; Some(false) = disarm)
 * @param skewArmed      the skew arm decision from the plugin
 *                       (None = no oracle; Some(true) = arm; Some(false) = disarm)
 */
final case class DecisionHints(
  broadcastArmed: Option[Boolean] = None,
  skewArmed: Option[Boolean] = None
)
```

```scala
// CHANGED: sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala
// (additive field; existing constructor stays binary-compatible via the new defaulted param)
final case class EngineContext(
 materializePolicy: MaterializePolicy,
 cachePolicy:   CachePolicy,
 auditPolicy:   AuditPolicy,
 joinHints:    JoinHints,
 timeout:    Duration,
 cancellation:   CancellationCapability,
 // ADR per-query decision oracle: populated by the platform from the
 // post-hook Context.meta; None means no oracle (adapter uses its
 // inline fallback). NOT in the SDK (Context/HookManager/Plugin are
 // frozen per ADR-007); this is a sm8-core/engine/ addition only.
 decisionHints: Option[DecisionHints] = None
) extends Product with Serializable

The existing `EngineContext.defaultContext` continues to work — `decisionHints = None` is the default.

### 3. The platform fold: INSIDE the engineExecutor thunk (not "after dispatcher returns")

The fold must live INSIDE the engineExecutor thunk — the only seam where the post-PreExecute Context is visible. The executor is currently a 5-line for-comp that calls `executeEngine(model, hookReq.mcpRequest, provider)`. The fold is the executor's first action:

```scala
// CHANGED: sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala
// (inside the engineExecutor thunk, replacing the existing for-comp body)
val engineExecutor: Context => Either[EngineError, Context] = { ctx =>
  val hookReq = ctx.request match {
    case hookReq: EngineHookRequest => hookReq
    case other =>
      return Left(EngineError.ProviderInvocationFailed(
        engine = "<dispatcher>",
        name   = "EngineHookDispatcher",
        reason = "UnexpectedRequestType",
        message =
          s"sm8: Context.request must be EngineHookRequest, got ${other.getClass.getName}"
      ))
  }
  // ADR per-query fold: post-PreExecute Context.meta carries the
  // broadcast/skew arm decisions (and the broadcast byte-gate
  // threshold) from any registered plugin's hook. None on each
  // field means "no oracle; the adapter uses its inline fallback".
  // The executor's existing executeEngine call gets a new 4th arg:
  // EngineContext.defaultContext.copy(decisionHints = Some(...))
  // — the fold is naturally gated by "we only build decisionCtx
  // when the executor fires".
  val decisionCtx: EngineContext = EngineContext.defaultContext.copy(
    decisionHints = Some(DecisionHints(
      broadcastArmed        = ctx.meta.get("sm8.broadcast.arm").collect { case b: Boolean => b },
      skewArmed             = ctx.meta.get("sm8.skew.arm").collect { case b: Boolean => b },
      broadcastThresholdBytes = ctx.meta.get("sm8.broadcast.thresholdBytes").collect { case l: Long => l }
    ))
  )
  for {
    provider <- selectEngine(model, request, registry)
    pqr      <- executeEngine(model, hookReq.mcpRequest, provider, decisionCtx)
  } yield ctx.copy(result = Some(EngineHookResult(pqr)))
}
```

def seedBroadcastThreshold(
 querySession: org.apache.spark.sql.SparkSession,
 eCtx:  io.sm8.core.engine.EngineContext,
 model: io.sm8.core.model.Model
): io.sm8.core.engine.EngineContext = {
 if (querySession == null) return eCtx
 // ADR per-query decision oracle: prefer the plugin's arm
 // (Boolean) AND its threshold (Long) when both are present.
 // Falls back to the inline presence rule (arm) and the
 // session threshold (Long). The two regimes differ on models
 // where est > BroadcastThresholdRows — the plugin disarms while
 // the inline rule arms (observable divergence).
 val broadcastOracle = eCtx.decisionHints
 val oracleArm: Option[Boolean]    = broadcastOracle.flatMap(_.broadcastArmed)
 val oracleThreshold: Option[Long] = broadcastOracle.flatMap(_.broadcastThresholdBytes)
 val sessionThreshold: Long = oracleThreshold.getOrElse(
  // unchanged: read spark.sql.autoBroadcastJoinThreshold (10 MiB default)
 )
### 4. The spark connector reads `eCtx.decisionHints` with an inline fallback

When the spark provider is constructed with `hookRunner = None` (the universal production default):
- The provider's connector-level hooks don't fire (the `hookRunner match { case None => ... }` branch runs directly).
- The platform's `EngineHookDispatcher` STILL fires any registered plugin hooks (e.g. `BroadcastStub`, `SkewStub`) on the platform Context. The platform's `runQueryWithHooks` does the fold regardless of the provider's hookRunner.
- The spark provider receives `eCtx` with `decisionHints` populated from the fold.
- The seed helpers consume `decisionHints`; if `Some(b)`, use the arm Boolean; if `None`, fall back to inline.

When no `BroadcastStub`/`SkewStub` is registered:
- The platform dispatcher fires but no hook writes `sm8.broadcast.arm` / `sm8.skew.arm` to the meta.
- The fold produces `DecisionHints(broadcastArmed = None, skewArmed = None)`.
- The seed helpers see `None` → inline fallback → byte-identical to today's behavior.

When `hookRunner = None` AND no platform dispatcher (unit-test path):
- No hook fires.
- The fold doesn't happen (the fold is in `runQueryWithHooks`).
- `EngineContext.defaultContext` is passed to `executeEngine` (the spark connector's `query()` accepts a context).
- `decisionHints = None` → inline fallback → byte-identical to today's behavior.

**Backwards-compatible in all three paths.**

### 5. The bare-deploy shape is unchanged (the 4 backwards-compat paths)

When `BroadcastPreStubHook.run()` throws (no internal try/catch — the hook does NOT swallow):
- The exception propagates out of `h.run(c)` to `EngineHookDispatcher.firePre`'s existing catch block (per ADR-008-AF v1.0, dispatcher L121-122 + L157-159).
- The dispatcher constructs `EngineError.HookFailed(engine = "<dispatcher>", name = h.name, priority = p, stage = "PreExecute", message = Option(e.getMessage).getOrElse(e.getClass.getSimpleName))` — the sanitized message per ADR-008-AF v1.0 (no JVM-internal variable-name leaks in NPE messages).
- The dispatcher returns `Left(HookFailed(...))` from `firePre`. `dispatcher.run` propagates the `Left` unchanged (it does NOT invoke the engineExecutor thunk — the executor never runs).
- `runQueryWithHooks` returns `Left(HookFailed(...))` to its caller. **`executeEngine` is NEVER invoked; the spark provider's `query()` is never called; the v0.5-r1 `querySessionTL` was never set** (because the executor thunk never ran). The next query on the same thread starts with `querySessionTL.get() == null` — verified by the no-leak assertion.
- The `clearQuerySessionTL()` tail is irrelevant (the provider was never called); the next query computes `createdQuerySessionHere = (querySessionTL.get() == null) = true` and creates a fresh per-query session.

**The throwing-oracle test relocates to the platform spec** (`EngineServiceRunQueryWithHooksSpec`), NOT the spark connector's `SparkBroadcastSeedSpec`. The platform spec registers a stub whose `PreExecute.run` throws, calls `runQueryWithHooks`, asserts `Left(EngineError.HookFailed(sparkEngineName, "broadcast-stub", 250, "PreExecute", "<sanitized message>"))` — the dispatcher's 5-field construction is the single owner. There is no need to construct `HookFailed` manually in the spark connector (P1-C closed).
### 7. The v0.5-r1 invariants are preserved

- **Per-query `newSession()`**: unchanged (`SparkEngineProvider.scala:204-216`). The decision is read from `eCtx.decisionHints` AFTER the per-query session is created; the seed helper writes to the per-query session's conf (race-free per ADR-009-c v0.5-r1).
- **`querySessionTL` seam**: unchanged. Tests using `withQuerySessionTL()` to verify conf still work.
- **`lastQuerySessionTL` seam**: unchanged.
- **Null-spark short-circuit**: unchanged.
- **No `return` inside flatMap**: the throwing-oracle path is handled by the dispatcher's fail-fast, NOT by an early `return` inside the spark provider's `query()`. The method tail `{ val _r = compiled; if (createdQuerySessionHere) clearQuerySessionTL(); _r }` always evaluates.

---

## Considered options (recap of the 4 prior attempts)

### Option A: SDK-level `engine.hooks.consultBroadcast(model)` (Attempt 3) — REJECTED

- Violates RFC §4 (core accumulates business logic).
- Violates RFC plugins.md (adapter calling a stub's `consult()` directly is an unshaped interaction).
- `HookManagerImpl` has no dispatch path to Plugin instances (it stores `PreHook` entries only).
- `SkewStub.consult` (`>=`) and `BroadcastStub.consult` (`<=`) share a signature with opposite semantics; an undifferentiated walk cross-wires decisions.
- The plugin's threshold parameter is a row count; the seed helper writes a byte budget; passing the byte budget recreates the rows-vs-bytes hazard.

### Option B: Implementation in stubs (`Option A` of ADR-008-AC, Attempt 1) — REJECTED

- Banned-dependencies violation (plugins can't import Spark).
- Per-request `conf.set` is racy on a shared session.
- Duplicates spark connector logic.
- SDK break (would need abstract methods on Plugin).

### Option C (ADR-009-d v0.1): Hook writes context.meta; connector reads context.meta — REJECTED (BLOCKED by dual review)

- Hook sample was presence-ARM (same as inline fallback), contradicting "value-consult" claims (P1-A).
- Production wiring gap: every construction site passes `hookRunner = None`; the platform's `Context.meta` is discarded at the provider boundary; the hook's arm never reaches the seed helpers in production (P1-B).
- HookFailed arity: 5 fields but the sample only provided 3 (P1-C).
- Early `return` from inside the flatMap reintroduces the v0.5-r1 race (P1-D).

### Option D (ADR-009-d v0.3): Hook writes context.meta; platform folds meta into `EngineContext.decisionHints` INSIDE the engineExecutor thunk; connector reads typed decision + threshold; throw propagates to the dispatcher's catch — CHOSEN
- Plugin's value-consult rule genuinely differs from the inline presence rule (P1-A closed; falsifiable tests construct a model where the two regimes disagree).
- The decision crosses the boundary as a typed `DecisionHints` field on `EngineContext` (P1-B closed; the platform fold is the production wiring).
- `HookFailed` is never constructed manually in the spark connector; the dispatcher's existing 5-field construction is the single owner (P1-C closed).
- No `return` inside any Either chain; the throwing-oracle path is handled by the dispatcher (P1-D closed).
- Zero SDK changes (`EngineContext.decisionHints` is a non-SDK core type).
- Zero banned-deps lift.
- v0.5-r1 invariants preserved.

---

## Consequences

### Positive

- **Single source of truth for the decision**. The plugin's `consult()` (now lifted into the hook's `run()`) is the only place the broadcast/skew decision is computed. The spark connector consumes the typed decision via `eCtx.decisionHints`; it doesn't duplicate the decision.
- **Engine-portable decision**. The same plugin works for any adapter (Trino, in-memory) — the adapter just reads `eCtx.decisionHints` and applies its own engine-specific seed (byte budget for Spark, operator-configured threshold for Trino, etc.).
- **No race introduced**. The conf writes stay on the per-query `SessionState` (v0.5-r1 invariant intact). The plugin doesn't write any Spark conf at all.
- **Falsifiable tests in both regimes**. The test suite covers both no-oracle (inline presence-ARM stays; today's behavior) and oracle-wired (value-consult; the stub's decision wins). The two regimes are constructed to differ on identical models with a disagreement.
- **Aligned with RFC Core Boundary**. The decision is in the plugin (the place that knows about business rules per RFC §3). The typed `DecisionHints` value crosses the boundary via `EngineContext` (a core type, not a plugin-defined string).
- **Backwards-compatible by construction**. Three independent paths all yield the same behavior as today when no plugin is wired: bare-deploy (`hookRunner = None`), no-platform-dispatcher (unit tests), and no-stub-registered (any deployment). The fold is additive.

### Negative

- **Deployment semantics change when stubs are wired**. A model with `estimatedRows > BroadcastThresholdRows` is today armed (presence-ARM) but would NOT be under the stub (value-consult). This is the intended consequence of having a real decision oracle. Documented here + asserted by tests in both regimes.
- **The `consult(model, threshold)` public method on the stubs is preserved** for engine-portable adapters (Trino, in-memory) that want the threshold-driven decision with a custom threshold. The spark connector does NOT pass a threshold; it just reads `decisionHints`.
- **The fold is one line in `EngineService.runQueryWithHooks`** but it's a real production-path change. Any future platform-level refactor of `runQueryWithHooks` must preserve the fold.

### Risk (named)

- **Forgetting the fold in `runQueryWithHooks`**. If a future refactor moves the executor call and drops the fold, the decision doesn't reach the seed helpers in production. Mitigation: a `EngineServiceRunQueryWithHooksSpec` test that constructs the production shape (registry + dispatcher + 2 stubs), runs `runQueryWithHooks`, and asserts the executed `EngineContext` has `decisionHints` populated.
- **Throwing oracle + ThreadLocal leak**. The P1-D fix is to rely on the dispatcher's fail-fast (which constructs `HookFailed` correctly) and to never `return` from inside the spark provider's `query()`. Mitigation: a test that simulates a throwing oracle (a stub whose `run` throws), asserts `query()` returns `Left(HookFailed)` with all 5 fields, AND asserts `querySessionTL.get() == null` after the call (no leak).
- **Wrong-type meta value**. `postHookCtx.meta.get("sm8.broadcast.arm").collect { case b: Boolean => b }` silently yields `None` for a wrong-typed value under the key. Mitigation: the key spelling is documented as private to the broadcast/skew plugin hooks; any non-Boolean value is treated as "no oracle" (inline fallback).
- **Meta-key collision across plugins**. If two plugins write to `sm8.broadcast.arm`, last-writer-wins (later-priority is later in the chain). Mitigation: document the key ownership (`sm8.broadcast.arm` is private to `BroadcastStub`'s hook; `sm8.skew.arm` is private to `SkewStub`'s hook); any other plugin writing these keys creates undefined semantics and should be discouraged.

---

## Implementation plan (PR + tests, ~120-160 LOC + 8-10 new tests)

### Step 1: New core type `DecisionHints`

**Files changed:**
- `sm8-core/src/main/scala/io/sm8/core/engine/DecisionHints.scala` (new, ~30 LOC)

**Change**: new final case class with `broadcastArmed` + `skewArmed` (both `Option[Boolean]`, defaulted to `None`). Scaladoc explains the ownership + the wrong-type fallback semantics.

**Tests added**: `sm8-core/src/test/scala/io/sm8/core/engine/DecisionHintsSpec.scala` (~50 LOC, 3-4 unit tests):
- `DecisionHints(broadcastArmed = None, skewArmed = None).broadcastArmed is None`
- `DecisionHints(broadcastArmed = Some(true), skewArmed = Some(false)).broadcastArmed is Some(true)`
- `DecisionHints().broadcastArmed is None (default arg)`

### Step 2: Add `decisionHints` field to `EngineContext`

**Files changed:**
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala` (additive change, +1 field with `None` default)

**Change**: add `decisionHints: Option[DecisionHints] = None` as the 7th field of the case class. The existing 6-arg constructor (and `defaultContext`) continue to work via the new default.

**Tests stay green**: every existing `EngineContextSpec` test uses the 6-arg ctor or `defaultContext`. With the defaulted 7th arg, they're binary-compatible.

### Step 3: Plugin stubs lift `consult()` into `PreExecute.run()`

**Files changed:**
- `plugins/broadcast-plugin/src/main/scala/io/sm8/plugins/broadcast/BroadcastStub.scala` (+30 LOC)
- `plugins/skew-plugin/src/main/scala/io/sm8/plugins/skew/SkewStub.scala` (+30 LOC)

**Change**: each `PreStubHook.run(context)` reads `context.request.model`, computes the arm Boolean via the value-consult rule with a private default threshold, writes `context.meta("sm8.broadcast.arm")` / `("sm8.skew.arm")`, returns the new Context. Wrapped in try/catch; on exception writes the error key + sets `stop = true` + sanitized message.

**Existing tests stay green**: `SkewStubContractSpec`, `SkewStubNoOpContractSpec`, `SkewStubSpec`, `BroadcastStub*` equivalents — they don't assert on `context.meta`; they assert on the `fires` counter. The counter still increments.

**New tests** (in each plugin's test/ folder):
- `SkewStub: PreExecute hook writes context.meta("sm8.skew.arm") = true when model has join with estimatedRows >= SkewThresholdRows`
- `SkewStub: PreExecute hook writes context.meta("sm8.skew.arm") = false when model has join with estimatedRows < SkewThresholdRows`
- `SkewStub: PreExecute hook writes context.meta("sm8.skew.arm") = false when model has no join with estimatedRows`
- `SkewStub: throwing the consult writes the error meta key + sets stop=true (sanitized message)`
- (4 mirror tests for BroadcastStub)

### Step 4: Platform fold INSIDE the engineExecutor thunk in `EngineService.runQueryWithHooks`

**Files changed:**
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` (+~25 LOC: engineExecutor thunk replacement + the fold + DecisionHints import)

**Change**: the engineExecutor thunk (the 5-line for-comp that calls `executeEngine(model, hookReq.mcpRequest, provider)`) gains a fold as its first action: read the post-PreExecute `ctx.meta`, build `DecisionHints(broadcastArmed, skewArmed, broadcastThresholdBytes)`, attach to `EngineContext.defaultContext.copy(decisionHints = Some(...))`, pass as the 4th arg to `executeEngine`. The fold is naturally gated by "we only build decisionCtx when the executor fires" (a throwing oracle short-circuits before the executor runs).

**Tests stay green**: every existing `EngineServiceSpec` test uses `defaultContext` or constructs an `EngineContext` directly; `decisionHints = None` is the default.

**New tests** (the production-wiring proof + the throwing-oracle proof — both live here, not in the spark connector):
- `EngineServiceRunQueryWithHooksSpec`: construct the production shape (`Engine.use(new BroadcastStub)`, `Engine.use(new SkewStub)`, build `EngineHookDispatcher`, run `runQueryWithHooks` with a model that has `estimatedRows = 100M` rows and `estimatedRows = 5B` rows), assert the executed `EngineContext` has `decisionHints.broadcastArmed = Some(true)` for the 100M model and `Some(false)` for the 5B model. **This test proves the wiring gap is closed.**
- `EngineServiceRunQueryWithHooksSpec`: throwing oracle — register a stub whose `PreExecute.run` throws (no internal try/catch in the hook), call `runQueryWithHooks`, assert `Left(EngineError.HookFailed(<engine>, "broadcast-stub", 250, "PreExecute", "<sanitized message>"))` is returned (the dispatcher's 5-field construction). **This test proves the throw-vs-swallow contract.**

**Change**: `seedBroadcastThreshold` reads `eCtx.decisionHints.flatMap(_.broadcastArmed)` (the Boolean arm) AND `eCtx.decisionHints.flatMap(_.broadcastThresholdBytes)` (the Long threshold); `seedSkewFactor` reads `eCtx.decisionHints.flatMap(_.skewArmed)`. The arms are preferred over the inline presence rule when present; `None` falls through to the inline rule (byte-identical to today). The threshold is preferred over `spark.sql.autoBroadcastJoinThreshold` when present; `None` falls through to the session default.

**Existing tests stay green**:
- 4 seed-behavior tests in `SparkBroadcastSeedSpec` (lines 110, 117, 125, 134) call `seedBroadcastThreshold(spark, ctx, model)` with `defaultContext` (no `decisionHints`); inline fallback fires; identical to today.
- 4 v0.5 wave tests (lines 142, 181, 210, 223) call `provider.query(...)` which goes through `compileSteps`; the seed helpers read `eCtx.decisionHints = None` → inline fallback; identical to today.

**New tests** (the falsifiable agreement/disagreement tests — the P1-A fix; the throwing-oracle test is in the platform spec per Step 4):
- `broadcast oracle armed: model with small estimatedRows → BroadcastHashJoinExec` — construct `EngineContext(..., decisionHints = Some(DecisionHints(broadcastArmed = Some(true), broadcastThresholdBytes = Some(10*1024*1024))))`, run a model with `estimatedRows = 100K` rows, assert the physical plan includes `BroadcastHashJoinExec`.
- `broadcast oracle disarmed: model with huge estimatedRows → SortMergeJoinExec` — construct `EngineContext(..., decisionHints = Some(DecisionHints(broadcastArmed = Some(false), broadcastThresholdBytes = Some(10*1024*1024))))`, run a model with `estimatedRows = 100B` rows (above `BroadcastThresholdRows = 10M`), assert the physical plan is `SortMergeJoinExec` (NOT `BroadcastHashJoinExec`).
- `broadcast no-oracle: model with huge estimatedRows → BroadcastHashJoinExec (inline rule)` — same model (100B rows), `decisionHints = None`, assert the inline presence rule ARMS the broadcast (this is the disagreement case).
- `broadcast oracle disagreement: oracle disarms but inline would arm → SortMergeJoinExec` — same model + `decisionHints = Some(DecisionHints(broadcastArmed = Some(false)))`, assert the physical plan is `SortMergeJoinExec`. **This is the falsifiable proof that the oracle wins.**
- (4 mirror tests for skew: armed, disarmed, no-oracle, disagreement — skew uses the pre-existing JoinHints.skewFactor = Some(f) precondition per ADR-009-c v0.5-r1 F2 fix)

### Step 6: Verify + close out

- Full reactor tests stay green (no breaking changes to existing signatures):
  - 4 existing seed-behavior tests in `SparkBroadcastSeedSpec` (lines 110, 117, 125, 134) — inline fallback path
  - 4 v0.5 wave tests in `SparkBroadcastSeedSpec` (lines 142, 181, 210, 223) — inline fallback path through `provider.query`
- New tests added (counted per file):
  - `DecisionHintsSpec` (sm8-core): 3 unit tests (default args + Some/None round-trip)
  - `BroadcastStubSpec` + `BroadcastStubNoOpContractSpec` (plugins): 3 unit tests (armed / disarmed / no-oracle; the throwing case is at the platform spec)
  - `SkewStubSpec` + `SkewStubNoOpContractSpec` (plugins): 3 unit tests (same shape)
  - `EngineServiceRunQueryWithHooksSpec` (sm8-platform): 2 integration tests (production-wiring proof + throwing-oracle proof — both live here, not in the spark connector)
  - `SparkBroadcastSeedSpec` (spark-connector): 8 integration tests (4 broadcast-oracle + 4 skew-oracle — armed / disarmed / no-oracle / disagreement)
  - **Total: 19 new tests** (3 + 3 + 3 + 2 + 8)
- Full reactor: sm8-core + sm8-platform + all 15 modules green
- Hygiene: memory < 90%, disk < 65%, 0 orphans
- Dual review (architect + data-eng) on the PR; per the standing directive, all findings fixed before merge
- ADR-009-d status promoted to Implemented after merge

---

## References

- **ADR-008-AC**: Stub plugins — rename + verify no-op contract. The 5 reasons not to wire stubs to Spark (banned-deps, racy conf.set, spark connector already handles it, Result API frozen, Plugin trait frozen).
- **ADR-008-AD**: Parent POM — hoist `bannedDependencies=org.apache.spark:*` to enforce Zero-Spark invariant globally.
- **ADR-008-AF v1.0**: `EngineHookDispatcher` fail-fast (sanitized `HookFailed.message`).
- **ADR-008-AE v1.0**: HookManager no-eviction invariant (hooks live for the manager's lifetime).
- **ADR-009-a**: Adapter-side join strategy from `JoinSpec.estimatedRows` — seed the Spark broadcast byte-threshold. Presence-ARM semantics for broadcast; `Option B (plugins own Spark)` rejected.
- **ADR-009-b v0.4**: AQE skew wiring — deferred. Per-query factor not expressible on shared session; the real fix is per-query SparkSession (which ADR-009-c delivered).
- **ADR-009-c v0.5-r1 (PR-171)**: per-query `newSession()` for `JoinHints.skewFactor`. The v0.5-r1 race fix (TL clear in `finally`) is intact; this ADR only changes the decision input, not the per-query session isolation.
- **ADR-007**: SDK freeze (Plugin trait, Context, HookManager all immutable).
- **RFC `semantic-layer-engine-architecture.md`** §3 (Core Boundary table), §4 (Core Concepts), §7/7a (examples), §11 (Repo Structure).
- **RFC `plugins.md`** Rule 1 (setup is idempotent registration only), Rule 3 (plugins communicate via `context.meta`).
- **RFC `hooks.md`** (8 attachment points; PreExecute hooks read `context.request`, write `context.meta`; hook never knows which adapter is in use).
- **RFC `adapters.md`** (adapter never knows about hooks/plugins; adapter implements the adapter contract).
- `sm8-core/src/main/scala/io/sm8/sdk/Context.scala` (frozen; `meta: Map[String, Any]`).
- `sm8-core/src/main/scala/io/sm8/sdk/HookManager.scala` (frozen trait; `registerPreHook` / `preHooksFor`).
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineContext.scala` (L45: existing case class; new `decisionHints: Option[DecisionHints] = None` field added at the end).
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineHookTypes.scala` (`EngineHookRequest(model, mcpRequest, cacheKey)` — the model is on the request).
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala` (L140-150: `HookFailed(engine, name, priority, stage, message)` — 5-field, no defaults; constructed ONLY by `EngineHookDispatcher`).
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala` L341-... (`runQueryWithHooks` — the production executor path; the fold is inserted here).
- `sm8-platform/src/main/scala/io/sm8/platform/query/hooks/EngineHookDispatcher.scala` (the platform dispatcher; fires PreExecute hooks; constructs `HookFailed` on failure).
- `connectors/spark-connector/src/main/scala/io/sm8/connectors/spark/SparkEngineProvider.scala` L819 (`seedBroadcastThreshold`), L871 (`seedSkewFactor`), L387-449 (query() hookRunner dispatch — unchanged), L204-216 (per-query `newSession()` — unchanged).
- `plugins/broadcast-plugin/src/main/scala/io/sm8/plugins/broadcast/BroadcastStub.scala` (current stub; the `PreStubHook.run()` is the new consult site).
- `plugins/skew-plugin/src/main/scala/io/sm8/plugins/skew/SkewStub.scala` (same).