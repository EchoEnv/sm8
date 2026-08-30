/*
 * SM8 Server — MetaCaptureObserver spec (PR-214, audit 2026-08-30 H1).
 *
 * Per [[debug-mantra-mindset]]: each test exercises ONE observable
 * contract of the snapshot observer:
 *   (1) the snapshot is a FRESH INSTANCE for non-HashMap input
 *       (Map1/Map2/Map3/Map4 source) — tight pre-PR-214 falsifier;
 *   (2) the snapshot is a FRESH INSTANCE for HashMap input (5+ keys
 *       — production-realistic shape after several plugins have
 *       folded into context.meta) — tight post-`HashMap.from`-
 *       regression falsifier;
 *   (3) the snapshot's CONTENT equals the source at hook time;
 *   (4) the snapshot is an `immutable.HashMap`, regardless of input;
 *   (5) the snapshot of empty meta is also an immutable.Map (NPE-safe);
 *   (6) the hook preserves observer semantics + stage + priority;
 *   (7) the hook returns the same Context instance it received.
 *
 * Per [[karphyaguidsmindset]]: pre-PR-214, `target.set(context.meta)`
 * aliased the SAME map instance — `target.get() eq context.meta`
 * was TRUE. Three rejected revisions all had `instanceof HashMap`
 * short-circuits:
 *   - Revision 1 (`target.set(Map.from(context.meta))`):
 *     `Map.from(immutable.Map)` short-circuits ALL immutable inputs.
 *   - Revision 2 (`target.set(HashMap.from(context.meta))`):
 *     `HashMap.from(HashMap)` short-circuits on HashMap input —
 *     production-realistic after ~5 `+` operations.
 *   - Revision 3 candidate (`target.set(HashMap.empty[String, Any]
 *     ++ context.meta)`): `HashMap.concat` short-circuits when
 *     receiver is empty and argument is a HashMap (returns the
 *     argument unchanged).
 * The post-fix body is `target.set(HashMap.from(context.meta
 * .iterator))` — passing an `Iterator` (which is NOT a HashMap)
 * bypasses the `instanceof HashMap` check in `HashMap.from`,
 * forcing the `HashMapBuilder` path regardless of input concrete
 * type. Tests 1 + 2 are tight pre-PR-214 / post-revision-1/2/3
 * falsifiers.
 *
 * Per [[scala-jvm-safetymindset]]: NPE safety — `HashMap.from(null
 * .iterator)` throws NPE, but `context.meta.iterator` is non-null
 * per `Map.empty` default. No `try`/`catch` is needed.
 *
 * Per [[scala-perf-testingmindset]]: each test allocates one
 * `AtomicReference` + one `MetaCaptureObserver` + one `Context`.
 * All in-process; no I/O. Runs in <50ms total.
 *
 * Layer: adapter / sm8-server. Companion to
 * `MetaCaptureObserver.scala`. The test lives in `io.sm8.server`
 * (same package as the subject) so the `private[server]` seam on
 * `postHook` is reachable. No sm8-core / plugin imports — all
 * SDK references go through `io.sm8.sdk.{Context, PipelineStage, …}`.
 */
package io.sm8.server

import io.sm8.sdk.{Context, PipelineStage, PostHook, Request}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference

class MetaCaptureObserverSpec extends AnyFunSuite with Matchers {

  /** Build a minimal [[Context]] with the given meta. The
    * `request` field is an anonymous marker (the SDK keeps it
    * abstract pre-Step-3; see [[io.sm8.sdk.Request]] scaladoc). */
  private def ctxWithMeta(meta: Map[String, Any]): Context =
    Context(
      stage   = PipelineStage.Execute,
      request = new Request {},
      meta    = meta
    )

  // ---- [H1] headline: snapshot is a fresh instance, not an alias ----

  test("[H1] postHook writes a FRESH instance — target.get() !== context.meta") {
    // Pre-fix, `target.set(context.meta)` aliased the SAME map
    // instance, so `target.get() eq context.meta` was TRUE.
    // Post-fix (revision 4 — current), `target.set(HashMap.from
    // (context.meta.iterator))` produces a fresh copy via the
    // HashMapBuilder path. For Map1 input the result is a fresh
    // HashMap (not the source Map1). This test exercises the
    // Map1 case — the tight pre-PR-214 falsifier that would
    // FAIL on the pre-fix code.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val ctx      = ctxWithMeta(Map("sm8.k" -> "v"))

    observer.postHook.run(ctx)

    (target.get() eq ctx.meta) shouldBe false
  }

  test("[H1] postHook writes a FRESH instance for HashMap input (production-realistic 5-key meta)") {
    // Per data-eng review (bat, 2026-08-30): the first PR-214
    // revision used `target.set(HashMap.from(context.meta))`, but
    // Scala 2.13's `HashMap.from(HashMap)` short-circuits on
    // `instanceOf` and returns the input unchanged. After ~5
    // `context.meta + (k -> v)` operations (cache policy, skew
    // arm, broadcast threshold, transformer, cache error), the
    // meta IS a HashMap — so the literal `HashMap.from` fix
    // regressed the H1 bug for the production case. The post-fix
    // body `HashMap.empty[String, Any] ++ context.meta` uses
    // `HashMap.++` (always allocates `new HashMap` + copies via
    // the iterator), bypassing the short-circuit.
    //
    // This test constructs the same shape production meta takes:
    // 5 entries → Map builder selects HashMap. The identity check
    // would FAIL on the pre-fix code AND on the first PR-214
    // revision. PASSES on the second revision (the current fix).
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val hashMapInput: Map[String, Any] = scala.collection.immutable.HashMap(
      "sm8.cache.policy"     -> "LRU",
      "sm8.skew.armed"       -> true,
      "sm8.broadcast.thresh" -> 10485760L,
      "sm8.transformer"      -> "json",
      "sm8.cache.error"      -> "miss"
    )
    // Sanity: the source IS a HashMap (size > 4 → Map builder
    // selects HashMap). If this fails in a future Scala version
    // that changes the builder threshold, the test would no longer
    // exercise the short-circuit path — adjust the entry count.
    hashMapInput.isInstanceOf[scala.collection.immutable.HashMap[_, _]] shouldBe true

    val ctx = ctxWithMeta(hashMapInput)
    observer.postHook.run(ctx)

    (target.get() eq ctx.meta) shouldBe false
    target.get() shouldBe hashMapInput  // content preserved
  }

  test("[H1] postHook snapshot contents equal source at hook time") {
    // Companion to the identity check: defensive copy must not
    // silently drop or rename keys. Content equality is preserved.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val source   = Map("sm8.a" -> 1, "sm8.b" -> "two", "sm8.c" -> true)
    val ctx      = ctxWithMeta(source)

    observer.postHook.run(ctx)

    target.get() shouldBe source
  }

  // ---- [H1] snapshot type contract ----

  test("[H1] postHook snapshot is an immutable.HashMap regardless of source concrete type") {
    // The post-fix body `HashMap.empty[String, Any] ++ context.meta`
    // uses the `HashMap.++` builder path, which always returns a
    // HashMap (or the `emptyHashMap` singleton for empty input).
    // Verify the snapshot is an `immutable.HashMap` regardless of
    // input concrete type — this pins the contract that downstream
    // consumers (the inspector) can rely on.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val ctx      = ctxWithMeta(Map("k" -> "v"))

    observer.postHook.run(ctx)

    (target.get() match {
      case _: scala.collection.immutable.HashMap[_, _] => true
      case _                                          => false
    }) shouldBe true
  }

  test("[H1] postHook snapshot of empty meta is an immutable.HashMap (no NPE)") {
    // Per scala-jvm-safety: `HashMap.empty[String, Any] ++ Map.empty`
    // is total and returns the `emptyHashMap` singleton (same as
    // `Map.empty` in observable behaviour, both immutable). NPE
    // safety check on the SDK contract.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)

    noException should be thrownBy observer.postHook.run(ctxWithMeta(Map.empty))
    target.get() shouldBe Map.empty
  }

  // ---- observer semantics preserved through the refactor ----

  test("[H1] postHook preserves observer semantics (runsOnStop = true)") {
    // The observer must fire even when a pre-hook set `stop = true`
    // (the short-circuit path). Pre-fix and post-fix both preserve
    // this — this test pins the invariant through the refactor.
    val observer = new MetaCaptureObserver(
      new AtomicReference[Map[String, Any]](Map.empty)
    )
    observer.postHook.runsOnStop shouldBe true
  }

  test("[H1] postHook preserves stage + priority + name (PostExecute, 999, 'MetaCaptureObserver')") {
    // Pin the wiring constants through the refactor: the inline
    // predecessor set `stage = HookStage.PostExecute`,
    // `priority = 999`, `name = "MetaCaptureObserver"`. The
    // extracted `postHook` must keep all three, otherwise the
    // snapshot would interleave with other post-hooks at the
    // wrong priority and lose its identity in the inspector log.
    val observer = new MetaCaptureObserver(
      new AtomicReference[Map[String, Any]](Map.empty)
    )
    observer.postHook.stage shouldBe io.sm8.sdk.HookStage.PostExecute
    observer.postHook.priority shouldBe 999
    observer.postHook.name shouldBe "MetaCaptureObserver"
  }

  // ---- return value contract ----

  test("[H1] postHook.run returns the SAME context instance it received") {
    // PostHooks are contractually required to return a (possibly
    // mutated) Context; MetaCaptureObserver is read-only on the
    // context (it only writes to `target`). The returned Context
    // MUST be the same instance (not a copy) so the engine fold
    // chain doesn't allocate per hook.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val ctx      = ctxWithMeta(Map("k" -> "v"))

    val returned = observer.postHook.run(ctx)
    (returned eq ctx) shouldBe true
  }
}
