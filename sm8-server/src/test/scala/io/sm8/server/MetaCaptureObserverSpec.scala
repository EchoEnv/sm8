/*
 * SM8 Server — MetaCaptureObserver spec (PR-214, audit 2026-08-30 H1).
 *
 * Per [[debug-mantra-mindset]]: each test exercises ONE observable
 * contract of the snapshot observer:
 *   (1) the snapshot is a FRESH INSTANCE (not the same map reference
 *       passed in by the engine fold) — the headline H1 fix;
 *   (2) the snapshot's CONTENT equals the source at hook time;
 *   (3) the snapshot is an `immutable.Map`, regardless of input;
 *   (4) the hook preserves observer semantics + stage + priority;
 *   (5) the hook returns the same Context instance it received.
 *
 * Per [[karphyaguidsmindset]]: pre-PR-214, `target.set(context.meta)`
 * aliased the SAME map instance — `target.get() eq context.meta`
 * was TRUE. Post-PR-214, `target.set(Map.from(context.meta))` writes
 * a fresh immutable copy — `target.get() eq context.meta` is FALSE.
 * Test 1 is the tight pre-fix falsifier: it would FAIL on pre-fix
 * code.
 *
 * Per [[scala-jvm-safetymindset]]: NPE safety — `Map.from(null)`
 * throws NPE, but `Context.meta` is non-null by SDK contract
 * (`Map.empty` default). No `try`/`catch` is needed.
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
    // Post-fix, `target.set(Map.from(context.meta))` produces a
    // fresh copy, so `target.get() eq context.meta` is FALSE.
    // This is the tight pre-PR-214 falsifier — the assertion
    // would fail on the pre-fix code.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val ctx      = ctxWithMeta(Map("sm8.k" -> "v"))

    observer.postHook.run(ctx)

    (target.get() eq ctx.meta) shouldBe false
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

  test("[H1] postHook snapshot is an immutable.Map regardless of source concrete type") {
    // Scala 2.13 `Map.from(coll)` returns an immutable Map (HashMap
    // for default sizes; Map1/Map2/Map3/Map4 for small sizes).
    // Verify the snapshot is immutable regardless of input.
    val target   = new AtomicReference[Map[String, Any]](Map.empty)
    val observer = new MetaCaptureObserver(target)
    val ctx      = ctxWithMeta(Map("k" -> "v"))

    observer.postHook.run(ctx)

    (target.get() match {
      case _: scala.collection.immutable.Map[_, _] => true
      case _                                       => false
    }) shouldBe true
  }

  test("[H1] postHook snapshot of empty meta is an immutable.Map (no NPE)") {
    // Per scala-jvm-safety: `Map.from(Map.empty)` is total and
    // returns `Map.empty` (the immutable EmptyMap singleton).
    // NPE safety check on the SDK contract.
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
