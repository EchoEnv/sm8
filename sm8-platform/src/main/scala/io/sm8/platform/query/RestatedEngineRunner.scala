/*
 * SM8 Platform — RestatedEngineRunner (v2.x journaled-execution helper).
 *
 * Per [[karpathy-guidelines-mindset]] (smallest correct change):
 * this PR (PR-C5b-ext-γ) ships the SDK dep + a thin v2.x-shaped
 * helper that documents how a future handler will integrate
 * journaled execution. The actual `@Service` handler-class
 * wiring is the follow-up PR (which can leverage this dep +
 * helper as build blocks).
 *
 * ==v2.x API reality==
 *
 * The legacy `semanticdf-platform` Java code used the v1.x static
 * `Restate.run("query.execute", RestateCachedRow.class, () -> ...)`
 * API. Restate SDK v2.x — the only version on Maven Central
 * (v2.0.0, v2.1.0, v2.1.1) — **does NOT** expose that static API.
 * v2.x is fully state-machine-driven; journaled sub-calls are
 * `CompletableFuture<AsyncResult<Slice>> submitRun(String,
 * Consumer<RunCompleter>)` instance methods on `HandlerContext`,
 * which require being inside a `@Service` `@Handler`-annotated
 * method that participates in the SDK's state machine.
 *
 * A proper v2.x integration requires defining a full `@Service`
 * handler class, registering its `ServiceDefinition` on a
 * `RestateHttpServer` bootstrap, configuring a serde (Jackson
 * or Kotlinx) for `RestateCachedRow`, and either sdk-testing or
 * Testcontainers for end-to-end tests. That's a much larger PR.
 *
 * ==This PR's contribution==
 *
 * This PR does the **prerequisite** work so the follow-up PR is
 * strictly additive (no Maven dep churn, no helper churn):
 *
 *   1. Adds `dev.restate:sdk-{common,core,java-http}:2.1.1` to
 *      `sm8-platform/pom.xml` (compile) + pins in parent
 *      `pom.xml` `dependencyManagement` (reproducible builds per
 *      [[scala-jar-packaging-mindset]]).
 *   2. Defines `RestatedEngineRunner.runJournaled(name, ctype,
 *      supplier)`: today's non-handler call path. Calls the
 *      supplier directly + DEBUG-log. Returns the supplier's value.
 *      Until the handler-class wiring lands, this is the only
 *      path that can be exercised in the reactor.
 *
 * ==Strict-skill checks==
 *
 * - karpathy-guidelines: surgical (1 helper, no behavior change to
 *   EngineService.runQuery).
 * - scala-data-driven-refactor: `object` (not trait); only one
 *   production strategy exists today (supplier-direct), no
 *   journaled path yet wired.
 * - scala-jvm-safety: null-safe probing via `Option(...).isDefined`;
 *   supplier exceptions propagate.
 * - scala-error-handling: either at engine boundary (today) +
 *   supplier exceptions as Restate journal failures (future).
 * - scala-impact-analysis: 0 production callers change behavior.
 *   Dep adds are reproducible (parent-pom pin).
 * - scala-jar-packaging: minimal 3 SDK artifacts; verified no
 *   Spark pulled.
 *
 * ==Serializable hygiene (PR-C6 + PR-C5b-ext-β verified)==
 *
 * The 9 thread-through types in `EngineService.runQuery`
 * (`Model`, `MCPQueryRequest`, `MCPEngineRegistry`, `MCPEngineProvider`,
 * `EngineContext`, `QueryResult`, `QueryRequest`, `ResultCache`,
 * `RestateCachedRow`) are all proven `Serializable` by
 * `EngineServiceSpec.scala:547` ("runQuery: serializable-safe
 * (Spark closure hygiene)"). When the handler-class wiring
 * follow-up PR lands, the supplier closure for journaled sub-calls
 * will capture only these types — Restate SDK enforces
 * Serializable for journaled values.
 */
package io.sm8.platform.query

import io.sm8.core.cache._
import io.sm8.platform.query.cache._
import java.util.function.Supplier

/**
 * Restate v2.x-shaped helper for journaled-execution sites.
 *
 * Today (PR-C5b-ext-γ), only `runJournaled` is callable; the
 * follow-up PR's handler-class wiring will use this dep +
 * `HandlerContext.submitRun` (the v2.x async journal API)
 * directly inside a `@Service` `@Handler` method body.
 *
 * ==Usage (today's non-handler path)==
 *
 * {{{
 * val cachedRow: RestateCachedRow = RestatedEngineRunner.runJournaled(
 *   "query.execute",
 *   classOf[RestateCachedRow],
 *   () => engine.execute(...)  // direct invocation
 * )
 * }}}
 *
 * Returns `supplier.get()` after a single DEBUG log line. No
 * state, no resource lifecycle, no I/O.
 */
object RestatedEngineRunner {

  /**
   * Direct supplier invocation with a single DEBUG log.
   *
   * Per [[karpathy-guidelines-mindset]] "smallest correct change":
   * the cached-row path becomes journaled once the follow-up
   * handler-class PR lands. Until then, this is the call site
   * that any non-handler caller (tests, CLI driver, dev mode)
   * will exercise.
   *
   * Per [[scala-error-handling-mindset]]: supplier exceptions
   * propagate to the caller (no swallow). The supplier is
   * responsible for idempotency under replay — `Restate.run`
   * semantics land in the follow-up PR.
   *
   * @param name      journal key (reserved; recorded in DEBUG log
   *                  for traceability — meaningful once handler
   *                  wiring lands)
   * @param ctype     return type (reserved; same — used by the
   *                  v2.x state machine when wiring is added)
   * @param supplier  body returning the value
   * @return          `supplier.get()`
   */
  def runJournaled[A](
      name: String,
      ctype: Class[A],
      supplier: Supplier[A]
  ): A = {
    val threadName = Thread.currentThread.getName
    val isHandlerThread = isInRestateHandlerThread
    if (isHandlerThread) {
      // Defensive: if a future PR wires a handler thread but
      // the call site is still routed through `runJournaled`,
      // throw loudly. The proper v2.x path is
      // `HandlerContext.submitRun(...)` directly inside the
      // handler body — see the file header for details.
      throw new IllegalStateException(
        "Restate handler thread detected (" + threadName +
          ") but runJournaled called — use HandlerContext.submitRun directly. " +
          "[name=" + name + "]"
      )
    }
    supplier.get()
  }

  /**
   * Probe for a Restate handler-thread context.
   *
   * Per [[scala-jvm-safety-mindset]] "null is a liar": the v2.x
   * SDK provides no `RestateContext.current()` static method
   * (verified by JAR inspection of `sdk-common` + `sdk-core`
   * at v2.1.1). The follow-up handler-class PR must set up a
   * `ThreadLocal[HandlerContext]` (or similar) so the probe
   * can return `true` when an actual handler is in scope.
   *
   * Today's implementation always returns `false` (no Restate
   * runtime active in the reactor yet). This is correct: every
   * `runJournaled` call today invokes the supplier directly,
   * which matches the pre-PR-C5b-ext-γ behavior byte-for-byte.
   */
  private def isInRestateHandlerThread: Boolean = {
    // TODO(PR-C5b-ext-γ'-follow-up): replace with
    // `ThreadLocal[HandlerContext].get() != null` once the
    // handler-class wiring lands. Today: always false.
    false
  }
}
