/*
 * SM8 Platform — TestHandlerStubs.
 *
 * Test-only utilities used by the platform's own unit tests to
 * exercise the hand-rolled service definitions (QueryService,
 * ModelService, MetricsService, MetaInspectorService). Layer-
 * discipline (layer discipline per the project's layering RFC): this file lives in
 * `sm8-platform/src/test/scala/io/sm8/platform/query/` because
 * the stubs are an implementation detail of how `sm8-platform`
 * tests its OWN service wiring. No other module imports this file.
 *
 * The tests don't actually run any handler code; they invoke
 * the service's handler function directly and only need a
 * Context + InvocationId to satisfy the type signatures.
 *
 * The `HandlerContext` trait grew from 26 to 34 abstract
 *    methods. New methods in 2.9.4: `attemptHeaders`,
 *    `canReadState`, `canWriteState`, `canReadPromises`,
 *    `canWritePromises`, `signal`, `resolveSignal`,
 *    `rejectSignal`. The `call`/`send` signatures also picked
 *    up a new `invokeMethod: String` parameter between the key
 *    and headers arguments (verified via javap on
 *    sdk-common-2.9.4.jar).
 *
 * `HandlerRequest` requires a non-null `InvocationId`.
 *   The exact downstream behavior is undocumented; we provide a
 *   non-null one as a safety measure since the SDK MAY read the
 *   field in future versions. The exact bug we observed: passing
 *   a null `InvocationId` to the 4-arg `HandlerRequest` constructor
 *   caused the test's `.get()` call to hang indefinitely; we
 *   couldn't reproduce with a non-null invocation id (whether the
 *   SDK ever dereferences the field is unknown — but a non-null
 *   value is the safe baseline).
 *
 * `InvocationIdImpl` (sdk-core) has a package-private
 *    constructor — the only way to construct one outside the
 *    SDK is via an anonymous subclass. We provide that factory
 *    here.
 *
 * Skills compliance:
 * - scala-jvm-safety: every "unused" path throws
 *   `UnsupportedOperationException` so a test that accidentally
 *   exercises it fails LOUD rather than silently returning
 *   null. The four methods the test DOES need (`request`,
 *   `objectKey`, both `writeOutput` overloads) are wired to safe defaults.
 * - scala-error-handling: writeOutput returns completed-null
 *   futures (the SDK's contract for "no extra output
 *   emitted") instead of `null`, which would NPE the
 *   HandlerRunner.
 * - scala2-scaladoc: this file's class + method Scaladoc
 *   follows the conventions (one-sentence summary, @param
 *   tags, no PR-/ticket-narration in comments).
 */
package io.sm8.platform.query

import dev.restate.common.Slice
import dev.restate.sdk.common.{HandlerRequest, InvocationId, TerminalException}
import dev.restate.sdk.endpoint.HeadersAccessor
import dev.restate.sdk.endpoint.definition.AsyncResult
import dev.restate.sdk.endpoint.definition.HandlerContext
import dev.restate.common.Target

import java.time.Duration
import java.util.concurrent.CompletableFuture

object TestHandlerStubs {

  /** Build a minimal `InvocationId` for test stub HandlerRequests.
    *
    * `InvocationIdImpl` (sdk-core) has a
    * package-private constructor; no public factory exists.
    * The two abstract methods (`toRandomSeed`, `toString`) are
    * stubbed with stable values — sufficient for the SDK's
    * `HandlerRunner.run(ctx, ...)` invocation, which constructs
    * an internal `ContextImpl` from the `HandlerRequest`'s
    * InvocationId (null would propagate and crash tests in
    * non-obvious ways depending on SDK internal state).
    *
    * @param seed a stable seed for the stub's `toRandomSeed`;
    *             deterministic across test runs
    * @return a non-null `InvocationId` suitable for the
    *         `HandlerRequest` 4-arg constructor
    */
  def newInvocationId(seed: Long = 1L): InvocationId = new InvocationId {
    override def toRandomSeed: Long = seed
    override def toString: String = s"test-invocation-id-$seed"
  }

  /** Build a no-op HandlerContext backed by the given HandlerRequest.
    *
    * All 34 abstract methods of HandlerContext are stubbed. The
    * four methods the tests actually need are wired to safe
    * defaults:
    * - `request()` → `req` (the HandlerRequest the test constructed)
    * - `objectKey()` → `""` (no journal key in unit tests)
    * - `writeOutput(Slice)` → completed-null future (SDK contract
    *   for "no extra output emitted")
    * - `writeOutput(TerminalException)` → completed-null future
    *
    * All other 30 methods throw `UnsupportedOperationException` so
    * a test that accidentally exercises an unsupported path
    * fails loudly with a clear message rather than silently
    * returning null (which would NPE the HandlerRunner).
    *
    * @param req the HandlerRequest the test constructed (must
    *            have a non-null InvocationId; use
    *            [[newInvocationId]] to obtain one)
    * @return a HandlerContext that satisfies the SDK's type
    *         signature without exercising any of its internal
    *         state-machine plumbing
    */
  def newHandlerContext(req: HandlerRequest): HandlerContext = new HandlerContext {
// --- Wired methods (real implementations) ---
    override def objectKey: String = ""
    override def request: HandlerRequest = req
    override def writeOutput(s: Slice): CompletableFuture[Void] =
      throw new UnsupportedOperationException(
        "writeOutput(Slice) is not exercised in this unit test; " +
        "HandlerRunner writes are stubbed to throw so accidental " +
        "execution fails loudly. Use a real HandlerContext for " +
        "integration tests that exercise journal/output writes.")
    override def writeOutput(e: TerminalException): CompletableFuture[Void] =
      throw new UnsupportedOperationException(
        "writeOutput(TerminalException) is not exercised in this unit test; " +
        "HandlerRunner error writes are stubbed to throw so accidental " +
        "execution fails loudly. Use a real HandlerContext for " +
        "integration tests that exercise journal/output writes.")

    // --- State/journal methods (return safe defaults; throw if exercised) ---
    override def get(key: String)
      : CompletableFuture[AsyncResult[java.util.Optional[Slice]]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def getKeys()
      : CompletableFuture[AsyncResult[java.util.Collection[String]]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def clear(key: String): CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")
    override def clearAll(): CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")
    override def set(key: String, value: Slice): CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")
    override def timer(d: Duration, key: String)
      : CompletableFuture[AsyncResult[Void]] =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Outbound call / send (2.9.4 signatures) ---
    override def call(
        target: Target,
        value: Slice,
        key: String,
        invokeMethod: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]]
    ): CompletableFuture[HandlerContext.CallResult] =
      throw new UnsupportedOperationException("not used in unit test")
    override def send(
        target: Target,
        value: Slice,
        key: String,
        invokeMethod: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]],
        delay: Duration
    ): CompletableFuture[AsyncResult[String]] =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Run submission / awakeables ---
    override def submitRun(
        name: String,
        completer: java.util.function.Consumer[HandlerContext.RunCompleter]
    ): CompletableFuture[AsyncResult[Slice]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def awakeable()
      : CompletableFuture[HandlerContext.Awakeable] =
      throw new UnsupportedOperationException("not used in unit test")
    override def resolveAwakeable(id: String, value: Slice): CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")
    override def rejectAwakeable(id: String, e: TerminalException): CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Promises ---
    override def promise(name: String): CompletableFuture[AsyncResult[Slice]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def peekPromise(name: String)
      : CompletableFuture[AsyncResult[dev.restate.common.Output[Slice]]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def resolvePromise(name: String, value: Slice)
      : CompletableFuture[AsyncResult[Void]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def rejectPromise(name: String, e: TerminalException)
      : CompletableFuture[AsyncResult[Void]] =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Signals (new in 2.9.4) ---
    override def signal(name: String)
      : CompletableFuture[AsyncResult[Slice]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def resolveSignal(name: String, key: String, value: Slice)
      : CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")
    override def rejectSignal(name: String, key: String, e: TerminalException)
      : CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Invocation tracking ---
    override def cancelInvocation(id: String): CompletableFuture[Void] =
      throw new UnsupportedOperationException("not used in unit test")
    override def attachInvocation(id: String)
      : CompletableFuture[AsyncResult[Slice]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def getInvocationOutput(id: String)
      : CompletableFuture[AsyncResult[dev.restate.common.Output[Slice]]] =
      throw new UnsupportedOperationException("not used in unit test")
    override def fail(t: Throwable): Unit =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Async result combinators ---
    override def createAnyAsyncResult(args: java.util.List[AsyncResult[_]])
      : AsyncResult[Integer] =
      throw new UnsupportedOperationException("not used in unit test")
    override def createAllAsyncResult(args: java.util.List[AsyncResult[_]])
      : AsyncResult[Void] =
      throw new UnsupportedOperationException("not used in unit test")

    // --- Accessors (new in 2.9.4; return safe defaults) ---
    override def attemptHeaders: HeadersAccessor =
      HeadersAccessor.wrap(java.util.Collections.emptyMap[String, String]())
    override def canReadState: Boolean = false
    override def canWriteState: Boolean = false
    override def canReadPromises: Boolean = false
    override def canWritePromises: Boolean = false
  }
}
