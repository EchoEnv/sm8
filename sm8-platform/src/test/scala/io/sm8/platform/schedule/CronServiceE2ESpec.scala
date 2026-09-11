/*
 * SM8 Platform — CronServiceE2ESpec.
 *
 * End-to-end test of the cron service: create a job (CRON HANDLER
 * round-trip), tick fires the target (TARGET HANDLER ROUND-TRIP),
 * cancel terminates the chain. Uses the wire-level HandlerContext stub
 * already proven by CronServiceSpec — no Docker, no Restate runtime.
 *
 * Per [[scala-error-handling-mindset]] "errors are data": a fixed
 * calculator makes the chain deterministic.
 */
package io.sm8.platform.schedule

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.sm8.core.schedule.{CronSchedule, JobDescriptor, JobTarget, NextFireTimeCalculator}

import dev.restate.common.{Slice, Target}
import dev.restate.sdk.common.TerminalException
import dev.restate.sdk.endpoint.definition.{AsyncResult, HandlerContext, HandlerRunner}
import io.opentelemetry.context.{Context => OtelContext}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CronServiceE2ESpec extends AnyFlatSpec with Matchers {

  behavior of "Cron E2E (no Docker)"

  /** Fixed-clock calculator: next fire = 60_000 ms in the future (so
    * the delayed send always has a positive delay). */
  private val fixedCalc = new NextFireTimeCalculator {
    override def nextFire(expression: String, afterMillis: Long): Either[String, Long] =
      Right(afterMillis + 60_000L)
  }

  /** Completed AsyncResult factory (any value). */
  private def completedAsyncResult[T](value: T): AsyncResult[T] =
    new AsyncResult[T] {
      override def poll(): java.util.concurrent.CompletableFuture[T] =
        java.util.concurrent.CompletableFuture.completedFuture(value)
      override def ctx(): HandlerContext =
        throw new UnsupportedOperationException("ctx not exercised")
      override def map[U](f: dev.restate.common.function.ThrowingFunction[T, java.util.concurrent.CompletableFuture[U]],
          g: dev.restate.common.function.ThrowingFunction[TerminalException, java.util.concurrent.CompletableFuture[U]]
      ): AsyncResult[U] = completedAsyncResult(f.apply(value).get())
      override def map[U](f: dev.restate.common.function.ThrowingFunction[T, java.util.concurrent.CompletableFuture[U]]
      ): AsyncResult[U] = completedAsyncResult(f.apply(value).get())
      override def mapFailure(g: dev.restate.common.function.ThrowingFunction[TerminalException, java.util.concurrent.CompletableFuture[T]]
      ): AsyncResult[T] = this
    }

  /**
   * Wire-level HandlerContext stub: state map + send-recorder.
   *
   * Implements the raw `HandlerContext` interface (Restate SDK 2.1.1)
   * because that's what `HandlerRunner.run` accepts. The 22 overrides
   * below fall into three buckets: (a) state (get/set/clear/clearAll/
   * getKeys/timer — backed by a map); (b) send (the 6-arg overload —
   * captures target + payload + delay onto a recorder; the
   * invokeMethod is null on the runner path and we don't depend on
   * it); (c) loud-fail for the
   * remaining 15 methods (signals/promises/awakeables/calls/submitRun
   * — the cron handlers never touch these; a drift fails the test
   * loudly instead of NoSuchMethodException-via-abstract).
   *
   * @param objectKey the virtual-object key the handler body sees
   * @param bodyJson the JSON-encoded request body HandlerRunner reads
   */
  private class Ctx(objectKey: String, bodyJson: Array[Byte]) extends HandlerContext {
    val state: mutable.Map[String, Slice] = mutable.Map.empty
    case class Sent(service: String, key: String, handler: String,
                    payload: String, delayMs: Long)
    val sent: mutable.ArrayBuffer[Sent] = mutable.ArrayBuffer.empty

    override def objectKey(): String = objectKey
    override def request(): dev.restate.sdk.common.HandlerRequest =
      new dev.restate.sdk.common.HandlerRequest(
        new dev.restate.sdk.common.InvocationId {
          override def toRandomSeed(): Long = 42L
          override def toString: String = "inv-test"
        },
        OtelContext.root(),
        Slice.wrap(bodyJson),
        java.util.Map.of())
    override def writeOutput(s: Slice): java.util.concurrent.CompletableFuture[Void] =
      java.util.concurrent.CompletableFuture.completedFuture(null)
    override def writeOutput(e: TerminalException): java.util.concurrent.CompletableFuture[Void] =
      java.util.concurrent.CompletableFuture.failedFuture(e)
    override def get(key: String): java.util.concurrent.CompletableFuture[AsyncResult[java.util.Optional[Slice]]] =
      java.util.concurrent.CompletableFuture.completedFuture(completedAsyncResult(
        java.util.Optional.ofNullable(state.get(key).orNull)))
    override def getKeys(): java.util.concurrent.CompletableFuture[AsyncResult[java.util.Collection[String]]] =
      java.util.concurrent.CompletableFuture.completedFuture(completedAsyncResult(state.keys.toSeq.asJavaCollection))
    override def clear(key: String): java.util.concurrent.CompletableFuture[Void] = {
      state.remove(key); java.util.concurrent.CompletableFuture.completedFuture(null)
    }
    override def clearAll(): java.util.concurrent.CompletableFuture[Void] = {
      state.clear(); java.util.concurrent.CompletableFuture.completedFuture(null)
    }
    override def set(key: String, value: Slice): java.util.concurrent.CompletableFuture[Void] = {
      state(key) = value; java.util.concurrent.CompletableFuture.completedFuture(null)
    }
    override def timer(d: java.time.Duration, k: String): java.util.concurrent.CompletableFuture[AsyncResult[Void]] =
      java.util.concurrent.CompletableFuture.completedFuture(completedAsyncResult(null))
    override def send(target: Target, value: Slice, key: String, invokeMethod: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]],
        delay: java.time.Duration): java.util.concurrent.CompletableFuture[AsyncResult[String]] = {
      sent += Sent(target.getService, Option(target.getKey).getOrElse(""), target.getHandler,
        new String(value.toByteArray, StandardCharsets.UTF_8),
        Option(delay).map(_.toMillis).getOrElse(-1L))
      java.util.concurrent.CompletableFuture.completedFuture(completedAsyncResult("inv-stub"))
    }
    // --- Loud-fail plumbing ---
    override def call(target: Target, value: Slice, key: String, invokeMethod: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]]
    ): java.util.concurrent.CompletableFuture[HandlerContext.CallResult] = unsupported
    override def attachInvocation(id: String): java.util.concurrent.CompletableFuture[AsyncResult[Slice]] = unsupported
    override def attemptHeaders: dev.restate.sdk.endpoint.HeadersAccessor =
      dev.restate.sdk.endpoint.HeadersAccessor.wrap(java.util.Collections.emptyMap[String, String]())
    override def awakeable(): java.util.concurrent.CompletableFuture[dev.restate.sdk.endpoint.definition.HandlerContext.Awakeable] = unsupported
    override def canReadPromises: Boolean = false
    override def canReadState: Boolean = true
    override def canWritePromises: Boolean = false
    override def canWriteState: Boolean = true
    override def cancelInvocation(id: String): java.util.concurrent.CompletableFuture[Void] = unsupported
    override def createAllAsyncResult(results: java.util.List[AsyncResult[_]]): AsyncResult[Void] = unsupported
    override def createAnyAsyncResult(results: java.util.List[AsyncResult[_]]): AsyncResult[Integer] = unsupported
    override def fail(t: Throwable): Unit = unsupported
    override def getInvocationOutput(id: String): java.util.concurrent.CompletableFuture[AsyncResult[dev.restate.common.Output[Slice]]] = unsupported
    override def peekPromise(name: String): java.util.concurrent.CompletableFuture[AsyncResult[dev.restate.common.Output[Slice]]] = unsupported
    override def promise(name: String): java.util.concurrent.CompletableFuture[AsyncResult[Slice]] = unsupported
    override def rejectAwakeable(id: String, e: TerminalException): java.util.concurrent.CompletableFuture[Void] = unsupported
    override def rejectPromise(name: String, e: TerminalException): java.util.concurrent.CompletableFuture[AsyncResult[Void]] = unsupported
    override def rejectSignal(name: String, key: String, e: TerminalException): java.util.concurrent.CompletableFuture[Void] = unsupported
    override def resolveAwakeable(id: String, payload: Slice): java.util.concurrent.CompletableFuture[Void] = unsupported
    override def resolvePromise(name: String, value: Slice): java.util.concurrent.CompletableFuture[AsyncResult[java.lang.Void]] = unsupported
    override def resolveSignal(name: String, key: String, value: Slice): java.util.concurrent.CompletableFuture[Void] = unsupported
    override def signal(name: String): java.util.concurrent.CompletableFuture[AsyncResult[Slice]] = unsupported
    override def submitRun(name: String, completer: java.util.function.Consumer[dev.restate.sdk.endpoint.definition.HandlerContext.RunCompleter]): java.util.concurrent.CompletableFuture[AsyncResult[Slice]] = unsupported
    private def unsupported: Nothing =
      throw new UnsupportedOperationException("not exercised in CronServiceE2ESpec")
  }

  private val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  private def managerSvc = CronJobManagerService.serviceDefinition(fixedCalc)
  private def jobSvc     = CronJobObject.serviceDefinition(fixedCalc)

  /** Drive a handler through HandlerRunner.run (the wire contract,
    * same surface as CronServiceSpec). Returns the deserialized
    * result + the (possibly mutated) ctx. */
  private def invoke[REQ, RES](
    svc: dev.restate.sdk.endpoint.definition.ServiceDefinition,
    handlerName: String,
    objectKey: String,
    request: REQ,
    reqClass: Class[REQ],
    expectedRes: Class[RES],
    stateHint: Ctx = null
  ): (RES, Ctx) = {
    val body = Slice.wrap(mapper.writeValueAsBytes(request))
    val ctx = if (stateHint != null) stateHint else new Ctx(objectKey, body.toByteArray)
    val handler = svc.getHandlers.asScala.find(_.getName == handlerName)
      .getOrElse(fail(s"handler '$handlerName' missing"))
    val runner = handler.getRunner.asInstanceOf[HandlerRunner[REQ, RES]]
    val factory = new dev.restate.serde.jackson.JacksonSerdeFactory(mapper)
    val reqSerde = factory.create(reqClass)
    val resSerde = factory.create(expectedRes)
    val responseSlice = runner.run(ctx, reqSerde, resSerde,
      new AtomicReference[Runnable]()).get()
    val res = resSerde.deserialize(responseSlice)
    (res, ctx)
  }

  "End-to-end: create → init → tick → target fire → self-reschedule" should "fire the target and chain to the next tick" in {
    // Step 1: create returns a jobId and schedules a delayed INIT send
    // to the keyed CronJob object.
    val (createRes, createCtx) = invoke(managerSvc, "create", "ignored",
      CronWire.CreateJobRequest("*/5 * * * *", "TargetService", "targetHandler",
        None, Some("""{"trace":"e2e"}""")),
      classOf[CronWire.CreateJobRequest], classOf[CronWire.CreateJobResult], null)
    val jobId = createRes.jobId
    jobId should startWith("cron-")
    createCtx.sent should have size 1
    createCtx.sent.head.handler shouldBe "init"
    createCtx.sent.head.key shouldBe jobId

    // Step 2: drive INIT on a fresh ctx keyed by jobId (it would be
    // journaled via the Restate ingress; here we invoke the handler
    // directly on the same ServiceDefinition with a synthesized
    // JobDescriptor — the production init is a delayed send of the
    // descriptor; the runner is the same handler either way).
    val initDesc = JobDescriptor(jobId, CronSchedule("*/5 * * * *"),
      JobTarget("TargetService", "targetHandler", None, Some("""{"trace":"e2e"}""")),
      firstFireEpochMs = System.currentTimeMillis() + 60_000L)
    val (initRes, initCtx) = invoke(jobSvc, "init", jobId, initDesc,
      classOf[JobDescriptor], classOf[CronWire.TickResult], null)
    initRes.nextFireEpochMs should be > 0L
    initCtx.state.get(CronJobManagerService.DescriptorKey.name()) shouldBe defined
    initCtx.sent should have size 1
    initCtx.sent.head.handler shouldBe "tick"
    initCtx.sent.head.key shouldBe jobId
    initCtx.sent.head.delayMs should be > 0L

    // Step 3: drive TICK — the descriptor is now in state; the tick
    // body fires the target AND self-reschedules.
    val bodyJson = mapper.writeValueAsBytes(CronWire.TickRequest(jobId))
    val tickCtx = new Ctx(jobId, bodyJson)
    initCtx.state.foreach { case (k, v) => tickCtx.state(k) = v }
    val runner = jobSvc.getHandlers.asScala.find(_.getName == "tick").get
      .getRunner.asInstanceOf[HandlerRunner[CronWire.TickRequest, CronWire.TickResult]]
    val factory = new dev.restate.serde.jackson.JacksonSerdeFactory(mapper)
    val tickRes = runner.run(tickCtx,
      factory.create(classOf[CronWire.TickRequest]),
      factory.create(classOf[CronWire.TickResult]),
      new AtomicReference[Runnable]()).get()
    val tickOut = mapper.readValue(tickRes.toByteArray, classOf[CronWire.TickResult])
    tickOut.nextFireEpochMs should be > 0L
    tickCtx.sent should have size 2
    val fire = tickCtx.sent.find(_.service == "TargetService")
      .getOrElse(fail("target send missing"))
    fire.handler shouldBe "targetHandler"
    fire.payload should include("\"trace\":\"e2e\"")
    val resched = tickCtx.sent.find(_.service == "CronJob")
      .getOrElse(fail("self-reschedule missing"))
    resched.handler shouldBe "tick"

    // Step 4: cancel clears the descriptor so the next tick terminates.
    val (cancelRes, _) = invoke(jobSvc, "cancel", jobId,
      CronWire.TickRequest(jobId),
      classOf[CronWire.TickRequest], classOf[CronWire.CancelResult], tickCtx)
    cancelRes.cancelled shouldBe true
    tickCtx.state.get(CronJobManagerService.DescriptorKey.name()) should not be defined

    // Step 5: the next tick on the cancelled (absent-descriptor) CronJob
    // returns TickResult(-1L) and emits zero sends (chain terminates).
    val afterCancel = invoke(jobSvc, "tick", jobId,
      CronWire.TickRequest(jobId),
      classOf[CronWire.TickRequest], classOf[CronWire.TickResult], null)
    afterCancel._1.nextFireEpochMs shouldBe -1L
    afterCancel._2.sent shouldBe empty
  }
}
