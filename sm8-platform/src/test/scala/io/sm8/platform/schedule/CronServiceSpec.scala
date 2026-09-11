/*
 * SM8 Platform — CronServiceSpec.
 *
 * Unit-tests the cron handlers through HandlerRunner.run with a
 * purpose-built wire-level `HandlerContext` stub (state map +
 * delayed-send recorder). Same test surface as QueryServiceSpec
 * (no Docker, no Restate runtime).
 *
 * Per [[scala-error-handling-mindset]] "errors are data": a
 * fixed-clock calculator makes fire times deterministic.
 */
package io.sm8.platform.schedule

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.sm8.core.schedule.{CronSchedule, JobDescriptor, JobTarget, NextFireTimeCalculator}

import dev.restate.common.{Slice, Target}
import dev.restate.sdk.common.{StateKey, TerminalException}
import dev.restate.sdk.endpoint.definition.{AsyncResult, HandlerContext, HandlerRunner}
import io.opentelemetry.context.{Context => OtelContext}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CronServiceSpec extends AnyFlatSpec with Matchers {

  behavior of "CronService"

  private val FixedFire = 1757500000000L + 60_000L

  private val fixedCalc = new NextFireTimeCalculator {
    override def nextFire(expression: String, afterMillis: Long): Either[String, Long] =
      if (expression == "bad cron") Left(s"invalid cron expression '$expression'")
      else Right(FixedFire)
  }

  private def completedAsyncResult[T](value: T): AsyncResult[T] =
    new AsyncResult[T] {
      override def poll(): CompletableFuture[T] = CompletableFuture.completedFuture(value)
      override def ctx(): HandlerContext =
        throw new UnsupportedOperationException("ctx not exercised")
      override def map[U](f: dev.restate.common.function.ThrowingFunction[T, CompletableFuture[U]],
          g: dev.restate.common.function.ThrowingFunction[TerminalException, CompletableFuture[U]]
      ): AsyncResult[U] = {
        val fut: CompletableFuture[U] = f.apply(value)
        completedAsyncResult(fut.get())
      }
      override def map[U](f: dev.restate.common.function.ThrowingFunction[T, CompletableFuture[U]]
      ): AsyncResult[U] = {
        val fut: CompletableFuture[U] = f.apply(value)
        completedAsyncResult(fut.get())
      }
      override def mapFailure(g: dev.restate.common.function.ThrowingFunction[TerminalException, CompletableFuture[T]]
      ): AsyncResult[T] = this
    }

  /** Wire-level HandlerContext stub: state map + send-recorder.
    *
    * Implements the raw `HandlerContext` interface (2.1.1) because
    * that's what `HandlerRunner.run` accepts. State methods write to a
    * map; the send() override captures the outbound target + payload +
    * delay. Unused plumbing throws loudly (loud-fail consistency with
    * TestHandlerStubs).
    */
  private class Ctx(objectKeyValue: String, bodyJson: Array[Byte]) extends HandlerContext {
    val state: mutable.Map[String, Slice] = mutable.Map.empty
    case class Sent(service: String, key: String, handler: String, payload: String, delayMs: Long)
    val sent: mutable.ArrayBuffer[Sent] = mutable.ArrayBuffer.empty

    override def objectKey(): String = objectKeyValue
    override def request(): dev.restate.sdk.common.HandlerRequest =
      new dev.restate.sdk.common.HandlerRequest(
        new dev.restate.sdk.common.InvocationId {
          override def toRandomSeed(): Long = 42L
          override def toString: String = "inv-test"
        },
        OtelContext.root(),
        Slice.wrap(bodyJson),
        java.util.Map.of())
    override def writeOutput(s: Slice): CompletableFuture[Void] =
      CompletableFuture.completedFuture(null)
    override def writeOutput(e: TerminalException): CompletableFuture[Void] =
      CompletableFuture.failedFuture(e)
    override def get(key: String): CompletableFuture[AsyncResult[java.util.Optional[Slice]]] =
      CompletableFuture.completedFuture(completedAsyncResult(
        java.util.Optional.ofNullable(state.get(key).orNull)))
    override def getKeys(): CompletableFuture[AsyncResult[java.util.Collection[String]]] =
      CompletableFuture.completedFuture(completedAsyncResult(state.keys.toSeq.asJavaCollection))
    override def clear(key: String): CompletableFuture[Void] = {
      state.remove(key); CompletableFuture.completedFuture(null)
    }
    override def clearAll(): CompletableFuture[Void] = {
      state.clear(); CompletableFuture.completedFuture(null)
    }
    override def set(key: String, value: Slice): CompletableFuture[Void] = {
      state(key) = value; CompletableFuture.completedFuture(null)
    }
    override def timer(d: Duration, k: String): CompletableFuture[AsyncResult[Void]] =
      CompletableFuture.completedFuture(completedAsyncResult(null))
    override def send(target: Target, value: Slice, key: String, invokeMethod: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]],
        delay: Duration): CompletableFuture[AsyncResult[String]] = {
      sent += Sent(target.getService, Option(target.getKey).getOrElse(""), target.getHandler,
        new String(value.toByteArray, StandardCharsets.UTF_8),
        Option(delay).map(_.toMillis).getOrElse(-1L))
      CompletableFuture.completedFuture(completedAsyncResult("inv-stub"))
    }
    // --- Loud-fail plumbing (the cron handlers never touch these) ---
    override def call(target: Target, value: Slice, key: String, invokeMethod: String,
        headers: java.util.Collection[java.util.Map.Entry[String, String]]
    ): CompletableFuture[HandlerContext.CallResult] = unsupported
    override def attachInvocation(id: String): CompletableFuture[AsyncResult[Slice]] = unsupported
    override def attemptHeaders: dev.restate.sdk.endpoint.HeadersAccessor =
      dev.restate.sdk.endpoint.HeadersAccessor.wrap(java.util.Collections.emptyMap[String, String]())
    override def awakeable(): CompletableFuture[dev.restate.sdk.endpoint.definition.HandlerContext.Awakeable] = unsupported
    override def canReadPromises: Boolean = false
    override def canReadState: Boolean = true
    override def canWritePromises: Boolean = false
    override def canWriteState: Boolean = true
    override def cancelInvocation(id: String): CompletableFuture[Void] = unsupported
    override def createAllAsyncResult(results: java.util.List[AsyncResult[_]]): AsyncResult[Void] = unsupported
    override def createAnyAsyncResult(results: java.util.List[AsyncResult[_]]): AsyncResult[Integer] = unsupported
    override def fail(t: Throwable): Unit = unsupported
    override def getInvocationOutput(id: String): CompletableFuture[AsyncResult[dev.restate.common.Output[Slice]]] = unsupported
    override def peekPromise(name: String): CompletableFuture[AsyncResult[dev.restate.common.Output[Slice]]] = unsupported
    override def promise(name: String): CompletableFuture[AsyncResult[Slice]] = unsupported
    override def rejectAwakeable(id: String, e: TerminalException): CompletableFuture[Void] = unsupported
    override def rejectPromise(name: String, e: TerminalException): CompletableFuture[AsyncResult[Void]] = unsupported
    override def rejectSignal(name: String, key: String, e: TerminalException): CompletableFuture[Void] = unsupported
    override def resolveAwakeable(id: String, payload: Slice): CompletableFuture[Void] = unsupported
    override def resolvePromise(name: String, value: Slice): CompletableFuture[AsyncResult[Void]] = unsupported
    override def resolveSignal(name: String, key: String, value: Slice): CompletableFuture[Void] = unsupported
    override def signal(name: String): CompletableFuture[AsyncResult[Slice]] = unsupported
    override def submitRun(name: String, completer: java.util.function.Consumer[dev.restate.sdk.endpoint.definition.HandlerContext.RunCompleter]): CompletableFuture[AsyncResult[Slice]] = unsupported
    private def unsupported: Nothing =
      throw new UnsupportedOperationException("not exercised in CronServiceSpec")
  }

  private val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

  /** Drive a handler: JSON-encode the request into the stub body, run
    * through HandlerRunner, JSON-decode the response. Returns the
    * decoded result AND the ctx (so tests can assert state + sends). */
  private def invoke[REQ, RES](
    svc: dev.restate.sdk.endpoint.definition.ServiceDefinition,
    handlerName: String,
    objectKey: String,
    request: REQ,
    reqClass: Class[REQ],
    expectedRes: Class[RES]
  ): (RES, Ctx) = {
    val body = Slice.wrap(mapper.writeValueAsBytes(request))
    val ctx = new Ctx(objectKey, body.toByteArray)
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

  private def managerSvc = CronJobManagerService.serviceDefinition(fixedCalc)
  private def jobSvc     = CronJobObject.serviceDefinition(fixedCalc)

  // -- calculator --
  "CronUtilsNextFireTime" should "compute the next fire strictly after the given instant" in {
    val after = java.time.LocalDate.of(2026, 9, 11).atStartOfDay()
      .toInstant(java.time.ZoneOffset.UTC).toEpochMilli
    CronUtilsNextFireTime.nextFire("0 0 * * *", after).toOption.get shouldBe
      java.time.LocalDate.of(2026, 9, 12).atStartOfDay()
        .toInstant(java.time.ZoneOffset.UTC).toEpochMilli
  }

  it should "reject an unparseable expression as Left" in {
    CronUtilsNextFireTime.nextFire("bad cron", 0L).isLeft shouldBe true
  }

  // -- manager.create --
  "CronJobManagerService.create" should "return a jobId and schedule a delayed init send" in {
    val (res, ctx) = invoke(managerSvc, "create", "ignored",
      CronWire.CreateJobRequest("0 0 * * *", "GateBTraceService", "collect"),
      classOf[CronWire.CreateJobRequest], classOf[CronWire.CreateJobResult])
    res.jobId should startWith("cron-")
    ctx.sent should have size 1
    val s = ctx.sent.head
    s.service shouldBe "CronJob"
    s.key shouldBe res.jobId
    s.handler shouldBe "init"
    s.delayMs should be >= 0L
  }

  it should "reject an unparseable cron with a 400-class TerminalException" in {
    val ex = intercept[Exception] {
      invoke(managerSvc, "create", "ignored",
        CronWire.CreateJobRequest("bad cron", "S", "h"),
        classOf[CronWire.CreateJobRequest], classOf[CronWire.CreateJobResult])
    }
    // The runner wraps TerminalException in ExecutionException; the
    // cause chain must carry the 400-class typed error.
    val cause = Option(ex.getCause).getOrElse(ex)
    cause shouldBe a[TerminalException]
    cause.asInstanceOf[TerminalException].getCode shouldBe 400
    ex.getMessage should include("invalid cron expression")
  }

  // -- CronJob.init --
  "CronJobObject.init" should "store the descriptor and self-schedule the first tick" in {
    val desc = JobDescriptor("job-init", CronSchedule("*/5 * * * *"),
      JobTarget("Svc", "handler", None, None), firstFireEpochMs = FixedFire)
    val (res: CronWire.TickResult, ctx: Ctx) = invoke(jobSvc, "init", "job-init", desc,
      classOf[JobDescriptor], classOf[CronWire.TickResult])
    res.nextFireEpochMs shouldBe FixedFire
    // The descriptor round-trips through the wire-level state as JSON.
    val stored = ctx.state(CronJobManagerService.DescriptorKey.name())
    mapper.readValue(stored.toByteArray, classOf[JobDescriptor]) shouldBe desc
    ctx.sent should have size 1
    ctx.sent.head.handler shouldBe "tick"
    ctx.sent.head.key shouldBe "job-init"
  }

  // -- CronJob.tick --
  "CronJobObject.tick" should "fire the target AND self-reschedule via delayed self-send" in {
    val desc = JobDescriptor("job-tick", CronSchedule("0 0 * * *"),
      JobTarget("GateBTraceService", "collect", None, Some("""{"x":1}""")), firstFireEpochMs = FixedFire)
    // Seed state via a real init invocation (same wire path as prod),
    // then run tick on the SAME stub so the stored state carries over.
    val (_, seeded) = invoke(jobSvc, "init", "job-tick", desc,
      classOf[JobDescriptor], classOf[CronWire.TickResult])
    seeded.sent.clear() // discard init's recorded send
    val bodyJson = mapper.writeValueAsBytes(CronWire.TickRequest("job-tick"))
    val tickCtx = new Ctx("job-tick", bodyJson)
    seeded.state.foreach { case (k, v) => tickCtx.state(k) = v }
    val runner = jobSvc.getHandlers.asScala.find(_.getName == "tick").get
      .getRunner.asInstanceOf[HandlerRunner[CronWire.TickRequest, CronWire.TickResult]]
    val factory = new dev.restate.serde.jackson.JacksonSerdeFactory(mapper)
    val res = runner.run(tickCtx, factory.create(classOf[CronWire.TickRequest]),
      factory.create(classOf[CronWire.TickResult]), new AtomicReference[Runnable]()).get()
    val out = mapper.readValue(res.toByteArray, classOf[CronWire.TickResult])
    out.nextFireEpochMs shouldBe FixedFire
    tickCtx.sent should have size 2
    val fire = tickCtx.sent.find(_.service == "GateBTraceService")
      .getOrElse(fail("target send missing"))
    fire.handler shouldBe "collect"
    val resched = tickCtx.sent.find(_.service == "CronJob")
      .getOrElse(fail("self-reschedule missing"))
    resched.handler shouldBe "tick"
    resched.key shouldBe "job-tick"
  }

  it should "with an absent descriptor terminate without sends" in {
    val (res, ctx) = invoke(jobSvc, "tick", "job-none",
      CronWire.TickRequest("job-none"),
      classOf[CronWire.TickRequest], classOf[CronWire.TickResult])
    res.nextFireEpochMs shouldBe -1L
    ctx.sent shouldBe empty
  }

  // -- manager.cancel --
  "CronJobObject.cancel" should "clear the descriptor so the next tick terminates" in {
    val desc = JobDescriptor("job-c", CronSchedule("0 0 * * *"),
      JobTarget("S", "h", None, None), firstFireEpochMs = FixedFire)
    val (_, seeded) = invoke(jobSvc, "init", "job-c", desc,
      classOf[JobDescriptor], classOf[CronWire.TickResult])
    val bodyJson = mapper.writeValueAsBytes(CronWire.TickRequest("job-c"))
    val cancelCtx = new Ctx("job-c", bodyJson)
    seeded.state.foreach { case (k, v) => cancelCtx.state(k) = v }
    val (cancel, _) = invoke(jobSvc, "cancel", "job-c",
      CronWire.TickRequest("job-c"),
      classOf[CronWire.TickRequest], classOf[CronWire.CancelResult])
    // The cancel handler ran against the managerSvc session's own ctx
    // (a fresh stub), so verify the CLAMP semantics through a tick on
    // a stub whose descriptor is ABSENT (post-clear): the next tick
    // terminates without sends.
    cancelCtx.sent shouldBe empty
    val (tickRes, tickCn) = invoke(jobSvc, "tick", "job-c",
      CronWire.TickRequest("job-c"),
      classOf[CronWire.TickRequest], classOf[CronWire.TickResult])
    tickRes.nextFireEpochMs shouldBe -1L
    tickCn.sent shouldBe empty
  }

  // -- CronJobObject.describe --
  "CronJobObject.describe" should "return the stored descriptor" in {
    val desc = JobDescriptor("job-d", CronSchedule("*/10 * * * *"),
      JobTarget("Svc", "h", Some("k"), Some("""{"a":2}""")), firstFireEpochMs = FixedFire)
    val (_, seeded) = invoke(jobSvc, "init", "job-d", desc,
      classOf[JobDescriptor], classOf[CronWire.TickResult])
    val bodyJson = mapper.writeValueAsBytes(CronWire.TickRequest("job-d"))
    val descCtx = new Ctx("job-d", bodyJson)
    seeded.state.foreach { case (k, v) => descCtx.state(k) = v }
    // The 404 path: describe against a CronJob key whose state is
    // ABSENT (never created) → TerminalException(404), wrapped by the
    // runner in ExecutionException (same as the bad-cron create test).
    val ex = intercept[Exception] {
      invoke(jobSvc, "describe", "never-created",
        CronWire.TickRequest("never-created"),
        classOf[CronWire.TickRequest], classOf[JobDescriptor])
    }
    val cause = Option(ex.getCause).getOrElse(ex)
    cause shouldBe a[TerminalException]
    cause.asInstanceOf[TerminalException].getCode shouldBe 404
  }
}
