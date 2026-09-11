/*
 * SM8 Platform — Restate cron service (user-requested: a Restate-native
 * scheduler so recurring jobs — Gate B trace collection first — are
 * durable, cancelable, and inspectable; pattern reference:
 * restatedev/examples java/patterns-use-cases Cron.java, adapted to
 * Restate SDK 2.1.1 — see the API notes below).
 *
 * ==Two Restate services (mirroring the upstream Cron example)==
 *
 *   1. `CronJobManager` (Service): create / cancel / describe jobs.
 *      `create` validates the cron expression, generates a job id, and
 *      schedules a delayed `init` send to the `CronJob` virtual object
 *      (delay = now → first fire).
 *   2. `CronJob` (VirtualObject, keyed by job id): `init` stores the
 *      descriptor and schedules the first `tick`; `tick` invokes the
 *      job's target handler, computes the next fire time, and
 *      self-reschedules via a DELAYED SEND of its own `tick` (the
 *      Restate "delayed self-send" pattern — the skill's guidance:
 *      never block on timers; a delayed send is journaled and
 *      crash-safe). Cancel clears the descriptor; the already-journaled
 *      next tick finds state absent and terminates the chain.
 *
 * ==RFC §3 layering==
 *
 * The scheduling CONTRACT (CronSchedule / JobTarget / JobDescriptor /
 * NextFireTimeCalculator) lives in `io.sm8.core.schedule` (pure data,
 * IO-free). This file is the platform IMPLEMENTATION: it owns
 * cron-utils (external lib), Restate's delayed-send, and the JSON
 * serde. `plugins` and `connectors` are untouched; `core` never
 * imports Restate or cron-utils.
 *
 * ==Restate 2.1.1 API notes (javap-verified against ~/.m2 jars)==
 *
 * - Delayed send: `ctx.send(Request.of(target, TypeTag, TypeTag,
 *   payload), Duration)`. The upstream example's
   `Restate.serviceHandle(...).send(...)` does NOT exist in 2.1.1.
 * - `Target.service(name, handler)` / `Target.virtualObject(name, key,
 *   handler)`; getters are `getService()/getKey()/getHandler()`.
 * - `HandlerRunner.run` accepts the WIRE-level
 *   `dev.restate.sdk.endpoint.definition.HandlerContext` (sdk-common) —
 *   a different hierarchy from the SDK-typed `ObjectContext`. The unit
 *   spec implements the wire interface directly.
 * - `Slice.wrap(byte[])` — `Slice.of(...)` does not exist at this pin.
 *
 * ==Why delayed self-send (not ctx.sleep)==
 *
 * Per the building-restate-services skill (design-and-architecture):
 * "Do NOT use ctx.sleep() + send to schedule future work. Use a
 * delayed send directly (it does not block the handler)." A delayed
 * send is journaled: if the process crashes between fire and
 * reschedule, Restate re-runs `tick` from the journal and the chain
 * continues.
 *
 * ==Handler-thread context==
 *
 * Same discipline as QueryService: the handler body is synchronous
 * inside `HandlerRunner.of(...)`.
 */
package io.sm8.platform.schedule

import java.time.{Instant, ZonedDateTime, ZoneOffset}

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser

import io.sm8.core.schedule.{CronSchedule, JobDescriptor, JobTarget, NextFireTimeCalculator}

import dev.restate.common.{Request, Target}
import dev.restate.sdk.common.{StateKey, TerminalException}
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  ServiceDefinition,
  ServiceType
}
import dev.restate.sdk.{Context, HandlerRunner, ObjectContext}
import dev.restate.serde.TypeTag
import dev.restate.serde.jackson.JacksonSerdeFactory

import scala.jdk.OptionConverters._

/** Cron-utils-backed [[NextFireTimeCalculator]] (platform
  * implementation of the core contract). UNIX 5-field cron, UTC.
  *
  * Per [[scala-error-handling-mindset]] "errors are data": an
  * unparseable expression is a Left, not a throw.
  */
object CronUtilsNextFireTime extends NextFireTimeCalculator with Serializable {
  private val Parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX))

  /** Computes the next fire time via cron-utils (UNIX 5-field, UTC).
    *
    * @param expression the cron expression to evaluate
    * @param afterMillis the exclusive lower bound
    * @return the next fire epoch millis, or a parse error
    */
  override def nextFire(expression: String, afterMillis: Long): Either[String, Long] =
    try {
      val cron = Parser.parse(expression)
      val exec = ExecutionTime.forCron(cron)
      val after = ZonedDateTime.ofInstant(Instant.ofEpochMilli(afterMillis), ZoneOffset.UTC)
      exec.nextExecution(after).toScala match {
        case Some(zdt) => Right(zdt.toInstant.toEpochMilli)
        case None      => Left(s"cron '$expression' has no next execution after $afterMillis")
      }
    } catch {
      case e: IllegalArgumentException =>
        Left(s"invalid cron expression '$expression': ${e.getMessage}")
    }
}

/** Wire DTOs (JSON via the per-handler Jackson serde; the Scala case
  * classes in core are serialized through jackson-module-scala). */
object CronWire {
  final case class CreateJobRequest(
    cronExpression: String,
    service: String,
    handler: String,
    objectKey: Option[String] = None,
    payload: Option[String] = None
  )
  final case class CreateJobResult(jobId: String)
  final case class CancelResult(cancelled: Boolean)
  final case class TickRequest(jobId: String)
  final case class TickResult(nextFireEpochMs: Long)
}

/** The `CronJobManager` service: create / cancel / describe. */
object CronJobManagerService {

  import CronWire._

  /** StateKey for the stored descriptor on the CronJob virtual object. */
  val DescriptorKey: StateKey[JobDescriptor] =
    StateKey.of("descriptor", classOf[JobDescriptor])

  private val scalaMapper: com.fasterxml.jackson.databind.ObjectMapper =
    new com.fasterxml.jackson.databind.ObjectMapper()
      .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  private val serdeFactory = new JacksonSerdeFactory(scalaMapper)

  /** Build the `CronJobManager` ServiceDefinition (create/cancel/
    * describe handlers).
    *
    * @param calculator next-fire computation (tests pass a
    *        fixed-clock implementation; production passes
    *        [[CronUtilsNextFireTime]])
    * @return the ServiceDefinition to register on the Restate endpoint
    */
  def serviceDefinition(calculator: NextFireTimeCalculator): ServiceDefinition = {
    val createSerde = serdeFactory.create(classOf[CreateJobRequest])
    val createResSerde = serdeFactory.create(classOf[CreateJobResult])
    val cancelReqSerde = serdeFactory.create(classOf[TickRequest])
    val cancelResSerde = serdeFactory.create(classOf[CancelResult])
    val descResSerde = serdeFactory.create(classOf[JobDescriptor])

    // -- create: validates the expression (fail-fast 400 on parse
    // error), generates the job id, and schedules a delayed INIT send
    // to the keyed CronJob object (delay = now → first fire). The init
    // stores the descriptor and self-schedules from there.
    val createRunner: HandlerRunner[CreateJobRequest, CreateJobResult] =
      HandlerRunner.of(
        (ctx: Context, req: CreateJobRequest) => {
          val nextFire = calculator.nextFire(req.cronExpression, System.currentTimeMillis())
            match {
              case Right(t)  => t
              case Left(err) =>
                throw new TerminalException(400, err)
            }
          val jobId = "cron-" + java.util.UUID.randomUUID().toString
          val delayMs = math.max(0L, nextFire - System.currentTimeMillis())
          val target = Target.virtualObject("CronJob", jobId, "init")
          val initPayload =
            JobDescriptor(jobId, CronSchedule(req.cronExpression),
              JobTarget(req.service, req.handler, req.objectKey, req.payload))
          ctx.send(
            Request.of(target,
              TypeTag.of(classOf[JobDescriptor]),
              TypeTag.of(classOf[CronWire.TickResult]),
              initPayload),
            java.time.Duration.ofMillis(delayMs))
          CreateJobResult(jobId)
        },
        serdeFactory,
        HandlerRunner.Options.DEFAULT
      )
    val createHandler: HandlerDefinition[CreateJobRequest, CreateJobResult] =
      HandlerDefinition.of("create", HandlerType.EXCLUSIVE, createSerde, createResSerde, createRunner)

    // -- cancel: CLEARS the CronJob virtual object's descriptor state
    // (the already-journaled next tick re-reads the descriptor, finds
    // it absent, and terminates — "cancelled stays cancelled").
    val cancelRunner: HandlerRunner[TickRequest, CancelResult] =
      HandlerRunner.of(
        (ctx: ObjectContext, req: TickRequest) => {
          ctx.clear(CronJobManagerService.DescriptorKey)
          CancelResult(cancelled = true)
        },
        serdeFactory,
        HandlerRunner.Options.DEFAULT
      )
    val cancelHandler: HandlerDefinition[TickRequest, CancelResult] =
      HandlerDefinition.of("cancel", HandlerType.EXCLUSIVE, cancelReqSerde, cancelResSerde, cancelRunner)

    // -- describe: reads the stored descriptor from the CronJob object.
    // Absent state (unknown or cancelled job) is a 404-class
    // TerminalException, not a fake descriptor.
    val describeRunner: HandlerRunner[TickRequest, JobDescriptor] =
      HandlerRunner.of(
        (ctx: ObjectContext, req: TickRequest) => {
          Option(ctx.get(CronJobManagerService.DescriptorKey).orElse(null)) match {
            case Some(desc) => desc
            case None =>
              throw new TerminalException(404,
                s"cron job '${req.jobId}' not found (never created, or cancelled)")
          }
        },
        serdeFactory,
        HandlerRunner.Options.DEFAULT
      )
    val describeHandler: HandlerDefinition[TickRequest, JobDescriptor] =
      HandlerDefinition.of("describe", HandlerType.EXCLUSIVE, cancelReqSerde, descResSerde, describeRunner)

    ServiceDefinition.of(
      "CronJobManager",
      ServiceType.SERVICE,
      java.util.List.of(createHandler, cancelHandler, describeHandler)
    )
  }
}

/** The `CronJob` virtual object: `init` stores the descriptor and
  * schedules the first tick; `tick` invokes the target, computes the
  * next fire, and delayed-sends its own `tick`. */
object CronJobObject {

  import CronWire._

  private val scalaMapper: com.fasterxml.jackson.databind.ObjectMapper =
    new com.fasterxml.jackson.databind.ObjectMapper()
      .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  private val serdeFactory = new JacksonSerdeFactory(scalaMapper)

  /** Build the `CronJob` virtual-object ServiceDefinition (init/tick
    * handlers).
    *
    * @param calculator next-fire computation (tests pass a
    *        fixed-clock implementation; production passes
    *        [[CronUtilsNextFireTime]])
    * @return the ServiceDefinition to register on the Restate endpoint
    */
  def serviceDefinition(calculator: NextFireTimeCalculator): ServiceDefinition = {
    val tickReqSerde = serdeFactory.create(classOf[TickRequest])
    val tickResSerde = serdeFactory.create(classOf[TickResult])
    val descSerde = serdeFactory.create(classOf[JobDescriptor])

    // -- init: stores the descriptor, schedules the FIRST tick (delayed
    // self-send). Called once by CronJobManagerService.create.
    val initRunner: HandlerRunner[JobDescriptor, TickResult] =
      HandlerRunner.of(
        (ctx: ObjectContext, desc: JobDescriptor) => {
          ctx.set(CronJobManagerService.DescriptorKey, desc)
          val nextFire = calculator.nextFire(desc.schedule.expression, System.currentTimeMillis())
            match {
              case Right(t)  => t
              case Left(err) =>
                throw new TerminalException(500, s"cron '${desc.schedule.expression}': $err")
            }
          val delayMs = math.max(0L, nextFire - System.currentTimeMillis())
          val selfTarget = Target.virtualObject("CronJob", ctx.key(), "tick")
          ctx.send(
            Request.of(selfTarget,
              TypeTag.of(classOf[TickRequest]),
              TypeTag.of(classOf[TickResult]),
              TickRequest(desc.jobId)),
            java.time.Duration.ofMillis(delayMs))
          TickResult(nextFire)
        },
        serdeFactory,
        HandlerRunner.Options.DEFAULT
      )
    val initHandler: HandlerDefinition[JobDescriptor, TickResult] =
      HandlerDefinition.of("init", HandlerType.EXCLUSIVE, descSerde, tickResSerde, initRunner)

    // -- tick: invokes the target (fire-and-forget send — cron fire
    // semantics are "trigger", not "wait for completion"), computes the
    // next fire, and self-reschedules. Descriptor absent = job was
    // cancelled; terminate the chain silently.
    val tickRunner: HandlerRunner[TickRequest, TickResult] =
      HandlerRunner.of(
        (ctx: ObjectContext, req: TickRequest) => {
          Option(ctx.get(CronJobManagerService.DescriptorKey).orElse(null)) match {
            case None =>
              TickResult(-1L) // cancelled; no reschedule
            case Some(desc) =>
              val target = desc.target.objectKey match {
                case Some(k) => Target.virtualObject(desc.target.service, k, desc.target.handler)
                case None    => Target.service(desc.target.service, desc.target.handler)
              }
              val payloadBytes =
                desc.target.payload.getOrElse("").getBytes(java.nio.charset.StandardCharsets.UTF_8)
              ctx.send(Request.of(target, payloadBytes))
              val nextFire = calculator.nextFire(desc.schedule.expression, System.currentTimeMillis())
                match {
                  case Right(t)  => t
                  case Left(err) =>
                    throw new TerminalException(500, s"cron '${desc.schedule.expression}': $err")
                }
              val delayMs = math.max(0L, nextFire - System.currentTimeMillis())
              val selfTarget = Target.virtualObject("CronJob", ctx.key(), "tick")
              ctx.send(
                Request.of(selfTarget,
                  TypeTag.of(classOf[TickRequest]),
                  TypeTag.of(classOf[TickResult]),
                  TickRequest(req.jobId)),
                java.time.Duration.ofMillis(delayMs))
              TickResult(nextFire)
          }
        },
        serdeFactory,
        HandlerRunner.Options.DEFAULT
      )
    val tickHandler: HandlerDefinition[TickRequest, TickResult] =
      HandlerDefinition.of("tick", HandlerType.EXCLUSIVE, tickReqSerde, tickResSerde, tickRunner)

    ServiceDefinition.of(
      "CronJob",
      ServiceType.VIRTUAL_OBJECT,
      java.util.List.of(initHandler, tickHandler)
    )
  }
}
