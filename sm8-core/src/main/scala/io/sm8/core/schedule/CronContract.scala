/*
 * SM8 Core — Cron scheduling contract (RFC §3: pure data + pure
 * functions, no IO, no Restate imports, no external systems).
 *
 * The platform's Restate cron service stores these specs in job state
 * and delegates fire-time computation to a `NextFireTimeCalculator`
 * (platform-provided, backed by cron-utils). Core owns the CONTRACT;
 * the platform owns the IMPLEMENTATION (the cron-utils parser, the
 * Restate virtual object, the delayed self-send).
 *
 * Per [[scala-data-driven-refactor-mindset]]: data (ScheduleSpec,
 * JobDescriptor) separated from behavior (NextFireTimeCalculator).
 */
package io.sm8.core.schedule

/** A parsed cron schedule (unix 5-field form, as accepted by the
  * platform's calculator).
  *
  * @param expression the cron expression, e.g. "0 0 * * *" (daily
  *           midnight UTC)
  */
final case class CronSchedule(expression: String) extends AnyVal

/** The target a cron job invokes when it fires.
  *
  * Restate-addressed: service + handler (optionally a virtual-object
  * key) + an opaque JSON payload the target handler parses.
  *
  * @param service the Restate service name (e.g. "GateBTraceService")
  * @param handler the handler name on that service
  * @param objectKey the virtual-object key, when the target is keyed
  * @param payload JSON string passed to the handler (opaque to the
  *           scheduler)
  */
final case class JobTarget(
  service: String,
  handler: String,
  objectKey: Option[String],
  payload: Option[String]
)

/** A registered cron job: what fires, where, and when it was created.
  *
  * @param jobId unique identifier (platform-assigned, opaque)
  * @param schedule the cron expression
  * @param target what to invoke on fire
  */
final case class JobDescriptor(
  jobId: String,
  schedule: CronSchedule,
  target: JobTarget
)

/** Computes the next fire time for a cron expression. Implemented by
  * the platform (cron-utils backed); injected into the platform's
  * cron service so tests can supply a fixed clock.
  *
  * Per [[scala-error-handling-mindset]] "errors are data": an
  * unparseable expression returns Left rather than throwing.
  */
trait NextFireTimeCalculator extends Serializable {

  /** @param expression the cron expression
    * @param afterMillis exclusive lower bound (epoch millis); the
    *           returned time is strictly after this instant
    * @return the next fire time in epoch millis, or Left with a
    *         human-readable parse error
    */
  def nextFire(expression: String, afterMillis: Long): Either[String, Long]
}
