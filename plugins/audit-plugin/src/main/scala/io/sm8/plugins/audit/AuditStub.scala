/*
 * SM8 audit Hook Plugin.
 *
 * registers a Post-hook at PostFormat that counts each fire via an
 * AtomicInteger. The structured-event sink (SLF4J) is a future
 * extension that lands separately from the counter.
 */
package io.sm8.plugins.audit

import java.util.concurrent.atomic.AtomicInteger

import io.sm8.sdk._

/**
 * Audit Hook Plugin. Records each engine.run via a PostFormat hook.
 *
 * Per the Spark-closure-safety rule (hooks must be Serializable and
 * capture no non-serializable engine state).
 */
final class AuditStub extends Plugin with java.io.Serializable {

  /** 
    * captured `fires` AtomicInteger. Round-trips through
    * ObjectOutputStream (verified by PluginSerializationSpec). */
  override def closedOverVars: Seq[String] = Seq("fires")

  /** Test-visible counter of hook fires. */
  val fires: AtomicInteger = new AtomicInteger(0)

  override def setup(engine: Engine): Unit = {
    engine.hooks.registerPostHook(
      HookStage.PostFormat,
      new AuditPostStubHook(fires),
      priority = 150
    )
  }
}

/**
 * PostFormat audit hook. Increments a counter on every fire. The
 * SLF4J-sinked structured event is a future extension.
 *
 * Serializable: captured in closures must serialize cleanly.
 */
private final class AuditPostStubHook(counter: AtomicInteger)
    extends PostHook with java.io.Serializable {
  override val name: String = "audit-stub"
  override val priority: Int = 150
  override def stage: HookStage = HookStage.PostFormat
  override def run(context: Context): Context = {
    counter.incrementAndGet()
    // Real implementation: build an audit event (timestamp, model
    // name, result size, status) and sink to SLF4J.
    context
  }
}
