/*
 * SM8 Core -- HookRunner (PR-M4 GAP 6).
 *
 * Minimal engine-portable abstraction for the hook-dispatch shape that
 * SparkEngineProvider needs. Engine-portable: no Spark types in the
 * trait signature (sm8-core cannot depend on the spark module). The
 * actual returned value is generic `A`; the connector instantiates
 * this with `A = org.apache.spark.sql.DataFrame`.
 *
 * The sm8-platform layer supplies a concrete `EngineHookDispatcher`
 * that satisfies this. The default impl is a no-op (no dispatcher);
 * production deployments inject a real one.
 */
package io.sm8.core.engine

trait HookRunner extends java.io.Serializable {
  /** Wrap a (ctx => build A) thunk. The dispatcher runs
    * pre-hooks, calls build, runs post-hooks; returns the result. */
  def run[A](
      ctx:   EngineContext,
      build: EngineContext => Either[EngineError, A],
  ): Either[EngineError, A]
}

object HookRunner {
  /** No-op runner: the default. Deployments that wire the
    * sm8-platform dispatcher supply a real one. */
  object Noop extends HookRunner {
    override def run[A](
        ctx:   EngineContext,
        build: EngineContext => Either[EngineError, A],
    ): Either[EngineError, A] = build(ctx)
  }
}
