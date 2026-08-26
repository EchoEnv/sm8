/*
 * SM8 cache Plugin — P2.5 regression test for the ctx.meta fold pattern.
 *
 * The Dual-review P2.5 site: CacheWritePostHook.run (CachePlugin.scala:226-230)
 * silently swallows typed-Left errors from `CachedRowDecoder.toRestateCachedRowFromPortable`
 * via `System.err.println` only — callers cannot observe the cause without
 * scraping stderr. ADR-009-d established the ctx.meta fold pattern (pre-hooks
 * write to ctx.meta; post-hooks read from ctx.meta) as the engine-portable
 * channel for cross-hook data sharing.
 *
 * P2.5 folds the typed Left into the ctx.meta channel via the key
 * `sm8.cache.write.error` (a new 3rd cache-meta key alongside the existing
 * `sm8.cache.policy` key from ADR-009-g + the read-side keys). The println
 * is preserved as a side-channel; the meta key is the primary signal.
 *
 * This spec asserts:
 *  1. A WriteThrough + journal-encode Left → returned Context's meta
 *     carries `sm8.cache.write.error` (the typed Left, observable via
 *     the engine-portable ctx.meta channel).
 */
package io.sm8.plugins.cache

import io.sm8.core.cache._
import io.sm8.core.engine.{EngineHookRequest, EngineHookResult, QueryRequest}
import io.sm8.core.model.CachePolicy
import io.sm8.sdk.{Context, PipelineStage}

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CachePluginP25Spec extends AnyFunSuite with Matchers {

  /** ResultCache stub: putJournaledWithModelAndVersion records writes
    * (so we can assert cache.putJournaled is NOT called on Left). */
  private final class LocalCache extends ResultCache {
    val writes = scala.collection.mutable.ListBuffer.empty[(String, RestateCachedRow)]
    override def getJournaled(key: String): Option[RestateCachedRow] = None
    override def putJournaledWithModelAndVersion(
        key: String, value: RestateCachedRow, model: String, version: Int
    ): Unit = { writes += ((key, value)); () }
  }

  /** Build a minimal WriteThrough context that exercises the
    * post-hook's `CachedRowDecoder.toRestateCachedRowFromPortable`
    * Left path. The PQR has a row whose cell count doesn't match
    * the schema field count, which forces the decoder's Left
    * branch (`row[$i].cells(N) != fieldCount`). */
  private def buildWriteThroughContextWithDecodeFailure(cache: ResultCache): Context = {
    val model = io.sm8.core.model.Model.of(
      name = "p25-model",
      version = 1,
      description = None,
      dimensions = Nil,
      measures = Nil,
      defaultPolicies = io.sm8.core.model.ModelPolicyDefaults(
        materialize = io.sm8.core.model.MaterializePolicy.None,
        cache = CachePolicy.NoCache,
        audit = io.sm8.core.model.AuditPolicy.NoAudit),
      source = io.sm8.core.model.SourceRef.ByName(table = "t"),
      status = io.sm8.core.model.ModelStatus.Draft,
      filters = Nil,
      calculatedMeasures = Nil,
      joins = Nil).toOption.get
    val request = EngineHookRequest(
      model = model,
      mcpRequest = QueryRequest.empty,
      cacheKey = "p25-key")
    val pqr = io.sm8.core.engine.PortableQueryResult(
      schema = io.sm8.core.engine.ResultSchema(List(
        io.sm8.core.schema.Field(
          name = "a",
          dataType = io.sm8.core.schema.SealedDataType.Varchar,
          nullable = false))),
      // Row with 1 cell, but schema has 1 field — so decoder
      // builds `fieldCount=1`, row.length=1, passes. To force
      // mismatch, schema has 1 field but row has 0 cells.
      rows = Vector(io.sm8.core.engine.ResultRow(
        values = Nil,
        schema = io.sm8.core.engine.ResultSchema(Nil))),
      metadata = Map.empty)
    Context(
      stage = PipelineStage.Execute,
      request = request,
      result = Some(EngineHookResult(pqr)),
      meta = Map("sm8.cache.policy" -> CachePolicy.WriteThrough("p25")))
  }

  test("CacheWritePostHook: journal encode Left surfaces via ctx.meta('sm8.cache.write.error')") {
    // P2.5 fold-in: the typed Left from the journal-encode failure
    // is now surfaced on the engine-portable ctx.meta channel via
    // the key `sm8.cache.write.error`. Callers (post-hooks, ops
    // dashboards, tests) can read the cause without scraping stderr.
    val cache = new LocalCache
    val counter = new AtomicInteger(0)
    val hook = new CacheWritePostHook(cache, counter)
    val in = buildWriteThroughContextWithDecodeFailure(cache)
    val out = hook.run(in)
    out.meta.contains("sm8.cache.write.error") shouldBe true
    // The println side-channel still fires (operators reading logs
    // see the diagnostic string); we don't capture stderr here but
    // assert the meta key is set as the primary signal.
  }
}
