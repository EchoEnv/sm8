/*
 * Conformance contract spec for the hook-firing-audit plugin.
 *
 * Exercises the plugin's probe + reporter hooks through the shared
 * contract bases (HookContractSpec, PluginContractSpec), verifying
 * the shape contract: non-empty names, in-range priorities, valid
 * stage wireNames, and request immutability across run().
 */
package io.sm8.plugins.hookfiringaudit

import io.sm8.core.EngineImpl
import io.sm8.sdk.contract.{HookContractSpec, PluginContractSpec}

import io.sm8.sdk.{Context, PipelineStage, PreHook, PostHook, Request, Result}

/** Minimal no-op Request for the contract baseline Context. */
case object HookFiringAuditConformanceRequest extends Request

/** Minimal no-op Result for the contract baseline Context. */
case object HookFiringAuditConformanceResult extends Result

/**
 * Hook-shape conformance for the plugin's reporter (the PostHook
 * representative) and its PreParse probe (the PreHook
 * representative).
 *
 * The behavioral contract (stamp accumulation, missing-stamp
 * anomaly classification) is covered by [[HookFiringAuditSpec]];
 * this spec pins the shape contract the shared bases assert.
 */
abstract class HookFiringAuditContractSpec extends HookContractSpec {

  /** The PreHook representative under test (the pre:parse probe).
    *
    * @return the PreHook under test
    */
  override def preHook: PreHook = new PreParseProbe

  /** The PostHook representative under test (the reporter).
    *
    * @return the PostHook under test
    */
  override def postHook: PostHook = new PostFormatReporter

  /** Baseline context carrying the conformance request/result pair.
    *
    * @return the baseline Context
    */
  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Parse,
      request = HookFiringAuditConformanceRequest,
      result  = Some(HookFiringAuditConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}
