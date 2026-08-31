# Breaking changes

## PR-213 — Remove the deprecated `Connector` SDK surface (ADR-011-a)

The deprecated `Connector` abstraction (`io.sm8.sdk.Connector` + its
supporting types) is removed. Production engine wiring has flowed
through the `EngineProvider` ServiceLoader seam since ADR-006 Post-#65
/ PR-191; the legacy SDK trait is now gone entirely.

### What breaks

1. **`Engine.connectors` is gone from the SDK trait.** Third-party
   plugins calling `engine.connectors.register(...)` will not compile.
   Migration: register hooks + transformers via `engine.hooks` /
   `engine.transformers`; data-source wiring is owned by the
   `EngineProvider` seam in the connector modules.
2. **sm8-core test-jar conformance surface change.** The abstract
   `ConnectorContractSpec` (RFC §12 4-assertion data-source conformance
   contract) is removed from the test-jar. Future data-source
   conformance is validated by the per-connector `EngineProvider` test
   suites under `connectors/` (typed `realize` grammar tests +
   `EngineRegistry` routing tests).
3. **`RequestResult.scala` types removed** (`ConnectorRequest`,
   `ConnectorResult`, `ConnectorError`). The in-tree fallback Pipeline
   now surfaces `PipelineError(engine, error)` (typed
   `EngineError.UnsupportedCapability` for unknown request types) and
   `PipelineSkipped(stage: PipelineStage)` (explicit short-circuit
   marker when a hook sets `Context.stop`), both in `io.sm8.core`.

### Why

Single data-source abstraction (`EngineProvider`) — ADR-008-P §AR-P1-7
deprecated this trait with removal promised "in v1.0.0"; ADR-011-a
(scope validated by dual reviewers across 2 rounds) executes the
removal pre-1.0 while the codebase is still pre-v0.1.0.
