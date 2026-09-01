# ADR-012-a: ModelService — Restate-handler surface for the loaded model(s)

> **Status:** Proposed. **Date:** 2026-09-01. **Author:** SM8 agent (per user directive "can we draft these as new ADR-012 series and pass to dual reviewers to approve first").

## Context and Problem Statement

As of PR-249 (`9e04779`), sm8-platform exposes exactly **two** Restate services via `HttpTransport.endpoint`:

| Service | Handler | Source | Purpose |
|---|---|---|---|
| `QueryService` | `runQuery` | `sm8-platform/.../QueryService.scala` | Execute a query |
| `MetaInspectorService` | `getMeta` | `sm8-platform/.../MetaInspectorService.scala` | Read `context.meta` for a key |

A third surface is **absent**: **no handler that answers "what models are loaded in this deployment"**. The operator passes `--model <yaml>` at boot (`sm8-server/src/main/scala/io/sm8/server/Main.scala:317-329`); the model is loaded once, held in `PlatformModelLoader`, and never surfaced back to clients. There is no `ModelRegistry`, no multi-model support, no hot-reload, no introspection.

Concrete consequences observed during PR-249 verification:

- The Restate web UI's **Services** page lists `QueryService` and `MetaInspectorService` but no model surface. A user cannot tell from the UI whether the deployment has a model named `smoke-e2e-model` or `production-sales` without shelling into the host.
- The Restate web UI's **Invocations** page shows `QueryService/runQuery` calls but cannot correlate them to a model — `QueryRequest.modelName` is in the payload but the UI doesn't show "model X was invoked Y times".
- Operators cannot audit "which models exist" or "which models are referenced by which invocations" without `grep`-ing deployment manifests on the host filesystem.
- A future dashboard feature (ADR-009-d's `context.meta`-driven broadcast/skew decisions, ADR-008-O's typed-explained-query flow) needs a model-discovery endpoint as a primitive.

The closest existing surface is `EngineRegistry` (`sm8-core/.../EngineRegistry`), which does what we want **for engines** (named lookup, default fallback, `EngineUnavailable` typed error) but **not for models**. There is no model equivalent.

## Decision

Add a **read-only** `ModelService` to `sm8-platform` that surfaces the currently-loaded model(s) through the existing Restate ingress. **Three handlers, no writes**:

| Handler | Wire | Returns |
|---|---|---|
| `listModels` | `()` | `ListModelsResponse(models: Seq[ModelSummary])` |
| `getModel` | `GetModelRequest(name: String)` | `GetModelResponse(summary: ModelSummary)` |
| `describe` | `()` (no-arg shorthand) | `DescribeResponse(model: ModelSummary)` — convenience handler returning the single primary model (the one sm8-server boots with today) |

### Scope — what is NOT in this ADR

To keep this small and reviewable, **the following are explicitly out of scope**:

- `registerModel(yaml)` / `deleteModel(name)` — **hot-add/remove at runtime**. Requires a `ModelRegistry` with concurrency + persistence (where do stored models live? in-memory? file? RocksDB?). Deferred to ADR-012-d (if needed).
- `setDefault(name)` / multi-model serving — same reason.
- Persistence — the ModelService reads from `PlatformModelLoader` only. No `ModelRegistry` data structure added.
- New `ModelRegistry` data structure — `PlatformModelLoader` is already the in-memory cache. ModelService is a thin reader over it.
- Wire-format change to `QueryService` / `MetaInspectorService` — ModelService is additive.

### Alternatives Considered

| Alternative | Reason rejected |
|---|---|
| **Expose the YAML source verbatim in `getModel`** | `PlatformModelLoader` does NOT expose a `toYaml(model): String` method (only `fromPath`/`fromString`/`validateAndLoad`/`readFile`). Adding a `toYaml` round-trip would introduce a new public API in `sm8-core` for one consumer. Dropped for now; can revisit in ADR-012-d. |
| **Service-level per-interface (multi-`ModelService` instances)** | Would require `ModelRegistry` data structure + lifecycle plumbing. Out of scope per the registry-deferral rationale above. |
| **Put the discovery endpoint on `MetaInspectorService`** | Conflates "diagnostic introspection of one invocation" with "registry of loaded models". Separate service is cleaner. |
| **Use Restate's journal to reconstruct the model surface** | Restate doesn't journal the boot-time model; it only journals invocations. Not feasible. |

### Layer discipline

Per `docs/rfcs/2026-08-12_v1_architecture-spec/` §3 (Core Boundary) and `ADR-009-c` (per-query clone-session, `0466841`):

| Layer | What lands there |
|---|---|
| **sm8-core** (`io.sm8.core.model`) | New `ModelSummary` case class — DTO projection of `Model`. No new public types beyond the case class. (Manifest layer is for parse/validate, NOT for DTOs.) |
| **sm8-platform** (`io.sm8.platform.query`) | New `ModelService` object with `definition(model: Model): ServiceDefinition`. Wire DTOs `ListModelsResponse`, `GetModelRequest`, `GetModelResponse`, `DescribeResponse`. `HttpTransport.endpoint` binds ModelService after QueryService. |
| **sm8-server** (`io.sm8.server.Main`) | **No change.** `PlatformModelLoader` is already passed to `HttpTransport`'s factory. ModelService piggybacks. |
| **plugins / hooks** | **No change.** ModelService is a stateless reader; hooks do not write to it. |

### Implementation sketch (corrected per dual-review round 1)

The original sketch had 3 errors caught by reviewers and **fixed here**: (a) `SourceRef.ByName` is 3-arg (catalog/namespace/table), not 1-arg; (b) `HandlerError.NotFound` does not exist — use `dev.restate.sdk.common.TerminalException` per `QueryService.scala:248`; (c) `PlatformModelLoader.toYaml(model)` does not exist, so the `yaml` field is **dropped** from `GetModelResponse`.

```scala
// sm8-core/src/main/scala/io/sm8/core/model/ModelSummary.scala
final case class ModelSummary(
    name:        String,
    version:     Int,
    status:      String,    // "draft" | "published" | "deprecated" (per ModelStatus)
    catalog:     Option[String],
    namespace:   Option[String],
    table:       String,    // empty string if SourceRef.ByName isn't used
    dimensions:  Int,
    measures:    Int,
    description: Option[String]
) extends Product with Serializable

object ModelSummary {
  def fromModel(model: Model): ModelSummary = {
    val (catalog, namespace, table) = model.source match {
      case SourceRef.ByName(c, n, t) => (c, n, t)
      case _                         => (None, None, "")
    }
    val status = model.status match {
      case ModelStatus.Draft      => "draft"
      case ModelStatus.Published  => "published"
      case ModelStatus.Deprecated => "deprecated"
    }
    ModelSummary(
      name        = model.name,
      version     = model.version,
      status      = status,
      catalog     = catalog,
      namespace   = namespace,
      table       = table,
      dimensions  = model.dimensions.size,
      measures    = model.measures.size,
      description = model.description
    )
  }
}
```

```scala
// sm8-platform/src/main/scala/io/sm8/platform/query/ModelService.scala
import dev.restate.sdk.common.TerminalException

object ModelService {
  def definition(model: Model): ServiceDefinition = {
    val scalaMapper = new ObjectMapper()
      .registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val listRunner = HandlerRunner.of(
      (ctx, _: ListModelsRequest) => {
        val summary = ModelSummary.fromModel(model)
        ListModelsResponse(Seq(summary))
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    val getRunner = HandlerRunner.of(
      (ctx, req: GetModelRequest) => {
        if (req.name == model.name) GetModelResponse(summary = ModelSummary.fromModel(model))
        else throw new TerminalException(404, s"model '${req.name}' not found")
      },
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    val describeRunner = HandlerRunner.of(
      (ctx, _: DescribeRequest) => DescribeResponse(model = ModelSummary.fromModel(model)),
      jacksonSerdeFactory,
      HandlerRunner.Options.DEFAULT
    )

    ServiceDefinition.of(
      "ModelService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of("listModels", HandlerType.SHARED, listRequestSerde, listResponseSerde, listRunner),
        HandlerDefinition.of("getModel",  HandlerType.SHARED, getRequestSerde,  getResponseSerde,  getRunner),
        HandlerDefinition.of("describe",  HandlerType.SHARED, describeRequestSerde, describeResponseSerde, describeRunner)
      )
    )
  }
}
```

`HttpTransport.endpoint` adds one line: `.bind(ModelService.definition(model))`.

### Wire DTOs

```scala
case class ListModelsRequest()
case class ListModelsResponse(models: Seq[ModelSummary])
case class GetModelRequest(name: String)
case class GetModelResponse(summary: ModelSummary)
case class DescribeRequest()
case class DescribeResponse(model: ModelSummary)
```

### Testing

Per the user's locked checklist §3 (E2E tests):

- **Unit**: `ModelServiceSpec` in `sm8-platform/src/test/`, mocking `Model`. Asserts handler returns the expected `ModelSummary` for each handler; the `getModel` error path throws `TerminalException(404)`.
- **In-process E2E** (per PR-248 pattern, `HttpTransportRestateIngressE2ESpec`): start a real Vert.x socket, send `POST ModelService/listModels` with the Restate media type, assert body has `name`, `version`, `status`, `catalog`, `namespace`, `table` fields. Also `POST ModelService/getModel` with the wrong name, assert 404.
- **Real-restate E2E** (per PR-249 pattern, `scripts/smoke-e2e.sh`): register sm8 with Restate, send `POST localhost:8080/ModelService/listModels`, assert 200 + JSON has `models[]` with one entry whose `name` matches the boot `--model`.
- **Scaladoc**: per `scala2-scaladoc` skill, all public methods documented. `check_scaladoc_noise.py` + `check_scaladoc_shape.py` clean.

### Reviewer checkpoints

Per the user's locked checklist §5 (dual reviewers per RULE#5):

- **arch reviewer** — verify the wire DTOs don't bleed into `sm8-core` (RULE#1); verify `HttpTransport.endpoint` composition order doesn't break `QueryService.runQuery` semantics.
- **data-eng reviewer** — verify zero `.scala` outside `sm8-core/.../model` and `sm8-platform/.../query`; verify E2E spec runs end-to-end via the real Restate ingress; verify NPE/OOM risks on the `Seq[ModelSummary]` allocation (always ≤ the loaded model count — bounded by `1` today, planned `N` for ADR-012-d).

### Risks

| Risk | Mitigation |
|---|---|
| `SourceRef.ByName` is 3-arg; ModelSummary drops catalog/namespace if those are None (correct) | `fromModel` handles both cases; `catalog`/`namespace` are `Option[String]`, never null |
| Adds a 3rd service to the Restate deployment, increase of the wire surface by ~3 handlers | Minimal handlers, all read-only |
| `getModel` returns no YAML (just summary) — caller can't reconstruct the model | Document; ADR-012-d can add YAML round-trip if a real user asks |

## Consequences

### Positive

- **Restate web UI** shows the model surface in the Services page (3rd service alongside `QueryService` + `MetaInspectorService`).
- **Auditability**: callers can discover the loaded model without host-shell access.
- **Foundation for ADR-012-d**: hot-add/register/delete becomes a thin layer over this read surface (instead of designing both at once).
- **Layer discipline preserved**: pure read adapter; no new data structures; no engine changes.

### Negative

- Adds **3 handlers** to the wire surface (bidi-stream protocol). Operator already deals with 2 services + 2 handlers in the UI.
- A **partial answer** to "where is the model registry?" — until ADR-012-d (if it ships), this is read-only. If hot-add is needed, another ADR cycle.
- `GetModelResponse` does NOT include the YAML source (deferred per the alternatives-considered section).

## Out-of-Scope Follow-ups (likely ADR-012-d if needed)

- Multi-model serving (`ModelRegistry` data structure, `setDefault(name)` handler, `registerModel(yaml)` handler with persistence + yaml round-trip)
- Model validation lifecycle (per-model schema versioning, deprecation markers)
- Model-to-invocation correlation in the Restate UI (requires journal inspection, out of scope here)

## References

- PR-249 (`9e04779`): real Restate ingress E2E smoke; demonstrates the current 2-service baseline
- PR-248 (`a8f6dd6`): in-process E2E spec; demonstrates the HttpClient pattern to reuse
- `sm8-platform/.../QueryService.scala`: template for hand-rolled `ServiceDefinition` + `TerminalException` throw
- `sm8-platform/.../MetaInspectorService.scala`: smaller template (single handler)
- `sm8-platform/.../PlatformModelLoader.scala`: provides the `model` parameter ModelService reads from
- `sm8-core/.../Model.scala`: `SourceRef.ByName(catalog, namespace, table)` 3-arg shape; `ModelStatus` sealed trait with Draft/Published/Deprecated
- `docs/rfcs/2026-08-12_v1_architecture-spec/adapters.md`: layer discipline
- ADR-009-c (`0466841`): per-query clone-session; precedent for keeping shared state out of the request path
- ADR-011-a: precedent for "additive, no production-code change in adapters when possible"
- `scala2-scaladoc` skill (locked checklist §4): all new public methods documented