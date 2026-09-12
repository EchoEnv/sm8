# ADR-0032: Manifest vs YAML — file format, typed ADT, and the loader seam

**Status:** Proposed.
**Date:** 2026-09-12.

## Context

The question "do we need a metric store (measurement layer / semantic
layer) as storage, rather than YAML?" conflates two different layers.
This ADR records the distinction so future discussions start from
shared vocabulary, and records where a store would attach if one is
ever needed. It changes no code.

## Decision

### 1. Vocabulary: YAML is the manifest's file format, not its alternative

```text
your-file.yaml               META-INF/                   core/
        │                  manifest.schema.v2.json        │
        ▼                       │                         │
ModelLoader.fromStream ───► YAMLFactory (Jackson)          │
   │                           │                         │
   │   JsonNode                ▼                         │
   ├────────► ManifestValidator  ──► JSON-Schema checks  ─┤
   │                                                       │
   ▼                                                       ▼
buildModel(root: Map[_,_])  ──────────────────────►  typed Model ADT
```

- **YAML** = the on-disk serialization: human-edited, git-versioned,
  one model per file. A format, not a concept.
- **Manifest layer** = `ModelLoader` + `ManifestValidator` + the
  JSON-Schema (`META-INF/sm8/manifest.schema.v2.json`) + the typed
  `Model` ADT (`Measure`, `CalculatedMeasure`, `TypedDimension`,
  `RollupSpec`, `JoinSpec`, `FreshnessPolicy`, `FilterSpec`,
  `ModelStatus`). The contract that makes sm8 recognize a definition
  as a model at all.

The engine never sees YAML — only the typed ADT. In the strict
sense the manifest already exists; YAML is how it travels on disk.

### 2. "Metric store" means runtime infrastructure, and sm8 defers it

A metric/semantic *store* (as in Cube Store, MetricFlow's state DB,
LookML's workbook backend) is not another way to write definitions —
it is infrastructure for:

- version history and cross-model lineage you can query,
- multi-team concurrent authoring with precedence/conflict
  resolution,
- access control and audit binding,
- usage telemetry feeding optimization.

sm8 currently has none of these, by design: definitions live in
git-ops'd YAML (review, versioning, rollback for free), and the
*materialized* semantic outputs already live in a real store —
Iceberg rollup tables per ADR-0028.

### 3. The loader seam is the extension point

Per RFC §3 (core is IO-free), definitions enter core only via
`ModelLoader.fromStream(stream, source)` (and `fromString` for
tests). Any future store — JDBC, REST, object storage, an actual
metric-store product — attaches as a new loader *behind that same
seam*, living in adapter/connector land:

```text
adapter/connector concern                    core (unchanged)
─────────────────────────                    ────────────────
JdbcModelLoader / RestModelLoader   ──►  ModelLoader.fromStream
reads from store, hands core        ──►  ManifestValidator
the same YAML/JSON bytes or builds       buildModel
the Map[_,_] directly               ──►  typed Model ADT
```

Zero core changes. Adopting a store later is additive, not a
migration.

### 4. Trigger criteria for revisiting

Adopt a store only when a real requirement appears, not for
fashion. The signals that would justify it:

- **Lineage history**: "which model fed this rollup number, and who
  changed the definition last month" — YAML-in-git answers this only
  via commit archaeology.
- **Multi-author governance**: multiple teams authoring models
  concurrently with precedence rules between overlapping models.
- **Runtime binding**: access control / audit requirements attached
  to definitions themselves.

Absent those, YAML remains the source of truth.

## Consequences

- Documentation only; no code, no schema, no loader changes.
- Future store work should cite this ADR and the loader seam, and
  live outside core per RFC §3.
- The wayfinder map (#378) records this as a decision context note.

## References

- RFC §3 Core Boundary (`docs/rfcs/2026-08-12_v1_architecture-spec/`)
- ADR-0028 (Iceberg rollup format standard — the materialized side)
- `sm8-core/src/main/scala/io/sm8/core/manifest/ModelLoader.scala`
- `sm8-core/src/main/scala/io/sm8/core/manifest/ManifestValidator.scala`
