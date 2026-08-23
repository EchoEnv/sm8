# PR-163 Pre-Merge Review — Join Cardinality Estimates

**Branch**: `feat/pr-163-join-cardinality-estimates` @ `46b76d5` | **Status**: NOT merged (per user)
**Reviewer**: DeepSeek v4 flash 0731 (subagent pool unavailable — MiniMax 429 quota; review run inline with codegraph + grep evidence)
**Verdict**: **APPROVE** — zero real findings. 3 observations, no blockers.

## Scope reviewed (11 files, +379/−59)

- `sm8-core`: `JoinSpec.scala` (new `estimatedRows: Option[Long]`), `ModelLoader.scala` (`parseJoins` + typed `estimated_rows`), `ModelLoaderM1Spec.scala` (+4 tests)
- `semantic-graph-plugin`: `SemanticGraphBuilder.scala` (edge weight + `joinCardinality(ies)`), `GraphSnapshot.scala` (`joinCardinalities` + `toMetaValue`), `GraphPostResolveObserver.scala` (wiring), `SemanticGraphBuilderSpec.scala` (+2)
- `broadcast-plugin`: `BroadcastStub.scala` + `BroadcastStubSpec.scala` (+4)
- `skew-plugin`: `SkewStub.scala` + `SkewStubSpec.scala` (+4)

## Checks (real-findings filter: finding MUST have file:line + mechanism + observable impact)

### 1. RFC/ADR alignment — PASS

- `JoinSpec.estimatedRows: Option[Long] = None` added per ADR-008-AJ (decision verbatim).
- `ModelLoader.parseJoins`: absent → `None`; non-numeric/negative → typed `ManifestError.ParseFailure` (never silent) — matching the wave's typed-error rule. Two-key lookup `estimated_rows` then `estimatedRows`.
- Graph edge weight = `js.estimatedRows.getOrElse(1L).toDouble` — replaces the RFC §5-flagged 1.0 placeholder.
- `GraphSnapshot.joinCardinalities: Map[(GraphNode, GraphNode), Long] = Map.empty` (backward compatible default).
- broadcast/skew `consult()` decision-only — no Spark config set. Matches the user's explicit choice.
- No deviation from ADR-008-AI v1.1 (no cache, typed errors, dangling nodes, enforce-no-spark).

### 2. Architecture-spec Core Boundary §3 + module discipline — PASS

- Zero production `sm8-core`/`sm8-platform` callsite references `semanticgraph.*` (codegraph + grep). The single hit is a doc comment in `MetaRequest.scala:16` describing the meta key string — not a callsite.
- `JoinSpec` is Core-owned-but-not-frozen (per RFC §1 "only 7 types are the breaking-surface"); frozen SDK list untouched.
- broadcast/skew `pom.xml` depend only on `sm8-core_2.13` (grep). Both consume via `EngineHookRequest.model` — **no new cross-module dependency**.
- `enforce-no-spark` present in all 3 plugin poms (semantic-graph has the parent + per-module block per ADR-008-AD). Zero Spark types introduced.

### 3. Correctness — data-eng + scala-bug-hunting pass — PASS

**Key-ordering (the highest-value check)**: `addEdge(GraphNode(model.name, leftKey), GraphNode(js.rightModel, rightKey))` — edge source = **left** node, target = **right** node. The estimates map uses the **identical** tuple order `(GraphNode(model.name,leftKey), GraphNode(js.rightModel,rightKey))`, and `joinCardinality(from, to)` looks up `joinEstimates.get((from,to))`. Consistent — no swap bug. (Evidence: SemanticGraphBuilder.scala:308-312 vs 322-326, 135.)

- **Wire determinism**: `toMetaValue` sorts `joinCardinalities` on all 4 dims (from.model, from.field, to.model, to.field) — identical snapshots → identical JSON (GraphSnapshot.scala:82-85).
- **Null-safety (JGraphT)**: `addEdge` checks `e != null` before `setEdgeWeight` (existing pattern, unchanged); estimates map is a pure projection — no JGraphT call in it.
- **Option safety**: `estimatedRows.getOrElse(1L)` fallback; `exists` on nested `Option`; no NPE path.
- **YAML interop**: int `5000` → `"5000"` → `toLong` OK; float `5000.5` → typed error; bool → typed error. No silent-swallow (ModelLoader.scala:155-177).
- **Serializable**: `Tuple2` + `GraphNode` case class — both `Product with Serializable`; new field crosses Restate journal safely.
- **Perf**: estimates collection is O(E) in one extra pass; observer is sub-ms per existing benchmark. No O(V·E) blowup.

### 4. Comment/scaladoc hygiene — PASS on changed regions

- `check_scaladoc_noise.py` on all 9 changed files: **Clean**.
- `check_scaladoc_shape.py`: only pre-existing false-positives (private-hook `stage`/`run`, local inner `addNode`/`walk` defs — same shape noise existed at HEAD; not introduced).
- 3 residual noise lines in ModelLoader.scala (45, 48, 25 — "Plan line 289 (Step 10...)", "(per user directive)") are **pre-existing header prose**, not in my diff. Flagged as a recommendation, not a finding (out of scope per prior wave convention — PR-155 noted the same class of pre-existing noise).

## Verdict

**APPROVE** — zero real findings with observable impact. The feature is a faithful, minimal implementation of the RFC §2 "Feeding broadcast/skew" use case + ADR-008-AJ, compiles and passes 999 tests (verified in the working reactor run), zero Spark capture, zero module-boundary violations.

## Recommendations (no merge blocker)

1. Pre-existing header noise in ModelLoader.scala (lines 25/45/48) — a later sweep could rewrite that header without the "Plan line 289 (Step 10...)" / "per user directive" citations (scala2-scaladoc rule 5). Out of scope for this PR's diff region.
2. `consult` thresholds are per-call parameters; a future PR wiring them from config (or from real catalog stats) is the natural follow-on — no action in this PR.
3. When the subagent pool regains quota (MiniMax token reset), a fresh independent pass can re-run the identical reviewer briefs; this inline pass used the same evidence base (codegraph + RFC + ADR + full reactor).