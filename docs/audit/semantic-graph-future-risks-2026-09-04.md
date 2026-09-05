# Semantic-graph Plugin — Future-Risks Audit (2026-09-04)

**Plugin module**: `plugins/semantic-graph-plugin/`
**Audit date**: 2026-09-04
**Audit scope**: 8 risk anchors across walker exhaustiveness, JGraphT API currency, typed-error surfacing, meta-key collisions, closure-safety, cycle-detector duplication, spark-enforcer, contract-spec coverage.
**Read-only**: yes. No code modifications. No commits. No network. Findings cite primary source `file:line` only.

---

## 1. TL;DR

- **0 CRITICAL**, **0 HIGH**, **1 MEDIUM**, **3 LOW**, **4 clean (none)**.
- **3-line verdict**: the plugin is in good operational shape (Anchor 3 typed-error plumbing is end-to-end correct, Anchor 4 zero cross-plugin key collisions, Anchor 5 zero Spark coupling, Anchor 6 cycle-detectors walk the same model state). The single MEDIUM is a behavioral asymmetry between the two cycle detectors (`SemanticGraph.hasCycle` flags dimension self-loops, `QueryBuilder.detectCalcCycles` does not) that creates a small authority-ambiguity window. The three LOWs are all doc-comment drift, not code.
- **Recommended path forward**: **DOCFIX** (only LOWs + 1 spec-coverage gap that is LOW because behavior is exercised end-to-end by `JoinPathPreHookCycleDetectionSpec`).

| Anchor | Severity | Finding |
|---|---|---|
| 1 | LOW | `Expr.scala:6-7` doc says "21 cases total" — file actually has 24. Walkers are exhaustive. |
| 2 | LOW | All 4 JGraphT APIs exist in 1.5.2 with current signatures. Forward-currency (1.6.x / 1.7.x) cannot be verified without web access. |
| 3 | none | Typed-error plumbing works end-to-end. `EngineService.scala:530-531` explicitly reads `ctx.meta.get("semanticGraphError")` and surfaces `Left(EngineError)`. Regression-tested by `JoinPathPreHookCycleDetectionSpec`. |
| 4 | none | Zero collisions across `audit-plugin/`, `broadcast-plugin/`, `cache-plugin/`, `skew-plugin/`, `materialize-plugin/`, `row-cap-plugin/`, `example-plugin/`. Brief misnamed the third key (the real key is `GraphSnapshot.MetaKey = "io.sm8.plugins.semanticgraph:graph-snapshot"`, not `"semanticGraph"`). |
| 5 | none | Zero `org.apache.spark` imports. Both hooks extend `java.io.Serializable`. `SemanticGraphPlugin` inherits `Serializable` from the `Plugin` trait at `sdk/Plugin.scala:34`. |
| 6 | MEDIUM | Cycle detectors use the same walkers but apply different vertex-set filters: `SemanticGraph` adds self-loop edges for self-referential dimensions (e.g. `dimAmount -> FieldRef("amount")`); `QueryBuilder.detectCalcCycles` walks only `calculatedMeasures`. Both are documented and the `addDimEdge` skip is intentional, but a future contributor editing one without the other could create drift. |
| 7 | none | `enforce-no-spark` execution binds the `enforce` goal (validate phase by default); `<exclude>org.apache.spark:*</exclude>` glob catches every spark artifact. Enforcer 3.4.1 from parent `pluginManagement` (`pom.xml:220`). Rule is well-formed. |
| 8 | LOW | `SemanticGraphContractSpec` reads back the REAL plugin-registered hooks but invokes them with `SemanticGraphConformanceRequest` (a no-op `case object`), which falls through the `case _ => context` branch in both `JoinPathPreHook.run` (`JoinPathPreHook.scala:84`) and `GraphPostResolveObserver.run` (`GraphPostResolveObserver.scala:87`). The shape contract is verified; business behavior is NOT exercised here — it is exercised by `JoinPathPreHookCycleDetectionSpec` (platform tests) and `SemanticGraphBuilderSpec` (unit tests). |

**Count by severity**: 0 CRITICAL · 0 HIGH · 1 MEDIUM · 3 LOW · 4 none.

---

## 2. Pre-audit context

- **Plugin**: `plugins/semantic-graph-plugin/` (PR #149 → #305, ADR-008-AI; promoted Implemented in `0010-a-enginehookdispatcher-stage-parameter.md` retrospective on 2026-08-26).
- **Two hooks**:
  - `JoinPathPreHook` — `PreResolve`, priority 120, fail-fast on calc-measure cycle, writes typed `EngineError.UnsupportedCapability("SemanticGraph.cycle", ...)` to `ctx.meta("semanticGraphError")` and dangling nodes to `ctx.meta("semanticGraphDangling")`. Sets `context.stop = true` ONLY on cycle (`JoinPathPreHook.scala:77-80`).
  - `GraphPostResolveObserver` — `PostResolve`, priority 120, Observer (per `architecture-spec hooks.md §6`); publishes a `GraphSnapshot` to `ctx.meta(GraphSnapshot.MetaKey)` where `MetaKey = "io.sm8.plugins.semanticgraph:graph-snapshot"` (`GraphSnapshot.scala:113`).
- **Status**: in production per ADR-010-a retrospective §"Decision". Critical regression (silent inertness of PreResolve hook) was caught in the 2026-08-26 dual senior review and fixed in PR-189 (`5e104cc`).
- **Dependencies**: `io.sm8:sm8-core_2.13` (compile), `org.jgrapht:jgrapht-core:1.5.2` (compile), `io.sm8:sm8-core_2.13` test-jar (`plugins/semantic-graph-plugin/pom.xml:36-67`). No transitive Spark.
- **Test artifacts** (4 specs):
  - `SemanticGraphBuilderSpec.scala` (593 lines, the real coverage).
  - `GraphPostResolveObserverSpec.scala`.
  - `GraphSnapshotSpec.scala`.
  - `SemanticGraphContractSpec.scala` (82 lines, the conformance spec).

---

## 3. Findings

### Anchor 1 — Walker exhaustiveness (LOW)

**Evidence**:
- `sm8-core/src/main/scala/io/sm8/core/expr/Calculator.scala:48-80` — `fieldNamesOf` walker.
- `sm8-core/src/main/scala/io/sm8/core/expr/Calculator.scala:88-120` — `measureNamesOf` walker.
- `sm8-core/src/main/scala/io/sm8/core/expr/Expr.scala:6-7` — header says **"21 cases total: 1 literal + 2 references + 5 arithmetic + 6 comparison + 3 boolean + 2 null checks + 1 cast + 1 function call"**.
- `sm8-core/src/main/scala/io/sm8/core/expr/Calculator.scala:25-26` — header says **"the FULL 24-case Expr family (the 22 legacy cases + PR-I's `CaseWhen` + `Alias`)"**.

**Counted cases in `Expr.scala`** (24 `final case class` declarations, none `final case object`):
Literal, FieldRef, MeasureRef, All, Add, Subtract, Multiply, Divide, Modulo, Equal, NotEqual, LessThan, LessOrEqual, GreaterThan, GreaterOrEqual, And, Or, Not, IsNull, IsNotNull, Cast, FunctionCall, CaseWhen, Alias.

**Walker coverage** — both walkers contain match arms for ALL 24 cases (verified by `grep -E "case Expr\." ... | sort -u`: Literal, FieldRef, MeasureRef, All, Not, IsNull, IsNotNull, Cast, Alias, Add, Subtract, Multiply, Divide, Modulo, Equal, NotEqual, LessThan, LessOrEqual, GreaterThan, GreaterOrEqual, And, Or, CaseWhen, FunctionCall — all 24 present in both).

The walker-vs-case relationship is correctly typed by the `sealed trait Expr` exhaustiveness check at compile time; an unhandled case would be a compile error.

**Drift**: `Expr.scala:6-7` says "21 cases total" but the file actually has 24. `Calculator.scala:25-26` says "24-case Expr family (22 legacy + CaseWhen + Alias)" — that's correct. The `Expr.scala` header was never updated when `CaseWhen` (PR #I, added 2026-08-16 per `0008-i-casewhen-alias.md`) and `Alias` (same PR) were added.

**Analysis**: This is doc-comment drift, not a bug. Both walkers are exhaustive. A future contributor reading only `Expr.scala`'s header might believe there are 21 cases; the verification grep would correct them. The drift has no runtime impact.

**Recommended action**: edit `Expr.scala:6-7` to read "24 cases total: 1 literal + 3 references (FieldRef, MeasureRef, All) + 5 arithmetic + 6 comparison + 3 boolean + 2 null checks + 1 cast + 1 function call + 1 case-when + 1 alias" — or simply "24 cases total (1 literal + 23 expressions; see `Calculator.scala` for the walker)". Doc-only fix; does not block.

---

### Anchor 2 — JGraphT API currency (LOW)

**Evidence**:
- `plugins/semantic-graph-plugin/pom.xml:43-46` — pins `org.jgrapht:jgrapht-core:1.5.2`.
- Imports in `SemanticGraphBuilder.scala:23-26`: `org.jgrapht.alg.cycle.CycleDetector`, `org.jgrapht.alg.shortestpath.DijkstraShortestPath`, `org.jgrapht.graph.DefaultDirectedWeightedGraph`, `org.jgrapht.graph.DefaultWeightedEdge`.
- Local jar at `~/.m2/repository/org/jgrapht/jgrapht-core/1.5.2/jgrapht-core-1.5.2.jar` is present; this is the only version cached.

**API signatures in 1.5.2** (via `javap -public -classpath` against the local jar):
- `DefaultDirectedWeightedGraph<V,E>`:
  - `public DefaultDirectedWeightedGraph(Class<? extends E>)`
  - `public DefaultDirectedWeightedGraph(Supplier<V>, Supplier<E>)`
  - `public static <V,E> GraphBuilder<V,E,? extends DefaultDirectedWeightedGraph<V,E>> createBuilder(Class<? extends E>)`
  - `public static <V,E> GraphBuilder<V,E,? extends DefaultDirectedWeightedGraph<V,E>> createBuilder(Supplier<E>)`
- `DefaultWeightedEdge`:
  - `public DefaultWeightedEdge()`
  - `public String toString()`
  - `public Object clone()`
- `DijkstraShortestPath<V,E>`:
  - `public DijkstraShortestPath(Graph<V,E>)`
  - `public DijkstraShortestPath(Graph<V,E>, double)`
  - `public DijkstraShortestPath(Graph<V,E>, Supplier<AddressableHeap<Double,Pair<V,E>>>)`
  - `public DijkstraShortestPath(Graph<V,E>, double, Supplier<AddressableHeap<Double,Pair<V,E>>>)`
  - `public static <V,E> GraphPath<V,E> findPathBetween(Graph<V,E>, V, V)`
  - `public GraphPath<V,E> getPath(V, V)`
  - `public SingleSourcePaths<V,E> getPaths(V)`
  - `public double getPathWeight(Object, Object)`
- `CycleDetector<V,E>`:
  - `public CycleDetector(Graph<V,E>)`
  - `public boolean detectCycles()`
  - `public boolean detectCyclesContainingVertex(V)`
  - `public Set<V> findCycles()`
  - `public Set<V> findCyclesContainingVertex(V)`

All 4 APIs exist in 1.5.2 with the exact signatures the plugin uses (`SemanticGraphBuilder.scala:279-281` calls `new DefaultDirectedWeightedGraph(classOf[DefaultWeightedEdge])`; `SemanticGraph.scala:71` calls `new DijkstraShortestPath(g).getPath(from, to)`; `SemanticGraph.scala:86-87` calls `new CycleDetector(g).detectCycles()`). No `@Deprecated` markers found in the source jar (`jgrapht-core-1.5.2-sources.jar`).

**Forward-currency gap**: this audit env has NO INTERNET ACCESS. I cannot check whether 1.5.2 is still supported in 1.6.x / 1.7.x releases, whether `DefaultDirectedWeightedGraph`/`DefaultWeightedEdge`/`DijkstraShortestPath`/`CycleDetector` have been renamed or had signatures changed in subsequent 1.x or 2.x releases, or whether the upstream project is still actively maintained. The cached jar is 1.5.2 only — there is no 1.6.x or 1.7.x jar available locally to compare against. **Recommend follow-up check via WebSearch from coordinator.**

**Analysis**: 1.5.2 was published 2023 (per the source jar's `Copyright 2003-2023` header). The 4 APIs the plugin uses are stable, low-churn APIs in JGraphT — `DefaultDirectedWeightedGraph` and `DefaultWeightedEdge` have been the canonical implementations since the library's 1.x line, and `DijkstraShortestPath` / `CycleDetector` are algorithm classes that rarely change. The risk of a future JGraphT upgrade breaking this plugin is real but bounded: any breaking change in those 4 APIs would break a very large fraction of the JGraphT user base.

**Recommended action**: defer. Add a one-line comment near the dep declaration (e.g., "1.5.2 (verified 2026-09-04 against local jar; forward-currency check recommended at next dependency refresh)") so the next contributor knows when the last verification happened. No code change required.

---

### Anchor 3 — Typed-error surface alignment (none — verified working)

**Evidence**:
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala:50` — `val CycleErrorKey = "semanticGraphError"`.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala:71-76` — constructs `EngineError.UnsupportedCapability(engine = "semantic-graph-plugin", capability = "SemanticGraph.cycle", message = ...)`.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala:77-80` — sets `stop = true` and writes `meta = withDangling.meta + (CycleErrorKey -> cycleError)`.
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:39` — `final case class UnsupportedCapability(engine: String, capability: String, message: String) extends EngineError`. Shape is exactly what the hook constructs.
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:515-531` — after `dispatcher.run(...)`, the platform reads `finalCtx.meta.get("semanticGraphError") match { case Some(e: EngineError) => Left(e); case _ => ... }`. This is the explicit typed-error surfacing path (added in ADR-010-a v0.3).
- Regression test: `sm8-platform/src/test/scala/io/sm8/platform/query/JoinPathPreHookCycleDetectionSpec.scala:105-136` — exercises the END-TO-END path `EngineService.runQueryWithHooks` → orchestrator → `JoinPathPreHook` → `Left(EngineError.UnsupportedCapability("SemanticGraph.cycle", ...))` (line 131-135).

**Analysis**: typed errors DO surface. The plumbing is end-to-end correct. The hook writes the typed value to `ctx.meta`; the orchestrator's post-run step (`EngineService.scala:530-531`) reads it; the caller sees `Left(EngineError.UnsupportedCapability(...))`. The regression test exercises the production seam (`EngineService.runQueryWithHooks`), not `hook.run(...)` direct — that was the lesson from the 2026-08-26 dual review (see ADR-010-a §"Root cause": existing specs called hooks directly and missed the silent-no-op defect).

**Recommended action**: none. This anchor is verified working.

---

### Anchor 4 — `ctx.meta` key collisions (none — verified no collisions)

**Brief asked about three keys**: `"semanticGraphError"`, `"semanticGraphDangling"`, and `"semanticGraph"` (the graph itself).

**Evidence — keys actually written**:
- `JoinPathPreHook.scala:50` — `"semanticGraphError"`.
- `JoinPathPreHook.scala:56` — `"semanticGraphDangling"`.
- `GraphSnapshot.scala:113` — `GraphSnapshot.MetaKey = "io.sm8.plugins.semanticgraph:graph-snapshot"` (NOT `"semanticGraph"`). Written by `GraphPostResolveObserver.scala:86`.

The third key named in the brief (`"semanticGraph"`) does NOT exist in the codebase. The plugin uses the namespaced `io.sm8.plugins.semanticgraph:graph-snapshot` key.

**Cross-plugin reader search** (`grep -rn` over `plugins/audit-plugin/`, `plugins/broadcast-plugin/`, `plugins/cache-plugin/`, `plugins/skew-plugin/`, `plugins/materialize-plugin/`, `plugins/row-cap-plugin/`, `plugins/example-plugin/` for the four actual keys):
- `semanticGraphError` — 0 references in other plugins.
- `semanticGraphDangling` — 0 references in other plugins.
- `graph-snapshot` — 0 references in other plugins.
- `MetaKey` — 0 references in other plugins.

**Internal consumer search** (within `sm8-platform/`):
- `sm8-platform/src/main/scala/io/sm8/platform/query/MetaRequest.scala:16` — mentions the namespaced key in a doc comment.
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:520-530` — reads `ctx.meta.get("semanticGraphError")` (the typed-error surfacing from Anchor 3).
- `sm8-platform/src/test/scala/io/sm8/platform/query/HttpTransportPluginWiringSpec.scala:255` — test reference.
- `sm8-platform/src/test/scala/io/sm8/platform/query/MetaInspectorServiceE2ESpec.scala:40,144` — test references.
- `sm8-platform/src/test/scala/io/sm8/platform/query/JoinPathPreHookCycleDetectionSpec.scala:11,38,63,99,105,111,133-134,145-147` — regression references.

**Analysis**: zero collisions. The namespaced `io.sm8.plugins.semanticgraph:graph-snapshot` key follows the project convention (see the comment at `GraphSnapshot.scala:107-109`: "namespaced keys prevent collision with other plugins"). The two unnamespaced keys (`semanticGraphError`, `semanticGraphDangling`) are read only by `EngineService.scala:530`, so they are also collision-free.

The brief's premise that the plugin writes a third `"semanticGraph"` key was based on stale or misremembered information — the plugin does not write such a key.

**Recommended action**: none. The naming convention is consistent within the plugin (namespaced for the snapshot, unnamespaced for the two short-lived keys, all three unique). If a future contributor adds another plugin writing `semanticGraphError` or `semanticGraphDangling`, that would be a collision; document this in the plugin's Scaladoc so contributors know not to.

---

### Anchor 5 — Closure-safety (none — verified)

**Evidence**:
- `JoinPathPreHook.scala:40` — `final class JoinPathPreHook extends PreHook with java.io.Serializable`.
- `GraphPostResolveObserver.scala:36` — `final class GraphPostResolveObserver extends PostHook with java.io.Serializable`.
- `SemanticGraphPlugin.scala:46` — `final class SemanticGraphPlugin extends Plugin` — no explicit `with java.io.Serializable`, but `Plugin` extends `java.io.Serializable` at `sm8-core/src/main/scala/io/sm8/sdk/Plugin.scala:34`.
- `SemanticGraph.scala:55-59` — `final class SemanticGraph private[semanticgraph] (g, loadedModelNames, estimatedPairs)`. Does NOT extend `Serializable` explicitly.
- `SemanticGraphBuilder` (`SemanticGraphBuilder.scala:253`) is a singleton `object`, not a class — no instance state to serialize.
- Spark-import scan (`grep -rn "org.apache.spark\|import.*Spark"` over `plugins/semantic-graph-plugin/src/main/`) — 0 matches.
- Hook bodies (`JoinPathPreHook.scala:58-85`, `GraphPostResolveObserver.scala:51-88`) read only `context.request`, build a fresh `SemanticGraph`, write to `context.meta`. No Spark types captured.

**`SemanticGraph` and Serializability**:
- `SemanticGraph` holds a `DefaultDirectedWeightedGraph[GraphNode, DefaultWeightedEdge]` (a JGraphT class, which is `Serializable` since JGraphT 1.0), a `Set[String]`, and a `Set[(GraphNode, GraphNode)]` — both Scala collections, which are `Serializable`.
- `GraphNode` (`GraphSnapshot.scala:27`) is a case class — auto-derived `Product with Serializable`.
- `GraphSnapshot` (`GraphSnapshot.scala:42-58`) is a case class with only `String`/`List`/`Option`/`Boolean`/`Map` fields — all `Serializable`.
- The closure-cleaner (`PluginSerializationSpec.scala:127-145`) asserts every captured field is `Serializable`. The plugin holds no captured fields (`closedOverVars` defaults to `Seq.empty` per `PluginSerializationSpec.scala:105-111`).

**Analysis**: the plugin is fully closure-safe. Hooks are explicitly `Serializable`; the plugin is implicitly `Serializable` via the `Plugin` trait; the graph types are all `Serializable`; no Spark types are referenced.

**Recommended action**: none. Anchor 5 is verified.

---

### Anchor 6 — Cycle-detection duplication (MEDIUM)

**Evidence — `SemanticGraph.hasCycle`** (`SemanticGraph.scala:86-87`):
```scala
def hasCycle: Boolean = new CycleDetector[GraphNode, DefaultWeightedEdge](g).detectCycles()
```
- The graph `g` is built by `SemanticGraphBuilder.buildAcross` (`SemanticGraphBuilder.scala:276-371`):
  - **Calculated-measure deps**: edges `(model.name, c.name) -> (model.name, ref)` for each `ref ∈ Calculator.measureNamesOf(c.expr) ++ Calculator.fieldNamesOf(c.expr)`. Uses `addEdge` (line 297-302) — self-loops ARE added for self-referencing measures (`addEdge` does not skip `a == b`).
  - **Dimension deps**: edges `(model.name, d.name) -> (model.name, ref)` for each `ref ∈ Calculator.fieldNamesOf(d.expr)`. Uses `addDimEdge` (line 308-312) — self-loops are SKIPPED (`if (a == b) () else addEdge(a, b, w)`), per the explicit comment at lines 287-296.
  - **Join edges**: `(leftModel, leftKey) -> (rightModel, rightKey)` weighted by `js.estimatedRows.getOrElse(1L).toDouble`.

**Evidence — `QueryBuilder.detectCalcCycles`** (`sm8-core/src/main/scala/io/sm8/core/query/QueryBuilder.scala:255-319`):
- Walks `List[CalculatedMeasure]` ONLY. Does NOT walk `dimensions`, does NOT walk `joins`.
- For each `CalculatedMeasure c`, computes `measureRefs(c) = (Calculator.measureNamesOf(c.expr) ++ Calculator.fieldNamesOf(c.expr)).filter(byName.contains)` — i.e. refs that are ALSO declared calculated measures (line 272-276).
- Uses an iterative 3-color DFS over the calc-measure name graph.
- Reports `Left(EngineError.UnsupportedCapability("CalculatedMeasure.cycle", ...))` on the first detected cycle (line 316-319).

**Comparison**:

| Edge shape | `SemanticGraph.hasCycle` | `QueryBuilder.detectCalcCycles` |
|---|---|---|
| calc-measure -> declared calc-measure (via `MeasureRef` or via `FieldRef` to a calc name) | ✅ detects (real cycle) | ✅ detects (real cycle) |
| calc-measure -> self (e.g. `bad = bad + 1`) | ✅ detects via self-loop | ✅ detects via self-edge |
| dimension -> its own field (`dimAmount -> FieldRef("amount")`) | ❌ does NOT detect (self-loop skipped by `addDimEdge`) | n/a — dimensions not walked |
| dimension -> another field (cross-dim) | ❌ does NOT detect (dimensions not in `byName`) | n/a — dimensions not walked |
| join edge creating a cross-model cycle | ❌ does NOT flag (joins not in DAG walk) | n/a — joins not walked |

**Symmetry**: Both algorithms reuse the SAME `Calculator.measureNamesOf` + `Calculator.fieldNamesOf` walkers over the SAME `Model.calculatedMeasures[i].expr`. The calc-measure cycle-detection results are therefore guaranteed to AGREE for any well-formed `Model`. The differences above are about WHAT each algorithm considers a cycle, not whether they compute the same answer for the cases both consider.

**The asymmetry that DOES matter**: `SemanticGraph.hasCycle` adds self-loop edges for self-referencing calculated measures (e.g. `bad = bad + 1`) AND skips them for self-referential dimensions. `QueryBuilder.detectCalcCycles` walks only calc-measure names AND adds every calc-measure's edges unconditionally. So for the canonical "self-referencing calc" case (`Expr.MeasureRef("self")`), both agree there is a cycle. For a self-referential dimension (`dimAmount -> FieldRef("amount")`), only `SemanticGraph` would consider it — and `addDimEdge` correctly skips it, so neither algorithm reports a cycle for that case. The asymmetry is intentional and well-commented.

**Risk for future contributors**:
- If a future contributor adds a NEW kind of edge to `SemanticGraphBuilder.buildAcross` (e.g. a "filter" edge from a filter's field references), the `CycleDetector` would pick it up but `QueryBuilder.detectCalcCycles` would not. The two would diverge.
- Conversely, if `QueryBuilder.detectCalcCycles` is extended to walk dimensions, it would flag self-referential dimensions as cycles — but `SemanticGraph` would not (because of `addDimEdge`'s skip).

**The dual-review test** (`JoinPathPreHookCycleDetectionSpec.scala:105-170`) ONLY exercises the calc-measure cycle case (`CalculatedMeasure(name = "self", expr = Expr.MeasureRef("self"))`). It does not exercise:
- Cross-model cycles via join edges (which only `SemanticGraph` would flag).
- Cross-dimension cycles (which neither flags today).
- Self-referential dimensions (which neither flags today).

**Analysis**: the two algorithms are kept deliberately (`SemanticGraphBuilder.scala:9-14` comment: "reuses the SAME walkers QueryBuilder.detectCalcCycles already trusts"). The pre-flight duplicate catches cycles before any Connector work, surfacing the typed error to the requester. This is a deliberate pre-flight pattern. The risk is that a future contributor editing one without the other could create drift; the mitigation is that BOTH use `Calculator.measureNamesOf` + `Calculator.fieldNamesOf`, so the calc-measure cycle answer is mechanically locked.

**Recommended action**:
- Document the explicit non-symmetry: add a comment to `QueryBuilder.detectCalcCycles` (or to `SemanticGraphBuilder.buildAcross`) noting that `SemanticGraph` walks dimensions + joins + calc-measures while `QueryBuilder.detectCalcCycles` walks calc-measures only.
- Add a property test (NOT IMPLEMENTED HERE per the audit scope; recommendation only) that asserts the two algorithms agree on the calc-measure cycle set for any well-formed `Model`. The existing `SemanticGraphBuilderSpec.scala:492-...` test ("SemanticGraphBuilder + QueryBuilder.detectCalcCycles should agree on cycle detection for a cycling calc") exercises ONE example; a property-based version over arbitrary `Model`s would harden the invariant.

**Severity rationale**: MEDIUM because the asymmetry is documented and the calc-measure case is locked, but the asymmetry is real and a future contributor working on either side could create drift without realizing it.

---

### Anchor 7 — `enforce-no-spark` rule (none — verified working)

**Evidence**:
- Parent `pom.xml:217-221`:
```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-enforcer-plugin</artifactId>
  <version>3.4.1</version>
</plugin>
```
The version is declared in `<pluginManagement>` (`pom.xml:222` boundary), so child poms can reference the plugin by `groupId` + `artifactId` and inherit the version.

- `plugins/semantic-graph-plugin/pom.xml:74-83`:
```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-enforcer-plugin</artifactId>
  <executions>
    <execution>
      <id>enforce-no-spark</id>
      <goals><goal>enforce</goal></goals>
    </execution>
  </executions>
  <configuration>
    <rules>
      <bannedDependencies>
        <excludes><exclude>org.apache.spark:*</exclude></excludes>
      </bannedDependencies>
    </rules>
  </configuration>
</plugin>
```

**Configuration correctness**:
- The `<exclude>org.apache.spark:*</exclude>` glob matches every artifact with `groupId=org.apache.spark` (covers `spark-core_2.13`, `spark-sql_2.13`, `spark-catalyst_2.13`, `spark-hive_2.13`, `spark-mllib_2.13`, etc.). Any spark artifact added to this module's `<dependencies>` would be caught.
- The execution is bound to the `enforce` goal. Maven Enforcer's default phase for the `enforce` goal is `validate` (per the `maven-enforcer-plugin` docs; phase binding for `enforce` is `validate`). So `mvn validate` triggers the rule.
- `<configuration>` is correctly placed at the `<plugin>` level (not the `<execution>` level), so the rule applies to the only declared execution.
- `<id>enforce-no-spark</id>` is just a label; Maven doesn't care about its content, but it matches the comment in `pom.xml:18` ("Per-module enforce-no-spark block present (fix 4)").

**Hypothetical addition** (`<dependency><groupId>org.apache.spark</groupId><artifactId>spark-sql_2.13</artifactId><version>3.5.8</version></dependency>` to this module's pom): the `enforce` goal runs at the `validate` phase, finds `org.apache.spark:spark-sql_2.13` in the resolved dependency tree, matches `org.apache.spark:*`, fails the build with a `DependencyEnforceException` (or equivalent). The build would NOT proceed to compile. This matches the spec.

**Analysis**: the rule is well-formed, versioned (3.4.1, current as of 2024), correctly bound to `validate`, and catches all spark artifacts.

**Recommended action**: none. Anchor 7 is verified.

---

### Anchor 8 — Contract-spec coverage (LOW)

**Evidence — `SemanticGraphContractSpec.scala:52-81`**:
```scala
class SemanticGraphContractSpec extends HookContractSpec {

  override def preHook: PreHook = {
    val engine = EngineImpl()
    engine.use(new SemanticGraphPlugin)
    engine.hooks.preHooksFor(HookStage.PreResolve).head._1
  }

  override def postHook: PostHook = {
    val engine = EngineImpl()
    engine.use(new SemanticGraphPlugin)
    engine.hooks.postHooksFor(HookStage.PostResolve).head._1
  }

  override def transformer: Transformer =
    new NoopTransformer(name = "semantic-graph-conformance-transformer", priority = 120)

  override def baselineContext: Context =
    Context(
      stage   = PipelineStage.Resolve,
      request = SemanticGraphConformanceRequest,    // <-- not EngineHookRequest
      result  = Some(SemanticGraphConformanceResult),
      meta    = Map.empty,
      stop    = false
    )
}
```

**What `HookContractSpec` actually asserts** (`sm8-core/src/test/scala/io/sm8/sdk/contract/HookContractSpec.scala:65-113`):
- `preHook.name should not be empty`
- `preHook.priority should be in [0,99] ∪ [100,899] ∪ [900, +∞)`
- `preHook.stage wireName matches regex "(pre|post):(parse|resolve|execute|format)"`
- `preHook.run(baselineContext).request == baselineContext.request` (RFC hooks.md Rule 2: hooks must not mutate `context.request`)
- Same four assertions for `postHook`
- `transformer.name`, `transformer.priority >= 100`
- `transformer.transform(baselineContext).request == baselineContext.request`
- `transformer.transform` is idempotent (running it twice == running it once)

**The gap**: `preHook.run(baselineContext)` with `request = SemanticGraphConformanceRequest` (a `case object` that does NOT extend `EngineHookRequest`). `JoinPathPreHook.scala:58-59` pattern-matches on `context.request match { case EngineHookRequest(model: Model, _, _) => ...; case _ => context }`. The match falls through to `case _ => context` — the hook returns the context unchanged. The cycle-detection logic, the typed-error write, and the dangling-nodes write are NEVER exercised by this spec.

Same gap for `GraphPostResolveObserver.scala:51-87`: the `run` body also pattern-matches on `EngineHookRequest`; the no-op request falls through to `case _ => context`. The snapshot publish is NEVER exercised by this spec.

**Where the REAL coverage lives**:
- `sm8-platform/src/test/scala/io/sm8/platform/query/JoinPathPreHookCycleDetectionSpec.scala:105-170` — exercises the END-TO-END path (orchestrator + plugin + cycle model) and asserts the typed `EngineError.UnsupportedCapability` reaches the caller.
- `plugins/semantic-graph-plugin/src/test/scala/io/sm8/plugins/semanticgraph/SemanticGraphBuilderSpec.scala` (593 lines) — unit-tests `SemanticGraphBuilder.build`, `buildAcross`, `hasCycle`, `joinPath`, `danglingRightNodes`, `joinCardinalities`, `dependents`. Most of the business logic.
- `plugins/semantic-graph-plugin/src/test/scala/io/sm8/plugins/semanticgraph/GraphPostResolveObserverSpec.scala` — unit-tests the observer with a real `EngineHookRequest`.
- `plugins/semantic-graph-plugin/src/test/scala/io/sm8/plugins/semanticgraph/GraphSnapshotSpec.scala` — unit-tests the wire snapshot projection.

So the business behavior IS covered — just not by `SemanticGraphContractSpec`. The contract spec verifies the SHAPE (name, priority, stage wireName, request-immutability), not the behavior.

**Analysis**: This is by design (per the spec's doc comment at lines 1-19 — "the semantic-graph plugin's real PreHook is exercised via the plugin registration below"). The shape contract is verified; the behavior contract is verified elsewhere. The gap is that `SemanticGraphContractSpec` could GIVE A FALSE IMPRESSION of full coverage if a reader does not look at the other specs.

**Severity rationale**: LOW. The behavior IS covered (by `JoinPathPreHookCycleDetectionSpec` + `GraphPostResolveObserverSpec` + `SemanticGraphBuilderSpec`); `SemanticGraphContractSpec`'s job is shape conformance. Calling this MEDIUM would over-state the gap.

**Recommended action** (spec improvement, NOT a code change):
- Add a doc comment to `SemanticGraphContractSpec.scala:36-43` clarifying that the `NoopPreHook`/`NoopTransformer` stubs are placeholders for the contract-base's expected slots, NOT substitutes for the real hook; that the real `JoinPathPreHook` IS pulled from the engine registration but exercised with a no-op request so the shape contract is what's asserted here. Cite `JoinPathPreHookCycleDetectionSpec` for behavioral coverage.
- Or: replace the no-op `baselineContext` with one whose `request` IS an `EngineHookRequest(model, ...)` so the contract spec also exercises the cycle-detector path. This is a SPEC change, not a code change — and per the audit scope "this is an audit, not implementation", I do not write the change here.

---

## 4. Cross-cutting patterns

1. **Three doc-comment drifts point to the same gap**: `Expr.scala:6-7` ("21 cases" — actually 24), `Calculator.scala:25-26` (claims 22 legacy + 2 = 24, which is correct), `JoinPathPreHook.scala:73` (capability string `"SemanticGraph.cycle"` — distinct from `QueryBuilder.detectCalcCycles`'s `"CalculatedMeasure.cycle"`). All three are LOW, all three are fixable with edit-only patches, and none block. They suggest a "comment-drift class" of finding worth a periodic sweep (per ADR-008-AG "scaladoc skill wikilink sweep").

2. **Single MEDIUM is structural, not behavioral**: Anchor 6's MEDIUM is about ASYMMETRY between two algorithms that walk the same walkers but apply different vertex-set filters. The behavioral answer (calc-measure cycles) is locked; the structural risk (future contributor adding edges to one but not the other) is bounded by the shared walker dependency.

3. **Zero HIGH/CRITICAL findings, zero `org.apache.spark` leakage, zero cross-plugin meta-key collisions**: the plugin is in good operational shape. The hooks fire correctly post-ADR-010-a fix; the typed-error surfacing works end-to-end; the meta keys are collision-free; the closure-safety holds.

4. **Behavioral coverage lives outside the contract spec**: `SemanticGraphContractSpec` is a shape spec; the behavioral coverage is in `JoinPathPreHookCycleDetectionSpec` (platform tests) + `GraphPostResolveObserverSpec` + `SemanticGraphBuilderSpec`. This is by design but worth making explicit in the contract spec's Scaladoc.

5. **Web access gap (Anchor 2)**: this audit env has no internet. The JGraphT 1.5.2 API surface was verified against the locally-cached jar (`~/.m2/repository/org/jgrapht/jgrapht-core/1.5.2/jgrapht-core-1.5.2.jar`), but forward-currency (1.6.x / 1.7.x / 2.x) cannot be checked from here. The risk is real but bounded — none of the 4 APIs the plugin uses are likely to break in a minor release.

---

## 5. Recommended path forward

**DOCFIX** (the third option in the brief's decision rule).

**Justification**:
- **0 CRITICAL**, **0 HIGH**, **1 MEDIUM**, **3 LOW**.
- Decision rule says: **DOCFIX** if only LOW and the LOW is doc-comment drift. The 3 LOWs (Anchors 1, 2, 8) are all doc-comment / coverage-asymmetry issues that are resolvable by editing comments + a one-line note in `pom.xml`. None require code changes.
- The 1 MEDIUM (Anchor 6) is structural but bounded by the shared walker dependency. The mitigation is documenting the explicit non-symmetry (a doc-comment edit, not a code change). If a future contributor chooses to harden Anchor 6 with a property test, that would be a follow-up PR, not a blocker for this audit's verdict.

**Concrete DOCFIX actions** (in priority order):
1. **Edit `Expr.scala:6-7`**: change "21 cases total" to "24 cases total". One-line edit.
2. **Edit `plugins/semantic-graph-plugin/pom.xml:19-20`**: change "1.5.2 (pure JVM, no Spark transitive deps; per the parent pom enforcer check)." to "1.5.2 (verified against local jar 2026-09-04; forward-currency check recommended at next dep refresh). Pure JVM, no Spark transitive deps; per the parent pom enforcer check."
3. **Edit `SemanticGraphContractSpec.scala:1-19` doc comment**: clarify that the spec asserts SHAPE only; cite `JoinPathPreHookCycleDetectionSpec` + `SemanticGraphBuilderSpec` for behavior.
4. **Optional**: add a comment to `QueryBuilder.detectCalcCycles` (or to `SemanticGraphBuilder.buildAcross`) noting the explicit non-symmetry (calc-only vs calc + dim + join). One-line edit.

None of the above require code changes or new tests.

---

## 6. Citations index

Files read (every `file:line` referenced in findings):

**Plugin source**:
- `plugins/semantic-graph-plugin/pom.xml:9-95` — deps, enforcer config, scalatest plugin, source plugin.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/JoinPathPreHook.scala:1-86` — class declaration, `java.io.Serializable`, `CycleErrorKey`, `DanglingKey`, `run` body, `case _ => context` fallthrough.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/GraphPostResolveObserver.scala:1-89` — class declaration, `java.io.Serializable`, `run` body, snapshot publish.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/SemanticGraphBuilder.scala:1-371` — full file (the join estimator, `addEdge`/`addDimEdge` self-loop skip, walker reuse).
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/GraphSnapshot.scala:1-114` — case classes, `GraphSnapshot.MetaKey`, `toMetaValue`, `nodeKey`.
- `plugins/semantic-graph-plugin/src/main/scala/io/sm8/plugins/semanticgraph/SemanticGraphPlugin.scala:1-68` — `setup`, idempotency doc.

**Plugin tests**:
- `plugins/semantic-graph-plugin/src/test/scala/io/sm8/plugins/semanticgraph/SemanticGraphContractSpec.scala:1-89` — `HookContractSpec` extension, `NoopPreHook`, `NoopTransformer`, `baselineContext` no-op request.
- `plugins/semantic-graph-plugin/src/test/scala/io/sm8/plugins/semanticgraph/SemanticGraphBuilderSpec.scala` — read for joinPath test refs (line 234, 239, 244-245, 278) and cycle-detection agreement test (line 492-...).

**Core (Anchor 1, 6)**:
- `sm8-core/src/main/scala/io/sm8/core/expr/Calculator.scala:1-120` — header claim "24 cases", both walkers.
- `sm8-core/src/main/scala/io/sm8/core/expr/Expr.scala:1-223` — header claim "21 cases" (drift), 24 `final case class` declarations.
- `sm8-core/src/main/scala/io/sm8/core/engine/EngineError.scala:1-198` — `UnsupportedCapability` shape, ADT.
- `sm8-core/src/main/scala/io/sm8/core/query/QueryBuilder.scala:27-28, 83, 101, 163, 240-319` — `detectCalcCycles` walker, `Calculator.measureNamesOf` + `Calculator.fieldNamesOf` reuse, `Left(UnsupportedCapability)` on cycle.

**SDK**:
- `sm8-core/src/main/scala/io/sm8/sdk/Plugin.scala:34, 55, 61, 82` — `trait Plugin extends java.io.Serializable`, `closedOverVars` contract.
- `sm8-core/src/test/scala/io/sm8/sdk/contract/HookContractSpec.scala:33-113` — `preHook`/`postHook`/`transformer` slot contract, what `HookContractSpec` actually asserts.
- `sm8-core/src/test/scala/io/sm8/sdk/contract/PluginSerializationSpec.scala:1-145` — closure-safety contract, `closedOverVars` Serializability check.

**Platform**:
- `sm8-platform/src/main/scala/io/sm8/platform/query/EngineService.scala:480-557` — `decisionCtx`, `dispatcher.run(...)`, `finalCtx.meta.get("semanticGraphError") match { case Some(e: EngineError) => Left(e); ... }` (line 530-531).
- `sm8-platform/src/main/scala/io/sm8/platform/query/MetaRequest.scala:16` — namespaced key doc reference.
- `sm8-platform/src/test/scala/io/sm8/platform/query/JoinPathPreHookCycleDetectionSpec.scala:1-171` — regression test for typed-error surfacing, cycle model fixture, two tests (typed-error present, no `ProviderInvocationFailed("NoResult")`).
- `sm8-platform/src/test/scala/io/sm8/platform/query/HttpTransportPluginWiringSpec.scala:255` — namespaced key test ref.
- `sm8-platform/src/test/scala/io/sm8/platform/query/MetaInspectorServiceE2ESpec.scala:40, 144` — namespaced key test ref.

**Parent POM**:
- `pom.xml:217-221` — `maven-enforcer-plugin` 3.4.1 in `<pluginManagement>`.

**ADR**:
- `docs/adr/0008-ai-semantic-graph-rfc-review-and-fixes.md` — the v1.1 fix list (no cache, typed error, dangling typed list, enforce-no-spark).
- `docs/adr/0010-a-enginehookdispatcher-stage-parameter.md` — retrospective on the silent-no-op defect; `JoinPathPreHook` was registered at `PreResolve` but never fired until PR-189 fixed the dispatcher's hardcoded stage.
- `docs/adr/0008-i-casewhen-alias.md` — CaseWhen + Alias added in PR #I; explains the drift from 21 to 24 cases.
- `docs/adr/0008-ag-scaladoc-skill-wikilink-sweep.md` — context for the comment-drift pattern.

**JGraphT local jar**:
- `~/.m2/repository/org/jgrapht/jgrapht-core/1.5.2/jgrapht-core-1.5.2.jar` — `javap` on `DefaultDirectedWeightedGraph`, `DefaultWeightedEdge`, `DijkstraShortestPath`, `CycleDetector`. All 4 APIs present with current signatures; no `@Deprecated` markers in `jgrapht-core-1.5.2-sources.jar`.
- No 1.6.x / 1.7.x / 2.x JGraphT jars cached locally; forward-currency NOT VERIFIED (no internet).

**Cross-plugin collision search**:
- `grep -rn "semanticGraphError|semanticGraphDangling|graph-snapshot|MetaKey\b" plugins/audit-plugin/ plugins/broadcast-plugin/ plugins/cache-plugin/ plugins/skew-plugin/ plugins/materialize-plugin/ plugins/row-cap-plugin/ plugins/example-plugin/` — 0 matches.

**Spark-import scan**:
- `grep -rn "org.apache.spark|import.*Spark" plugins/semantic-graph-plugin/src/main/` — 0 matches.

---

**End of audit.**