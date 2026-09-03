# AGENTS.md — sm8 project

Repository-level guidelines for any AI agent working on the sm8 project.

## Project context

- **Repo**: `EchoEnv/sm8` (semantic-layer engine in Scala 2.13 + Spark)
- **Host**: Hetzner vServer, aarch64, 7.5 GiB RAM
- **Module layout** (RFC §3 layer discipline):
  - `core` = `sm8-core` (engine interface, contracts, no I/O, no Spark)
  - `adapter` = `sm8-server`, `sm8-cli`, `sm8-platform` (wires core to runtimes)
  - `plugin` = `sm8-plugins/*` (cache, observers, custom engines)
  - `hook` = `sm8-platform/hooks/` (ADR-010-a)
  - `connector` = `connectors/` (external integrations)

## Layer discipline (RULE#1)

- **Never** import a plugin's IMPLEMENTATION class from an adapter. Only the
  SDK interfaces (in `core`) may be referenced.
- **Never** use `new io.sm8.core.EngineImpl()` from adapters. Use
  `io.sm8.core.EngineFactory.create(plugins)` to construct a wired
  Engine, and `io.sm8.core.PluginDiscovery.discoverFromConfig()` to
  load the plugin set. Each is the sole outward seam from adapters
  for its concern (construction vs discovery) per RFC §3.
- **Hooks fire only via** `HookRunnerOrchestration` (sm8-platform), not direct
  calls from adapters.
- **No transitive plugin-impl dep** in adapter `pom.xml` files. Adapter
  `pom.xml`s should reference only `sm8-core` and `sm8-platform`.

## Working conventions (RULE#5 dual-review)

- **Always use a working branch** — never PR directly to main.
- **Dual-review required before merge**: spawn two subagents, both
  `auto/best-reasoning` (or `auto/best-reasoning` for design/investigation/
  review/verification per swarm default).
- **Minimal-scope briefs**: ~7k token briefs survive; ~1.2M token briefs die
  on the model's endpoint.
- **Read-only contract for reviewers**: `read`, `agentgrep`,
  `mcp__codegraph__codegraph_explore`, `mcp__metals__*_hover/_definition/_references`.
  Forbidden: `write`, `edit`, `apply_patch`, `multiedit`, bash with side
  effects, metals `_compile_file`/`_run_tests`/`_scalafix`/`_scalafmt`.

## RFC / ADR / skills (RULE#2)

- **Read RFC architecture docs first** (`docs/`, `docs/adr/`). Name the layer
  (core / adapter / plugin / hook / connector) on every code reference.
- **9 skills**: karpathy-guidelines, debug-mantra, scala-data-driven-refactor,
  scala-jvm-safety, scala-error-handling, scala-impact-analysis,
  scala-perf-testing, scala-spark-batch-bugs, scala-jar-packaging.
- **Scaladoc/comments** follow `scala2-scaladoc`.
- **Use `/debug-mantra`** when debugging.
- **Use `codegraph` MCP + `metals` MCP** aggressively for impact analysis.

## Test conventions

- Tests are ScalaTest `AnyFlatSpec with Matchers`.
- File naming: `<Subject>Spec.scala` next to `<Subject>.scala`.
- Run targeted: `mvn test -pl sm8-<module> -Dtest=<SpecName>`.
- The `-Dtest=` filter doesn't isolate in this build (full suite runs) — trust
  the count of "All tests passed" line in the output.

## Output format (anti-noise discipline)

- No enumerated file lists unless referenced in findings.
- "none" for empty sections.
- Layer label on every code reference (`core | adapter | plugin | hook | connector`).
- Severity tags: CRITICAL (block ship) | HIGH (PR required) | MEDIUM
  (cosmetic/PR-nice) | LOW (note).
- ADR/RFC citations on findings.

## User preferences

- **No mermaid diagrams** — use plain text with box-drawing (`│`, `▼`, `┌─┐`).
- **Signal proactively** — silence-detection is a high-priority requirement
  (see jcode memory facts).
- **No ADR-011 series** for audit findings unless a real architectural
  decision emerged; engineering fixes stay as PRs.
- **DM `tldr=`** for messages > 240 chars.
- **Don't fabricate** — zero references = say so, ask for scope rather than
  guess.
- **Wants honest disclosure** — prefer "auto-notify is unreliable for our
  shape" over "auto-notify is broken".
- **Hook paths use absolute paths** in `~/.jcode/config.toml` (`~/` is not
  expanded by direct-execute hooks).

## jcode setup (current as of 2026-08-28)

- Ambient mode: enabled, `proactive_work=false` (per-project garden-only),
  `pause_on_active_session=true`, intervals `min=5, max=15`.
- Hooks configured: `turn_start` (banner pump), `post_tool` (review queue).
  Both real, both working in jcode v0.81.1.
- **DO NOT manually `schedule.create` for ambient wake** — swarm
  auto-forwarded completion reports + soft-interrupt queue handle it.
- Resource sentry script at `~/.jcode/scripts/resource-sentry.sh` is the
  memory-pressure backstop.

## Common gotchas

- **`sm8` swarm CLI doesn't exist** on shell PATH. The `swarm` MCP tool is
  inside the agent process only. Bash scripts can't poll swarm status.
- **Adapter `sm8-server/pom.xml`** must NOT pull `cache-plugin_2.13` or any
  other plugin impl (this was the H1 regression in PR-191).
- **`EngineImpl.discoverFromConfig()`** was renamed via `PluginDiscovery`
  factory in PR-191. Doc references to the old name are stale (audit found
  one such ref in `docs/review/graph-display-design-review.md:45`).
- **PluginDiscoverySpec** in `sm8-core/src/test/` exists as of PR-193
  (2026-08-28) — do not delete.

## Recent shipped PRs (2026-08-27 to 2026-08-28)

- #191 — typed-realize + PluginDiscovery factory (audit C1+C2+H4)
- #192 — drop cache-plugin dep, use `ResultCache.NoOp` (H1 layer-leak fix)
- #193 — LOW cosmetic cleanup (MainSpec comments + PluginDiscoverySpec)
- All merged; `main` HEAD = `94f28e3` (see `git log`).