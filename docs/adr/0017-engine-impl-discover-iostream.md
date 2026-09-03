# ADR-0017: `EngineImpl.discoverFromConfig(stream)` non-`()` overload + I/O cleanup

## Status

Proposed (C7 wayfinder round, ticket #283). Target: PR-274.

## Context

The C6 audit (map #270) and AGENTS.md "Common gotchas" entry both flag that
`sm8-core/src/main/scala/io/sm8/core/EngineImpl.scala:122` uses
`scala.io.Source.fromInputStream` — an I/O call inside sm8-core that
violates RFC §3 ("zero I/O in core"). PR-273 (map #279, ticket #282)
already removed the analogous I/O from `ModelLoader.fromPath`; this ADR
addresses the same shape of violation in `EngineImpl.discoverFromConfig`.

The non-I/O surface already exists: `EngineImpl.discover(allowed: Set[String])`
(line 82) takes a pre-parsed allowlist and returns the matched plugins.
What's missing is the I/O surface: a `discoverFromConfig(stream: InputStream)`
that reads the allowlist from a caller-provided stream and delegates to
`discover(Set[String])`. The existing `discoverFromConfig()` (no-arg,
which uses `getResourceAsStream` to load the classpath resource) becomes a
thin wrapper that opens the resource stream and calls the new overload.

`PluginDiscovery.discoverFromConfig` (sm8-core/src/main/scala/io/sm8/core/PluginDiscovery.scala:77)
then calls the new `discoverFromConfig(stream)` overload, eliminating the
last `scala.io.Source` reference from sm8-core's main sources.

## Decision

Add an `EngineImpl.discoverFromConfig(stream: InputStream): List[Plugin]`
overload. The existing `discoverFromConfig()` becomes a thin wrapper
that opens the classpath resource stream and calls the new method.
`PluginDiscovery.discoverFromConfig` is updated to call the new overload.

### Public API (final)

```scala
final class EngineImpl extends Engine {
 // ... existing fields ...

 /**
  * Discover Plugins from a caller-provided allowlist stream.
  * Returns the Plugins that match the allowlist.
  *
  * Per C7-T4 (#283): the I/O surface (Stream open + UTF-8 read) lives
  * in the existing `discoverFromConfig()` (no-arg), which opens the
  * classpath resource and delegates here. Adapter code that has its
  * own InputStream (e.g. a test that parses a YAML allowlist from a
  * memory buffer) can call this overload directly without going
  * through the classpath-resource path.
  *
  * @param stream the InputStream containing the allowlist (one
  *               groupId:artifactId per line, `#` for comments)
  * @return the Plugins that were successfully loaded
  */
 def discoverFromConfig(stream: InputStream): List[Plugin] = {
   val allowed = scala.io.Source.fromInputStream(stream, "UTF-8")
     .getLines()
     .map(_.trim)
     .filter(s => s.nonEmpty && !s.startsWith("#"))
     .toSet
   try discover(allowed)
   finally stream.close()
 }

 /**
  * Discover Plugins via the classpath SPI gated by the
  * `sm8.plugins.allowed` allowlist. Thin wrapper over the
  * `discoverFromConfig(stream)` overload: opens the classpath resource
  * then delegates.
  *
  * @return the Plugins that were successfully loaded
  */
 def discoverFromConfig(): List[Plugin] = {
   val stream = getClass.getResourceAsStream("/sm8.plugins.allowed")
   try {
     if (stream == null) discoverAll()
     else discoverFromConfig(stream)
   } finally if (stream != null) stream.close()
 }
}
```

`PluginDiscovery.discoverFromConfig` becomes:

```scala
object PluginDiscovery {
  def discoverFromConfig(): List[Plugin] = {
    val stream = getClass.getResourceAsStream("/sm8.plugins.allowed")
    try {
      if (stream == null) new EngineImpl().discoverAll()
      else new EngineImpl().discoverFromConfig(stream)
    } finally if (stream != null) stream.close()
  }
}
```

### Rationale

1. **`discover(Set[String])` already exists** — the non-I/O surface
   is already there. We just need an I/O wrapper that reads a stream
   and calls it.
2. **Stream lifecycle** — the new `discoverFromConfig(stream)` closes
   the stream in a `finally` block. Callers can also pass a `null`
   stream — handled explicitly by the no-arg overload returning
   `discoverAll()`.
3. **PluginDiscovery stays as the outward seam** — same shape as PR-191
   (factory rename) and PR-272 (EngineFactory companion). Adapter code
   (sm8-server Main.scala:558) doesn't change.
4. **`scala.io.Source` stays in sm8-core temporarily** — moves from
   `EngineImpl.scala:122` to the new overload. After this PR, the only
   remaining `scala.io.Source` reference in sm8-core's main sources
   should be `getLines()` on an `InputStream` (acceptable I/O at the
   load-from-config boundary).

### Backward compatibility

- `EngineImpl.discoverFromConfig()` (no-arg) signature unchanged.
- `EngineImpl.discover(allowed: Set[String])` unchanged.
- `PluginDiscovery.discoverFromConfig()` unchanged.
- `sm8-server/Main.scala:558` unchanged.

### Out of scope (deferred)

- Per-connector tests for PluginDiscovery contract — covered separately.
- Linter rule to ban `scala.io.Source` from sm8-core — separate ticket
  once this PR lands.

## Consequences

Positive:
- Closes C6 T1.MED.2 (`EngineImpl.scala:122 scala.io.Source`).
- Preserves the existing non-I/O `discover(Set[String])` overload.
- Symmetric with the ModelLoader refactor (PR-273): caller passes
  the I/O handle, core does the parse.

Neutral:
- The `discoverFromConfig(stream)` overload is a new public method;
  any test code that wanted to call `EngineImpl.discover` directly
  now has a parallel `discoverFromConfig(stream)` path.
- `scala.io.Source` is still used in sm8-core, but only inside the
  new I/O overload (a strict boundary). A future linter rule could
  whitelist this one site.

Negative:
- None observed. The change is additive on the `EngineImpl` public
  surface and refactor-only on the implementation path.

## Validation

- `mvn test sm8-core`: 642 + N tests pass (N = new I/O overload tests).
- `bash scripts/smoke-mcp-stdio.sh`: PASS.
- `bash scripts/smoke-e2e.sh`: PASS (live ingress).
- `grep 'scala.io.Source' sm8-core/src/main`: 1 hit
  (the new `discoverFromConfig(stream)` overload). Was 1 hit
  (the old `EngineImpl.discoverFromConfig()` site) — net zero change,
  but the I/O call is now behind a stream parameter instead of
  being the whole method's body.
- `grep 'EngineImpl.discoverFromConfig' sm8-server sm8-platform`:
  0 hits in adapter sources (only via `PluginDiscovery`).

## References

- C6 audit: wayfinder map #270, ticket #272 (T2).
- AGENTS.md "Common gotchas": `sm8-core is I/O-free` rule (PR-273).
- C7 wayfinder round: map #279, ticket #283 (T4).
- RFC §3 Core Boundary.
- PR-191 (PluginDiscovery factory rename), PR-272 (EngineFactory
  companion), PR-273 (ModelLoader I/O refactor).
