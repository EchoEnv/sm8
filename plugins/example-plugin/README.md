# example-plugin — copy this folder to write your first plugin

This module is the **template** for a new SM8 plugin: a complete,
minimal, working plugin with one hook, conformance tests, and the
ServiceLoader wiring already in place. Copy the directory, rename,
and you have a plugin.

## What a plugin is (60-second version)

A plugin is a class implementing `io.sm8.sdk.Plugin`. The engine
calls `setup(engine)` exactly once at startup. Inside `setup` you
register **hooks** — functions bound to one of the 8 pipeline points
(pre/post × parse/resolve/execute/format) — with a priority number
(lower runs first):

| Range | Owner |
|---|---|
| 0–99 | Core / built-in behavior |
| 100–899 | Official/first-party plugins (this one uses 200) |
| 900+ | Community / user plugins |

Rules that the engine enforces or expects:

- `setup` must not open connections or do IO — registration only.
- A hook that **throws kills the whole pipeline** (fail-fast). An
  observer hook like this one must never throw; a validator hook
  throws deliberately to reject a bad request.
- Hooks must not mutate `context.request`. Write derived values to
  `context.meta` under a namespaced key.
- Anything a hook captures must be `Serializable` (the engine may
  journal the closure). No `SparkSession`, no sockets, no
  driver-only caches. Declare captured fields in `closedOverVars`.

## The 5 things you must change after copying

1. **`pom.xml`** — rename `artifactId`, `<name>`, `<description>`.
   Keep the `enforce-no-spark` block and the `sm8-core` test-jar
   dependency.
2. **Package + classes** — rename `io.sm8.plugins.example` and
   `ExamplePlugin` / `ExampleTraceHook` to your own names.
3. **`src/main/resources/META-INF/services/io.sm8.sdk.Plugin`** —
   one line: the fully-qualified name of YOUR plugin class. This is
   how the engine discovers your plugin at startup.
4. **`src/main/resources/META-INF/sm8/plugin.properties`** — your
   `artifactId` (and optionally your `groupId`; first-party plugins
   typically keep `groupId=io.sm8.plugins` and only rename the
   `artifactId`). The engine reads this file via the Maven-coordinates
   allowlist at boot. Your `groupId` may stay
   `io.sm8.plugins` if you want to ship as a first-party plugin; only
   `artifactId` is certain to change.
5. **Root `pom.xml`** — add `<module>plugins/your-plugin</module>`
   to the `<modules>` list.

## The files, and what each is for

```
plugins/example-plugin/
├── pom.xml                          # module build: sm8-core dep, no-Spark enforcer, test-jar dep
├── README.md                        # this file
└── src/
    ├── main/
    │   ├── resources/META-INF/
    │   │   ├── services/io.sm8.sdk.Plugin    # SPI line: your plugin FQN
    │   │   └── sm8/plugin.properties         # allowlist coordinates
    │   └── scala/io/sm8/plugins/example/
    │       └── ExamplePlugin.scala           # the plugin + its hook (read this first)
    └── test/scala/io/sm8/plugins/example/
        ├── ExamplePluginSpec.scala           # behavioral tests (copy-and-adapt)
        └── ExamplePluginContractSpec.scala   # conformance bases (keep as-is)
```

## What this example plugin actually does

`ExamplePlugin` registers one `PostExecute` **Observer** hook. After
the execute stage produces a raw result, the hook writes a
deterministic trace tag (`sm8.example.traceTag` →
`example:<RequestClass>`) into `context.meta` and bumps a fire
counter. That's the whole thing — deliberately. It demonstrates
every rule above with the least code.

## Verify your copy works

```bash
mvn -pl plugins/example-plugin -am test
```

You should see the contract specs (`ExamplePluginContractSpec`,
`ExamplePluginContractPluginSpec`) and the behavioral spec
(`ExamplePluginSpec`) pass. If the contract specs fail, your hook
broke one of the rules — the failure message names which one.

## Where to go next

- `docs/rfcs/2026-08-12_v1_architecture-spec/plugins.md` — plugin
  types and rules.
- `docs/rfcs/2026-08-12_v1_architecture-spec/hooks.md` — the 8
  attachment points and the 5 hook behavioral types (validator,
  short-circuit/cache, enricher, mutator, observer).
- `plugins/audit-plugin/` — a real (non-template) plugin with the
  same shape, one step further along (a counter hook that will grow
  an SLF4J sink).
