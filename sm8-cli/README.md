# sm8 — the sm8 CLI

`sm8` is a first-class Maven module (`sm8-cli`) — a thin HTTP+JSON
client for the [sm8](https://github.com/EchoEnv/sm8) REST
APIs (MCP REST + Platform Restate ingest). It does no Spark work, no model
loading — it just makes HTTP requests and pretty-prints the responses. So
it's fast (~0.5s startup), dependency-light (jackson-databind +
scala-library only), and a faithful probe for the REST contract.

**Pedagogical note.** `sm8` deliberately depends on **zero** `sm8-*`
modules. This proves the REST surface is the contract — any HTTP+JSON
client can drive the platform. To verify:

```bash
mvn -q -pl sm8-cli dependency:tree | grep sm8-   # empty
```

## Install

Requirements: JDK 17+, Maven 3.6+.

```bash
# From the repo root
mvn -q -pl sm8-cli install -DskipTests

# Add the wrapper to your PATH (optional)
ln -s "$(pwd)/sm8-cli/bin/sm8" /usr/local/bin/sm8
```

The `bin/sm8` wrapper builds the classpath once and caches it; subsequent
invocations start in ~0.5s.

## Commands

```
sm8 list                            list available models
sm8 describe <model>                show dimensions/measures/filters/joins
sm8 query <model> -d <dim> -m <m>   run a semantic query, print a table
sm8 explain <model> -d <dim> -m <m> show the semantic plan (no execution)
sm8 audit-tail                      show recent audit events (PLATFORM RESTATE — durable)

# query/explain options:
  -d, --dim <name>                dimension (repeatable)
  -m, --measure <name>            measure (repeatable)
  -o, --order <field[:asc|desc]>  order by field (repeatable; asc default)
  --limit <n>                     row limit
  --engine <name>                 engine routing hint (v0.3.1+, server-side PR #431)

# audit-tail options:
  --tenant <id>                   tenant ID (default: default; matches [a-zA-Z0-9_-]{1,64})
  --limit <n>                     row limit
  --since <iso8601>               start of time window (e.g. 2026-01-01T00:00:00Z)
  --until <iso8601>               end of time window

# global options:
  --url <base>                    MCP REST URL (default $SDF_URL or http://localhost:8080)
  --restate-url <base>            Restate ingress URL for audit-tail (default $RESTATE_URL)
  --token-file <path>             bearer token file (default $SDF_TOKEN); chmod 600 it
  --json                          print raw JSON response
  -h, --help                      show this help
  -v, --version                   print version
```

## Examples

```bash
$ sm8 list
MODEL     STATUS     DESCRIPTION
--------  ---------  ---------------------------------------
carriers  published  Airline carrier reference data (lookup)
flights   published  Flight facts

$ sm8 describe flights
Model:        flights
Version:      0
Status:       published

Dimensions:
NAME           EXPR
-------------  -------------
carrier        carrier
flight_date    flight_date
...

Measures:
NAME              KIND  EXPR
----------------  ----  ----------------------------------------
flight_count      base  count(1)
total_distance    base  sum(distance)

$ sm8 query flights -d origin -m total_distance -o total_distance:desc --limit 5
origin  total_distance
------  --------------
LAX     17896
JFK     14516
SFO     6261
BOS     6034
SEA     5241

5 rows

$ sm8 explain flights -d carrier -m flight_count
PLAN SUMMARY
────────────
  table:   flights + carriers
  group by: carrier
  compute:  flight_count
...

$ sm8 audit-tail --limit 5
TS                     TENANT   MODEL    STATUS  ROWS  MS    EVENT
---------------------- -------- -------- ------  ----  ----  -----
2026-08-11T15:42:00Z  default  flights  ok         3  412  QUERY
2026-08-11T15:41:30Z  default  orders   ok         7   88  QUERY
2026-08-11T15:40:12Z  default  flights  ok        10  320  QUERY

3 events
```

## `audit-tail` — durable only via Restate

`sm8 audit-tail` reads the platform's audit log. Two surfaces exist in this
repo, and they have **different durability properties**:

| Surface | Storage | Survives restart? | Multi-tenant? |
|---|---|---|---|
| **Platform Restate** (default for `audit-tail`) | Postgres via `AuditEventStore` | ✅ yes | ✅ yes |
| **MCP REST** (in-memory audit ring) | `AuditSink.inMemory(1024)` | ❌ no (dies with MCP) | ❌ no |

`sm8 audit-tail` therefore **requires** `--restate-url <base>` or
`$RESTATE_URL`. Without it, the CLI exits 2 with a clear message rather
than silently fetching the wrong (volatile) data.

**`audit-tail` is durable only when targeting Restate.** Operators relying
on audit history must run the platform and set `--restate-url`. The MCP
standalone audit endpoint is not exposed by `sm8` — that's the next
architectural step (see `docs/agents/cli-vs-restate.md`).

```bash
# Required: point at the platform's Restate ingress (default :9080).
export RESTATE_URL=http://localhost:9080

# Recent activity for the default tenant.
sm8 audit-tail --limit 10

# All activity for a specific tenant in a window.
sm8 audit-tail --tenant acme --since 2026-08-01T00:00:00Z --until 2026-08-12T00:00:00Z

# Raw envelope (for piping to jq).
sm8 audit-tail --limit 5 --json | jq '.output[]'
```

The wire call is `POST /AuditService/queryRecent/send` (Restate HTTP
ingress — `@Shared` handler, no key). The response is the Restate native
envelope `{ "status": "ok", "output": [...] }`.

## Auth

`--token-file` and `$SDF_TOKEN` set the bearer token used on **every**
request (MCP REST and Restate). The header is `Authorization: Bearer <token>`.

```bash
# File-based (recommended — chmod 600 it).
echo 's3cret' > /tmp/sm8.tok && chmod 600 /tmp/sm8.tok
sm8 list --token-file /tmp/sm8.tok

# Env var.
export SDF_TOKEN=s3cret
sm8 list
```

Precedence: `--token-file <path>` > `$SDF_TOKEN` > none. Per
`scala-error-handling §1`, errors are data — a missing or unreadable
`--token-file` is a typed exit-2 error (not a silent fallback to the env
var), so a typo fails fast at the CLI instead of producing a confusing
401 in the server log.

The token file is `.trim`-ed on read, so a trailing newline from
`echo "$TOK" > tok` does not corrupt the header.

## Engine routing: `--engine <name>`

(Available since v0.3.1 — server-side: PR #431, CLI side: PR #432.)

By default the server decides which engine compiles a query (engine-portable
path when an `EngineRegistry` is configured, legacy `Models` +
`SemanticTable` path otherwise — see `docs/design/multi-engine-design.md` §6.4).

Pass `--engine <name>` on `query` or `explain` to force the engine-portable
path and pick a specific engine (e.g. `spark`, `trino`, `duckdb`,
`postgresql`):

```bash
sm8 query flights -d carrier -m flight_count --engine spark
sm8 query flights -d carrier -m flight_count --engine postgresql
```

If the named engine is not registered, the server returns `EngineError.EngineUnavailable`
and the CLI surfaces it as exit code 1.

## Streaming models over `sm8`

The `sm8` binary is **model-only** with respect to streaming. Lifecycle (start / stop / hold a stream for an unbounded time) is the operator's program — there is no `sm8 start`, `sm8 stop`, no implicit streaming query. The five verbs (`list` / `describe` / `query` / `explain` / `audit-tail`) DO interact with streaming-rooted models through their static schema, identically to batch:

```bash
$ sm8 describe events
Model:        events
Version:      1
Source table: events_stream          ← streaming read name

Dimensions:
NAME             EXPR
---------------  ----------------
event_type       type
timestamp_bucket timestamp

Measures:
NAME         KIND  EXPR
-----------  ----  --------
event_count  base  count(1)
total_value  base  sum(value)

$ sm8 query events -d event_type -m event_count
ERROR streaming-terminal: groupBy(...).aggregate(...) requires a window spec
in StreamingQueryOptions (set StreamingQueryOptions.window)
```

The error message is the *correct* answer, not a silent failure. `sm8 query` ran the streaming terminal's validator against the op tree — the same validator the library runs — and it correctly rejected an aggregation against a streaming model that has no window spec (`sm8` doesn't carry operator-side `StreamingQueryOptions`, so aggregation is operator-only).

(filter-only returns rows matching the filter at the moment of the call; useful for spot-checks, *not* a continuous tail.)

The streaming terminal (`model.toStreamingQuery(spark, cfg)`) lives in the operator's program. The `sm8` CLI has no opinion on lifecycle. For the canonical operator workflow — opening the source, constructing `StreamingConfig`, calling `toStreamingQuery`, running for N seconds, calling `.stop()` — see [`examples/streaming-events`](../examples/streaming-events/).

## Lifecycle warnings

When the MCP server touches a model whose `status` is `Deprecated` or `Draft`, the response envelope carries a `warnings: List[String]` field. `sm8` prints these to **stderr** as `WARN:` lines (one per warning), so they don't pollute `--json` output on stdout:

```bash
$ sm8 describe legacy_flights
WARN: model 'legacy_flights' is deprecated
Model:        legacy_flights
Version:      3
Status:       deprecated
...
```

`list` adds a `STATUS` column to the table; `query` and `explain` carry the
warning before the result table / plan text. The strings are display text
(LLM-readable), not identifiers — see `mcp-contract.md` §"Lifecycle
warnings" for the full contract.

## Exit codes

| Code | Meaning |
|-----:|---------|
| 0    | success |
| 1    | server returned a domain error (e.g. MODEL_NOT_FOUND, RESULT_TOO_LARGE) |
| 2    | usage error (unknown flag, missing args, missing `--restate-url` for `audit-tail`) |
| 3    | transport error (can't connect to server) |

## Why a separate module, not bundled in sm8-mcp

The CLI is a **client**, not a server. Treated as a first-class Maven module
(rather than an `examples/` entry), it gets the same `mvn test` gate, the
same CI matrix, and the same dependency hygiene as the library. It depends
only on jackson-databind + scala-library — no Spark, no sm8 library,
no MCP SDK.

## See also

- `docs/agents/cli-vs-restate.md` — next-step architecture: MCP becomes a
  stateless proxy in front of Restate; the CLI drops the MCP-REST code path
  entirely.
- `docs/DOCS_MAP.md` — top-level docs index.
