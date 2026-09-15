#!/usr/bin/env bash
# sm8 in-process stdio MCP transport smoke test (PR-266 followup to PR-265).
#
# Per the stdio design verification criteria: spawn sm8-server with
# --mcp-transport stdio (in-process stdio MCP server), pipe the full
# MCP handshake + tools/list via subprocess stdin, close stdin (EOF),
# and assert:
# - The handshake completes (initialize response + tools/list
#   response, both valid JSON-RPC on stdout)
# - tools/list returns all 8 tools (incl. validate_query, D4)
# - validate_query tools/call (good + bad model) returns a
#   ValidationOutcome and a typed ValidationFailure respectively
# - The JVM exits cleanly on EOF (within 15s CI tolerance)
# - java's exit code is 0 (C5-de-H2 — previously not verified)
# - Every stdout line PARSES as valid JSON (not just prefix check;
#   PR-265 de-M2 fix). The python -c json.loads check catches
#   truncated writes that a prefix check would miss.
# - The 4 sm8-server stdout banners (server listening, etc.) are now
#   on stderr (no `sm8: ` prefix in stdout).
#
# Usage: scripts/smoke-mcp-stdio.sh [options]
#   --jar <path>    Path to sm8-server jar (default: $REPO_ROOT/sm8-server/target/...)
#   --model <path>  Model YAML (default: same as smoke-e2e.sh)
#   --help          Show this help

# Per C5-arch-L5: enable strict mode — unset vars, error-exit, and
# pipefail so any command failure aborts the smoke. Previously only
# `set -u` was set; a failing `cat "$CP_FILE"` mid-script silently
# continued and a later check could pass with stale state.
set -eo pipefail

# Per C5-arch-L1: resolve repo root from the script's location so the
# smoke works on any developer machine, not just /home/emilio. Falls
# back to git rev-parse if the script isn't under a repo checkout.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

JAR_DEFAULT="${REPO_ROOT}/sm8-server/target/sm8-server_2.13-0.1.0-SNAPSHOT.jar"
JAR="$JAR_DEFAULT"
# Default the model + log + sentinel paths under $JCODE_SCRATCH_DIR
# so the smoke doesn't litter /tmp/ on every run. Falls back to /tmp/
# when JCODE_SCRATCH_DIR is unset, matching the scripts/smoke-e2e.sh
# convention.
JCODE_SCRATCH_DIR="${JCODE_SCRATCH_DIR:-/tmp}"
MODEL_DEFAULT="${JCODE_SCRATCH_DIR}/sm8-smoke-mcp-stdio-model.yaml"
MODEL="$MODEL_DEFAULT"
STDERR_LOG="${JCODE_SCRATCH_DIR}/smoke-mcp-stdio.stderr"
EXIT_SENTINEL="${JCODE_SCRATCH_DIR}/smoke-mcp-stdio.exit"
CP_FILE="${JCODE_SCRATCH_DIR}/sm8-smoke-cp.txt"
# Truncate the stderr log + exit sentinel at script start so the
# "stderr log not captured" + "exit sentinel" preconditions are
# unambiguous across reruns.
: > "$STDERR_LOG"
: > "$EXIT_SENTINEL"

# java runs synchronously inside the OUTPUT=$(...) substitution
# below; the trap fires AFTER the substitution completes, so it
# cannot catch a hung java (java will already be gone or hung on
# its own). Best-effort: reap any orphan java whose cmdline contains
# the JAR basename. The pkill -f pattern is acceptable here because
# the smoke is single-purpose: there is exactly one java invocation
# in this script, and the JAR basename is the build version
# (sm8-server_2.13-0.1.0-SNAPSHOT.jar) — collisions with unrelated
# JVMs are extremely unlikely in CI.
# NOTE: the cleanup definition lives after the ingress-holder boot
# below — it needs INGRESS_PID, which only exists once the holder is
# spawned.

while [ "$#" -gt 0 ]; do
  case "$1" in
  --jar) JAR="$2"; shift 2 ;;
  --model) MODEL="$2"; shift 2 ;;
  --help|-h) sed -n '2,22p' "$0"; exit 0 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Classpath entries: COMPILED CLASSES dirs primary (always present after
# mvn test-compile), packaged jars as fallback. Same rationale as the
# StdioEndToEndSpec.scala fix (PR #372): mvn test never packages jars,
# so jar-only classpaths silently pointed at nonexistent files.
SERVER_DIR="${REPO_ROOT}/sm8-server/target/classes"
SERVER_JAR="${REPO_ROOT}/sm8-server/target/sm8-server_2.13-0.1.0-SNAPSHOT.jar"
CONN_DIR="${REPO_ROOT}/connectors/in-memory-connector/target/classes"
CONN_JAR="${REPO_ROOT}/connectors/in-memory-connector/target/in-memory-connector_2.13-0.1.0-SNAPSHOT.jar"
SERVER_ENTRY="$([ -d "$SERVER_DIR" ] && echo "$SERVER_DIR" || echo "$SERVER_JAR")"
CONN_ENTRY="$([ -d "$CONN_DIR" ] && echo "$CONN_DIR" || echo "$CONN_JAR")"

# Default --jar is the packaged jar; allow the compiled classes dir as a fallback
# when the user did NOT override --jar (mvn test does not produce the jar).
if [ "$JAR" = "$JAR_DEFAULT" ]; then
  [ -d "$SERVER_DIR" ] || [ -f "$JAR" ] || { echo "smoke-mcp-stdio FAIL: neither $SERVER_DIR nor $JAR found" >&2; exit 1; }
else
  [ -f "$JAR" ] || { echo "smoke-mcp-stdio FAIL: jar not found: $JAR" >&2; exit 1; }
fi
# $MODEL is created by the heredoc below; no pre-existence check.

fail() { echo "smoke-mcp-stdio FAIL: $*" >&2; exit "${2:-1}"; }

# Build classpath (cached) per scripts/smoke-e2e.sh convention.
# Same pattern as smoke-e2e.sh: only the file-existence check, no
# ad-hoc mcp-core grep (the latter was over-defensive per de-M1).
[ -s "$CP_FILE" ] || {
  echo "building dependency classpath (first run) ..." >&2
  (cd "$REPO_ROOT" && mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE") >&2
}
[ -s "$CP_FILE" ] || fail "classpath file empty" 2

# In-memory connector JAR (META-INF/services/io.sm8.core.engine.EngineProvider).
CONN="${REPO_ROOT}/connectors/in-memory-connector/target/in-memory-connector_2.13-0.1.0-SNAPSHOT.jar"
[ -f "$CONN" ] || fail "connector jar not found: $CONN (build it first: mvn -pl connectors/in-memory-connector install)"

# Build a tiny model file (schema-validated against manifest.schema.v2.json:
# source.byName requires `table` field with minLength: 1).
cat > "$MODEL" <<'YAML'
name: smoke-mcp-stdio-model
version: 1
source:
  byName:
    table: smoke_stdio_table
YAML

# Per the stdio design: the MCP stdio server runs in-process with the
# Restate ingress. The 7 tools delegate to the Restate ingress; this
# smoke asserts the wire protocol (handshake + tools/list + EOF exit +
# stdout cleanliness). Tool execution is covered by smoke-e2e.sh.

# Time the whole run: PR-264 requires the process to EXIT on EOF
# (within ~3-5s typical). 15s CI tolerance accounts for slow runners
# (per arch-L9). The latch budget itself is `awaitClose(timeoutSeconds
# = 30)` in Main.scala; the smoke's 15s is the CI budget for the
# FULL handshake-to-exit path, not the latch spec.
# Exit sentinel truncated at startup; see init block above.
START_EPOCH=$(date +%s)

# ---- ingress holder process ---------------------------------------------
#
# The validate_query tools/call assertions need a backing HTTP server
# to forward to. In stdio mode Main skips the HTTP bind (per C5-de-M2:
# stdio MCP forwards tool calls to a separate ingress via
# HttpIngressClient; the canonical deployment is a colocated process
# pair). Rather than spawn a second sm8-server AND a real Restate
# container (the smoke-e2e.sh shape — minutes, not milliseconds), this
# smoke runs a small Java program that uses the JDK's
# `com.sun.net.httpserver.HttpServer` to mock the Restate ingress: it
# replies 200 to POST /QueryValidationService/validate with a canned
# ValidationOutcome, 400 with a failure message when the forwarded
# model name is unknown (matching the TerminalException(400) wire
# shape the real handler throws), a benign 200 on any other POST, and
# 204 on GET/HEAD so the startup probe does not warn.
#
# The stdio MCP process reuses the same Sm8ToolHandlers factory as
# the HTTP transport — the tools available to a client are the same
# 8 whether invoked via stdio or HTTP MCP.
#
# Pattern matches StdioEndToEndSpec.scala's mock-ingress setup (same
# JDK server class, same response shapes) — proven fast + hermetic.

INGRESS_DIR="$(mktemp -d)"
INGRESS_PORT_FILE="${JCODE_SCRATCH_DIR}/sm8-smoke-mcp-stdio-ingress-port"
INGRESS_STDERR="${JCODE_SCRATCH_DIR}/sm8-smoke-mcp-stdio-ingress.stderr"
: > "$INGRESS_PORT_FILE"; : > "$INGRESS_STDERR"

cat > "$INGRESS_DIR/MockIngress.java" <<'JAVA'
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.*;

public class MockIngress {
  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
    server.createContext("/", ex -> {
      String method = ex.getRequestMethod();
      // Match the StdioEndToEndSpec mock: HEAD/GET -> 204 no-op
      // (the probeIngressOrWarn HEAD at startup needs a non-error
      // response so it doesn't print a WARNING banner on stderr).
      if ("HEAD".equals(method) || "GET".equals(method)) {
        ex.sendResponseHeaders(204, -1);
        ex.getResponseBody().close();
        return;
      }
      // Drain the request body so the client doesn't see a broken pipe.
      byte[] in = ex.getRequestBody().readAllBytes();
      String path = ex.getRequestURI().getPath();
      byte[] out;
      if ("/QueryValidationService/validate".equals(path)) {
        // Extract the modelName out of the request body so the canned
        // ValidationOutcome names the model in its error message —
        // proves the request body was forwarded end-to-end (not
        // stubbed out by the mock).
        String body = new String(in, StandardCharsets.UTF_8);
        String modelName = extractField(body, "model");
        if (modelName.contains("no-such-model")) {
          // Map the request-side handler's typed failure onto the
          // wire shape: a Restate service would throw a
          // TerminalException(400, message); the MCP wrapper maps
          // statusCode>=400 to isError=true. Mock the failure shape.
          String msg = "[\"request\"] unknown model: " + modelName;
          out = msg.getBytes(StandardCharsets.UTF_8);
          ex.getResponseHeaders().add("Content-Type", "application/json");
          ex.sendResponseHeaders(400, out.length);
        } else {
          // The canned ValidationOutcome: tablesTouched + engineSelection
          // + compiledSql set to null (deployment didn't supply a
          // compiledSqlFn — matches the stdio smoke shape).
          String outcome = "{\"modelVersion\":1," +
            "\"rollupDecision\":{\"rewriteApplied\":false,\"reason\":\"none\"}," +
            "\"decisionHints\":null," +
            "\"engineSelection\":\"in-memory\"," +
            "\"tablesTouched\":[\"smoke_stdio_table\"]," +
            "\"compiledSql\":null}";
          out = outcome.getBytes(StandardCharsets.UTF_8);
          ex.getResponseHeaders().add("Content-Type", "application/json");
          ex.sendResponseHeaders(200, out.length);
        }
      } else {
        // For any other path, return a benign 200 with empty body.
        out = new byte[0];
        ex.sendResponseHeaders(200, -1);
      }
      if (out.length > 0) ex.getResponseBody().write(out);
      ex.getResponseBody().close();
    });
    server.setExecutor(Executors.newSingleThreadExecutor());
    server.start();
    // Print the bound port on stdout; the shell script parses it.
    System.out.println(server.getAddress().getPort());
  }
  private static String extractField(String json, String field) {
    int i = json.indexOf("\"" + field + "\"");
    if (i < 0) return "";
    int q1 = json.indexOf('"', i + field.length() + 2);
    int q2 = json.indexOf('"', q1 + 1);
    return json.substring(q1 + 1, q2);
  }
}
JAVA
# Compile + run in the background; the java process prints the
# ephemeral port to stdout, which the script reads from a file once
# the bind banner arrives. Wait up to 15s for the port file to be
# non-empty — fail loud if the mock never binds.
javac -d "$INGRESS_DIR" "$INGRESS_DIR/MockIngress.java" || fail "mock ingress compile failed"
java -cp "$INGRESS_DIR" MockIngress 0 >"$INGRESS_PORT_FILE" 2>"$INGRESS_STDERR" &
INGRESS_PID=$!
for _ in $(seq 1 30); do
  [ -s "$INGRESS_PORT_FILE" ] && break
  if ! kill -0 "$INGRESS_PID" 2>/dev/null; then
    echo "smoke-mcp-stdio: mock ingress died during boot:" >&2
    cat "$INGRESS_STDERR" >&2
    fail "mock ingress process exited before binding a port"
  fi
  sleep 0.5
done
INGRESS_PORT=$(cat "$INGRESS_PORT_FILE")
[ -n "$INGRESS_PORT" ] || fail "mock ingress never printed a port (15s); stderr: $(cat "$INGRESS_STDERR")"
echo "smoke-mcp-stdio: mock ingress holder up on ephemeral port $INGRESS_PORT (pid $INGRESS_PID)"

# Cleanup: kill both the stdio MCP java (via JAR basename pattern) and
# the mock ingress holder; remove the temp dir. The earlier
# pkill-fallback handles orphans if the trap is bypassed by a signal.
cleanup() {
  local rc=$?
  pkill -f "java .*${JAR##*/}" 2>/dev/null || true
  if [ -n "${INGRESS_PID:-}" ] && kill -0 "$INGRESS_PID" 2>/dev/null; then
    kill "$INGRESS_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$INGRESS_PID" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$INGRESS_PID" 2>/dev/null || true
    wait "$INGRESS_PID" 2>/dev/null || true
  fi
  [ -n "${INGRESS_DIR:-}" ] && rm -rf "$INGRESS_DIR" 2>/dev/null || true
  exit $rc
}
trap cleanup EXIT INT TERM

# Flat form per sibling smoke-mcp.sh (de-L3): keep stdin open for the
# server's read loop via a single subshell with a process substitution,
# then run java with stderr redirected to the log file.
#
# Per C5-de-H2: capture java's exit code via $? inside the
# command-substitution subshell, then assert it was 0 after the
# substitution completes. Previously only elapsed time + stdout parse
# were checked; a non-zero exit (runtime error, boot failure) could
# silently pass if elapsed <= 15s and stdout happened to parse.
EXIT=0
OUTPUT=$(
  exec 0< <(
    printf '%s\n' \
      '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-mcp-stdio","version":"0"}}}' \
      '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
      '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
      '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"validate_query","arguments":{"modelName":"smoke-mcp-stdio-model","dimensions":["day"],"measures":["cnt"]}}}' \
      '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"validate_query","arguments":{"modelName":"no-such-model","dimensions":[],"measures":[]}}}' \
    # Per C5-de-L1: reduce sleep from 2s to 0.5s. BufferedReader.readLine
    # blocks until newline OR EOF; the 3 messages arrive in <50ms
    # locally. 0.5s gives ample margin (10x) without slowing the smoke.
    sleep 0.5
  )
  java -cp "$SERVER_ENTRY:$CONN_ENTRY:$(cat "$CP_FILE")" io.sm8.server.Main \
    --model "$MODEL" \
    --port 0 \
    --metrics-port 0 \
    --mcp-transport stdio \
    --ingress-url "http://127.0.0.1:$INGRESS_PORT" \
    2>"$STDERR_LOG"
  # Capture java's exit code INSIDE the substitution so $? survives
  # the subshell. We use a sentinel file because bash's $? across
  # `$(...)` boundaries is reset.
  echo $? > "$EXIT_SENTINEL"
) || EXIT=$?
[ -f "$EXIT_SENTINEL" ] || fail "java exit sentinel not written"
EXIT=$(cat "$EXIT_SENTINEL")
[ "$EXIT" -eq 0 ] || fail "java exited with code $EXIT (expected 0)"

ELAPSED=$(( $(date +%s) - START_EPOCH ))
echo "smoke-mcp-stdio: process exited cleanly on EOF in ${ELAPSED}s (exit=0)"
[ "$ELAPSED" -le 15 ] || fail "server took >15s to exit after stdin EOF (latch bug? elapsed=${ELAPSED}s)"

# Verify: every stdout line must PARSE as valid JSON-RPC (PR-265 de-M2
# fix — prefix check is too lax for catching truncated writes). Use
# python json.loads which catches malformed envelopes that pass a
# regex prefix match.
LINE_NUM=0
PARSED=0
while IFS= read -r line; do
  LINE_NUM=$((LINE_NUM+1))
  [ -z "$line" ] && continue
  if ! echo "$line" | python3 -c 'import json,sys; json.loads(sys.stdin.read())' 2>/dev/null; then
    echo "smoke-mcp-stdio line $LINE_NUM fails JSON-envelope parse: $line"
    fail "line $LINE_NUM is not valid JSON: $line"
  fi
  PARSED=$((PARSED+1))
done <<< "$OUTPUT"

echo "smoke-mcp-stdio: $PARSED stdout lines, all parse as JSON envelopes"
[ "$PARSED" -ge 2 ] || fail "expected at least 2 JSON-RPC messages, got $PARSED"

# Verify: no "sm8: " prefix anywhere in stdout (the 4 banners should be
# on stderr after PR-264 Main.scala banner-stderr fix).
if echo "$OUTPUT" | grep -q '^sm8: '; then
  echo "smoke-mcp-stdio: stdout contains 'sm8: ' prefix (banners leaked to stdout):"
  echo "$OUTPUT" | grep '^sm8: ' | head -3
  fail "stdout contains sm8-server banners (must be on stderr)"
fi
echo "smoke-mcp-stdio: stdout is clean (no sm8: banners)"

# Verify: the initialize response includes serverInfo.name=sm8 + protocolVersion.
INIT_RESP=$(echo "$OUTPUT" | head -1)
echo "$INIT_RESP" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); assert d["result"]["serverInfo"]["name"] == "sm8"; assert d["result"]["protocolVersion"] == "2024-11-05"' \
  || fail "initialize response missing serverInfo.name=sm8 or protocolVersion=2024-11-05: $INIT_RESP"
echo "smoke-mcp-stdio: initialize response carries serverInfo.name=sm8 + protocolVersion"

# Verify: tools/list response has the result with a NON-EMPTY tools
# array (per the stdio design the in-process stdio server carries the
# same 7 tools as the HTTP transport).
TOOLS_RESP=$(echo "$OUTPUT" | grep -F '"id":2' | head -1)
[ -n "$TOOLS_RESP" ] || fail "tools/list response not found: $OUTPUT"
TOOL_COUNT=$(echo "$TOOLS_RESP" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(len(d["result"]["tools"]))')
[ "$TOOL_COUNT" -eq 8 ] || fail "expected 8 tools, got $TOOL_COUNT (tools/list response: $TOOLS_RESP)"
echo "smoke-mcp-stdio: tools/list response has all 8 tools (count=$TOOL_COUNT)"

# Verify: stderr DOES contain the expected startup banners.
[ -f "$STDERR_LOG" ] || fail "stderr log not captured"
if ! grep -q 'sm8:.*listening on port' "$STDERR_LOG"; then
  echo "stderr content:"
  cat "$STDERR_LOG"
  fail "expected 'sm8:.*listening on port' banner in stderr (post-redirect)"
fi
echo "smoke-mcp-stdio: 'sm8:.*listening on port' banner correctly on stderr"

# ---- validate_query tools/call (E2E of the shipped validate tool) ----
#
# The known-good call validates the model this script wrote above
# (smoke-mcp-stdio-model). Expected: isError=false and the text
# content carries the serialized ValidationOutcome (tablesTouched
# names the model's physical source table). The known-bad call uses a
# model name that was never loaded; the service answers 400
# (TerminalException from the validate handler) and the MCP wrapper
# maps statusCode>=400 to isError=true with the failure message.

GOOD_RESP=$(echo "$OUTPUT" | grep -F '"id":3' | head -1)
[ -n "$GOOD_RESP" ] || fail "validate_query (good) response not found: $OUTPUT"
echo "$GOOD_RESP" | python3 -c '
import json, sys
d = json.loads(sys.stdin.read())
r = d["result"]
assert r.get("isError") is False, "expected isError=false, got: %r" % (r,)
text = r["content"][0]["text"]
assert "tablesTouched" in text, "no tablesTouched in ValidationOutcome: %s" % text
assert "smoke_stdio_table" in text, "tablesTouched missing the model source table: %s" % text
assert "engineSelection" in text, "no engineSelection in ValidationOutcome: %s" % text
' || fail "validate_query (good) did not return a ValidationOutcome: $GOOD_RESP"
echo "smoke-mcp-stdio: validate_query (good) returned a ValidationOutcome (tablesTouched + engineSelection)"

BAD_RESP=$(echo "$OUTPUT" | grep -F '"id":4' | head -1)
[ -n "$BAD_RESP" ] || fail "validate_query (bad) response not found: $OUTPUT"
echo "$BAD_RESP" | python3 -c '
import json, sys
d = json.loads(sys.stdin.read())
r = d["result"]
assert r.get("isError") is True, "expected isError=true for unknown model, got: %r" % (r,)
text = r["content"][0]["text"]
assert "no-such-model" in text, "failure text missing the unknown model name: %s" % text
' || fail "validate_query (bad) did not return a typed failure: $BAD_RESP"
echo "smoke-mcp-stdio: validate_query (bad) returned a typed failure naming the unknown model"

echo "SMOKE-MCP-STDIO PASS (in-process stdio MCP: handshake + tools/list + validate_query E2E + EOF-exit + stdout-clean)"
