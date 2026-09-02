#!/usr/bin/env bash
# sm8 in-process stdio MCP transport smoke test (PR-265 followup to PR-264).
#
# Per the stdio design verification criteria: spawn sm8-server with
# --mcp-transport stdio (in-process stdio MCP server), pipe the full
# MCP handshake + tools/list via subprocess stdin, close stdin (EOF),
# and assert:
# - The handshake completes (initialize response + tools/list
#   response, both valid JSON-RPC on stdout)
# - tools/list returns all 5 tools
# - The process exits naturally on EOF (within 15s CI tolerance)
# - Every stdout line PARSES as valid JSON (not just prefix check;
#   PR-265 de-M2 fix). The python -c json.loads check catches
#   truncated writes that a prefix check would miss.
# - The 4 sm8-server stdout banners (server listening, etc.) are now
#   on stderr (no `sm8: ` prefix in stdout).
#
# Usage: scripts/smoke-mcp-stdio.sh [options]
#   --jar <path>    Path to sm8-server jar (default: built target)
#   --model <path>  Model YAML (default: same as smoke-e2e.sh)
#   --help          Show this help

set -u

JAR_DEFAULT="/home/emilio/app/projects/sm8/sm8-server/target/sm8-server_2.13-0.1.0-SNAPSHOT.jar"
JAR="$JAR_DEFAULT"
MODEL="/tmp/pr264-model.yaml"
# Per scripts/smoke-e2e.sh convention: cache the dependency classpath
# in $JCODE_SCRATCH_DIR.
JCODE_SCRATCH_DIR="${JCODE_SCRATCH_DIR:-/tmp}"
CP_FILE="${JCODE_SCRATCH_DIR}/sm8-smoke-cp.txt"
# PR-265 de-L4: truncate the stderr log at script start so the
# "stderr log not captured" precondition is unambiguous.
: > /tmp/smoke-mcp-stdio.stderr

while [ "$#" -gt 0 ]; do
  case "$1" in
  --jar) JAR="$2"; shift 2 ;;
  --model) MODEL="$2"; shift 2 ;;
  --help|-h) sed -n '2,20p' "$0"; exit 0 ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[ -f "$JAR" ] || { echo "smoke-mcp-stdio FAIL: jar not found: $JAR" >&2; exit 1; }
[ -f "$MODEL" ] || { echo "smoke-mcp-stdio FAIL: model not found: $MODEL" >&2; exit 1; }

fail() { echo "smoke-mcp-stdio FAIL: $*" >&2; exit "${2:-1}"; }

# Build classpath (cached) per scripts/smoke-e2e.sh convention.
# Same pattern as smoke-e2e.sh: only the file-existence check, no
# ad-hoc mcp-core grep (the latter was over-defensive per de-M1).
[ -s "$CP_FILE" ] || {
  echo "building dependency classpath (first run) ..." >&2
  (cd /home/emilio/app/projects/sm8 && mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE") >&2
}
[ -s "$CP_FILE" ] || fail "classpath file empty" 2

# In-memory connector JAR (META-INF/services/io.sm8.core.engine.EngineProvider).
CONN="/home/emilio/app/projects/sm8/connectors/in-memory-connector/target/in-memory-connector_2.13-0.1.0-SNAPSHOT.jar"
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
# Restate ingress. The 5 tools delegate to the Restate ingress; this
# smoke asserts the wire protocol (handshake + tools/list + EOF exit +
# stdout cleanliness). Tool execution is covered by smoke-e2e.sh.

# Time the whole run: PR-264 requires the process to EXIT on EOF
# (within ~3-5s typical). 15s CI tolerance accounts for slow runners
# (per arch-L9). The latch budget itself is `awaitClose(timeoutSeconds
# = 30)` in Main.scala:588; the smoke's 15s is the CI budget for the
# FULL handshake-to-exit path, not the latch spec.
START_EPOCH=$(date +%s)

# Flat form per sibling smoke-mcp.sh (de-L3): keep stdin open for the
# server's read loop via a single subshell with a process substitution,
# then run java with stderr redirected to the log file.
OUTPUT=$(
  exec 0< <(
    printf '%s\n' \
      '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-mcp-stdio","version":"0"}}}' \
      '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
      '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
    # Give the server time to read all 3 lines, then close stdin.
    # The server must exit on that EOF.
    sleep 2
  )
  java -cp "$JAR:$CONN:$(cat "$CP_FILE")" io.sm8.server.Main \
    --model "$MODEL" \
    --port 0 \
    --metrics-port 0 \
    --mcp-transport stdio \
    --ingress-url http://127.0.0.1:8080 \
    2>/tmp/smoke-mcp-stdio.stderr
)

ELAPSED=$(( $(date +%s) - START_EPOCH ))
echo "smoke-mcp-stdio: process exited cleanly on EOF in ${ELAPSED}s"
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
# same 5 tools as the HTTP transport).
TOOLS_RESP=$(echo "$OUTPUT" | grep -F '"id":2' | head -1)
[ -n "$TOOLS_RESP" ] || fail "tools/list response not found: $OUTPUT"
TOOL_COUNT=$(echo "$TOOLS_RESP" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print(len(d["result"]["tools"]))')
[ "$TOOL_COUNT" -eq 5 ] || fail "expected 5 tools, got $TOOL_COUNT (tools/list response: $TOOLS_RESP)"
echo "smoke-mcp-stdio: tools/list response has all 5 tools (count=$TOOL_COUNT)"

# Verify: stderr DOES contain the expected startup banners.
[ -f /tmp/smoke-mcp-stdio.stderr ] || fail "stderr log not captured"
if ! grep -q 'sm8: server listening on port' /tmp/smoke-mcp-stdio.stderr; then
  echo "stderr content:"
  cat /tmp/smoke-mcp-stdio.stderr
  fail "expected 'sm8: server listening on port' banner in stderr (post-redirect)"
fi
echo "smoke-mcp-stdio: 'sm8: server listening on port' banner correctly on stderr"

echo "SMOKE-MCP-STDIO PASS (in-process stdio MCP: handshake + tools/list + EOF-exit + stdout-clean)"