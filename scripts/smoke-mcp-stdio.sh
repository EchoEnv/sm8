#!/usr/bin/env bash
# sm8 in-process stdio MCP transport smoke test (a prior PR / the stdio design).
#
# Per the stdio design §Verification criteria: spawn sm8-server with
# --mcp-transport stdio (in-process stdio MCP server per the stdio design),
# pipe the full MCP handshake + tools/list via subprocess stdin,
# close stdin (EOF), and assert:
# - The handshake completes (initialize response + tools/list
# response, both valid JSON-RPC on stdout)
# - tools/list returns all 5 tools
# - The process exits naturally on EOF (within 10s — a prior PR latch)
# - Every stdout line parses as valid JSON-RPC (per the stdio design r2 Q4
# fix — not just prefix absence, but JSON envelope parse)
# - The 4 sm8-server stdout banners (Usage, server listening, etc.)
# are now on stderr (no `sm8: ` prefix in stdout)
#
# Usage: scripts/smoke-mcp-stdio.sh [options]
# --jar <path> Path to sm8-server jar (default: built target)
# --model <path> Model YAML (default: same as smoke-e2e.sh)
# --help Show this help

set -u

JAR_DEFAULT="/home/emilio/app/projects/sm8/sm8-server/target/sm8-server_2.13-0.1.0-SNAPSHOT.jar"
JAR="$JAR_DEFAULT"
MODEL="/tmp/pr264-model.yaml"
# Per scripts/smoke-e2e.sh convention: cache the dependency classpath
# in $JCODE_SCRATCH_DIR. The script needs BOTH the connector jar
# (in-memory) AND the server jar + sm8-platform classes on the
# classpath, exactly as smoke-e2e.sh builds it.
JCODE_SCRATCH_DIR="${JCODE_SCRATCH_DIR:-/tmp}"
CP_FILE="${JCODE_SCRATCH_DIR}/sm8-smoke-cp.txt"
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
# Note: the cache key doesn't see pom changes — regenerate when the
# MCP SDK deps are missing (a prior PR added mcp-core + mcp-json-jackson3
# to sm8-platform, so a pre-a prior PR cache fails with NoClassDefFound).
[ -s "$CP_FILE" ] || {
 echo "building dependency classpath (first run) ..." >&2
 (cd /home/emilio/app/projects/sm8 && mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE") >&2
}
grep -q "mcp-core" "$CP_FILE" 2>/dev/null || {
 echo "regenerating classpath (stale cache without MCP SDK) ..." >&2
 (cd /home/emilio/app/projects/sm8 && mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$CP_FILE") >&2
}
[ -s "$CP_FILE" ] || fail "classpath file empty" 2

# In-memory connector JAR (META-INF/services/io.sm8.core.engine.EngineProvider).
CONN="/home/emilio/app/projects/sm8/connectors/in-memory-connector/target/in-memory-connector_2.13-0.1.0-SNAPSHOT.jar"
[ -f "$CONN" ] || fail "connector jar not found: $CONN (build it first: mvn -pl connectors/in-memory-connector install)"

# Build a tiny model file
cat > "$MODEL" <<'YAML'
name: smoke-mcp-stdio-model
version: 1
source:
  byName:
    table: smoke_stdio_table
YAML

# Per the stdio design §Wiring: the MCP stdio server runs in-process with the
# Restate ingress. The 5 tools delegate to the Restate ingress; the
# smoke here asserts the full wire protocol (handshake + tools/list +
# EOF exit + stdout cleanliness). Tool *execution* is covered by
# smoke-e2e.sh (a prior PR/a prior PR).

# Time the whole run: a prior PR requires the process to EXIT on EOF
# (within ~10s). A server that lingers after EOF (e.g. the pre-a prior PR
# latch bug) fails the timeout.
START_EPOCH=$(date +%s)

OUTPUT=$(
 (exec 0< <(
 (
 printf '%s\n' \
 '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-mcp-stdio","version":"0"}}}' \
 '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
 '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
 # Give the server time to read all 3 lines, then close stdin.
 # The server must exit on that EOF.
 sleep 2
)
)
 # Stderr to log file; stdout to our OUTPUT variable.
 java -cp "$JAR:$CONN:$(cat "$CP_FILE")" io.sm8.server.Main \
 --model "$MODEL" \
 --port 0 \
 --metrics-port 0 \
 --mcp-transport stdio \
 --ingress-url http://127.0.0.1:8080 \
 2>/tmp/smoke-mcp-stdio.stderr
) 2>&1
)

ELAPSED=$(($(date +%s) - START_EPOCH))
echo "smoke-mcp-stdio: process exited cleanly on EOF in ${ELAPSED}s"
[ "$ELAPSED" -le 10 ] || fail "server took >10s to exit after stdin EOF (latch bug? elapsed=${ELAPSED}s)"

# Verify: every stdout line must parse as valid JSON-RPC.
# If the in-process server leaked any "sm8: server listening on port N"
# banner, that line would fail JSON parsing.
LINE_NUM=0
PARSED=0
while IFS= read -r line; do
 LINE_NUM=$((LINE_NUM+1))
 [ -z "$line" ] && continue
 # Quick JSON-RPC validation: must start with `{"jsonrpc":` (the
 # SDK writes exactly that prefix per the source; use grep first to
 # avoid a slow python invocation on every line).
 if ! echo "$line" | grep -q '^{"jsonrpc":'; then
 echo " smoke-mcp-stdio line $LINE_NUM fails JSON-RPC prefix check: $line"
 fail "line $LINE_NUM is not a JSON-RPC message: $line"
 fi
 PARSED=$((PARSED+1))
done <<< "$OUTPUT"

echo "smoke-mcp-stdio: $PARSED stdout lines, all parse as JSON-RPC"

# Verify: at least 2 messages (initialize response + tools/list response).
[ "$PARSED" -ge 2 ] || fail "expected at least 2 JSON-RPC messages, got $PARSED"

# Verify: no "sm8: " prefix anywhere in stdout (the 4 banners should be
# on stderr after a prior PR).
if echo "$OUTPUT" | grep -q '^sm8: '; then
 echo "smoke-mcp-stdio: stdout contains 'sm8: ' prefix (banners leaked to stdout):"
 echo "$OUTPUT" | grep '^sm8: ' | head -3
 fail "stdout contains sm8-server banners (must be on stderr)"
fi
echo "smoke-mcp-stdio: stdout is clean (no sm8: banners)"

# Verify: the initialize response includes serverInfo.name=sm8 + protocolVersion.
INIT_RESP=$(echo "$OUTPUT" | head -1)
echo "$INIT_RESP" | grep -q '"serverInfo"' || fail "initialize response missing serverInfo: $INIT_RESP"
echo "$INIT_RESP" | grep -q '"name":"sm8"' || fail "initialize response missing serverInfo.name=sm8: $INIT_RESP"
echo "$INIT_RESP" | grep -q '"protocolVersion":"2024-11-05"' || fail "initialize response missing protocolVersion: $INIT_RESP"
echo "smoke-mcp-stdio: initialize response carries serverInfo.name=sm8 + protocolVersion"

# Verify: tools/list response has the result with a NON-EMPTY tools
# array (per the stdio design §Verification the in-process stdio server
# carries the same 5 tools as the HTTP transport; a prior PR wires the
# tool handler chain through McpStdioRoute -> Sm8ToolHandlers -> HTTP
# ingress client).
TOOLS_RESP=$(echo "$OUTPUT" | grep -F '"id":2' | head -1)
[ -n "$TOOLS_RESP" ] || fail "tools/list response not found: $OUTPUT"
echo "$TOOLS_RESP" | grep -q '"result"' || fail "tools/list response missing result: $TOOLS_RESP"
echo "$TOOLS_RESP" | grep -q '"tools":' || fail "tools/list response missing tools array: $TOOLS_RESP"
# Count tools by splitting on the SDK's tool-name field prefix.
TOOL_NAMES=$(echo "$TOOLS_RESP" | grep -oE '"name":"[a-z_]+"' | grep -v '"name":"sm8"' || true)
TOOL_COUNT=$(echo "$TOOL_NAMES" | wc -l)
[ "$TOOL_COUNT" -eq 5 ] || fail "expected 5 tools, got $TOOL_COUNT (tools found: $TOOL_NAMES)"
echo "smoke-mcp-stdio: tools/list response has all 5 tools ($TOOL_NAMES)"

# Verify: stderr DOES contain the expected startup banners (redirected
# from stdout).
[ -f /tmp/smoke-mcp-stdio.stderr ] || fail "stderr log not captured"
if ! grep -q 'sm8: server listening on port' /tmp/smoke-mcp-stdio.stderr; then
 echo "stderr content:"
 cat /tmp/smoke-mcp-stdio.stderr
 fail "expected 'sm8: server listening on port' banner in stderr (post-redirect)"
fi
echo "smoke-mcp-stdio: 'sm8: server listening on port' banner correctly on stderr"

echo "SMOKE-MCP-STDIO PASS (in-process stdio MCP: handshake + tools/list + EOF-exit + stdout-clean)"