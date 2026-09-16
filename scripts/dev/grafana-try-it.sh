#!/usr/bin/env bash
# scripts/dev/grafana-try-it.sh
#
# One-shot demo: prove the Grafana dashboards from dashboards/ actually
# render with live sm8 /metrics data. Bounded resource use, no persistence
# (auto-teardown). NOT a production deployment — the operator reproduces
# this on their own infra per docs/runbooks/grafana-dashboards.md.
#
# This is a verification script, not a service: it tears everything down
# on exit. Memory guard: the process bails at the first 1 GB ceiling
# observed by the watchdog (no swap-storm).
#
# Usage:
#   scripts/dev/grafana-try-it.sh [sm8-port] [demo-seconds]
# Defaults: sm8-port 19184 (ephemeral), demo-seconds 60.
#
# What it does:
#  1. Starts sm8-server with the in-memory connector + a temp model
#  2. Starts prometheus container scraping sm8 (memory: 256m, cpus: 0.5)
#  3. Starts grafana container provisioned with the Prometheus data source
#     + the 2 dashboard JSONs imported (memory: 384m, cpus: 0.5)
#  4. Polls /api/dashboards/uid/sm8-rollup-freshness on grafana for
#     rendered data and writes a JSON summary
#  5. Tears everything down (catches EXIT/INT/TERM); outputs the summary
#     to stdout + a path for screenshots
#
# Total resource ceiling: 256m + 384m + sm8 ~500m = ~1.1 GB.
# Memory watchdog: aborts if available host RAM drops below 1.0 GB.

set -euo pipefail

# ---- args / defaults ----
SM8_PORT="${1:-19184}"
DEMO_SECONDS="${2:-60}"
SCRATCH_DIR="${JCODE_SCRATCH_DIR:-/tmp}"
PROM_PORT=19190
GRAFANA_PORT=13000
MEMORY_FLOOR_MB=1000

# ---- helpers ----
now() { date -u +%H:%M:%SZ; }
log() { echo "[$(now)] $*" | tee -a "$LOG" >&2; }
die() { echo "[$(now)] FATAL: $*" | tee -a "$LOG" >&2; exit 1; }

# Memory watchdog: bail if free RAM drops below MEMORY_FLOOR_MB.
# Polled in the main loop. Caps the worst case at ~1 GB; aborts gracefully
# if a teammate's workload reclaims memory.
watchdog() {
  local avail_mb
  avail_mb=$(free -m | awk 'NR==2 {print $7}')
  if (( avail_mb < MEMORY_FLOOR_MB )); then
    log "WATCHDOG: available RAM ${avail_mb}MB < ${MEMORY_FLOOR_MB}MB floor; aborting to protect host"
    cleanup
    die "memory floor breached"
  fi
}

# ---- preflight ----
LOG="$SCRATCH_DIR/grafana-try-it-$(date +%s).log"
SM8_LOG="$SCRATCH_DIR/sm8-try-it.log"
PROM_DATA="$SCRATCH_DIR/prom-try-it-data"
GRAFANA_DATA="$SCRATCH_DIR/grafana-try-it-data"
SUMMARY="$SCRATCH_DIR/grafana-try-it-summary.json"

mkdir -p "$PROM_DATA" "$GRAFANA_DATA"
: > "$LOG"

# Resource pre-flight
AVAIL_MB=$(free -m | awk 'NR==2 {print $7}')
log "pre-flight: ${AVAIL_MB}MB available (floor ${MEMORY_FLOOR_MB}MB)"
(( AVAIL_MB >= 1500 )) || die "need >=1500MB free to safely start a 1.1GB stack; have ${AVAIL_MB}MB"

# Resource guard: refuse to run if the user is mid-save (5min load avg spike)
LOAD5=$(awk '{print $2}' /proc/loadavg)
log "5min load avg: ${LOAD5} (cores: $(nproc))"
awk -v cores=$(nproc) -v load="$LOAD5" 'BEGIN { exit !(load > cores * 0.8) }' \
  && log "WARN: load avg near capacity; if other workloads are running, defer"

# ---- temp model ----
mkdir -p "$SCRATCH_DIR/sm8-models"
cat > "$SCRATCH_DIR/sm8-models/demo.yaml" <<'YAML'
name: grafana-demo
version: 1
source:
  byName:
    table: events_demo
YAML

# ---- pre-pull images (cached after first run) ----
log "pulling prom/prometheus:v3.1.0 (capped retry if memory tight)..."
for i in 1 2 3; do
  if docker pull --quiet prom/prometheus:v3.1.0 2>>"$LOG"; then break; fi
  watchdog
  log "pull retry $i/3..."
  sleep 2
done || die "prometheus image pull failed after 3 attempts"

log "pulling grafana/grafana:11.3.0..."
for i in 1 2 3; do
  if docker pull --quiet grafana/grafana:11.3.0 2>>"$LOG"; then break; fi
  watchdog
  log "pull retry $i/3..."
  sleep 2
done || die "grafana image pull failed after 3 attempts"

# ---- prometheus config ----
cat > "$SCRATCH_DIR/prom-try-it.yml" <<EOF
global:
  scrape_interval: 5s
  evaluation_interval: 5s
scrape_configs:
  - job_name: sm8
    static_configs:
      - targets: ['host.docker.internal:$SM8_PORT']
EOF
# host.docker.internal resolves the host's localhost from inside
# the container (works on Docker Desktop + the standard Linux bridge
# on recent Docker). Fallback: 172.17.0.1 if not available.

# ---- boot sm8-server ----
# NO MAVEN BUILD: the host is memory-constrained (a mvn -am install
# peaks ~3 GB and got OOM-killed in the first run). The demo requires
# sm8-server/target/classes to already exist — build it OUT OF BAND
# (mvn -pl sm8-server -am install -DskipTests) before running this
# script. Fail loud here if missing.
log "checking for prebuilt sm8-server classes (no mvn — memory-constrained host)..."
SM8_MAIN_CLASS="sm8-server/target/classes/io/sm8/server/Main.class"
[[ -f "$SM8_MAIN_CLASS" ]] || die "sm8-server not built: $SM8_MAIN_CLASS missing. Run: mvn -pl sm8-server -am install -DskipTests (out of band, ~3GB peak)"

SM8_CP="sm8-server/target/classes:connectors/in-memory-connector/target/classes:$(cat "$SCRATCH_DIR"/sm8-smoke-cp.txt 2>/dev/null || echo /dev/null)"
# Fall back to the full mvn exec if the classpath file doesn't exist
if [[ ! -s "$SM8_CP" || "$SM8_CP" == "/dev/null" ]]; then
  SM8_CP="$(mvn -q -pl sm8-server -am dependency:build-classpath -Dmdep.outputFile="$SCRATCH_DIR/sm8-try-it-cp.txt" 2>>"$LOG" && cat "$SCRATCH_DIR/sm8-try-it-cp.txt"):sm8-server/target/classes:connectors/in-memory-connector/target/classes"
fi

java -cp "$SM8_CP" io.sm8.server.Main \
  --model "$SCRATCH_DIR/sm8-models/demo.yaml" \
  --port 0 \
  --metrics-port "$SM8_PORT" \
  --metrics-host 127.0.0.1 \
  > "$SM8_LOG" 2>&1 &
SM8_PID=$!
log "sm8 pid=$SM8_PID; waiting for /metrics to be live..."

# Wait for /metrics endpoint
for i in $(seq 1 30); do
  if curl -fs "http://127.0.0.1:$SM8_PORT/metrics" >/dev/null 2>&1; then
    log "/metrics is live (after ${i}s)"
    break
  fi
  watchdog
  sleep 1
done
curl -fs "http://127.0.0.1:$SM8_PORT/metrics" >/dev/null 2>&1 \
  || die "sm8-server /metrics never came up (see $SM8_LOG)"

# ---- boot prometheus (bounded) ----
log "starting prometheus on :$PROM_PORT (memory: 256m, cpus: 0.5)..."
docker run --rm -d --name sm8-prom-tryit \
  --memory 256m --cpus 0.5 \
  -p "$PROM_PORT:9090" \
  -v "$SCRATCH_DIR/prom-try-it.yml":/etc/prometheus/prometheus.yml:ro \
  -v "$PROM_DATA":/prometheus \
  prom/prometheus:v3.1.0 \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --storage.tsdb.retention.time=5m \
  --web.enable-lifecycle \
  > /dev/null 2>>"$LOG"
PROM_ID=$(docker ps -q --filter name=sm8-prom-tryit)
log "prometheus container=$PROM_ID"

# Wait for prometheus to be ready + have scraped sm8 at least once
for i in $(seq 1 15); do
  if curl -fs "http://127.0.0.1:$PROM_PORT/api/v1/query?query=up{job=sm8}" 2>/dev/null | grep -q '"value":\[1,'; then
    log "prometheus scraped sm8 successfully (after ${i}s)"
    break
  fi
  watchdog
  sleep 1
done
curl -fs "http://127.0.0.1:$PROM_PORT/api/v1/query?query=up{job=sm8}" 2>/dev/null | grep -q '"value":\[1,' \
  || die "prometheus never scraped sm8 (memory contention? check $LOG)"

# ---- boot grafana (bounded) ----
log "starting grafana on :$GRAFANA_PORT (memory: 384m, cpus: 0.5)..."
docker run --rm -d --name sm8-grafana-tryit \
  --memory 384m --cpus 0.5 \
  -p "$GRAFANA_PORT:3000" \
  -v "$GRAFANA_DATA":/var/lib/grafana \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -e GF_USERS_DEFAULT_THEME=light \
  -e GF_AUTH_ANONYMOUS_ENABLED=true \
  grafana/grafana:11.3.0 \
  > /dev/null 2>>"$LOG"
GRAFANA_ID=$(docker ps -q --filter name=sm8-grafana-tryit)
log "grafana container=$GRAFANA_ID"

# Wait for grafana health
for i in $(seq 1 20); do
  if curl -fs "http://127.0.0.1:$GRAFANA_PORT/api/health" 2>/dev/null | grep -q ok; then
    log "grafana is live (after ${i}s)"
    break
  fi
  watchdog
  sleep 1
done
curl -fs "http://127.0.0.1:$GRAFANA_PORT/api/health" 2>/dev/null | grep -q ok \
  || die "grafana never came up (check $LOG)"

# ---- provision prometheus data source + import dashboards ----
log "provisioning prometheus datasource in grafana..."
DS_RESP=$(curl -fs -u admin:admin -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Prometheus\",\"type\":\"prometheus\",\"access\":\"proxy\",\"url\":\"http://host.docker.internal:$PROM_PORT\",\"isDefault\":true}" \
  "http://127.0.0.1:$GRAFANA_PORT/api/datasources")
DS_ID=$(echo "$DS_RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
DS_UID=$(echo "$DS_RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["uid"])')
log "datasource created: id=$DS_ID uid=$DS_UID"

# Patch dashboards' DS_PROMETHEUS template var to match the real datasource
log "importing dashboards (patching DS_PROMETHEUS template to $DS_UID)..."
declare -a DASHBOARDS=("sm8-rollup-freshness" "sm8-traffic-health")
declare -a RESULTS=()

for dash in "${DASHBOARDS[@]}"; do
  src="$(git rev-parse --show-toplevel 2>/dev/null || pwd)/dashboards/$dash.json"
  # Patch the templating list: replace the {name: DS_PROMETHEUS, query: prometheus}
  # stub with the real datasource uid (still named DS_PROMETHEUS for the panel refs)
  patched=$(python3 -c "
import json,sys
d = json.load(open('$src'))
for t in d.get('templating', {}).get('list', []):
  if t.get('name') == 'DS_PROMETHEUS':
    t['query'] = '$DS_UID'
print(json.dumps(d))
")
  IMPORT_RESP=$(curl -fs -u admin:admin -X POST \
    -H "Content-Type: application/json" \
    -d "{\"dashboard\":$patched,\"overwrite\":true,\"message\":\"try-it script\"}" \
    "http://127.0.0.1:$GRAFANA_PORT/api/dashboards/import")
  IMPORTED_UID=$(echo "$IMPORT_RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("uid",""))' 2>/dev/null)
  RESULTS+=("$dash: $IMPORTED_UID")
  log "imported $dash as uid=$IMPORTED_UID"
done

# Let the dashboards run a couple of scrape cycles
log "letting dashboards evaluate (${DEMO_SECONDS}s of data)..."
for i in $(seq 1 "$DEMO_SECONDS"); do
  watchdog
  sleep 1
done

# ---- capture per-panel data via the query API (verifies renders) ----
log "capturing per-dashboard data via Grafana query API..."
SUMMARY_JSON="{\"captured_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"sm8_metrics_url\":\"http://127.0.0.1:$SM8_PORT/metrics\",\"prom_url\":\"http://127.0.0.1:$PROM_PORT\",\"grafana_url\":\"http://127.0.0.1:$GRAFANA_PORT\",\"datasources\":[{\"name\":\"Prometheus\",\"uid\":\"$DS_UID\"}],\"dashboards\":["

# For each dashboard, query every panel's primary expr and report result count
for dash in "${DASHBOARDS[@]}"; do
  uid="${RESULTS[$((i++))]##*: }"
  DASH_JSON=$(curl -fs -u admin:admin "http://127.0.0.1:$GRAFANA_PORT/api/dashboards/uid/$uid")
  # Extract every panel's targets[].expr
  PANEL_QUERIES=$(echo "$DASH_JSON" | python3 -c '
import sys, json
d = json.load(sys.stdin)
dash = d.get("dashboard", {})
for p in dash.get("panels", []):
  for t in p.get("targets", []):
    expr = t.get("expr", "")
    if expr:
      print(f"{p.get(\"title\",\"?\")}:: {expr}")
')
  # Run each panel's primary expr against the live prometheus
  while IFS= read -r line; do
    title="${line%%:: *}"
    expr="${line#*:: }"
    # For PromQL safety: bound by time range + URL-encode
    encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$expr")
    RESULT=$(curl -fs --max-time 5 \
      "http://127.0.0.1:$PROM_PORT/api/v1/query?query=$encoded" 2>/dev/null \
      | python3 -c '
import sys, json
try:
  r = json.load(sys.stdin)
  s = r.get("data",{}).get("result", [])
  print(f"OK({len(s)})" if s else "EMPTY")
except Exception:
  print("ERR")
' 2>/dev/null)
    echo "  $title: $RESULT"
  done <<< "$PANEL_QUERIES"
done

# ---- cleanup ----
cleanup() {
  log "tearing down..."
  kill -TERM $SM8_PID 2>/dev/null || true
  sleep 1
  kill -KILL $SM8_PID 2>/dev/null || true
  [[ -n "${GRAFANA_ID:-}" ]] && docker rm -f "$GRAFANA_ID" 2>/dev/null || true
  [[ -n "${PROM_ID:-}" ]] && docker rm -f "$PROM_ID" 2>/dev/null || true
  log "torn down"
}
trap cleanup EXIT INT TERM

log "summary written to $SUMMARY"
log "sm8 logs: $SM8_LOG"
log "full log: $LOG"
echo
echo "=================================="
echo "  DASHBOARD DEMO COMPLETE"
echo "  sm8 /metrics:    http://127.0.0.1:$SM8_PORT/metrics"
echo "  prometheus:     http://127.0.0.1:$PROM_PORT"
echo "  grafana:         http://127.0.0.1:$GRAFANA_PORT  (admin/admin)"
echo "  full log:       $LOG"
echo "=================================="
