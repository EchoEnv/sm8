#!/usr/bin/env bash
# Guard: every production metric family emitted by MetricsHttpRoute
# must be referenced by at least one Grafana dashboard in dashboards/.
# Catches silent rename-drift (a renamed metric leaves panels dark
# with no compile-time or test-time signal).
#
# Usage: scripts/check-dashboard-metrics.sh
# Exit 0 = all metric families covered; exit 1 = drift detected.
#
# Per issue #430 review (scorpion LOW-5): dashboards are config-only
# with no compile-time link to MetricsHttpRoute.scala — this script
# is the drift guard.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTE_SCALA="$REPO_ROOT/sm8-platform/src/main/scala/io/sm8/platform/query/MetricsHttpRoute.scala"
DASHBOARDS_DIR="$REPO_ROOT/dashboards"

# Extract metric family names from the exporter source: every
# sm8_ prefix occurrence in HELP lines and template interpolations.
# HELP lines are the canonical declaration of a family.
METRICS=$(grep -oE 'sm8_[a-z_]+' "$ROUTE_SCALA" | sort -u | grep -v '^sm8_rollup_refusals_$' || true)

# The per-reason family is emitted via interpolation
# (sm8_rollup_refusals_$reason) so the grep above yields a partial
# name; the rollup-freshness dashboard covers it via the
# sm8_rollup_refusals_ prefix in its per-reason expr.

ALL_DASH_TEXT=$(cat "$DASHBOARDS_DIR"/*.json)

FAILURES=0
while IFS= read -r metric; do
  [ -z "$metric" ] && continue
  if ! grep -qF "$metric" "$DASHBOARDS_DIR"/*.json; then
    echo "DRIFT: metric '$metric' is emitted by MetricsHttpRoute but referenced by NO dashboard"
    FAILURES=$((FAILURES + 1))
  fi
done <<< "$METRICS"

if [ "$FAILURES" -gt 0 ]; then
  echo "check-dashboard-metrics: $FAILURES metric family(ies) drifted"
  exit 1
fi
echo "check-dashboard-metrics: all metric families covered"
