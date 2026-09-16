#!/usr/bin/env bash
# Drift guard: every production metric family emitted by
# MetricsHttpRoute must be referenced by at least one shipped Grafana
# dashboard. Catches silent panel death when the platform renames a
# metric — the failure mode the #430 review flagged (config has no
# compile-time link to the code).
#
# Usage: scripts/check-dashboard-metric-drift.sh
# Exit 0 = no drift; exit 1 = drift (missing/renamed metrics listed).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROUTE_SCALA="$REPO_ROOT/sm8-platform/src/main/scala/io/sm8/platform/query/MetricsHttpRoute.scala"
DASHBOARDS_DIR="$REPO_ROOT/dashboards"

[ -f "$ROUTE_SCALA" ] || { echo "FAIL: $ROUTE_SCALA not found" >&2; exit 1; }

# Extract the static (non-labeled) metric names the exporter emits.
# The exporter builds its body as a Scala triple-quoted string with
# $-interpolations; the metric names appear as literal tokens after
# HELP/TYPE or on their own lines. We grep the sm8_ tokens and strip
# the per-reason dynamic suffix family separately.
EMITTED=$(grep -oE 'sm8_[a-z_]+' "$ROUTE_SCALA" | sort -u)

# Extract the metric names referenced by the dashboards (exprs +
# descriptions). Same token grep.
REFERENCED=$(cat "$DASHBOARDS_DIR"/*.json | grep -oE 'sm8_[a-z_]+' | sort -u)

# The per-reason family is dynamic: sm8_rollup_refusals_<reason> —
# the dashboards match it via the __name__ prefix regex, so the base
# token counts as covered.
MISSING=""
for m in $EMITTED; do
  # skip the per-reason suffix tokens: the base family
  # (sm8_rollup_refusals_total / _permanent_total) is the static part
  case "$m" in
    sm8_rollup_refusals_) ;; # grep artifact (trailing underscore)
    sm8_rollup_refusals_total|sm8_rollup_refusals_permanent_total) ;;
    sm8_rollup_refusals_*) continue ;; # dynamic per-reason names
  esac
  if ! grep -q "$m" <<< "$REFERENCED"; then
    MISSING="$MISSING $m"
  fi
done

if [ -n "$MISSING" ]; then
  echo "DASHBOARD DRIFT: emitted metric(s) not referenced by any dashboard:" >&2
  for m in $MISSING; do echo "  - $m" >&2; done
  echo "Fix: add a panel/query for each metric, or update dashboards/*.json." >&2
  exit 1
fi

echo "OK: all emitted sm8_* metric families are referenced by dashboards"
