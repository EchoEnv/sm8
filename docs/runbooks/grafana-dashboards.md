# Runbook — Grafana dashboards for SM8

Two pre-built Grafana dashboards ship in `dashboards/`:

| File | Shows |
|---|---|
| `dashboards/rollup-freshness.json` | Per-rollup FRESH/STALE verdict, age since last refresh, watermark bucket counts, refusal totals + per-reason breakdown, freshness-probe failure signal |
| `dashboards/traffic-health.json` | Invocation rate (total/succeeded/failed), process uptime, cache hit ratio, counters since startup, errors by type |

Both chart the Prometheus `/metrics` endpoint that sm8-server exposes
on its own port (default 9090) — see `--metrics-port`.

---

## Prerequisites

1. **sm8-server is running with `--metrics-port <n>`** (default 9090).
   Verify: `curl http://127.0.0.1:9090/metrics` returns Prometheus
   text (`# HELP sm8_invocation_total ...`).

2. **A Prometheus server scrapes that endpoint.** Minimal
   `prometheus.yml` job:

   ```yaml
   scrape_configs:
     - job_name: sm8
       scrape_interval: 15s
       static_configs:
         - targets: ['<sm8-host>:9090']
   ```

   ⚠️ **Bind-host prerequisite (#429):** since the `--metrics-host`
   fix, sm8-server binds the metrics endpoint to **127.0.0.1** by
   default. A Prometheus running on a DIFFERENT host needs sm8 started
   with `--metrics-host 0.0.0.0` (or the specific interface address).
   Same for Docker/K8s port-maps. This default was a deliberate
   security change — see
   `docs/release-notes/pr429-metrics-host-loopback-default.md`.

3. **Grafana 10.x or newer** with a Prometheus data source configured.

---

## Import

1. Grafana → **Dashboards → New → Import**.
2. **Upload JSON file** → pick `dashboards/rollup-freshness.json`
   (repeat for `traffic-health.json`).
3. At the import prompt, select your **Prometheus data source** for
   the `DS_PROMETHEUS` dropdown.
4. **Import**. The dashboards appear under the SM8 tag.

Set the default time range to your liking (ships as *last 6 hours*);
both dashboards refresh every 30s.

---

## Panels reference

### rollup-freshness.json

| Panel | Metric | Notes |
|---|---|---|
| Rollup freshness (stat grid) | `sm8_rollup_freshness` | 1 = every bucket `is_final`, 0 = some bucket still non-final |
| Age since last refresh | `sm8_rollup_freshness_age_seconds` | NaN renders as "never refreshed". Panel thresholds (yellow ≥ 1h, red ≥ 24h) are defaults, not an SLA — tune to your freshness expectation |
| Freshness probe (stat) | `sm8_rollup_freshness_probe_failed` | 1 = the SERVER-side freshness reader failed — check server logs; distinct from per-rollup staleness |
| Watermark bucket count | `sm8_rollup_freshness_buckets` | Growth = new buckets refreshed; drop = reset |
| Rewrites / refusals | `sm8_rollup_rewrites_total`, `sm8_rollup_refusals_total`, `sm8_rollup_refusals_permanent_total` | Since startup |
| Refusals by reason | `sm8_rollup_refusals_<reason>` family (matched by `__name__` prefix, prefix stripped in the legend) | Reasons are the `RollupRewriteRefusal` case names rendered as snake_case metric suffixes (`noGroupSetMatch` → `_noGroupSetMatch`, `rollupSchemaStale` → `_rollupSchemaStale`, …) |

### traffic-health.json

| Panel | Metric | Notes |
|---|---|---|
| Invocation rate | `rate(sm8_invocation_*[5m])` | total / succeeded / failed |
| Process uptime | `sm8_process_uptime_seconds` | Sawtooth = restarts |
| Process started at | `sm8_process_start_time_seconds * 1000` | Wall-clock correlation with logs |
| Cache hit ratio | `sum(hits) / (sum(hits) + sum(misses))` | Global; NaN before the first cache read |
| Counters since startup | `sm8_invocation_total`, `sm8_cache_*`, `sm8_rollup_rewrites_total` | Totals, not rates |
| Errors by reason | `sm8_error_audit_sink_unavailable_total`, `sm8_error_timed_out_total` | Both counted in `invocations.failed` |

---

## Troubleshooting

**Dashboard imports but panels show "No data"**

- Check the data source: Grafana → Connections → Data sources → your
  Prometheus → **Save & test**.
- Check the scrape: Prometheus → Status → Targets → the `sm8` job
  should be UP with a recent last-scrape time.
- Check the bind: if the Prometheus target is remote and the server
  stderr says `metrics endpoint listening on 127.0.0.1:9090`, the
  bind-host default is the cause — restart sm8-server with
  `--metrics-host 0.0.0.0` (#429).

**Freshness panels empty, traffic panels have data**

- The freshness family requires the spark-connector on the classpath
  AND a model with declared rollups. A deployment without rollups
  reports no freshness gauges by design (absent ≠ all-fresh).

**`sm8_rollup_freshness_probe_failed = 1`**

- The server-side freshness reader failed (Iceberg table missing,
  Spark session down, connector JAR absent). Check the server stderr
  for the reason line (`sm8: freshness reader failed: ...`). The
  gauge stays 1 until a probe succeeds — it is a latched signal, not
  a counter.

**`rollup-status` CLI vs these dashboards**

- `sm8 rollup-status` reads the same gauges and renders a point-in-time
  text table — use it for a quick check; use Grafana for history and
  alerting.
---


## Metric-name drift guard

`scripts/check-dashboard-metrics.sh` fails if a metric family emitted
by `MetricsHttpRoute` is not referenced by any dashboard JSON — run it
after renaming any `sm8_*` metric. (Renaming a metric without updating
the dashboards leaves panels dark with no runtime error.)
