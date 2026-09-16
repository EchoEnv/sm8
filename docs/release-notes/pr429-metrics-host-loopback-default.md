# Breaking changes

## #429 — Metrics endpoint binds loopback by default (`--metrics-host` added)

The Prometheus `/metrics` endpoint previously bound `0.0.0.0` (all
interfaces) with **no authentication**. It now defaults to `127.0.0.1`
(loopback). Remote scraping requires an explicit opt-in.

### What breaks

1. **Remote Prometheus scrapers stop receiving metrics** after
   upgrade: the scraper connects to a host interface that no longer
   serves the port. Migration: pass `--metrics-host 0.0.0.0` (or the
   specific interface address) to `sm8-server`.
2. **Docker/K8s port mappings** (`-p 9090:9090`, hostPort
   declarations) stop carrying traffic for the same reason — the
   process inside the container no longer listens on the container's
   external interface. Migration: add `--metrics-host 0.0.0.0` to the
   container command.

### Why

The endpoint carries no authentication (counters, refusal reasons,
freshness timestamps). Defaulting to all-interfaces was an accidental
recon surface on multi-tenant or internet-adjacent hosts. Loopback by
default + explicit opt-in is the standard posture for unauthenticated
observability endpoints.

### Detection

After upgrade, a broken scraper shows `sm8: metrics endpoint listening
on 127.0.0.1:9090` in the server stderr while the scraper reports
connection refused / timeout. The banner now includes the bind host
precisely so this is greppable.
