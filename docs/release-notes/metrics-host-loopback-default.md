# Breaking changes

## Metrics endpoint binds loopback by default (`--metrics-host` added)

The Prometheus `/metrics` endpoint previously bound `0.0.0.0` (all
interfaces) with no authentication. It now binds `127.0.0.1` by
default; network exposure is opt-in.

### What breaks

1. **Remote Prometheus scrapers stop receiving metrics after upgrade.**
   A Prometheus configured to scrape `sm8-host:9090` over the network
   will get connection-refused, because the endpoint is loopback-only.
2. **Docker/K8s port mappings of 9090 stop working** unless the
   container process itself binds `0.0.0.0` — the default no longer
   does.

### Migration

Pass the new flag explicitly at sm8-server startup:

```
sm8-server ... --metrics-host 0.0.0.0
```

(or bind to a specific interface address, e.g.
`--metrics-host 192.168.1.10`, for a narrower exposure.)

`--metrics-host` accepts any hostname or IP literal that Vert.x
`HttpServerOptions.setHost` accepts (the value is passed through to
the bind call); an unresolvable name fails loudly at boot with the
existing 30-second bind-timeout `IllegalStateException`.

### Why

The endpoint carries no authentication (documented in
ADR-012-b-export). Binding loopback by default makes the secure
posture the default and network exposure an explicit, auditable
choice.
