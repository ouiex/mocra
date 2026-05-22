# HTTP API

The HTTP API is enabled through the `[api]` configuration section. It exposes runtime health, metrics, task dispatch, configuration, DLQ, debug, and control endpoints.

## Authentication

Public endpoints:

- `GET /metrics`
- `GET /health`

All other endpoints are protected by the configured API key.

## Health and Metrics

```text
GET /health
GET /metrics
```

Use these endpoints for load balancers, readiness probes, and metrics scraping.

## Task Dispatch

```text
POST /tasks/dispatch
```

Dispatches work into the runtime through the configured task/queue path.

## Cluster

```text
GET /cluster/nodes
GET /cluster/leader
```

Returns Raft/control-plane cluster information when the deployment is configured for it.

## Configuration

```text
/config/accounts
/config/platforms
/config/modules
/config/middlewares
/config/profiles
```

These endpoints manage runtime configuration models used by task/profile resolution.

## DLQ

```text
GET /dlq/messages
POST /dlq/messages/{id}/requeue
DELETE /dlq/messages/{id}
```

Use DLQ endpoints to inspect failed messages, requeue recoverable messages, or delete messages that should not be retried.

## Debug

```text
/debug/status/*
/debug/cache/response/{cache_key}
/debug/config/{account}/{platform}/{module}
/debug/profile/{account}/{platform}/{module}
```

Debug endpoints are intended for inspecting runtime status, cached responses, resolved config, and resolved profiles.

## Control

```text
/control/pause
/control/resume
/control/fallback-gates/{module}
```

Use control endpoints for operational pause/resume and module fallback-gate inspection or updates.

