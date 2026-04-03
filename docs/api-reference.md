# API Reference

mocra includes a built-in HTTP control plane powered by [Axum](https://github.com/tokio-rs/axum). Enable it by adding an `[api]` section to your `config.toml`.

## Configuration

```toml
[api]
port = 8080
api_key = "your-secret-key"
rate_limit = 100        # requests per window
rate_limit_window = 60  # seconds
```

## Authentication

Protected routes require an API key sent as:

- **Header:** `Authorization: Bearer <api_key>` or `x-api-key: <api_key>`

The health and metrics endpoints do not require authentication (metrics is rate-limited).

## Endpoints

### GET /health

Health check endpoint.

**Auth:** None  
**Rate-limited:** No

**Response:**
```json
{
  "status": "ok"
}
```

---

### GET /metrics

Prometheus metrics in text exposition format.

**Auth:** None  
**Rate-limited:** Yes

**Response:**
```
# HELP mocra_requests_total Total requests processed
# TYPE mocra_requests_total counter
mocra_requests_total{module="my_module"} 1234
...
```

---

### POST /start_work

Inject a manual task into the engine's processing queue.

**Auth:** Required  
**Rate-limited:** Yes

**Request body:**
```json
{
  "module": "my_module",
  "platform": "example",
  "account": "demo",
  "meta": {
    "custom_key": "custom_value"
  }
}
```

The body is a `TaskEvent` JSON object. At minimum, specify the `module` name.

**Response:** `200 OK` (no body)

---

### GET /nodes

List active nodes in the cluster (distributed mode).

**Auth:** Required  
**Rate-limited:** Yes

**Response:**
```json
[
  {
    "node_id": "node-abc123",
    "last_heartbeat": "2024-01-15T10:30:00Z",
    "modules": ["module_a", "module_b"]
  }
]
```

---

### GET /dlq

Inspect the Dead Letter Queue.

**Auth:** Required  
**Rate-limited:** Yes

**Response:** Returns DLQ entries with failed task details.

---

### POST /control/pause

Pause the global engine. All processor runners will stop consuming from queues.

**Auth:** Required  
**Rate-limited:** Yes

**Response:** `200 OK`

---

### POST /control/resume

Resume a paused engine.

**Auth:** Required  
**Rate-limited:** Yes

**Response:** `200 OK`

## Rate Limiting

All routes except `/health` are rate-limited. The rate limiter uses a sliding window based on the client IP.

Configure via:
```toml
[api]
rate_limit = 100        # max requests per window
rate_limit_window = 60  # window duration in seconds
```
