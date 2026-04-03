# Deployment Guide

This guide covers deploying mocra in single-node and distributed modes.

## Single-Node Mode

The simplest deployment — no external dependencies beyond a database.

### When to Use

- Development and testing
- Low-volume data collection
- Standalone scripts

### Setup

1. Use a local database (SQLite or PostgreSQL):
   ```toml
   [db]
   url = "sqlite://data/crawler.db?mode=rwc"
   ```

2. Do **not** configure Redis:
   ```toml
   [cache]
   ttl = 60
   # no redis = {} section
   ```

3. Run a single instance:
   ```bash
   cargo run --release
   ```

### What Happens

- Queues use in-process Tokio mpsc channels
- Cache is in-memory (no persistence across restarts)
- No distributed locks — single-process concurrency only
- All modules run in one process

## Distributed Mode

Enable distributed mode by configuring Redis. The engine auto-detects this from `cache.redis`.

### When to Use

- High-volume data collection
- Multiple worker processes
- Cross-node deduplication and rate-limiting
- Fault tolerance with queue persistence

### Setup

1. Configure Redis:
   ```toml
   [cache]
   ttl = 60

   [cache.redis]
   url = "redis://redis-host:6379"
   ```

2. Optionally configure Redis Streams as the queue backend:
   ```toml
   [channel_config]
   capacity = 5000
   minid_time = 12

   [channel_config.redis]
   url = "redis://redis-host:6379"
   ```

3. Or use Kafka:
   ```toml
   [channel_config.kafka]
   brokers = "kafka-host:9092"
   ```

4. Run multiple instances (same config):
   ```bash
   # Node 1
   cargo run --release

   # Node 2 (same binary, same config.toml)
   cargo run --release
   ```

### What Happens

- Queues use Redis Streams or Kafka with consumer groups
- Cache is Redis-backed (shared across nodes)
- Distributed locks via Redis for rate-limiting and deduplication
- Node heartbeats and cluster awareness
- Automatic work distribution across nodes

## Queue Backend Comparison

| Feature | Local (Tokio) | Redis Streams | Kafka |
|---|---|---|---|
| Setup complexity | None | Low | Medium |
| Persistence | No | Yes | Yes |
| Multi-process | No | Yes | Yes |
| Ordering | FIFO | FIFO per stream | Per partition |
| DLQ support | Limited | Full | Full |
| Back-pressure | Channel capacity | Stream trimming | Consumer lag |
| Encoding | In-memory | MsgPack / JSON | MsgPack / JSON |

## Queue Encoding

Remote queues default to **MsgPack** for efficiency. Switch to JSON for debugging:

```toml
[channel_config]
queue_codec = "json"  # "msgpack" (default) or "json"
```

## Compression

Large payloads are automatically compressed with **zstd** when they exceed the configured threshold. Configure via `channel_config` settings.

## Monitoring

### Prometheus Metrics

Enable the API server and scrape `/metrics`:

```toml
[api]
port = 8080
api_key = "secret"
```

Metrics include:
- Request counts per module
- Error rates
- Queue depths
- Download latencies
- Active worker counts

### Health Check

Use `/health` for load balancer health probes:

```bash
curl http://localhost:8080/health
```

### Logging

mocra uses the `log` crate. Configure with `RUST_LOG`:

```bash
RUST_LOG=mocra=info,warn cargo run
```

Or for more detail:
```bash
RUST_LOG=mocra=debug cargo run
```

## Docker Compose

The repository includes Docker Compose files for supporting services:

- **`docker-compose.kafka.yml`** — Kafka + Zookeeper for Kafka queue backend
- **`docker-compose.monitoring.yml`** — Prometheus + Grafana for monitoring

```bash
# Start Kafka
docker compose -f docker-compose.kafka.yml up -d

# Start monitoring stack
docker compose -f docker-compose.monitoring.yml up -d
```

## Production Checklist

- [ ] Use PostgreSQL (not SQLite) for metadata storage
- [ ] Configure Redis for distributed mode
- [ ] Set `api.api_key` to a strong secret
- [ ] Configure `RUST_LOG` for appropriate log level
- [ ] Set `request_max_retries` and `module_max_errors` in `[crawler]`
- [ ] Monitor `/metrics` with Prometheus/Grafana
- [ ] Use `/health` for load balancer health checks
- [ ] Configure appropriate `rate_limit` in `[download_config]`
- [ ] Enable `mimalloc` feature for production builds (default)
