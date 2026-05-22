# Operations

This document covers the operational model for applications embedding `mocra`.

## Local Development

Use local cache and omit Kafka/Raft sections for the simplest development setup.

```toml
[cache]
ttl = 3600
backend = "local"

[channel_config]
minid_time = 0
capacity = 1024
queue_codec = "msgpack"
```

Run the application binary that embeds `Engine`. Register modules before `engine.start().await`.

## Kafka Queue Deployment

Add `channel_config.kafka` when queue transport should use Kafka.

Operational requirements:

- Kafka brokers must be reachable from every runtime node.
- Topic naming and envelope routing must be consistent across nodes.
- `queue_codec` should be the same for all nodes in one deployment.
- DLQ monitoring should be enabled for production queues.

## Raft/RocksDB Coordination

Add `[raft]` and use `cache.backend = "raft_rocksdb"` when distributed coordination and state should be backed by Raft/RocksDB.

Operational requirements:

- `name` identifies the namespace.
- `raft.addr` must be reachable by peer nodes.
- `raft.peers` should contain at least one reachable peer when joining an existing group.
- `raft.data_dir` should be persisted.
- Heartbeat and election timeouts should reflect network latency.

## HTTP API

Enable `[api]` for operational endpoints. Keep `api_key` private and rotate it through the application deployment process.

Use:

- `/health` for health checks;
- `/metrics` for scraping;
- `/dlq/messages` for failure inspection;
- `/control/pause` and `/control/resume` for runtime control.

## Monitoring

Monitor at least:

- process health;
- request and parser throughput;
- queue lag;
- DLQ size;
- retry counts;
- module error counts;
- Raft leader and peer health when Raft is enabled.

## Failure Handling

Failed queue messages can move to DLQ depending on `nack_max_retries` and `nack_backoff_ms`. Inspect DLQ messages before requeueing them.

For parser or module failures, first identify the module name, node key, run ID, and profile/config version involved in the failure.

## Upgrade Notes

Upgrade applications by validating:

- `cargo check`;
- module DAG compilation;
- request generation tests;
- parser output tests;
- config loading from production TOML;
- startup with the target queue/cache/control-plane settings.

Do not carry Redis configuration into new deployments.

