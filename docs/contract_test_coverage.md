# Contract Test Coverage

Date: 2026-02-10

## Scope

This report tracks P1 contract and failure scenario coverage across Queue, Sync, and Engine.

## Queue Coverage

- Redis duplicate delivery and out-of-order ack: [queue/src/tests.rs](queue/src/tests.rs)
- Redis retry exhaustion -> DLQ: [queue/src/tests.rs](queue/src/tests.rs)
- Redis consumer crash redelivery: [queue/src/tests.rs](queue/src/tests.rs)
- Redis concurrent out-of-order ack: [queue/src/tests.rs](queue/src/tests.rs)
- Kafka retry then ack: [queue/src/tests.rs](queue/src/tests.rs)
- Kafka out-of-order ack: [queue/src/tests.rs](queue/src/tests.rs)

Environment hints:
- Redis: set `REDIS_HOST`, `REDIS_PORT` when running locally.
- Kafka: set `KAFKA_BROKERS` (and optional `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TLS`).

## Sync Coverage

- Disallow rollback ignores older version: [sync/src/tests/test_sync_rollback.rs](sync/src/tests/test_sync_rollback.rs)
- Poison stream update ignored: [sync/src/tests/test_sync_rollback.rs](sync/src/tests/test_sync_rollback.rs)

## Engine Coverage

- Processor failure triggers Nack (poison message path): [engine/src/runner.rs](engine/src/runner.rs)

## Pending Gaps

- Redis multi-consumer contention edge cases beyond single subscriber.
- Full end-to-end poison decode path through actual chains and DLQ.
- CI coverage report publishing (artifact generation).
