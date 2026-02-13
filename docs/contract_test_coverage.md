# Contract Test Coverage

Date: 2026-02-13

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

- Redis multi-consumer contention edge cases beyond current script-level coverage.
- Full end-to-end poison decode path through actual chains and DLQ.
- CI coverage report publishing (artifact generation).

## CI Gate (Optional Redis)

Contract test scripts now include an optional Engine Redis integration gate:

- PowerShell: [scripts/ci_contract_tests.ps1](scripts/ci_contract_tests.ps1)
- Bash: [scripts/ci_contract_tests.sh](scripts/ci_contract_tests.sh)

Behavior:

- Always run Queue + Sync contract tests.
- Run `cargo test -p engine redis_ -- --nocapture` only when one of these env vars is set:
	- `REDIS_URL`
	- `MOCRA_REDIS_TEST_URL`
- If both are missing, Redis integration tests are explicitly skipped.

Recommended CI snippet:

- Linux/macOS runners:
	- `export REDIS_URL=redis://:password@redis-host:6379/0`
	- `bash scripts/ci_contract_tests.sh`
- Windows runners:
	- `$env:REDIS_URL = "redis://:password@redis-host:6379/0"`
	- `pwsh -File scripts/ci_contract_tests.ps1`
