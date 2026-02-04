> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Queue Design Document

## Overview
The Queue module provides a unified abstraction for message passing between system components, supporting both Redis and Kafka backends. It is designed for high throughput, reliability, and distributed scalability.

## Architecture

### Queue Abstraction
The system defines standard traits for Queue operations (`MqBackend`), allowing seamless switching between backends without changing business logic.

### Redis Queue
-   **Reliability**: Uses `XADD` (Streams) with Consumer Groups to ensure at-least-once delivery.
-   **Performance**: Uses Pipelines for batch operations (`publish_batch`) to minimize network round-trips.
-   **Persistence**: Supports disk persistence configurations in Redis.
-   **DLQ (Dead Letter Queue)**: Failed messages (after retries) are moved to a separate DLQ stream for inspection.
-   **Claimer**: A background `XAUTOCLAIM` process monitors for stuck messages (e.g., crashed workers) and re-queues them after `min_idle_time` (default 10 mins).

### Kafka Queue
-   **High Throughput**: Optimized for high-volume message streams.
-   **Consumer Groups**: Native Kafka consumer groups for load balancing.
-   **Batching**: Overrides `publish_batch` to use `futures::join_all` for parallel dispatch.

### QueueManager
Manages the `tokio::mpsc` channels that bridge the external queue (Redis/Kafka) and internal processors.
-   **Buffering**: Configurable in-memory buffer sizes (`channel_config.capacity`) to absorb bursts.
-   **Compensator**: Optional separate Redis-based compensator for legacy list-based queues (if used), ensuring task recovery.

## Configuration
The queue is configured via `config.toml` under `[channel]`:
-   `type`: "redis" or "kafka"
-   `capacity`: Internal channel buffer size.
-   `minid_time`: Message retention period (in minutes) for stream trimming and claimer idle check.
