> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Engine Design Document

## Overview
The Engine is the core orchestrator of the Mocra crawler framework. It manages the lifecycle of tasks, requests, and responses through a pipeline of processors, ensuring efficient, distributed, and resilient crawling.

## Architecture

The system is built on an event-driven, pipeline-based architecture, coordinated by an `EventBus` and `QueueManager`.

### Pipeline Stages
1.  **Task Processor**: Consumes `Task` -> Generates `Request`.
    -   Uses `TaskModelChain` to execute module-specific logic.
2.  **Download Processor**: Consumes `Request` -> Downloads content -> Generates `Response`.
    -   Uses `DownloadChain` or `WebSocketDownloadChain`.
    -   Supports Middleware (`before_request`, `after_response`).
3.  **Response Processor**: Consumes `Response` -> Parses content -> Generates `ParserTask`.
    -   Uses `ParserChain` to execute module-specific parsing logic.
4.  **Parser Processor**: Consumes `ParserTask` -> Extracts data/new tasks.
    -   Uses `ParserTaskChain`.
5.  **Error Processor**: Handles failed tasks and retries.
    -   Moves permanently failed tasks to DLQ.

## Components

### Event Bus
A centralized `EventBus` handles system events (Startup, Shutdown, HealthCheck, Metrics). It supports pluggable handlers:
-   `LogEventHandler`: Structured JSON logging.
-   `MetricsEventHandler`: Prometheus metrics.
-   `RedisEventHandler`/`DbEventHandler`: Persistence of event logs.

### Middleware Manager
Manages three types of middleware:
-   `DownloadMiddleware`: Intercepts requests/responses (e.g., Signatures, Auth).
-   `DataMiddleware`: Processes extracted data (e.g., Cleaning, Validation).
-   `DataStoreMiddleware`: Handles data persistence (e.g., MySQL, ElasticSearch).

### Task Manager
Registers and manages functional `Module`s which define the crawling logic (URL generation, Parsing rules).

### Scheduler
-   **CronScheduler**: Handles distributed scheduled tasks using Redis for coordination and Leader Election to prevent duplicate triggers.

### Node Registry
Each Engine instance registers itself in Redis with a UUID and sends heartbeats. This allows the Control Plane to track active nodes.

### Control Plane
Exposes HTTP APIs for management:
-   `GET /metrics`: Prometheus metrics.
-   `GET /api/nodes`: List active nodes.
-   `POST /api/control/pause`: Global pause.
-   `POST /api/control/resume`: Global resume.
-   `GET /api/dlq`: Inspect failed tasks.

## Concurrency & Safety
-   **Processors**: Run in concurrent Tokio tasks, supervised for panic recovery.
-   **Channels**: Communicate via `QueueManager` (Redis/Kafka backed).
-   **Graceful Shutdown**: Handles SIGINT/SIGTERM, draining active tasks before exit.
-   **Idempotency**: Parser tasks are deduplicated using Redis to ensure exactly-once processing side-effects where possible.
