> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Common Module Design

## Overview
The `common` module serves as the foundational layer of the framework, providing shared data structures, interfaces, and state management logic used by all other crates (`engine`, `downloader`, `queue`, etc.).

## Core Components

### 1. Global State Management (`State`)
The `State` struct is the central context passed around the application. It initializes and manages connection pools and shared services:
- **Database**: `sea-orm` connection to PostgreSQL.
- **Configuration**: Dynamic configuration via `RwLock<Config>` with hot-reload support.
- **Caching**: `CacheService` instances for general cache and cookies.
- **Concurrency Control**: `DistributedLockManager` (Redis-based) and `DistributedSlidingWindowRateLimiter`.
- **Status Tracking**: `StatusTracker` for monitoring task health and error rates.

### 2. Configuration System (`config`)
Abstracts configuration loading and watching.
- **`ConfigProvider` Trait**: Defines methods to load and watch for changes.
- **Implementations**:
  - `FileConfigProvider`: Loads from `config.toml`, polls for changes.
  - `RedisConfigProvider`: Loads from Redis key, subscribes to Pub/Sub for updates.

### 3. Service Registry (`registry`)
Implements a lightweight service discovery mechanism using Redis.
- Nodes register themselves with a unique ID and metadata upon startup.
- Periodic heartbeats maintain the registration (TTL-based).
- Allows the Control Plane to list active nodes and their status.

### 4. Interfaces & Traits (`interface`)
Defines the contracts that ensure modularity.
- **`Module`**: The core unit of work (e.g., specific crawler logic).
- **`Middleware`**: Intercepts request/response/data flow.
- **`MiddlewareManager`**: Orchestrates the execution of middleware chains.

### 5. Status Tracking (`status_tracker`)
Provides distributed tracking of task processing status.
- Counts errors per task/module.
- Implements exponential backoff logic (via `get_retry_delay`).
- Decides when a task should be moved to the Dead Letter Queue (DLQ) based on max retries.
- Uses Redis to persist error counts across restarts/nodes.

## Data Models (`model`)
Contains shared DTOs (Data Transfer Objects) and database entities:
- **`TaskModel`**: Represents a unit of work.
- **`LogModel`**: Structure for logging events to the database.
- **`Config`**: The mapped structure of the configuration file.

## Design Philosophy
- **Statelessness**: Logic should be stateless where possible; state is stored in Redis/Postgres.
- **Dependency Injection**: `State` is injected into processors and handlers.
- **Asynchrony**: deeply integrated with `tokio` and `async-trait`.
