> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Node Registry Design

## Overview
The Node Registry tracks active nodes in the distributed crawler cluster using Redis.
It enables dynamic discovery of workers and monitoring of cluster health.

## Architecture

### NodeRegistry Component
The `NodeRegistry` is responsible for:
1.  **Registration**: Registering the current node with a unique ID (UUID).
2.  **Heartbeat**: Periodically sending heartbeats to Redis to maintain active status.
3.  **Discovery**: Listing all active nodes in the cluster.

### Implementation
- **Storage**: Redis is used as the backend.
- **Key Schema**: `nodes:<node_id>`
- **TTL**: Keys expire after a short duration (e.g., 30s) if not refreshed.
- **Protocol**: 
    - Nodes send heartbeat every 10s.
    - Heartbeat updates the key expiration.

### Integration
The `Engine` initializes a `NodeRegistry` at startup.
A background task runs the heartbeat loop, ensuring the node remains visible as long as the engine is running.
On shutdown, the node naturally expires from the registry (or can be explicitly removed).
