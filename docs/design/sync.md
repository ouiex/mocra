> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Sync Module Design

## Overview
The `sync` module provides distributed synchronization primitives to coordinate state and actions across multiple nodes in a cluster. It enables leader election, shared state synchronization, and atomic updates.

## Core Components

### 1. Backend Abstraction (`MqBackend`)
A trait defining the necessary operations for distributed coordination:
- `get(key)` / `set(key, val)`: Key-Value storage.
- `publish(topic, msg)` / `subscribe(topic)`: Pub/Sub messaging.
- `acquire_lock(key, val, ttl)`: Atomic lock acquisition (e.g., Redis `SET NX`).
- `renew_lock(key, val, ttl)`: Extend lock duration.
- `cas(key, old, new)`: Compare-And-Swap for atomic state updates.

### 2. Synchronization Service (`SyncService`)
The main entry point for sharing state.
- **State Synchronization**: Keeps a local `DistributedSync<T>` updated with the global state stored in the backend.
- **Optimistic Locking**: `optimistic_update` allows safe concurrent modifications to shared state using CAS loops.
- **Hybrid Mode**: Supports both distributed (Redis/Kafka) and local (in-memory) modes transparently.
- **Local Store**: Local mode shares a process-wide store to keep instances consistent within a node.
- **Refresh Fallback**: Distributed subscribers periodically refresh from KV in case Pub/Sub messages are missed.

### 3. Distributed State (`DistributedSync<T>`)
A handle to a synchronized value of type `T`.
- Wraps a `tokio::sync::watch::Receiver`.
- Provides `get()` to access the current value.
- Provides `changed()` to await updates.

### 4. Leader Election (`LeaderElector`)
Ensures only one node acts as the leader for critical tasks (e.g., scheduling).
- **Mechanism**:
  - Tries to acquire a distributed lock (`key`) with a TTL.
  - If successful, becomes leader and periodically renews the lock.
  - If failed, waits and retries.
- **Signal**: Exposes a `watch::Receiver<bool>` to notify the application of leadership status changes.

## Data Flow
1. **State Update**: Node A calls `optimistic_update`.
2. **CAS Loop**: Service fetches current KV value, applies change, attempts CAS.
3. **Notification**: On success, Service publishes new value to Pub/Sub topic.
4. **Propagation**: All nodes (including A) receive Pub/Sub message.
5. **Local Update**: `DistributedSync` updates its internal `watch` channel.

## Usage
```rust
// 1. Initialize Service
let sync_service = SyncService::new(backend, "app".to_string());

// 2. Leader Election
let (elector, mut rx) = LeaderElector::new(backend.clone(), "leader_lock".into(), 5000);
tokio::spawn(async move { elector.start().await; });

// 3. Shared State
#[derive(Serialize, Deserialize, Clone)]
struct AppConfig { rate: u32 }
impl SyncAble for AppConfig { fn topic() -> String { "config".into() } }

let mut sync = sync_service.sync::<AppConfig>().await?;
// Wait for changes
sync.changed().await?;
println!("New rate: {}", sync.get().unwrap().rate);
```
