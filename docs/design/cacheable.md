> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Cacheable Module Design

## Overview
The `cacheable` module provides a unified interface for caching objects, supporting both local in-memory storage (for testing/single-node) and Redis storage (for distributed production environments). It includes features like automatic serialization, compression, and expiration.

## Core Components

### `CacheBackend` Trait
Abstracts the underlying storage engine.
- `get(key)`: Retrieve raw bytes.
- `set(key, value, ttl)`: Store raw bytes with optional TTL.
- `del(key)`: Delete a key.
- `keys(pattern)`: List keys matching a pattern.
- `set_nx(key, value, ttl)`: Set only if not exists (atomic lock primitive).

### Implementations
1. **`LocalBackend`**:
   - Uses `DashMap` for thread-safe in-memory storage.
   - Handles manual expiration checking on retrieval.
   - Useful for unit tests and local development.
   
2. **`RedisBackend`**:
   - Uses `deadpool_redis` for connection pooling.
   - Supports transparent Gzip compression for values exceeding a configurable threshold.
   - Leverages Redis native TTL and atomic operations.

### `CacheService`
The main entry point for the application.
- Manages the `CacheBackend` instance.
- Handles namespace scoping for keys (format: `namespace:field:id`).
- Configures default TTL and compression settings.

### `CacheAble` Trait
A trait for structs that need to be cached.
- Requires `Serialize` and `Deserialize`.
- Provides high-level methods:
  - `send(id, service)`: Cache the object.
  - `sync(id, service)`: Retrieve the object.
  - `delete(id, service)`: Remove the object.
  - `scan(pattern, service)`: Find related keys.

## Data Flow
1. **Serialization**: Object is serialized to JSON.
2. **Compression**: If using `RedisBackend` and size > threshold, data is Gzipped.
3. **Storage**: Data is stored in backend with `namespace:field:id` key.
4. **Retrieval**: Data is fetched, decompressed (if Gzip header detected), and deserialized.

## Usage
Implement `CacheAble` for your struct:
```rust
#[derive(Serialize, Deserialize)]
struct UserSession {
    user_id: String,
    token: String,
}

impl CacheAble for UserSession {
    fn field() -> impl AsRef<str> {
        "session"
    }
}
```
Use `CacheService` to store/retrieve:
```rust
let service = CacheService::new(pool, "app".to_string(), None, None);
let session = UserSession { ... };
session.send("user123", &service).await?;
```
