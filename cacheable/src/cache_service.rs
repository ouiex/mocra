
use dashmap::DashMap;
use deadpool_redis::Pool;
use errors::CacheError;
use std::sync::Arc;
use std::time::{Duration, Instant};
// once_cell removed: global singleton not used
use deadpool_redis::redis::AsyncCommands;
use serde_json;
use serde::{Serialize, Deserialize};
use log::{debug, warn};
use metrics::{counter, histogram};


#[async_trait::async_trait]
pub trait CacheBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;
    async fn del(&self, key: &str) -> Result<(), CacheError>;
    /// Batch delete multiple keys using pipeline (more efficient than individual deletes)
    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError>;
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError>;
    /// Scan keys with a limit to prevent memory exhaustion and long blocking
    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError>;
    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError>;
    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError>;
    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError>;
    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError>;
    async fn ping(&self) -> Result<(), CacheError>;
    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError>;
    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError>;
    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError>;
}

struct LocalBackend {
    store: DashMap<String, (Vec<u8>, Option<Instant>)>,
    zstore: DashMap<String, Vec<(f64, Vec<u8>)>>,
}

impl LocalBackend {
    fn new() -> Self {
        Self {
            store: DashMap::new(),
            zstore: DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl CacheBackend for LocalBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        if let Some(entry) = self.store.get(key) {
            let (val, expires_at) = entry.value();
            if let Some(exp) = expires_at {
                 if Instant::now() > *exp {
                    // Lazy deletion: found expired item, remove it.
                    // Just removing is simpler and arguably safer than remove_if logic for now 
                    // unless we are super strict about precise millisecond races.
                    drop(entry);
                    self.store.remove(key);
                    return Ok(None);
                 }
            }
            return Ok(Some(val.clone()));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.store
            .insert(key.to_string(), (value.to_vec(), expires_at));
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        self.store.remove(key);
        Ok(())
    }

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        let mut count = 0u64;
        for key in keys {
            if self.store.remove(*key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        // Default to no limit
        self.keys_with_limit(pattern, usize::MAX).await
    }

    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        let prefix = pattern.trim_end_matches('*');
        let now = Instant::now();
        let mut keys = Vec::new();

        for entry in self.store.iter() {
            if keys.len() >= limit {
                break;
            }
            let key = entry.key();
            let (_, expires_at) = entry.value();
            if let Some(exp) = expires_at {
                if now > *exp {
                    // Lazily remove expired entries instead of tracking a separate list.
                    let key_to_remove = key.clone();
                    drop(entry);
                    self.store.remove(&key_to_remove);
                    continue;
                }
            }
            if key.starts_with(prefix) {
                keys.push(key.clone());
            }
        }
        Ok(keys)
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        let now = Instant::now();
        let expires_at = ttl.map(|d| now + d);
        
        let entry = self.store.entry(key.to_string());
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let (_, old_expires_at) = occupied.get();
                if let Some(exp) = old_expires_at {
                    if now < *exp {
                        // Exists and valid
                        return Ok(false);
                    }
                } else {
                    // Exists and no expiry (permanent)
                    return Ok(false);
                }
                
                // Exists but expired, replace it
                occupied.insert((value.to_vec(), expires_at));
                Ok(true)
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert((value.to_vec(), expires_at));
                Ok(true)
            }
        }
    }

    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.set_nx(key, value, ttl).await?);
        }
        Ok(results)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        // Simple atomic-like emulation for DashMap
        // We assume value is stored as string representation of i64 or raw bytes of i64?
        // Let's assume text for compatibility with Redis default which treats numbers as strings
        let entry = self.store.entry(key.to_string());
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let (val, _) = occupied.get();
                // Try parse as string
                let s = String::from_utf8(val.clone()).unwrap_or_default();
                let current = s.parse::<i64>().unwrap_or(0);
                let new_val = current + delta;
                occupied.insert((new_val.to_string().into_bytes(), None));
                Ok(new_val)
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert((delta.to_string().into_bytes(), None));
                Ok(delta)
            }
        }
    }

    async fn ping(&self) -> Result<(), CacheError> {
        Ok(())
    }

    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        let mut entry = self.zstore.entry(key.to_string()).or_insert(Vec::new());
        let vec = entry.value_mut();
        
        // Remove existing member if any
        let mut removed = false;
        if let Some(pos) = vec.iter().position(|(_, m)| m == member) {
            vec.remove(pos);
            removed = true;
        }
        
        vec.push((score, member.to_vec()));
        // Sort by score
        vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(if removed { 0 } else { 1 })
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        if let Some(entry) = self.zstore.get(key) {
             let vec = entry.value();
             let res = vec.iter()
                 .filter(|(s, _)| *s >= min && *s <= max)
                 .map(|(_, m)| m.clone())
                 .collect();
             return Ok(res);
        }
        Ok(Vec::new())
    }

    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        if let Some(mut entry) = self.zstore.get_mut(key) {
             let vec = entry.value_mut();
             let len_before = vec.len();
             vec.retain(|(s, _)| *s < min || *s > max);
             return Ok((len_before - vec.len()) as i64);
        }
        Ok(0)
    }
}

pub struct RedisBackend {
    pool: Pool,
    compression_threshold: usize,
}

fn is_gzip(bytes: &[u8]) -> bool {
    bytes.len() > 2 && bytes[0] == 0x1f && bytes[1] == 0x8b
}

fn is_zstd(bytes: &[u8]) -> bool {
    // Zstd magic number is 4 bytes (Little Endian: FD 2F B5 28)
    bytes.len() > 4 && bytes[0] == 0x28 && bytes[1] == 0xb5 && bytes[2] == 0x2f && bytes[3] == 0xfd
}

fn decode_compressed(bytes: Vec<u8>) -> Result<Vec<u8>, CacheError> {
    if is_gzip(&bytes) {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded)?;
        return Ok(decoded);
    }
    if is_zstd(&bytes) {
        let decoded = zstd::stream::decode_all(std::io::Cursor::new(bytes.as_slice()))?;
        return Ok(decoded);
    }
    Ok(bytes)
}

fn decode_compressed_best_effort(bytes: Vec<u8>) -> Vec<u8> {
    if is_gzip(&bytes) {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut decoded = Vec::new();
        if decoder.read_to_end(&mut decoded).is_ok() {
            return decoded;
        }
        return bytes;
    }
    if is_zstd(&bytes) {
        if let Ok(decoded) = zstd::stream::decode_all(std::io::Cursor::new(bytes.as_slice())) {
            return decoded;
        }
        return bytes;
    }
    bytes
}

impl RedisBackend {
    pub fn new(pool: Pool, compression_threshold: usize) -> Self {
        Self { pool, compression_threshold }
    }
}




#[async_trait::async_trait]
impl CacheBackend for RedisBackend {
    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let result: i64 = deadpool_redis::redis::cmd("ZADD")
            .arg(key)
            .arg(score)
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let min_str = if min == f64::NEG_INFINITY { "-inf".to_string() } else { min.to_string() };
        let max_str = if max == f64::INFINITY { "+inf".to_string() } else { max.to_string() };
        
        let result: Vec<Vec<u8>> = deadpool_redis::redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min_str)
            .arg(max_str)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let min_str = if min == f64::NEG_INFINITY { "-inf".to_string() } else { min.to_string() };
        let max_str = if max == f64::INFINITY { "+inf".to_string() } else { max.to_string() };

        let result: i64 = deadpool_redis::redis::cmd("ZREMRANGEBYSCORE")
            .arg(key)
            .arg(min_str)
            .arg(max_str)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        let result: Option<Vec<u8>> = conn.get(key).await.map_err(CacheError::Redis)?;
        
        if let Some(bytes) = result {
            if is_gzip(&bytes) || is_zstd(&bytes) {
                let decoded = tokio::task::spawn_blocking(move || decode_compressed(bytes))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??;
                return Ok(Some(decoded));
            }
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let threshold = self.compression_threshold;
        
        let final_value = if value.len() > threshold {
             // Use spawn_blocking for CPU-intensive compression to avoid blocking the async runtime
             let val = value.to_vec();
             tokio::task::spawn_blocking(move || {
                 zstd::stream::encode_all(std::io::Cursor::new(val), 3)
                    .map_err(|e| CacheError::Pool(e.to_string()))
             }).await.map_err(|e| CacheError::Pool(e.to_string()))?
            ?
        } else {
            value.to_vec()
        };

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        if let Some(duration) = ttl {
            let _: () = conn
                .set_ex(key, final_value, duration.as_secs())
                .await
                .map_err(CacheError::Redis)?;
        } else {
            let _: () = conn.set(key, final_value).await.map_err(CacheError::Redis)?;
        }
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        conn.del(key).await.map_err(CacheError::Redis)
    }

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        // Use pipeline for batch delete - much more efficient than individual deletes
        let mut pipe = deadpool_redis::redis::pipe();
        for key in keys {
            pipe.del(*key);
        }
        
        let results: Vec<i64> = pipe.query_async(&mut conn).await.map_err(CacheError::Redis)?;
        Ok(results.iter().sum::<i64>() as u64)
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        // Default: no limit (backward compatible)
        self.keys_with_limit(pattern, usize::MAX).await
    }

    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let mut keys: Vec<String> = Vec::new();
        // Use SCAN with COUNT hint for better performance
        let mut cursor: u64 = 0;
        let scan_count = std::cmp::min(limit, 1000); // Batch size per SCAN
        
        loop {
            let (new_cursor, batch): (u64, Vec<String>) = deadpool_redis::redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(scan_count)
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)?;
            
            for key in batch {
                if keys.len() >= limit {
                    return Ok(keys);
                }
                keys.push(key);
            }
            
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }
        Ok(keys)
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;

        let result: Option<String> = if let Some(ttl) = ttl {
            deadpool_redis::redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .arg("EX")
                .arg(ttl.as_secs())
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)?
        } else {
            deadpool_redis::redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)?
        };
        Ok(result.is_some())
    }

    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;

        let mut pipe = deadpool_redis::redis::pipe();
        
        for key in keys {
             let mut cmd = deadpool_redis::redis::cmd("SET");
             cmd.arg(key)
                .arg(value)
                .arg("NX");
             
             if let Some(d) = ttl {
                 cmd.arg("EX").arg(d.as_secs());
             }
             pipe.add_command(cmd);
        }

        // The results will be a vector of Option<String> (OK or Nil) or booleans depending on parsing.
        // We can parse as Vec<Option<String>>.
        // Or Vec<bool> if redis crate supports it for SET NX.
        // Let's use Vec<Option<String>> to be safe.
        let results: Vec<Option<String>> = pipe
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
            
        Ok(results.into_iter().map(|r| r.is_some()).collect())
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;

        // Use MGET
        let values: Vec<Option<Vec<u8>>> = deadpool_redis::redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;

        let has_compressed = values.iter().any(|val| {
            val.as_ref().is_some_and(|bytes| is_gzip(bytes) || is_zstd(bytes))
        });
        if !has_compressed {
            return Ok(values);
        }

        let decoded_values = tokio::task::spawn_blocking(move || {
            values
                .into_iter()
                .map(|val| val.map(decode_compressed_best_effort))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| CacheError::Pool(e.to_string()))?;

        Ok(decoded_values)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        deadpool_redis::redis::cmd("INCRBY")
            .arg(key)
            .arg(delta)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)
    }

    async fn ping(&self) -> Result<(), CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        let _: String = deadpool_redis::redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(())
    }
}

/// Two-Level Cache Backend: L1 (Local DashMap) + L2 (Redis)
/// Provides significant performance improvements by reducing Redis round trips
pub struct TwoLevelCacheBackend {
    l1_cache: Arc<LocalBackend>,
    l2_cache: Arc<RedisBackend>,
    l1_ttl: Duration,
    l1_max_entries: usize,
}

impl TwoLevelCacheBackend {
    pub fn new(redis_pool: Pool, compression_threshold: usize, l1_ttl_secs: u64, l1_max_entries: usize) -> Self {
        Self {
            l1_cache: Arc::new(LocalBackend::new()),
            l2_cache: Arc::new(RedisBackend::new(redis_pool, compression_threshold)),
            l1_ttl: Duration::from_secs(l1_ttl_secs),
            l1_max_entries,
        }
    }

    /// Check if L1 cache size exceeds limit and evict randomly if needed
    async fn maybe_evict_l1(&self) {
        let current_size = self.l1_cache.store.len();
        if current_size > self.l1_max_entries {
            // Evict ~10% of entries randomly
            let to_evict = current_size / 10;
            let mut count = 0;
            
            let keys_to_remove: Vec<String> = self.l1_cache.store.iter()
                .take(to_evict)
                .map(|entry| entry.key().clone())
                .collect();
            
            for key in keys_to_remove {
                self.l1_cache.store.remove(&key);
                count += 1;
            }
            
            if count > 0 {
                debug!("Evicted {} entries from L1 cache (size: {} -> {})", count, current_size, self.l1_cache.store.len());
                counter!("cache_l1_evictions").increment(count as u64);
            }
        }
    }
}

#[async_trait::async_trait]
impl CacheBackend for TwoLevelCacheBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let start = Instant::now();
        
        // 1. Try L1 cache first
        if let Some(value) = self.l1_cache.get(key).await? {
            histogram!("cache_get_latency_us", "level" => "l1", "result" => "hit").record(start.elapsed().as_micros() as f64);
            counter!("cache_hits", "level" => "l1").increment(1);
            return Ok(Some(value));
        }
        
        // 2. L1 miss, try L2 (Redis)
        counter!("cache_misses", "level" => "l1").increment(1);
        
        if let Some(value) = self.l2_cache.get(key).await? {
            // 2.1 L2 hit: write back to L1
            let value_clone = value.clone();
            let key_str = key.to_string();
            let l1_cache = self.l1_cache.clone();
            let l1_ttl = self.l1_ttl;
            
            // Async write to L1, don't block the return
            tokio::spawn(async move {
                if let Err(e) = l1_cache.set(&key_str, &value_clone, Some(l1_ttl)).await {
                    warn!("Failed to write L2 hit back to L1: {}", e);
                }
            });
            
            histogram!("cache_get_latency_us", "level" => "l2", "result" => "hit").record(start.elapsed().as_micros() as f64);
            counter!("cache_hits", "level" => "l2").increment(1);
            return Ok(Some(value));
        }
        
        // 3. Both miss
        counter!("cache_misses", "level" => "l2").increment(1);
        histogram!("cache_get_latency_us", "level" => "l2", "result" => "miss").record(start.elapsed().as_micros() as f64);
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let start = Instant::now();
        
        // 1. Write to L2 (Redis) first for durability
        self.l2_cache.set(key, value, ttl).await?;
        
        // 2. Write to L1 with shorter TTL
        let l1_ttl_final = ttl.map(|t| t.min(self.l1_ttl)).unwrap_or(self.l1_ttl);
        self.l1_cache.set(key, value, Some(l1_ttl_final)).await?;
        
        // 3. Maybe evict L1 if too large
        self.maybe_evict_l1().await;
        
        histogram!("cache_set_latency_us").record(start.elapsed().as_micros() as f64);
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        // Delete from both layers
        let _ = self.l1_cache.del(key).await;
        self.l2_cache.del(key).await
    }

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        // Delete from both layers
        let _ = self.l1_cache.del_batch(keys).await;
        self.l2_cache.del_batch(keys).await
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        // Keys query should go to L2 (authoritative source)
        self.l2_cache.keys(pattern).await
    }

    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        self.l2_cache.keys_with_limit(pattern, limit).await
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        // SET NX must be atomic, only on L2
        let result = self.l2_cache.set_nx(key, value, ttl).await?;
        
        if result {
            // If set succeeded, also update L1
            let l1_ttl_final = ttl.map(|t| t.min(self.l1_ttl)).unwrap_or(self.l1_ttl);
            let _ = self.l1_cache.set(key, value, Some(l1_ttl_final)).await;
        }
        
        Ok(result)
    }

    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        let results = self.l2_cache.set_nx_batch(keys, value, ttl).await?;
        
        // Update L1 for successful sets
        let l1_ttl_final = ttl.map(|t| t.min(self.l1_ttl)).unwrap_or(self.l1_ttl);
        for (i, &success) in results.iter().enumerate() {
            if success {
                let _ = self.l1_cache.set(keys[i], value, Some(l1_ttl_final)).await;
            }
        }
        
        Ok(results)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut results = vec![None; keys.len()];
        let mut l2_keys_indices = Vec::new();
        let mut l2_keys = Vec::new();
        
        // 1. Check L1 for all keys
        for (i, key) in keys.iter().enumerate() {
            if let Some(value) = self.l1_cache.get(key).await? {
                results[i] = Some(value);
                counter!("cache_hits", "level" => "l1", "op" => "mget").increment(1);
            } else {
                l2_keys_indices.push(i);
                l2_keys.push(*key);
                counter!("cache_misses", "level" => "l1", "op" => "mget").increment(1);
            }
        }
        
        // 2. Fetch misses from L2
        if !l2_keys.is_empty() {
            let l2_results = self.l2_cache.mget(&l2_keys).await?;
            
            for (idx, value_opt) in l2_results.into_iter().enumerate() {
                let original_idx = l2_keys_indices[idx];
                if let Some(value) = value_opt {
                    results[original_idx] = Some(value.clone());
                    counter!("cache_hits", "level" => "l2", "op" => "mget").increment(1);
                    
                    // Write back to L1 asynchronously
                    let key_str = l2_keys[idx].to_string();
                    let l1_cache = self.l1_cache.clone();
                    let l1_ttl = self.l1_ttl;
                    tokio::spawn(async move {
                        let _ = l1_cache.set(&key_str, &value, Some(l1_ttl)).await;
                    });
                } else {
                    counter!("cache_misses", "level" => "l2", "op" => "mget").increment(1);
                }
            }
        }
        
        Ok(results)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        // INCR must be atomic on L2, invalidate L1
        let _ = self.l1_cache.del(key).await;
        self.l2_cache.incr(key, delta).await
    }

    async fn ping(&self) -> Result<(), CacheError> {
        self.l2_cache.ping().await
    }

    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        // Sorted sets only on L2
        self.l2_cache.zadd(key, score, member).await
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        self.l2_cache.zrangebyscore(key, min, max).await
    }

    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        self.l2_cache.zremrangebyscore(key, min, max).await
    }
}

#[async_trait::async_trait]
pub trait CacheAble: Send + Sync + Sized
where
    Self: Serialize+ for<'de> Deserialize<'de> + 'static,
{
    fn field() -> impl AsRef<str>;

    fn serialized_size_hint(&self) -> Option<usize> {
        None
    }

    fn clone_for_serialize(&self) -> Option<Self> {
        None
    }

    async fn send(&self,id:&str, sync: &CacheService) -> Result<(), CacheError> {
        // Construct key: namespace:cache:field
        // Optimization: In single node mode or high throughput scenarios, 
        // aggressively caching every single request might be overkill if not strictly needed for fallback.
        // However, assuming logic requires it.
        
        let key = Self::cache_id(id,sync);

        let content = if self
            .serialized_size_hint()
            .map_or(false, |size| size >= sync.serialize_blocking_threshold)
        {
            if let Some(data) = self.clone_for_serialize() {
                tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??
            } else {
                serde_json::to_vec(self)?
            }
        } else {
            serde_json::to_vec(self)?
        };

        // Use default_ttl from CacheService
        sync.backend.set(&key, &content, sync.default_ttl).await?;
        Ok(())
    }

    async fn send_with_ttl(&self, id: &str, sync: &CacheService, ttl: Duration) -> Result<(), CacheError> {
        let key = Self::cache_id(id, sync);
        let content = if self
            .serialized_size_hint()
            .map_or(false, |size| size >= sync.serialize_blocking_threshold)
        {
            if let Some(data) = self.clone_for_serialize() {
                tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??
            } else {
                serde_json::to_vec(self)?
            }
        } else {
            serde_json::to_vec(self)?
        };
        sync.backend.set(&key, &content, Some(ttl)).await?;
        Ok(())
    }

    async fn send_nx(&self, id: &str, sync: &CacheService, ttl: Option<Duration>) -> Result<bool, CacheError> {
        let key = Self::cache_id(id, sync);
        let content = if self
            .serialized_size_hint()
            .map_or(false, |size| size >= sync.serialize_blocking_threshold)
        {
            if let Some(data) = self.clone_for_serialize() {
                tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??
            } else {
                serde_json::to_vec(self)?
            }
        } else {
            serde_json::to_vec(self)?
        };
        sync.backend.set_nx(&key, &content, ttl).await
    }

    async fn sync(id:&str,sync: &CacheService) -> Result<Option<Self>, CacheError> {
        let key =  Self::cache_id(id,sync);
        if let Some(bytes) = sync.backend.get(&key).await? {
            let val = serde_json::from_slice(&bytes).map_err(CacheError::Serde)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
    async fn delete(id:&str,sync: &CacheService) -> Result<(), CacheError> {
        let key =  Self::cache_id(id,sync);
        sync.backend.del(&key).await?;
        Ok(())
    }

    async fn scan(pattern_suffix: &str, sync: &CacheService) -> Result<Vec<String>, CacheError> {
         let pattern = format!("{}:{}:{}", sync.namespace, Self::field().as_ref(), pattern_suffix);
         sync.backend.keys(&pattern).await
    }

     fn cache_id(id:&str,cache:&CacheService) ->String{
        format!("{}:{}:{id}", cache.namespace, Self::field().as_ref())
    }

}

pub struct CacheService {
    backend: Arc<dyn CacheBackend>,
    namespace: String,
    default_ttl: Option<Duration>,
    serialize_blocking_threshold: usize,
}

impl CacheService {
    pub fn new(
        pool: Option<Pool>,
        namespace: String,
        default_ttl: Option<Duration>,
        compression_threshold: Option<usize>,
    ) -> Self {
        Self::new_with_l1_config(pool, namespace, default_ttl, compression_threshold, false, 30, 10000)
    }

    /// Create CacheService with configurable L1 cache
    /// 
    /// # Arguments
    /// * `pool` - Optional Redis connection pool (None = local only)
    /// * `namespace` - Cache key namespace
    /// * `default_ttl` - Default TTL for cache entries
    /// * `compression_threshold` - Compress payloads larger than this (bytes)
    /// * `enable_l1` - Enable L1 local cache layer (only with Redis)
    /// * `l1_ttl_secs` - L1 cache TTL in seconds
    /// * `l1_max_entries` - Maximum L1 cache entries before eviction
    pub fn new_with_l1_config(
        pool: Option<Pool>,
        namespace: String,
        default_ttl: Option<Duration>,
        compression_threshold: Option<usize>,
        enable_l1: bool,
        l1_ttl_secs: u64,
        l1_max_entries: usize,
    ) -> Self {
        let threshold = compression_threshold.unwrap_or(1024);
        
        let backend: Arc<dyn CacheBackend> = match pool {
            Some(p) => {
                if enable_l1 {
                    Arc::new(TwoLevelCacheBackend::new(p, threshold, l1_ttl_secs, l1_max_entries))
                } else {
                    Arc::new(RedisBackend::new(p, threshold))
                }
            },
            None => Arc::new(LocalBackend::new()),
        };

        CacheService {
            backend,
            namespace,
            default_ttl,
            serialize_blocking_threshold: 64 * 1024,
        }
    }

    pub async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        self.backend.set_nx(key, value, ttl).await
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        self.backend.zadd(key, score, member).await
    }

    pub async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        self.backend.zrangebyscore(key, min, max).await
    }
    
    pub async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        self.backend.zremrangebyscore(key, min, max).await
    }

    pub async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        self.backend.set_nx_batch(keys, value, ttl).await
    }

    pub async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        self.backend.mget(keys).await
    }

    pub async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        self.backend.incr(key, delta).await
    }

    pub async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        self.backend.set(key, value, ttl).await
    }

    pub async fn del(&self, key: &str) -> Result<(), CacheError> {
        self.backend.del(key).await
    }

    /// Batch delete multiple keys using pipeline (much more efficient than individual deletes)
    pub async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        self.backend.del_batch(keys).await
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        self.backend.keys(pattern).await
    }

    /// Scan keys with a limit to prevent memory exhaustion and long blocking.
    /// This is recommended over `keys()` for large keyspaces.
    pub async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        self.backend.keys_with_limit(pattern, limit).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        self.backend.get(key).await
    }

    pub async fn ping(&self) -> Result<(), CacheError> {
        self.backend.ping().await
    }

    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl
    }
}



#[cfg(test)]
mod tests {
    #[derive(Deserialize, Serialize, Debug)]
    struct MyConfig {
        name: String,
        value: i32,
    }

    impl CacheAble for MyConfig {
        fn field() -> impl AsRef<str> {
            "global_config".to_string()
        }
    }
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn test() {
        // Example Usage:

        // 1. Initialize for Local Mode (DashMap) with namespace "myapp" and 60s TTL
        let sync_service =
            Arc::new(CacheService::new(None, "myapp".to_string(), Some(Duration::from_secs(60)), None)) ;

        let config = MyConfig {
            name: "test".to_string(),
            value: 123,
        };

        // 2. Developer calls send()
        if let Err(e) = config.send(&"test",sync_service.as_ref()).await {
            eprintln!("Failed to send: {}", e);
        }

        // 3. Developer calls sync()
        match MyConfig::sync(&"test",sync_service.as_ref()).await {
            Ok(Some(fetched)) => println!("Synced: {:?}", fetched),
            Ok(None) => println!("No data found"),
            Err(e) => eprintln!("Error syncing: {}", e),
        }

        println!("StateSync implemented.");
    }
}
