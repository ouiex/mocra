use super::backend::CacheBackend;
use super::local_backend::LocalBackend;
use super::redis_backend::RedisBackend;
use deadpool_redis::redis::Value as RedisValue;
use deadpool_redis::Pool;
use crate::errors::CacheError;
use log::{debug, warn};
use metrics::{counter, histogram};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

    async fn maybe_evict_l1(&self) {
        let current_size = self.l1_cache.store.len();
        if current_size > self.l1_max_entries {
            let to_evict = current_size / 10;
            let mut count = 0;

            let keys_to_remove: Vec<String> = self
                .l1_cache
                .store
                .iter()
                .take(to_evict)
                .map(|entry| entry.key().clone())
                .collect();

            for key in keys_to_remove {
                self.l1_cache.store.remove(&key);
                count += 1;
            }

            if count > 0 {
                debug!(
                    "Evicted {} entries from L1 cache (size: {} -> {})",
                    count,
                    current_size,
                    self.l1_cache.store.len()
                );
                counter!("cache_l1_evictions").increment(count as u64);
            }
        }
    }
}

#[async_trait::async_trait]
impl CacheBackend for TwoLevelCacheBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let start = Instant::now();

        if let Some(value) = self.l1_cache.get(key).await? {
            histogram!("cache_get_latency_us", "level" => "l1", "result" => "hit")
                .record(start.elapsed().as_micros() as f64);
            counter!("cache_hits", "level" => "l1").increment(1);
            return Ok(Some(value));
        }

        counter!("cache_misses", "level" => "l1").increment(1);

        if let Some(value) = self.l2_cache.get(key).await? {
            let value_clone = value.clone();
            let key_str = key.to_string();
            let l1_cache = self.l1_cache.clone();
            let l1_ttl = self.l1_ttl;

            tokio::spawn(async move {
                if let Err(e) = l1_cache.set(&key_str, &value_clone, Some(l1_ttl)).await {
                    warn!("Failed to write L2 hit back to L1: {}", e);
                }
            });

            histogram!("cache_get_latency_us", "level" => "l2", "result" => "hit")
                .record(start.elapsed().as_micros() as f64);
            counter!("cache_hits", "level" => "l2").increment(1);
            return Ok(Some(value));
        }

        counter!("cache_misses", "level" => "l2").increment(1);
        histogram!("cache_get_latency_us", "level" => "l2", "result" => "miss")
            .record(start.elapsed().as_micros() as f64);
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let start = Instant::now();

        self.l2_cache.set(key, value, ttl).await?;

        let l1_ttl_final = ttl.map(|t| t.min(self.l1_ttl)).unwrap_or(self.l1_ttl);
        self.l1_cache.set(key, value, Some(l1_ttl_final)).await?;

        self.maybe_evict_l1().await;

        histogram!("cache_set_latency_us").record(start.elapsed().as_micros() as f64);
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        let _ = self.l1_cache.del(key).await;
        self.l2_cache.del(key).await
    }

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        let _ = self.l1_cache.del_batch(keys).await;
        self.l2_cache.del_batch(keys).await
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        self.l2_cache.keys(pattern).await
    }

    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        self.l2_cache.keys_with_limit(pattern, limit).await
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        let result = self.l2_cache.set_nx(key, value, ttl).await?;

        if result {
            let l1_ttl_final = ttl.map(|t| t.min(self.l1_ttl)).unwrap_or(self.l1_ttl);
            let _ = self.l1_cache.set(key, value, Some(l1_ttl_final)).await;
        }

        Ok(result)
    }

    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        let results = self.l2_cache.set_nx_batch(keys, value, ttl).await?;

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

        if !l2_keys.is_empty() {
            let l2_results = self.l2_cache.mget(&l2_keys).await?;

            for (idx, value_opt) in l2_results.into_iter().enumerate() {
                let original_idx = l2_keys_indices[idx];
                if let Some(value) = value_opt {
                    results[original_idx] = Some(value.clone());
                    counter!("cache_hits", "level" => "l2", "op" => "mget").increment(1);

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
        let _ = self.l1_cache.del(key).await;
        self.l2_cache.incr(key, delta).await
    }

    async fn ping(&self) -> Result<(), CacheError> {
        self.l2_cache.ping().await
    }

    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        self.l2_cache.zadd(key, score, member).await
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        self.l2_cache.zrangebyscore(key, min, max).await
    }

    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        self.l2_cache.zremrangebyscore(key, min, max).await
    }

    async fn script_load(&self, script: &str) -> Result<String, CacheError> {
        self.l2_cache.script_load(script).await
    }

    async fn evalsha(&self, sha: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        self.l2_cache.evalsha(sha, keys, args).await
    }

    async fn eval_lua(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        self.l2_cache.eval_lua(script, keys, args).await
    }
}
