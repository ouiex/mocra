use errors::Result;
use deadpool_redis::{redis::{AsyncCommands, SetOptions, ExistenceCheck, SetExpiry}, Pool};
use log::{error, info};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::time::{self, Duration};
use tokio::sync::RwLock;
use chrono::Utc;
use bloomfilter::Bloom;
use metrics::{counter, histogram};

/// Three-layer deduplication system:
/// L0: Bloom Filter (ultra-fast negative check, no false positives for "definitely new")
/// L1: DashMap (fast local cache for known duplicates)
/// L2: Redis (distributed authoritative source)
#[derive(Clone)]
pub struct Deduplicator {
    pool: Pool,
    ttl: usize, // seconds
    namespace: String,
    // L0: Bloom Filter (probabilistic, fast pre-filter)
    bloom: Arc<RwLock<Bloom<String>>>,
    bloom_capacity: usize,
    bloom_fp_rate: f64,
    // L1 Cache: hash -> expire_at (timestamp)
    // Stores KNOWN duplicates (present in Redis).
    local_cache: Arc<DashMap<String, i64>>,
}

impl Deduplicator {
    pub fn new(pool: Pool, ttl: usize, namespace: impl Into<String>) -> Self {
        Self::new_with_bloom_config(pool, ttl, namespace, 10_000_000, 0.01)
    }

    /// Create deduplicator with configurable Bloom Filter
    /// 
    /// # Arguments
    /// * `pool` - Redis connection pool
    /// * `ttl` - TTL for dedup entries in seconds
    /// * `namespace` - Key namespace
    /// * `bloom_capacity` - Expected number of items (default: 10M)
    /// * `bloom_fp_rate` - False positive rate (default: 0.01 = 1%)
    pub fn new_with_bloom_config(
        pool: Pool,
        ttl: usize,
        namespace: impl Into<String>,
        bloom_capacity: usize,
        bloom_fp_rate: f64,
    ) -> Self {
        let local_cache = Arc::new(DashMap::new());
        let cache_clone = local_cache.clone();
        
        // Initialize Bloom Filter
        // With 10M capacity and 0.01 FP rate: ~11.98 MB memory, 7 hash functions
        let bloom = Arc::new(RwLock::new(Bloom::new_for_fp_rate(
            bloom_capacity,
            bloom_fp_rate,
        )));
        let bloom_clone = bloom.clone();
        
        info!(
            "Initialized Bloom Filter deduplicator: capacity={}, fp_rate={}, estimated_memory={}MB",
            bloom_capacity,
            bloom_fp_rate,
            (bloom_capacity as f64 * (-bloom_fp_rate.ln() / (2.0_f64.ln().powi(2)))) / 8.0 / 1024.0 / 1024.0
        );
        
        // Background cleaner for local cache and Bloom Filter
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = Utc::now().timestamp();
                
                // Clean L1 cache
                if cache_clone.len() > 1_000_000 {
                    info!("Deduplicator L1 cache exceeded 1M entries, clearing all to prevent OOM");
                    cache_clone.clear();
                } else {
                    cache_clone.retain(|_, &mut expire_at| expire_at > now);
                }
                
                // Periodically reset Bloom Filter to prevent saturation
                // Reset every 10 minutes to keep FP rate low
                let bloom_size = {
                    let bloom_guard = bloom_clone.read().await;
                    bloom_guard.number_of_hash_functions()
                };
                
                // Reset bloom every 10 minutes
                if bloom_size > 0 {
                    // Check if we should reset (simple time-based heuristic)
                    static mut LAST_RESET: i64 = 0;
                    unsafe {
                        if LAST_RESET == 0 {
                            LAST_RESET = now;
                        }
                        if now - LAST_RESET > 600 { // 10 minutes
                            let mut bloom_guard = bloom_clone.write().await;
                            bloom_guard.clear();
                            LAST_RESET = now;
                            info!("Reset Bloom Filter to prevent saturation");
                            counter!("dedup_bloom_resets").increment(1);
                        }
                    }
                }
            }
        });

        Self {
            pool,
            ttl,
            namespace: namespace.into(),
            bloom,
            bloom_capacity,
            bloom_fp_rate,
            local_cache,
        }
    }

    /// Returns true if the request is new (not duplicate).
    /// Adds the hash to Redis if it's new.
    pub async fn check_and_set(&self, hash: &str) -> Result<bool> {
        let start = std::time::Instant::now();
        let now = Utc::now().timestamp();
        let hash_owned = hash.to_string();
        
        // L0: Check Bloom Filter (read lock, very fast)
        let bloom_says_exists = {
            let bloom_guard = self.bloom.read().await;
            bloom_guard.check(&hash_owned)
        };
        
        if bloom_says_exists {
            // Bloom says "might exist", need further verification
            counter!("dedup_bloom_hits", "result" => "maybe_exists").increment(1);
        } else {
            // Bloom says "definitely new" - shortcut!
            // Still need to set it in Redis and Bloom
            counter!("dedup_bloom_hits", "result" => "definitely_new").increment(1);
            
            // Add to Bloom immediately (write lock)
            {
                let mut bloom_guard = self.bloom.write().await;
                bloom_guard.set(&hash_owned);
            }
            
            // Set in Redis
            let mut conn = match self.pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Deduplicator: Failed to get Redis connection: {}", e);
                    return Err(errors::Error::from(errors::CacheError::Pool(e.to_string())));
                }
            };

            let key = if self.namespace.is_empty() {
                format!("dedup:{hash}")
            } else {
                format!("{}:dedup:{hash}", self.namespace)
            };
            
            let opts = SetOptions::default()
                .conditional_set(ExistenceCheck::NX)
                .with_expiration(SetExpiry::EX(self.ttl as u64));

            let _: Option<String> = conn.set_options(&key, "1", opts).await
                .map_err(|e| errors::Error::from(errors::CacheError::Redis(e)))?;
            
            // Add to L1 cache
            self.local_cache.insert(hash_owned.clone(), now + self.ttl as i64);
            
            histogram!("dedup_check_latency_us", "path" => "bloom_new").record(start.elapsed().as_micros() as f64);
            return Ok(true);
        }
        
        // L1: Check local cache
        if let Some(expire_at) = self.local_cache.get(&hash_owned) {
            if *expire_at > now {
                histogram!("dedup_check_latency_us", "path" => "l1_hit").record(start.elapsed().as_micros() as f64);
                counter!("dedup_l1_hits").increment(1);
                return Ok(false); // Known duplicate
            } else {
                // Expired, drop it
                drop(expire_at);
                self.local_cache.remove(&hash_owned);
            }
        }
        
        // L2: Check Redis
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!("Deduplicator: Failed to get Redis connection: {}", e);
                return Err(errors::Error::from(errors::CacheError::Pool(e.to_string())));
            }
        };

        let key = if self.namespace.is_empty() {
            format!("dedup:{hash}")
        } else {
            format!("{}:dedup:{hash}", self.namespace)
        };
        
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .with_expiration(SetExpiry::EX(self.ttl as u64));

        let result: Option<String> = conn.set_options(&key, "1", opts).await
            .map_err(|e| errors::Error::from(errors::CacheError::Redis(e)))?;

        let is_new = result.is_some();
        
        if !is_new {
            // It was already in Redis, update L1 cache
            self.local_cache.insert(hash_owned.clone(), now + self.ttl as i64);
            counter!("dedup_l2_hits").increment(1);
        } else {
            // New entry, add to L1 cache
            self.local_cache.insert(hash_owned.clone(), now + self.ttl as i64);
            counter!("dedup_l2_new").increment(1);
        }

        histogram!("dedup_check_latency_us", "path" => "redis").record(start.elapsed().as_micros() as f64);
        Ok(is_new)
    }

    /// Batch version of check_and_set.
    /// Returns a boolean vector corresponding to input hashes.
    /// Uses Bloom Filter for pre-filtering and Redis pipeline for performance.
    pub async fn check_and_set_batch(&self, hashes: &[String]) -> Result<Vec<bool>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let start = std::time::Instant::now();
        let now = Utc::now().timestamp();
        let mut results = vec![false; hashes.len()];
        let mut indices_to_check = Vec::with_capacity(hashes.len());

        // L0: Check Bloom Filter (batch read)
        let bloom_results = {
            let bloom_guard = self.bloom.read().await;
            hashes.iter().map(|h| bloom_guard.check(h)).collect::<Vec<bool>>()
        };
        
        let mut definitely_new_indices = Vec::new();
        for (i, &bloom_exists) in bloom_results.iter().enumerate() {
            if !bloom_exists {
                // Bloom says definitely new
                definitely_new_indices.push(i);
                results[i] = true; // Mark as new immediately
            }
        }
        
        // Add definitely new items to Bloom Filter
        if !definitely_new_indices.is_empty() {
            let mut bloom_guard = self.bloom.write().await;
            for &idx in &definitely_new_indices {
                bloom_guard.set(&hashes[idx]);
            }
            counter!("dedup_bloom_batch_new").increment(definitely_new_indices.len() as u64);
        }

        // L1: Check local cache for items Bloom says "might exist"
        for (i, hash) in hashes.iter().enumerate() {
            if results[i] {
                continue; // Already determined as new by Bloom
            }
            
            if let Some(expire_at) = self.local_cache.get(hash) {
                if *expire_at > now {
                    results[i] = false; // Duplicate
                    counter!("dedup_l1_batch_hits").increment(1);
                    continue;
                }
            }
            // Need to check Redis
            indices_to_check.push(i);
        }

        if indices_to_check.is_empty() {
            // All requests were handled by Bloom + L1
            histogram!("dedup_batch_latency_us", "path" => "bloom_l1").record(start.elapsed().as_micros() as f64);
            
            // Still need to set the definitely_new ones in Redis
            if !definitely_new_indices.is_empty() {
                self.set_batch_in_redis(hashes, &definitely_new_indices, now).await?;
            }
            
            return Ok(results);
        }

        // L2: Check Redis for remaining items
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!("Deduplicator: Failed to get Redis connection: {}", e);
                return Err(errors::Error::from(errors::CacheError::Pool(e.to_string())));
            }
        };

        let mut pipe = deadpool_redis::redis::pipe();
        
        let prefix = if self.namespace.is_empty() {
            "dedup:".to_string()
        } else {
            format!("{}:dedup:", self.namespace)
        };

        // Build pipeline for Redis SET NX operations
        let mut all_redis_indices = indices_to_check.clone();
        all_redis_indices.extend(&definitely_new_indices);
        
        for &idx in &all_redis_indices {
            let hash = &hashes[idx];
            let key = format!("{prefix}{hash}");
            pipe.cmd("SET").arg(key).arg("1").arg("NX").arg("EX").arg(self.ttl);
        }

        let pipe_results: Vec<Option<String>> = pipe.query_async(&mut conn).await
            .map_err(|e| errors::Error::from(errors::CacheError::Redis(e)))?;

        // Process results
        for (pipe_idx, result) in pipe_results.into_iter().enumerate() {
            let original_idx = all_redis_indices[pipe_idx];
            let is_new = result.is_some();
            results[original_idx] = is_new;
            
            // Update L1 cache
            self.local_cache.insert(hashes[original_idx].clone(), now + self.ttl as i64);
            
            if is_new {
                counter!("dedup_l2_batch_new").increment(1);
            } else {
                counter!("dedup_l2_batch_hits").increment(1);
            }
        }

        histogram!("dedup_batch_latency_us", "path" => "full").record(start.elapsed().as_micros() as f64);
        Ok(results)
    }
    
    async fn set_batch_in_redis(&self, hashes: &[String], indices: &[usize], now: i64) -> Result<()> {
        if indices.is_empty() {
            return Ok(());
        }
        
        let mut conn = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                error!("Deduplicator: Failed to get Redis connection: {}", e);
                return Err(errors::Error::from(errors::CacheError::Pool(e.to_string())));
            }
        };

        let prefix = if self.namespace.is_empty() {
            "dedup:".to_string()
        } else {
            format!("{}:dedup:", self.namespace)
        };

        let mut pipe = deadpool_redis::redis::pipe();
        for &idx in indices {
            let key = format!("{}{}", prefix, &hashes[idx]);
            pipe.cmd("SET").arg(key).arg("1").arg("NX").arg("EX").arg(self.ttl);
            
            // Update L1 cache
            self.local_cache.insert(hashes[idx].clone(), now + self.ttl as i64);
        }

        let _: Vec<Option<String>> = pipe.query_async(&mut conn).await
            .map_err(|e| errors::Error::from(errors::CacheError::Redis(e)))?;
        
        Ok(())
    }
}
