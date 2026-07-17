#![allow(unused)]
use crate::errors::Result;
use crate::errors::error::RateLimitError;
use crate::utils::lock::{AdvancedDistributedLock, DistributedLockManager};
use dashmap::DashMap;
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Rate limiter configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimitConfig {
    /// Maximum requests per second.
    pub max_requests_per_second: f32,
    /// Window size in milliseconds (default: 1 second).
    pub window_size_millis: u64,
    /// Baseline max requests per second (used for restoration).
    #[serde(default)]
    pub base_max_requests_per_second: Option<f32>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 2.0,
            window_size_millis: 1000,
            base_max_requests_per_second: None,
        }
    }
}

impl RateLimitConfig {
    pub fn new(max_requests_per_second: f32) -> Self {
        Self {
            max_requests_per_second,
            window_size_millis: 1000,
            base_max_requests_per_second: Some(max_requests_per_second),
        }
    }

    pub fn with_window_size(mut self, window_size: Duration) -> Self {
        self.window_size_millis = window_size.as_millis() as u64;
        self
    }
}

/// In-process smoothing rate limiter (can optionally divide a global limit across cluster
/// members).
///
/// All rate-limiting state is kept in-process (`DashMap`). In cluster mode (embedded Raft,
/// etc.), once a [`CoordinationBackend`](crate::utils::coordination::CoordinationBackend) is
/// injected, the global per-second limit is divided across nodes by member count, giving
/// approximate distributed rate limiting.
///
/// Features:
/// 1. Independent, smoothly paced rate limiting per identifier (computed per interval).
/// 2. Read-modify-write is guarded by a [`DistributedLockManager`].
/// 3. Supports per-key rates and dynamic updates.
#[derive(Clone)]
pub struct DistributedSlidingWindowRateLimiter {
    /// Distributed lock manager.
    lock_manager: Arc<DistributedLockManager>,
    /// Key prefix.
    key_prefix: String,
    /// Sub prefix
    sub_prefix: String,
    /// Key for default config.
    default_config_key: String,
    /// Key prefix for per-key configs.
    config_key_prefix: String,
    /// Default rate-limit config.
    default_config: RateLimitConfig,
    /// Local cache of last request time (`identifier -> last_request_timestamp`).
    local_last_request: Arc<DashMap<String, u64>>,
    /// Local config cache (`identifier -> RateLimitConfig`).
    local_configs: Arc<DashMap<String, RateLimitConfig>>,
    /// Local default-config cache.
    local_default_config: Arc<RwLock<Option<RateLimitConfig>>>,
    /// Local suspension cache (`identifier -> suspend_until_timestamp`).
    local_suspended: Arc<DashMap<String, u64>>,
    /// Local wait cache (identifier -> wait_until_timestamp).
    local_wait_until: Arc<DashMap<String, u64>>,
    /// Cache for suspension checks.
    /// Key: identifier, Value: (Last Check Time, Suspend Until Timestamp)
    suspend_cache: Arc<DashMap<String, (Instant, Option<u64>)>>,
    /// Optional coordination backend (embedded Raft, etc.). When set, its member count is used
    /// to **divide the global limit per node** (approximate distributed rate limiting); when
    /// unset the limit is not divided (single-node semantics).
    coordination: Option<Arc<dyn crate::utils::coordination::CoordinationBackend>>,
}

impl std::fmt::Debug for DistributedSlidingWindowRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedSlidingWindowRateLimiter")
            .field("has_coordination", &self.coordination.is_some())
            .field("key_prefix", &self.key_prefix)
            .field("default_config", &self.default_config)
            .finish()
    }
}

impl DistributedSlidingWindowRateLimiter {
    /// Creates a new rate limiter instance.
    ///
    /// # Parameters
    /// * `locker` - Distributed lock manager protecting read/modify/write.
    /// * `namespace` - Key prefix used to namespace limiter instances.
    /// * `default_config` - Default rate-limit config.
    pub fn new(
        locker: Arc<DistributedLockManager>,
        namespace: &str,
        default_config: RateLimitConfig,
    ) -> Self {
        Self::new_with_coordination(locker, None, namespace, default_config)
    }

    /// Construct with a coordination backend: in cluster mode the global limit is divided
    /// across nodes by member count.
    pub fn new_with_coordination(
        locker: Arc<DistributedLockManager>,
        coordination: Option<Arc<dyn crate::utils::coordination::CoordinationBackend>>,
        namespace: &str,
        default_config: RateLimitConfig,
    ) -> Self {
        let sub_prefix = "rate_limiter".to_string();
        let key_prefix = format!("{namespace}:{sub_prefix}");
        let default_config_key = format!("{key_prefix}:default_config");
        let config_key_prefix = format!("{key_prefix}:config");

        Self {
            lock_manager: locker,
            key_prefix,
            sub_prefix,
            default_config_key,
            config_key_prefix,
            default_config,
            local_last_request: Arc::new(DashMap::new()),
            local_configs: Arc::new(DashMap::new()),
            local_default_config: Arc::new(RwLock::new(None)),
            local_suspended: Arc::new(DashMap::new()),
            local_wait_until: Arc::new(DashMap::new()),
            suspend_cache: Arc::new(DashMap::new()),
            coordination,
        }
    }

    /// Divide the per-second limit by the cluster member count (only when a coordination
    /// backend is present); without one, the limit is not divided. The return value is always
    /// > 0, to avoid a division by zero in the interval computation.
    fn share_rps(&self, rps: f32) -> f32 {
        let n = self
            .coordination
            .as_ref()
            .map(|c| c.cluster_size().max(1))
            .unwrap_or(1) as f32;
        (rps / n).max(f32::MIN_POSITIVE)
    }

    /// Resolve the **effective** rate-limit config for an identifier: applies the cluster
    /// division on top of the global config from [`get_key_config`](Self::get_key_config).
    /// Every rate-limiting decision path should go through this rather than reading the global
    /// config directly.
    async fn effective_config(&self, identifier: &str) -> RateLimitConfig {
        let mut cfg = match self.get_key_config(identifier).await {
            Ok(c) => c,
            Err(e) => {
                error!("{e:?}");
                self.default_config.clone()
            }
        };
        cfg.max_requests_per_second = self.share_rps(cfg.max_requests_per_second);
        cfg
    }

    /// Returns key for stored last-request timestamp.
    fn get_last_request_key(&self, identifier: &str) -> String {
        format!("{}:last_request:{}", self.key_prefix, identifier)
    }

    /// Returns distributed-lock key.
    fn get_lock_key(&self, identifier: &str) -> String {
        format!("{}:lock:{}", self.sub_prefix, identifier)
    }

    /// Returns per-key config key.
    fn get_config_key(&self, identifier: &str) -> String {
        format!("{}:{}", self.config_key_prefix, identifier)
    }

    /// Returns suspension key.
    fn get_suspended_key(&self, identifier: &str) -> String {
        format!("{}:suspended:{}", self.key_prefix, identifier)
    }

    /// Suspends requests for specified identifier (circuit breaker).
    pub async fn suspend(&self, identifier: &str, duration: Duration) -> Result<()> {
        let current_time = self.get_current_timestamp().await?;
        let suspend_until = current_time + duration.as_millis() as u64;

        self.local_suspended
            .insert(identifier.to_string(), suspend_until);
        // Invalidate cache
        self.suspend_cache.remove(identifier);
        Ok(())
    }

    /// Checks whether identifier is currently suspended.
    async fn check_suspended(&self, identifier: &str) -> Result<Option<u64>> {
        // Check short-lived cache first.
        // IMPORTANT: Extract values immediately and drop the DashMap guard before
        // any `.await` — holding a `parking_lot` read-lock across a yield point
        // can deadlock the Tokio runtime when a writer on the same shard blocks
        // an OS thread.
        let cached = self.suspend_cache.get(identifier).map(|entry| {
            let (checked_at, suspend_until_opt) = entry.value();
            (*checked_at, *suspend_until_opt)
        });
        // guard dropped here
        if let Some((checked_at, suspend_until_opt)) = cached {
            if checked_at.elapsed() < Duration::from_millis(500) {
                let current_time = self.get_current_timestamp().await?;
                if let Some(suspend_until) = suspend_until_opt {
                    if suspend_until > current_time {
                        return Ok(Some(suspend_until - current_time));
                    }
                } else {
                    return Ok(None);
                }
            }
        }

        let current_time = self.get_current_timestamp().await?;
        let mut found_suspend_until: Option<u64> = None;

        if let Some(suspend_until) = self.local_suspended.get(identifier) {
            if *suspend_until > current_time {
                found_suspend_until = Some(*suspend_until);
            } else {
                // Lazy cleanup
                drop(suspend_until); // release lock
                self.local_suspended.remove(identifier);
            }
        }

        // Update cache
        self.suspend_cache.insert(
            identifier.to_string(),
            (Instant::now(), found_suspend_until),
        );

        if let Some(suspend_until) = found_suspend_until {
            Ok(Some(suspend_until - current_time))
        } else {
            Ok(None)
        }
    }

    /// Returns current timestamp in milliseconds (local monotonic wall clock).
    async fn get_current_timestamp(&self) -> Result<u64> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64)
    }

    /// Sets rate-limit config for a specific key.
    pub async fn set_key_config(&self, key: &str, config: RateLimitConfig) -> Result<()> {
        // Check cache first to avoid redundant writes
        if let Some(existing) = self.local_configs.get(key) {
            if *existing == config {
                return Ok(());
            }
        }
        self.local_configs.insert(key.to_string(), config);
        Ok(())
    }

    pub async fn set_limit(&self, id: &str, limit: f32) -> Result<()> {
        let config = RateLimitConfig::new(limit);
        self.set_key_config(id, config).await
    }

    /// Sets global limit configuration (affects all keys).
    pub async fn set_all_limit(&self, limit: f32) -> Result<()> {
        // Update local default config.
        let mut default_config = self.default_config.clone();
        default_config.max_requests_per_second = limit;
        *self.local_default_config.write().await = Some(default_config);

        // Update all local configs.
        for mut entry in self.local_configs.iter_mut() {
            entry.value_mut().max_requests_per_second = limit;
        }
        Ok(())
    }

    /// Sets rate-limit configs for multiple keys in batch.
    pub async fn set_key_configs(&self, configs: HashMap<String, RateLimitConfig>) -> Result<()> {
        for (key, config) in configs {
            self.local_configs.insert(key, config);
        }
        Ok(())
    }

    /// Removes a key-specific config (falls back to default config).
    pub async fn remove_key_config(&self, key: &str) -> Result<()> {
        self.local_configs.remove(key);
        Ok(())
    }

    /// Gets config for a specific key.
    ///
    /// # Returns
    /// Key-specific config, or default config when not set.
    pub async fn get_key_config(&self, key: &str) -> Result<RateLimitConfig> {
        if let Some(config) = self.local_configs.get(key) {
            return Ok(config.clone());
        }
        if let Some(config) = self.local_default_config.read().await.as_ref() {
            return Ok(config.clone());
        }
        // Fallback to hard-coded default config.
        Ok(self.default_config.clone())
    }

    /// Returns all configured keys and their configs.
    pub async fn get_all_key_configs(&self) -> Result<HashMap<String, RateLimitConfig>> {
        let mut configs = HashMap::new();
        for entry in self.local_configs.iter() {
            configs.insert(entry.key().clone(), entry.value().clone());
        }
        Ok(configs)
    }

    /// Records one request.
    pub async fn record(&self, identifier: &str) -> Result<()> {
        let lock_key = self.get_lock_key(identifier);
        let last_request_key = self.get_last_request_key(identifier);

        // Acquire distributed lock to protect the read/modify/write.
        if let Ok(acquired) = self
            .lock_manager
            .acquire_lock(&lock_key, 30, Duration::from_secs(5))
            .await
        {
            if !acquired {
                return Err(RateLimitError::Backend("Failed to acquire lock".into()).into());
            }

            let current_time = self.get_current_timestamp().await?;
            self.local_last_request
                .insert(last_request_key, current_time);

            // Release lock.
            let _ = self.lock_manager.release_lock(&lock_key).await;
        } else {
            return Err(RateLimitError::Backend("Failed to acquire lock".into()).into());
        }

        Ok(())
    }

    /// Tries to acquire permit (atomic operation).
    pub async fn acquire(&self, identifier: &str, _permits: f64) -> Result<()> {
        let wait_ms = self.check_and_update(identifier).await?;
        if wait_ms > 0 {
            let wait_secs = wait_ms.div_ceil(1000);
            return Err(RateLimitError::WaitTime(wait_secs).into());
        }
        Ok(())
    }

    /// Verifies whether max limit has been reached.
    ///
    /// # Returns
    /// * `Ok(None)` - Limit not reached.
    /// * `Ok(Some(wait_ms))` - Limit reached; wait time in milliseconds.
    pub async fn verify(&self, identifier: &str) -> Result<Option<u64>> {
        // Check suspension first
        if let Some(wait) = self.check_suspended(identifier).await? {
            return Ok(Some(wait));
        }

        let config = self.effective_config(identifier).await;

        let last_request_key = self.get_last_request_key(identifier);
        let min_interval_millis =
            (config.window_size_millis as f64 / config.max_requests_per_second as f64) as u64;

        let current_time = self.get_current_timestamp().await?;

        if let Some(last_time) = self.local_last_request.get(&last_request_key).map(|v| *v) {
            let elapsed = current_time.saturating_sub(last_time);
            if elapsed < min_interval_millis {
                return Ok(Some(min_interval_millis - elapsed));
            }
        }
        Ok(None)
    }

    /// Atomically checks and updates rate-limit state (competitive model).
    ///
    /// # Returns
    /// * `Ok(0)` - Permit acquired.
    /// * `Ok(wait_ms)` - Must wait `wait_ms` milliseconds then retry.
    pub async fn check_and_update(&self, identifier: &str) -> Result<u64> {
        // Check suspension first
        if let Some(wait) = self.check_suspended(identifier).await? {
            return Ok(wait);
        }

        let current_time = self.get_current_timestamp().await?;

        // Check local wait cache to reduce contention.
        if let Some(entry) = self.local_wait_until.get(identifier) {
            let wait_until = *entry;
            if wait_until > current_time {
                return Ok(wait_until - current_time);
            }
            drop(entry);
            self.local_wait_until.remove(identifier);
        }

        let config = self.effective_config(identifier).await;

        let last_request_key = self.get_last_request_key(identifier);
        let min_interval_millis =
            (config.window_size_millis as f64 / config.max_requests_per_second as f64) as u64;

        // Local-mode implementation (locking via DashMap entry).
        let entry = self.local_last_request.entry(last_request_key);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                let last_time = *occ.get();
                let elapsed = current_time.saturating_sub(last_time);
                if elapsed < min_interval_millis {
                    return Ok(min_interval_millis - elapsed);
                }
                occ.insert(current_time);
                Ok(0)
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                vac.insert(current_time);
                Ok(0)
            }
        }
    }

    /// Reserves a unique rate-limit time slot for the given identifier.
    ///
    /// Each caller is assigned its own future slot atomically (via the DashMap
    /// entry lock).  With N concurrent callers the slots are T, T+interval, … so
    /// every caller makes exactly one pass — no thundering-herd.
    ///
    /// # Returns
    /// * `Ok(0)` — Permit acquired immediately.
    /// * `Ok(wait_ms)` — Slot reserved; caller should sleep then proceed.
    async fn reserve_permit(&self, identifier: &str) -> Result<u64> {
        let current_time = self.get_current_timestamp().await?;

        let config = self.effective_config(identifier).await;

        let last_request_key = self.get_last_request_key(identifier);
        let min_interval_millis =
            (config.window_size_millis as f64 / config.max_requests_per_second as f64) as u64;

        // DashMap entry lock gives atomicity on one process.
        let entry = self.local_last_request.entry(last_request_key);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                let last_time = *occ.get();
                let next_available = (last_time + min_interval_millis).max(current_time);
                let wait = next_available.saturating_sub(current_time);
                occ.insert(next_available);
                Ok(wait)
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                vac.insert(current_time);
                Ok(0)
            }
        }
    }

    /// Waits until a rate-limit permit is available, then returns.
    ///
    /// Recommended entry point for download tasks.  Handles:
    /// 1. **Suspension** (circuit breaker / 429 backoff) — waits until cleared.
    /// 2. **Slot reservation** — each caller gets a unique future time slot, then
    ///    sleeps exactly the right amount.
    ///
    /// No polling loop, no thundering-herd, no request abandonment.
    pub async fn wait_for_permit(&self, identifier: &str) -> Result<()> {
        // Phase 1: wait for any active suspension to clear.
        while let Some(remaining_ms) = self.check_suspended(identifier).await? {
            tokio::time::sleep(Duration::from_millis(remaining_ms)).await;
        }

        // Phase 2: reserve a slot in the rate-limit queue.
        let wait_ms = self.reserve_permit(identifier).await?;
        if wait_ms > 0 {
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
        }
        Ok(())
    }

    /// Returns time until next request can proceed (milliseconds).
    pub async fn get_next_available_time(&self, identifier: &str) -> Result<u64> {
        self.verify(identifier).await.map(|res| res.unwrap_or(0))
    }

    /// Decreases rate-limit throughput (backpressure).
    pub async fn decrease_limit(&self, identifier: &str, factor: f32) -> Result<f32> {
        let mut config = self.get_key_config(identifier).await?;

        // Ensure `base_max_requests_per_second` is initialized.
        if config.base_max_requests_per_second.is_none() {
            config.base_max_requests_per_second = Some(config.max_requests_per_second);
        }

        // Compute new limit.
        let new_limit = config.max_requests_per_second * factor;
        // Apply minimum floor to avoid zero.
        config.max_requests_per_second = new_limit.max(0.1);

        self.set_key_config(identifier, config.clone()).await?;
        Ok(config.max_requests_per_second)
    }

    /// Tries to restore rate-limit throughput.
    pub async fn try_restore_limit(&self, identifier: &str, step_factor: f32) -> Result<f32> {
        let mut config = self.get_key_config(identifier).await?;

        if let Some(base) = config.base_max_requests_per_second
            && config.max_requests_per_second < base
        {
            let new_limit = config.max_requests_per_second * step_factor;
            config.max_requests_per_second = new_limit.min(base);
            self.set_key_config(identifier, config.clone()).await?;
        }

        Ok(config.max_requests_per_second)
    }

    /// Cleans all expired records (reclaims local memory).
    pub async fn cleanup(&self) -> Result<u64> {
        let mut total_cleaned = 0u64;
        let current_time = self.get_current_timestamp().await?;

        // Collect keys to delete to avoid mutating while iterating.
        let mut keys_to_remove = Vec::new();

        for entry in self.local_last_request.iter() {
            let last_time = *entry.value();

            let min_interval_millis = (self.default_config.window_size_millis as f64
                / self.default_config.max_requests_per_second as f64)
                as u64;
            let max_valid_age = min_interval_millis * 2;

            if current_time.saturating_sub(last_time) > max_valid_age {
                keys_to_remove.push(entry.key().clone());
            }
        }

        for key in keys_to_remove {
            if self.local_last_request.remove(&key).is_some() {
                total_cleaned += 1;
            }
        }

        Ok(total_cleaned)
    }

    /// Returns number of identifiers currently stored.
    pub async fn get_identifier_count(&self) -> Result<usize> {
        Ok(self.local_last_request.len())
    }

    /// Resets records for a specific identifier.
    pub async fn reset(&self, identifier: &str) -> Result<()> {
        let key = self.get_last_request_key(identifier);
        self.local_last_request.remove(&key);
        Ok(())
    }

    /// Resets all records.
    pub async fn reset_all(&self) -> Result<()> {
        self.local_last_request.clear();
        Ok(())
    }

    /// Returns key prefix (for debugging).
    pub fn get_key_prefix(&self) -> &str {
        &self.key_prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Minimal CoordinationBackend that only overrides `cluster_size`, used to verify that the
    /// limit is divided correctly.
    struct SizeBackend(usize);
    #[async_trait::async_trait]
    impl crate::utils::coordination::CoordinationBackend for SizeBackend {
        async fn publish(&self, _: &str, _: &[u8]) -> std::result::Result<(), String> {
            Ok(())
        }
        async fn subscribe(
            &self,
            _: &str,
        ) -> std::result::Result<tokio::sync::mpsc::Receiver<Vec<u8>>, String> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }
        async fn set(&self, _: &str, _: &[u8]) -> std::result::Result<(), String> {
            Ok(())
        }
        async fn get(&self, _: &str) -> std::result::Result<Option<Vec<u8>>, String> {
            Ok(None)
        }
        async fn cas(
            &self,
            _: &str,
            _: Option<&[u8]>,
            _: &[u8],
        ) -> std::result::Result<bool, String> {
            Ok(true)
        }
        async fn acquire_lock(
            &self,
            _: &str,
            _: &[u8],
            _: u64,
        ) -> std::result::Result<bool, String> {
            Ok(true)
        }
        async fn renew_lock(&self, _: &str, _: &[u8], _: u64) -> std::result::Result<bool, String> {
            Ok(true)
        }
        fn cluster_size(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn shares_global_limit_by_cluster_size() {
        // 4-node cluster: a global limit of 8 per second divides to 2 per node.
        let locker = Arc::new(DistributedLockManager::new("t"));
        let backend: Arc<dyn crate::utils::coordination::CoordinationBackend> =
            Arc::new(SizeBackend(4));
        let limiter = DistributedSlidingWindowRateLimiter::new_with_coordination(
            locker,
            Some(backend),
            "ns",
            RateLimitConfig::new(8.0),
        );
        assert_eq!(limiter.share_rps(8.0), 2.0);

        // No coordination backend: the limit is not divided (single-node semantics unchanged).
        let plain = DistributedSlidingWindowRateLimiter::new(
            Arc::new(DistributedLockManager::new("t")),
            "ns",
            RateLimitConfig::new(8.0),
        );
        assert_eq!(plain.share_rps(8.0), 8.0);
    }

    #[tokio::test]
    async fn test_rate_limiter_local() {
        let lock_manager = Arc::new(DistributedLockManager::new("test"));
        let config = RateLimitConfig::new(1.0); // 1 request per second
        let limiter = DistributedSlidingWindowRateLimiter::new(lock_manager, "test", config);

        // First request should succeed
        assert!(limiter.verify("user1").await.unwrap().is_none());
        limiter.record("user1").await.unwrap();

        // Second request immediately after should be delayed
        // interval is 1000ms
        let res = limiter.verify("user1").await.unwrap();
        assert!(res.is_some());
    }

    #[tokio::test]
    async fn test_cleanup() {
        let lock_manager = Arc::new(DistributedLockManager::new("test_cleanup"));
        let config = RateLimitConfig::new(10.0); // 10 req/s => 100ms interval => 200ms ttl
        let limiter = DistributedSlidingWindowRateLimiter::new(lock_manager, "test", config);

        limiter.record("user_cleanup").await.unwrap();
        assert!(limiter.get_identifier_count().await.unwrap() > 0);

        // Wait for expiration (max valid age is roughly 200ms)
        tokio::time::sleep(Duration::from_millis(300)).await;

        let cleaned = limiter.cleanup().await.unwrap();
        assert!(cleaned > 0);
        assert_eq!(limiter.get_identifier_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_adaptive_rate_limit() {
        let lock_manager = Arc::new(DistributedLockManager::new("test_adaptive"));
        let config = RateLimitConfig::new(10.0);
        let limiter = DistributedSlidingWindowRateLimiter::new(lock_manager, "test", config);

        limiter.decrease_limit("adaptive", 0.5).await.unwrap();
        let limit = limiter
            .get_key_config("adaptive")
            .await
            .unwrap()
            .max_requests_per_second;
        assert_eq!(limit, 5.0);

        limiter.try_restore_limit("adaptive", 1.5).await.unwrap();
        let limit = limiter
            .get_key_config("adaptive")
            .await
            .unwrap()
            .max_requests_per_second;
        assert_eq!(limit, 7.5);
    }

    #[tokio::test]
    async fn test_suspend() {
        let lock_manager = Arc::new(DistributedLockManager::new("test_suspend"));
        let config = RateLimitConfig::new(10.0);
        let limiter = DistributedSlidingWindowRateLimiter::new(lock_manager, "test", config);

        limiter
            .suspend("user_suspend", Duration::from_millis(200))
            .await
            .unwrap();

        let res = limiter.verify("user_suspend").await.unwrap();
        assert!(res.is_some());

        tokio::time::sleep(Duration::from_millis(300)).await;

        // After wait, verify should pass (if no record exists, verify returns None)
        let res = limiter.verify("user_suspend").await.unwrap();
        assert!(res.is_none());
    }
}
