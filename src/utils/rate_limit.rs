#![allow(unused)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Rate limiter configuration.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per second.
    pub max_requests_per_second: f32,
    /// Window size (default: 1 second).
    pub window_size: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 2.0,
            window_size: Duration::from_secs(1),
        }
    }
}

impl RateLimitConfig {
    pub fn new(max_requests_per_second: f32) -> Self {
        Self {
            max_requests_per_second,
            window_size: Duration::from_secs(1),
        }
    }

    pub fn with_window_size(mut self, window_size: Duration) -> Self {
        self.window_size = window_size;
        self
    }
}

/// Sliding-window rate limiter.
///
/// Uses a sliding-window strategy to throttle requests per identifier.
/// Optimizations:
/// 1. Uses read/write locks to reduce contention and allow concurrent reads.
/// 2. Stores timestamps directly to reduce memory overhead.
/// 3. Optimized cleanup logic to avoid unnecessary scans.
/// 4. Supports per-key rate limits.
/// 5. Supports dynamic key-level config updates.
#[derive(Debug)]
pub struct SlidingWindowRateLimiter {
    /// Default rate-limit config (used for keys without explicit config).
    default_config: Arc<RwLock<RateLimitConfig>>,
    /// Per-key custom configuration.
    key_configs: Arc<RwLock<HashMap<String, RateLimitConfig>>>,
    /// Timestamp lists per identifier.
    records: Arc<RwLock<HashMap<String, Vec<Instant>>>>,
}
impl Default for SlidingWindowRateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

impl SlidingWindowRateLimiter {
    /// Creates a new rate limiter instance.
    ///
    /// # Parameters
    /// * `default_config` - Default rate-limit configuration.
    pub fn new(default_config: RateLimitConfig) -> Self {
        Self {
            default_config: Arc::new(RwLock::new(default_config)),
            key_configs: Arc::new(RwLock::new(HashMap::new())),
            records: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a simple limiter (backward-compatible helper).
    ///
    /// # Parameters
    /// * `max_requests_per_second` - Maximum requests per second (supports decimals).
    pub fn simple(max_requests_per_second: f32) -> Self {
        Self::new(RateLimitConfig::new(max_requests_per_second))
    }

    /// Sets rate-limit config for a specific key.
    ///
    /// # Parameters
    /// * `key` - Target key.
    /// * `config` - Rate-limit configuration.
    pub async fn set_key_config(&self, key: String, config: RateLimitConfig) {
        let mut configs = self.key_configs.write().await;
        configs.insert(key, config);
    }
    /// Sets global limit (affects all keys).
    pub async fn set_limit(&self, limit: f32) {
        self.default_config.write().await.max_requests_per_second = limit;
        let mut config = self.key_configs.write().await;
        config.iter_mut().for_each(|(_, config)| {
            config.max_requests_per_second = limit;
        })
    }

    /// Sets multiple key configs in batch.
    ///
    /// # Parameters
    /// * `configs` - Mapping from key to config.
    pub async fn set_key_configs(&self, configs: HashMap<String, RateLimitConfig>) {
        let mut key_configs = self.key_configs.write().await;
        for (key, config) in configs {
            key_configs.insert(key, config);
        }
    }

    /// Removes config for a key (falls back to default config).
    ///
    /// # Parameters
    /// * `key` - Key whose config should be removed.
    pub async fn remove_key_config(&self, key: &str) {
        let mut configs = self.key_configs.write().await;
        configs.remove(key);
    }

    /// Gets config for a specific key.
    ///
    /// # Parameters
    /// * `key` - Key to query.
    ///
    /// # Returns
    /// Key-specific config, or default config if none is set.
    pub async fn get_key_config(&self, key: &str) -> RateLimitConfig {
        let configs = self.key_configs.read().await;
        if let Some(config) = configs.get(key) {
            config.clone()
        } else {
            self.default_config.read().await.clone()
        }
    }

    /// Returns all configured keys and their configs.
    pub async fn get_all_key_configs(&self) -> HashMap<String, RateLimitConfig> {
        let configs = self.key_configs.read().await;
        configs.clone()
    }

    /// Records a request.
    ///
    /// # Parameters
    /// * `identifier` - Identifier used to distinguish request sources.
    pub async fn record(&self, identifier: String) {
        let config = self.get_key_config(&identifier).await;
        let mut records = self.records.write().await;
        let entry = records.entry(identifier.clone()).or_insert_with(Vec::new);

        // Add new timestamp.
        entry.push(Instant::now());

        // Clean expired records (return value ignored because entry was just appended).
        let _ = self.clean_expired_records_internal(entry, &config);

        // Opportunistic cleanup to avoid long-term buildup of inactive identifiers.
        self.opportunistic_cleanup(&mut records).await;
    }

    /// Verifies whether request limit has been reached.
    ///
    /// # Parameters
    /// * `identifier` - Identifier used to distinguish request sources.
    ///
    /// # Returns
    /// * `Ok(())` - Limit not reached; request can proceed.
    /// * `Err(u64)` - Limit reached; wait time in milliseconds.
    pub async fn verify(&self, identifier: &str) -> Result<(), u64> {
        let config = self.get_key_config(identifier).await;
        let records = self.records.read().await;

        if let Some(entry) = records.get(identifier) {
            // No records: allow immediately.
            if entry.is_empty() {
                return Ok(());
            }

            // Compute minimum interval between adjacent requests.
            let min_interval = Duration::from_millis(
                (config.window_size.as_millis() as f64 / config.max_requests_per_second as f64)
                    as u64,
            );

            // Get timestamp of the last request.
            let last_request_time = entry.last().unwrap();
            let elapsed_since_last = last_request_time.elapsed();

            // Check whether interval is sufficient.
            if elapsed_since_last >= min_interval {
                Ok(())
            } else {
                // Interval too short; compute remaining wait time.
                let wait_time = min_interval - elapsed_since_last;
                Err(wait_time.as_millis() as u64)
            }
        } else {
            // No records: allow immediately.
            Ok(())
        }
    }

    /// Verifies and records a request (atomic operation).
    ///
    /// # Parameters
    /// * `identifier` - Identifier used to distinguish request sources.
    ///
    /// # Returns
    /// * `Ok(())` - Limit not reached; request was recorded.
    /// * `Err(u64)` - Limit reached; wait time in milliseconds.
    pub async fn verify_and_record(&self, identifier: String) -> Result<(), u64> {
        let config = self.get_key_config(&identifier).await;
        let mut records = self.records.write().await;
        // Perform opportunistic cleanup while holding write lock.
        self.opportunistic_cleanup(&mut records).await;

        let entry = records.entry(identifier).or_insert_with(Vec::new);
        // Clean expired records.
        self.clean_expired_records_internal(entry, &config);

        let now = Instant::now();

        // No records: allow immediately.
        if entry.is_empty() {
            entry.push(now);
            return Ok(());
        }

        // Compute minimum interval between adjacent requests.
        // Interval = window_size / max_requests_per_second.
        let min_interval = Duration::from_millis(
            (config.window_size.as_millis() as f64 / config.max_requests_per_second as f64) as u64,
        );

        // Get timestamp of the last request.
        let last_request_time = entry.last().unwrap();
        let elapsed_since_last = now.duration_since(*last_request_time);

        // Check whether interval is sufficient.
        if elapsed_since_last >= min_interval {
            // Interval is sufficient; record request.
            entry.push(now);
            Ok(())
        } else {
            // Interval too short; compute remaining wait time.
            let wait_time = min_interval - elapsed_since_last;
            Err(wait_time.as_millis() as u64)
        }
    }

    /// Internal helper: cleans expired timestamps.
    ///
    /// # Parameters
    /// * `records` - Timestamp list to clean.
    /// * `config` - Config for this key.
    ///
    /// # Returns
    /// * `true` - No records remain after cleanup; identifier can be removed.
    /// * `false` - Valid records remain.
    fn clean_expired_records_internal(
        &self,
        records: &mut Vec<Instant>,
        config: &RateLimitConfig,
    ) -> bool {
        let now = Instant::now();
        let cutoff_time = now - config.window_size;

        // Keep records within current window.
        records.retain(|&timestamp| timestamp > cutoff_time);

        // Return whether identifier should be removed.
        records.is_empty()
    }

    /// Opportunistic cleanup: prune expired identifiers during write operations.
    /// This prevents long-inactive identifiers from lingering in memory indefinitely.
    async fn opportunistic_cleanup(&self, records: &mut HashMap<String, Vec<Instant>>) {
        let total_identifiers = records.len();

        // Clean up at most 5 identifiers, or 10% of total (minimum 1), whichever is smaller.
        // For small maps, still perform lightweight cleanup to avoid stale key retention.
        let max_cleanup_count = std::cmp::min(5, (total_identifiers / 10).max(1));

        let mut keys_to_remove = Vec::new();

        // Iterate over a subset of identifiers for cleanup.
        for (checked_count, (key, entry)) in records.iter_mut().enumerate() {
            if checked_count >= max_cleanup_count {
                break;
            }

            // Get config for this key.
            let config = self.get_key_config(key).await;
            if self.clean_expired_records_internal(entry, &config) {
                keys_to_remove.push(key.clone());
            }
        }

        // Remove empty identifiers.
        for key in keys_to_remove {
            records.remove(&key);
        }
    }

    /// Counts valid records without mutating source data.
    fn count_valid_records(&self, records: &[Instant], config: &RateLimitConfig) -> f32 {
        let now = Instant::now();
        let cutoff_time = now - config.window_size;

        records
            .iter()
            .filter(|&&timestamp| timestamp > cutoff_time)
            .count() as f32
    }

    /// Calculates wait time in milliseconds.
    ///
    /// # Parameters
    /// * `records` - Timestamp list.
    /// * `config` - Config for this key.
    ///
    /// # Returns
    /// Milliseconds to wait.
    fn calculate_wait_time(&self, records: &[Instant], config: &RateLimitConfig) -> u64 {
        if records.is_empty() {
            return 0;
        }

        // Find oldest record.
        let oldest_record = match records.iter().min() {
            Some(record) => record,
            None => return 0, // Should not happen because `is_empty()` is checked above.
        };

        // Compute elapsed duration since oldest record.
        let elapsed = oldest_record.elapsed();

        // If still in window, compute remaining wait time.
        if elapsed < config.window_size {
            let remaining = config.window_size - elapsed;
            remaining.as_millis() as u64 + 1 // Add 1ms to ensure next check can pass.
        } else {
            0
        }
    }

    /// Returns current request count for identifier.
    ///
    /// # Parameters
    /// * `identifier` - Identifier.
    ///
    /// # Returns
    /// Number of requests in current window.
    pub async fn get_current_count(&self, identifier: &str) -> usize {
        let config = self.get_key_config(identifier).await;
        let records = self.records.read().await;
        if let Some(entry) = records.get(identifier) {
            self.count_valid_records(entry, &config) as usize
        } else {
            0
        }
    }

    /// Returns time until next available request (milliseconds).
    ///
    /// # Parameters
    /// * `identifier` - Identifier.
    ///
    /// # Returns
    /// Wait time in milliseconds; `0` means request is immediately available.
    pub async fn get_next_available_time(&self, identifier: &str) -> u64 {
        let config = self.get_key_config(identifier).await;
        let records = self.records.read().await;

        if let Some(entry) = records.get(identifier) {
            if entry.is_empty() {
                return 0;
            }

            // Compute minimum interval between adjacent requests.
            let min_interval = Duration::from_millis(
                (config.window_size.as_millis() as f64 / config.max_requests_per_second as f64)
                    as u64,
            );

            // Get timestamp of the last request.
            let last_request_time = entry.last().unwrap();
            let elapsed_since_last = last_request_time.elapsed();

            if elapsed_since_last >= min_interval {
                0
            } else {
                let wait_time = min_interval - elapsed_since_last;
                wait_time.as_millis() as u64
            }
        } else {
            0
        }
    }

    /// Cleans all expired records.
    /// This can be called periodically to reclaim memory.
    /// Note: methods already perform automatic cleanup; this is primarily for manual cleanup.
    pub async fn cleanup(&self) {
        let mut records = self.records.write().await;
        let mut keys_to_remove = Vec::new();

        for (key, entry) in records.iter_mut() {
            let config = self.get_key_config(key).await;
            if self.clean_expired_records_internal(entry, &config) {
                keys_to_remove.push(key.clone());
            }
        }

        // Remove empty entries.
        for key in keys_to_remove {
            records.remove(&key);
        }
    }

    /// Returns number of identifiers currently stored.
    pub async fn get_identifier_count(&self) -> usize {
        let records = self.records.read().await;
        records.len()
    }

    /// Resets records for a specific identifier.
    pub async fn reset(&self, identifier: &str) {
        let mut records = self.records.write().await;
        records.remove(identifier);
    }

    /// Resets all records.
    pub async fn reset_all(&self) {
        let mut records = self.records.write().await;
        records.clear();
    }
}

impl Clone for SlidingWindowRateLimiter {
    fn clone(&self) -> Self {
        Self {
            default_config: self.default_config.clone(),
            key_configs: Arc::clone(&self.key_configs),
            records: Arc::clone(&self.records),
        }
    }
}
