use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::errors::Result;
use crate::errors::error::RateLimitError;
use crate::utils::distributed_lock::DistributedLockManager;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimitConfig {
    pub max_requests_per_second: f32,
    pub window_size_millis: u64,
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

#[derive(Clone)]
pub struct DistributedSlidingWindowRateLimiter {
    backend: Option<Arc<dyn RateLimitBackend>>,
    key_prefix: String,
    default_config: RateLimitConfig,
    local_last_request: Arc<DashMap<String, u64>>,
    local_configs: Arc<DashMap<String, RateLimitConfig>>,
    local_default_config: Arc<tokio::sync::RwLock<Option<RateLimitConfig>>>,
    local_suspended: Arc<DashMap<String, u64>>,
    local_wait_until: Arc<DashMap<String, u64>>,
    suspend_cache: Arc<DashMap<String, (Instant, Option<u64>)>>,
}

impl std::fmt::Debug for DistributedSlidingWindowRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedSlidingWindowRateLimiter")
            .field("key_prefix", &self.key_prefix)
            .field("has_backend", &self.backend.is_some())
            .finish()
    }
}

impl DistributedSlidingWindowRateLimiter {
    pub fn new(
        _limit_pool: Option<()>,
        _locker: Arc<DistributedLockManager>,
        namespace: &str,
        default_config: RateLimitConfig,
    ) -> Self {
        Self::build(None, namespace, default_config)
    }

    pub fn with_backend(
        backend: Arc<dyn RateLimitBackend>,
        namespace: &str,
        default_config: RateLimitConfig,
    ) -> Self {
        Self::build(Some(backend), namespace, default_config)
    }

    fn build(
        backend: Option<Arc<dyn RateLimitBackend>>,
        namespace: &str,
        default_config: RateLimitConfig,
    ) -> Self {
        let key_prefix = format!("{namespace}:rate_limiter");
        Self {
            backend,
            key_prefix,
            default_config,
            local_last_request: Arc::new(DashMap::new()),
            local_configs: Arc::new(DashMap::new()),
            local_default_config: Arc::new(tokio::sync::RwLock::new(None)),
            local_suspended: Arc::new(DashMap::new()),
            local_wait_until: Arc::new(DashMap::new()),
            suspend_cache: Arc::new(DashMap::new()),
        }
    }

    fn get_last_request_key(&self, identifier: &str) -> String {
        format!("{}:last_request:{}", self.key_prefix, identifier)
    }

    fn get_suspended_key(&self, identifier: &str) -> String {
        format!("{}:suspended:{}", self.key_prefix, identifier)
    }

    pub async fn suspend(&self, identifier: &str, duration: Duration) -> Result<()> {
        let current_time = self.get_current_timestamp().await?;
        let suspend_until = current_time + duration.as_millis() as u64;

        if let Some(backend) = &self.backend {
            backend
                .suspend(
                    &self.get_suspended_key(identifier),
                    suspend_until,
                    duration.as_millis() as u64,
                )
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
        } else {
            self.local_suspended
                .insert(identifier.to_string(), suspend_until);
        }

        self.suspend_cache.remove(identifier);
        Ok(())
    }

    async fn check_suspended(&self, identifier: &str) -> Result<Option<u64>> {
        if let Some(entry) = self.suspend_cache.get(identifier) {
            let (checked_at, suspend_until_opt) = *entry.value();
            drop(entry);
            if checked_at.elapsed() < Duration::from_millis(500) {
                let current_time = self.get_current_timestamp().await?;
                return Ok(suspend_until_opt
                    .filter(|suspend_until| *suspend_until > current_time)
                    .map(|suspend_until| suspend_until - current_time));
            }
        }

        let current_time = self.get_current_timestamp().await?;
        let found_suspend_until = if let Some(backend) = &self.backend {
            backend
                .check_suspended(&self.get_suspended_key(identifier), current_time)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?
                .map(|remaining| current_time + remaining)
        } else if let Some(suspend_until) = self.local_suspended.get(identifier) {
            let value = *suspend_until;
            drop(suspend_until);
            if value > current_time {
                Some(value)
            } else {
                self.local_suspended.remove(identifier);
                None
            }
        } else {
            None
        };

        self.suspend_cache.insert(
            identifier.to_string(),
            (Instant::now(), found_suspend_until),
        );

        Ok(found_suspend_until.map(|suspend_until| suspend_until - current_time))
    }

    async fn get_current_timestamp(&self) -> Result<u64> {
        if let Some(backend) = &self.backend {
            return backend
                .current_time_ms()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()).into());
        }

        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64)
    }

    pub async fn set_key_config(&self, key: &str, config: RateLimitConfig) -> Result<()> {
        if self
            .local_configs
            .get(key)
            .is_some_and(|existing| *existing == config)
        {
            return Ok(());
        }
        self.local_configs.insert(key.to_string(), config);
        Ok(())
    }

    pub async fn set_limit(&self, id: &str, limit: f32) -> Result<()> {
        self.set_key_config(id, RateLimitConfig::new(limit)).await
    }

    pub async fn set_all_limit(&self, limit: f32) -> Result<()> {
        let mut default_config = self.default_config.clone();
        default_config.max_requests_per_second = limit;
        *self.local_default_config.write().await = Some(default_config);

        for mut entry in self.local_configs.iter_mut() {
            entry.value_mut().max_requests_per_second = limit;
        }
        Ok(())
    }

    pub async fn set_key_configs(&self, configs: HashMap<String, RateLimitConfig>) -> Result<()> {
        for (key, config) in configs {
            self.local_configs.insert(key, config);
        }
        Ok(())
    }

    pub async fn remove_key_config(&self, key: &str) -> Result<()> {
        self.local_configs.remove(key);
        Ok(())
    }

    pub async fn get_key_config(&self, key: &str) -> Result<RateLimitConfig> {
        if let Some(config) = self.local_configs.get(key) {
            return Ok(config.clone());
        }
        if let Some(default_config) = self.local_default_config.read().await.clone() {
            return Ok(default_config);
        }
        Ok(self.default_config.clone())
    }

    pub async fn get_all_key_configs(&self) -> Result<HashMap<String, RateLimitConfig>> {
        Ok(self
            .local_configs
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect())
    }

    pub async fn record(&self, identifier: &str) -> Result<()> {
        let current_time = self.get_current_timestamp().await?;
        let config = self.get_key_config(identifier).await?;
        let ttl_ms = (config.window_size_millis * 2).max(1);
        let key = self.get_last_request_key(identifier);

        if let Some(backend) = &self.backend {
            backend
                .record(&key, current_time, ttl_ms)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
        } else {
            self.local_last_request
                .insert(identifier.to_string(), current_time);
        }
        self.local_wait_until.remove(identifier);
        Ok(())
    }

    pub async fn acquire(&self, identifier: &str, _permits: f64) -> Result<()> {
        self.wait_for_permit(identifier).await
    }

    pub async fn verify(&self, identifier: &str) -> Result<Option<u64>> {
        if let Some(wait_ms) = self.check_suspended(identifier).await? {
            return Ok(Some(wait_ms));
        }

        let current_time = self.get_current_timestamp().await?;
        let config = self.get_key_config(identifier).await?;
        let min_interval = Self::min_interval_ms(&config);

        if let Some(backend) = &self.backend {
            let wait = backend
                .check_and_update(
                    &self.get_last_request_key(identifier),
                    current_time,
                    min_interval,
                    0,
                )
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            return Ok((wait > 0).then_some(wait));
        }

        if let Some(last_time) = self.local_last_request.get(identifier) {
            let elapsed = current_time.saturating_sub(*last_time);
            if elapsed < min_interval {
                return Ok(Some(min_interval - elapsed));
            }
        }
        Ok(None)
    }

    pub async fn check_and_update(&self, identifier: &str) -> Result<u64> {
        if let Some(wait_ms) = self.check_suspended(identifier).await? {
            return Ok(wait_ms);
        }

        let current_time = self.get_current_timestamp().await?;
        let config = self.get_key_config(identifier).await?;
        let min_interval = Self::min_interval_ms(&config);
        let ttl_ms = (config.window_size_millis * 2).max(min_interval * 2).max(1);

        if let Some(backend) = &self.backend {
            return backend
                .check_and_update(
                    &self.get_last_request_key(identifier),
                    current_time,
                    min_interval,
                    ttl_ms,
                )
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()).into());
        }

        match self.local_last_request.entry(identifier.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let elapsed = current_time.saturating_sub(*entry.get());
                if elapsed < min_interval {
                    Ok(min_interval - elapsed)
                } else {
                    entry.insert(current_time);
                    Ok(0)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(current_time);
                Ok(0)
            }
        }
    }

    async fn reserve_permit(&self, identifier: &str) -> Result<u64> {
        let current_time = self.get_current_timestamp().await?;
        let config = self.get_key_config(identifier).await?;
        let min_interval = Self::min_interval_ms(&config);
        let ttl_ms = (config.window_size_millis * 2).max(min_interval * 2).max(1);

        if let Some(backend) = &self.backend {
            return backend
                .reserve_permit(
                    &self.get_last_request_key(identifier),
                    current_time,
                    min_interval,
                    ttl_ms,
                )
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()).into());
        }

        match self.local_last_request.entry(identifier.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let next_available = (*entry.get() + min_interval).max(current_time);
                let wait = next_available.saturating_sub(current_time);
                entry.insert(next_available);
                Ok(wait)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(current_time);
                Ok(0)
            }
        }
    }

    pub async fn wait_for_permit(&self, identifier: &str) -> Result<()> {
        if let Some(wait_ms) = self.check_suspended(identifier).await? {
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
        }

        if let Some(wait_until) = self.local_wait_until.get(identifier) {
            let current_time = self.get_current_timestamp().await?;
            if *wait_until > current_time {
                tokio::time::sleep(Duration::from_millis(*wait_until - current_time)).await;
            }
        }

        let wait_ms = self.reserve_permit(identifier).await?;
        if wait_ms > 0 {
            self.local_wait_until.insert(
                identifier.to_string(),
                self.get_current_timestamp().await? + wait_ms,
            );
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
        }
        Ok(())
    }

    pub async fn get_next_available_time(&self, identifier: &str) -> Result<u64> {
        let current_time = self.get_current_timestamp().await?;
        Ok(self
            .verify(identifier)
            .await?
            .map(|wait| current_time + wait)
            .unwrap_or(current_time))
    }

    pub async fn decrease_limit(&self, identifier: &str, factor: f32) -> Result<f32> {
        let mut config = self.get_key_config(identifier).await?;
        let base = config
            .base_max_requests_per_second
            .unwrap_or(config.max_requests_per_second);
        config.base_max_requests_per_second = Some(base);
        config.max_requests_per_second = (config.max_requests_per_second * factor).max(0.1);
        let new_limit = config.max_requests_per_second;
        self.set_key_config(identifier, config).await?;
        Ok(new_limit)
    }

    pub async fn try_restore_limit(&self, identifier: &str, step_factor: f32) -> Result<f32> {
        let mut config = self.get_key_config(identifier).await?;
        let base = config
            .base_max_requests_per_second
            .unwrap_or(config.max_requests_per_second);
        config.max_requests_per_second = (config.max_requests_per_second * step_factor).min(base);
        let new_limit = config.max_requests_per_second;
        self.set_key_config(identifier, config).await?;
        Ok(new_limit)
    }

    pub async fn cleanup(&self) -> Result<u64> {
        let current_time = self.get_current_timestamp().await?;
        let cutoff = current_time.saturating_sub(self.default_config.window_size_millis * 2);

        if let Some(backend) = &self.backend {
            return backend
                .cleanup(&self.key_prefix, self.default_config.window_size_millis * 2)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()).into());
        }

        let mut removed = 0;
        self.local_last_request.retain(|_, last| {
            if *last < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        Ok(removed)
    }

    pub async fn get_identifier_count(&self) -> Result<usize> {
        Ok(self.local_last_request.len())
    }

    pub async fn reset(&self, identifier: &str) -> Result<()> {
        let key = self.get_last_request_key(identifier);
        if let Some(backend) = &self.backend {
            backend
                .cleanup(&key, 0)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
        }
        self.local_last_request.remove(identifier);
        self.local_wait_until.remove(identifier);
        Ok(())
    }

    pub async fn reset_all(&self) -> Result<()> {
        self.local_last_request.clear();
        self.local_wait_until.clear();
        self.local_suspended.clear();
        self.suspend_cache.clear();
        Ok(())
    }

    pub fn get_key_prefix(&self) -> &str {
        &self.key_prefix
    }

    fn min_interval_ms(config: &RateLimitConfig) -> u64 {
        if config.max_requests_per_second <= 0.0 {
            config.window_size_millis.max(1)
        } else {
            (config.window_size_millis as f32 / config.max_requests_per_second).ceil() as u64
        }
        .max(1)
    }
}

#[async_trait]
pub trait RateLimitBackend: Send + Sync + std::fmt::Debug {
    async fn check_and_update(
        &self,
        key: &str,
        current_time_ms: u64,
        min_interval_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<u64, String>;

    async fn reserve_permit(
        &self,
        key: &str,
        current_time_ms: u64,
        min_interval_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<u64, String>;

    async fn current_time_ms(&self) -> std::result::Result<u64, String>;

    async fn cleanup(&self, prefix: &str, older_than_ms: u64) -> std::result::Result<u64, String>;

    async fn record(
        &self,
        key: &str,
        current_time_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<(), String>;

    async fn suspend(
        &self,
        key: &str,
        suspend_until_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<(), String>;

    async fn check_suspended(
        &self,
        key: &str,
        current_time_ms: u64,
    ) -> std::result::Result<Option<u64>, String>;
}

#[derive(Debug)]
pub struct LocalRateLimitBackend {
    last_request: Arc<DashMap<String, u64>>,
    suspended: Arc<DashMap<String, u64>>,
}

impl Default for LocalRateLimitBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalRateLimitBackend {
    pub fn new() -> Self {
        Self {
            last_request: Arc::new(DashMap::new()),
            suspended: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl RateLimitBackend for LocalRateLimitBackend {
    async fn check_and_update(
        &self,
        key: &str,
        current_time_ms: u64,
        min_interval_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<u64, String> {
        if ttl_ms == 0 {
            if let Some(last_time) = self.last_request.get(key) {
                let elapsed = current_time_ms.saturating_sub(*last_time);
                return Ok(min_interval_ms.saturating_sub(elapsed));
            }
            return Ok(0);
        }

        match self.last_request.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let elapsed = current_time_ms.saturating_sub(*entry.get());
                if elapsed < min_interval_ms {
                    Ok(min_interval_ms - elapsed)
                } else {
                    entry.insert(current_time_ms);
                    Ok(0)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(current_time_ms);
                Ok(0)
            }
        }
    }

    async fn reserve_permit(
        &self,
        key: &str,
        current_time_ms: u64,
        min_interval_ms: u64,
        _ttl_ms: u64,
    ) -> std::result::Result<u64, String> {
        match self.last_request.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let next_available = (*entry.get() + min_interval_ms).max(current_time_ms);
                let wait = next_available.saturating_sub(current_time_ms);
                entry.insert(next_available);
                Ok(wait)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(current_time_ms);
                Ok(0)
            }
        }
    }

    async fn current_time_ms(&self) -> std::result::Result<u64, String> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64)
    }

    async fn cleanup(&self, prefix: &str, older_than_ms: u64) -> std::result::Result<u64, String> {
        let cutoff = self.current_time_ms().await?.saturating_sub(older_than_ms);
        let mut removed = 0;
        self.last_request.retain(|key, last| {
            if key.starts_with(prefix) && *last < cutoff {
                removed += 1;
                false
            } else {
                true
            }
        });
        Ok(removed)
    }

    async fn record(
        &self,
        key: &str,
        current_time_ms: u64,
        _ttl_ms: u64,
    ) -> std::result::Result<(), String> {
        self.last_request.insert(key.to_string(), current_time_ms);
        Ok(())
    }

    async fn suspend(
        &self,
        key: &str,
        suspend_until_ms: u64,
        _ttl_ms: u64,
    ) -> std::result::Result<(), String> {
        self.suspended.insert(key.to_string(), suspend_until_ms);
        Ok(())
    }

    async fn check_suspended(
        &self,
        key: &str,
        current_time_ms: u64,
    ) -> std::result::Result<Option<u64>, String> {
        if let Some(entry) = self.suspended.get(key) {
            let suspend_until = *entry;
            drop(entry);
            if suspend_until > current_time_ms {
                return Ok(Some(suspend_until - current_time_ms));
            }
            self.suspended.remove(key);
        }
        Ok(None)
    }
}

pub struct RaftRateLimitBackend {
    store: Arc<ProfileControlPlaneStore>,
    namespace: String,
}

impl std::fmt::Debug for RaftRateLimitBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftRateLimitBackend")
            .field("namespace", &self.namespace)
            .finish()
    }
}

impl RaftRateLimitBackend {
    pub fn new(store: Arc<ProfileControlPlaneStore>, namespace: impl Into<String>) -> Self {
        Self {
            store,
            namespace: namespace.into(),
        }
    }

    fn last_request_key(&self, key: &str) -> String {
        format!("ratelimit:last:{key}")
    }

    fn suspend_key(&self, key: &str) -> String {
        format!("ratelimit:suspended:{key}")
    }

    fn bucket_key(&self, group: &str, key: &str, bucket_ms: u64) -> String {
        format!("ratelimit:{group}:{key}:{bucket_ms}")
    }

    pub async fn hit(
        &self,
        group: &str,
        key: &str,
        window_ms: u64,
        max_requests: u64,
    ) -> std::result::Result<(bool, u64), String> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let bucket_size_ms = 1000u64.max(window_ms / 10);
        let current_bucket = self.bucket_key(group, key, now_ms / bucket_size_ms * bucket_size_ms);

        let mut total: u64 = 0;
        for i in 0..=(window_ms / bucket_size_ms) {
            let bucket_ms = (now_ms / bucket_size_ms).saturating_sub(i) * bucket_size_ms;
            let bucket = self.bucket_key(group, key, bucket_ms);
            if let Some(value) = self.store.read_cache_counter(&self.namespace, &bucket) {
                total = total.saturating_add(value as u64);
            }
        }

        if total >= max_requests {
            return Ok((false, total));
        }

        self.store
            .submit_cache_incr_by(&self.namespace, &current_bucket, 1, Some(window_ms * 2))
            .await
            .map_err(|e| format!("{e}"))?;
        Ok((true, total.saturating_add(1)))
    }

    pub async fn cleanup_buckets(
        &self,
        group: &str,
        key: &str,
        older_than_ms: u64,
    ) -> std::result::Result<usize, String> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now_ms.saturating_sub(older_than_ms);
        let mut removed = 0usize;

        for i in 0..100u64 {
            let bucket_ms = cutoff.saturating_sub(i * 1000);
            if bucket_ms == 0 {
                break;
            }
            let bucket = self.bucket_key(group, key, bucket_ms);
            if self
                .store
                .read_cache_counter(&self.namespace, &bucket)
                .is_some()
            {
                self.store
                    .submit_cache_del(&self.namespace, &bucket)
                    .await
                    .map_err(|e| format!("{e}"))?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[async_trait]
impl RateLimitBackend for RaftRateLimitBackend {
    async fn check_and_update(
        &self,
        key: &str,
        current_time_ms: u64,
        min_interval_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<u64, String> {
        let lk = self.last_request_key(key);
        if let Some(last_time_bytes) = self.store.read_cache_value(&self.namespace, &lk)
            && let Ok(last_time_str) = String::from_utf8(last_time_bytes)
            && let Ok(last_time) = last_time_str.parse::<u64>()
        {
            let elapsed = current_time_ms.saturating_sub(last_time);
            if elapsed < min_interval_ms {
                return Ok(min_interval_ms - elapsed);
            }
        }

        if ttl_ms > 0 {
            self.store
                .submit_cache_set(
                    &self.namespace,
                    &lk,
                    current_time_ms.to_string().into_bytes(),
                    Some(ttl_ms),
                )
                .await
                .map_err(|e| format!("{e}"))?;
        }
        Ok(0)
    }

    async fn reserve_permit(
        &self,
        key: &str,
        current_time_ms: u64,
        min_interval_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<u64, String> {
        let lk = self.last_request_key(key);
        let next_available = if let Some(last_time_bytes) =
            self.store.read_cache_value(&self.namespace, &lk)
            && let Ok(last_time_str) = String::from_utf8(last_time_bytes)
            && let Ok(last_time) = last_time_str.parse::<u64>()
        {
            (last_time + min_interval_ms).max(current_time_ms)
        } else {
            current_time_ms
        };
        let wait = next_available.saturating_sub(current_time_ms);
        self.store
            .submit_cache_set(
                &self.namespace,
                &lk,
                next_available.to_string().into_bytes(),
                Some(ttl_ms.max(min_interval_ms * 2)),
            )
            .await
            .map_err(|e| format!("{e}"))?;
        Ok(wait)
    }

    async fn current_time_ms(&self) -> std::result::Result<u64, String> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64)
    }

    async fn cleanup(
        &self,
        _prefix: &str,
        _older_than_ms: u64,
    ) -> std::result::Result<u64, String> {
        let keys = self
            .store
            .list_cache_keys(&self.namespace, "ratelimit:last:", 100);
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        if !key_refs.is_empty() {
            self.store
                .submit_cache_del_batch(&self.namespace, &key_refs)
                .await
                .map_err(|e| format!("{e}"))?;
        }
        Ok(keys.len() as u64)
    }

    async fn record(
        &self,
        key: &str,
        current_time_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<(), String> {
        self.store
            .submit_cache_set(
                &self.namespace,
                &self.last_request_key(key),
                current_time_ms.to_string().into_bytes(),
                Some(ttl_ms),
            )
            .await
            .map_err(|e| format!("{e}"))
    }

    async fn suspend(
        &self,
        key: &str,
        suspend_until_ms: u64,
        ttl_ms: u64,
    ) -> std::result::Result<(), String> {
        self.store
            .submit_cache_set(
                &self.namespace,
                &self.suspend_key(key),
                suspend_until_ms.to_string().into_bytes(),
                Some(ttl_ms),
            )
            .await
            .map_err(|e| format!("{e}"))
    }

    async fn check_suspended(
        &self,
        key: &str,
        current_time_ms: u64,
    ) -> std::result::Result<Option<u64>, String> {
        if let Some(value) = self
            .store
            .read_cache_value(&self.namespace, &self.suspend_key(key))
            && let Ok(value_str) = String::from_utf8(value)
            && let Ok(suspend_until) = value_str.parse::<u64>()
            && suspend_until > current_time_ms
        {
            return Ok(Some(suspend_until - current_time_ms));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_local() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test"));
        let limiter = DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager,
            "test",
            RateLimitConfig::new(1.0),
        );

        assert!(limiter.verify("user1").await.unwrap().is_none());
        limiter.record("user1").await.unwrap();
        assert!(limiter.verify("user1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_cleanup() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_cleanup"));
        let limiter = DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager,
            "test",
            RateLimitConfig::new(10.0),
        );

        limiter.record("user_cleanup").await.unwrap();
        assert!(limiter.get_identifier_count().await.unwrap() > 0);
        let expired_at = limiter
            .get_current_timestamp()
            .await
            .unwrap()
            .saturating_sub(limiter.default_config.window_size_millis * 3);
        limiter
            .local_last_request
            .insert("user_cleanup".to_string(), expired_at);
        assert!(limiter.cleanup().await.unwrap() > 0);
        assert_eq!(limiter.get_identifier_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_adaptive_rate_limit() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_adaptive"));
        let limiter = DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager,
            "test",
            RateLimitConfig::new(10.0),
        );

        limiter.decrease_limit("adaptive", 0.5).await.unwrap();
        assert_eq!(
            limiter
                .get_key_config("adaptive")
                .await
                .unwrap()
                .max_requests_per_second,
            5.0
        );

        limiter.try_restore_limit("adaptive", 1.5).await.unwrap();
        assert_eq!(
            limiter
                .get_key_config("adaptive")
                .await
                .unwrap()
                .max_requests_per_second,
            7.5
        );
    }

    #[tokio::test]
    async fn test_suspend() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_suspend"));
        let limiter = DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager,
            "test",
            RateLimitConfig::new(10.0),
        );

        limiter
            .suspend("user_suspend", Duration::from_millis(200))
            .await
            .unwrap();
        assert!(limiter.verify("user_suspend").await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(limiter.verify("user_suspend").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raft_rate_limit_hits_to_limit() {
        let store = ProfileControlPlaneStore::default();
        let backend = RaftRateLimitBackend::new(Arc::new(store), "ns");

        for _ in 0..5 {
            let (allowed, _) = backend.hit("g1", "k1", 10_000, 5).await.unwrap();
            assert!(allowed);
        }
        let (allowed, count) = backend.hit("g1", "k1", 10_000, 5).await.unwrap();
        assert!(!allowed);
        assert!(count >= 5);
    }

    #[tokio::test]
    async fn raft_rate_limit_different_groups_isolated() {
        let store = ProfileControlPlaneStore::default();
        let backend = RaftRateLimitBackend::new(Arc::new(store), "ns");

        for _ in 0..3 {
            assert!(backend.hit("g1", "k1", 10_000, 3).await.unwrap().0);
        }
        assert!(!backend.hit("g1", "k1", 10_000, 3).await.unwrap().0);
        assert!(backend.hit("g2", "k1", 10_000, 3).await.unwrap().0);
    }

    #[tokio::test]
    async fn raft_rate_limit_cleanup_does_not_error() {
        let store = ProfileControlPlaneStore::default();
        let backend = RaftRateLimitBackend::new(Arc::new(store), "ns");

        backend.hit("gc", "kc", 10_000, 100).await.unwrap();
        let _ = backend.cleanup_buckets("gc", "kc", 10_000).await.unwrap();
    }
}
