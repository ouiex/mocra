#![allow(unused)]
use crate::redis_lock::{AdvancedDistributedLock, DistributedLockManager};
use dashmap::DashMap;
use deadpool_redis::{
    Pool,
    redis::{AsyncCommands, cmd},
};
use errors::Result;
use errors::error::RateLimitError;
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// 限流器配置项
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimitConfig {
    /// 每秒最大请求次数
    pub max_requests_per_second: f32,
    /// 窗口大小（默认1秒）
    pub window_size_millis: u64,
    /// 基准每秒最大请求次数（用于恢复）
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

/// 基于Redis的分布式平滑限流器
/// 使用平滑限流算法来限制特定标识符的访问频率
/// 功能特性：
/// 1. 分布式限流，支持多实例共享限流状态
/// 2. 平滑限流：计算每个请求的间隔时间，确保请求平均分布
/// 3. 使用AdvancedDistributedLock保护Redis数据读写操作
/// 4. 支持为不同的key设置不同的速率限制
/// 5. 支持动态添加和修改key的速率配置
/// 6. 本地计算，Redis仅用于存储和同步最后请求时间
/// 7. 每个key的配置存储在Redis中，支持持久化和动态更新
#[derive(Debug, Clone)]
pub struct DistributedSlidingWindowRateLimiter {
    /// Redis连接池
    pool: Option<Arc<Pool>>,
    /// 分布式锁管理器
    lock_manager: Arc<DistributedLockManager>,
    /// Redis key前缀
    key_prefix: String,
    /// Sub prefix
    sub_prefix: String,
    /// 默认配置Redis key
    default_config_key: String,
    /// 个别配置Redis key前缀
    config_key_prefix: String,
    /// 默认限流配置
    default_config: RateLimitConfig,
    /// 本地最后请求时间缓存 (identifier -> last_request_timestamp)
    local_last_request: Arc<DashMap<String, u64>>,
    /// 本地配置缓存 (identifier -> RateLimitConfig)
    local_configs: Arc<DashMap<String, RateLimitConfig>>,
    /// 本地默认配置缓存
    local_default_config: Arc<RwLock<Option<RateLimitConfig>>>,
    /// 本地暂停状态缓存 (identifier -> suspend_until_timestamp)
    local_suspended: Arc<DashMap<String, u64>>,
    /// Local wait cache to reduce Redis contention (identifier -> wait_until_timestamp)
    local_wait_until: Arc<DashMap<String, u64>>,
/// Time offset between local clock and Redis clock (local - redis)
/// Used to reduce Redis TIME commands
    time_offset: Arc<RwLock<Option<(i64, Instant)>>>,
    /// Cache for suspension checks to reduce Redis load
    /// Key: identifier, Value: (Last Check Time, Suspend Until Timestamp)
    suspend_cache: Arc<DashMap<String, (Instant, Option<u64>)>>,
}

const TIME_OFFSET_REFRESH_SECS: u64 = 60;

impl DistributedSlidingWindowRateLimiter {
    /// 创建新的分布式限流器实例
    ///
    /// # 参数
    /// * `pool` - Redis连接池
    /// * `key_prefix` - Redis key前缀，用于区分不同的限流器实例
    /// * `default_config` - 默认的限流配置
    pub fn new(
        limit_pool: Option<Arc<Pool>>,
        locker: Arc<DistributedLockManager>,
        namespace: &str,
        default_config: RateLimitConfig,
    ) -> Self {
        let sub_prefix = "rate_limiter".to_string();
        let key_prefix = format!("{namespace}:{sub_prefix}");
        let default_config_key = format!("{key_prefix}:default_config");
        let config_key_prefix = format!("{key_prefix}:config");

        Self {
            pool: limit_pool,
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
            time_offset: Arc::new(RwLock::new(None)),
            suspend_cache: Arc::new(DashMap::new()),
        }
    }

    /// 获取Redis记录的key（存储最后请求时间戳）
    fn get_last_request_key(&self, identifier: &str) -> String {
        format!("{}:last_request:{}", self.key_prefix, identifier)
    }

    /// 获取分布式锁的key
    fn get_lock_key(&self, identifier: &str) -> String {
        format!("{}:lock:{}", self.sub_prefix, identifier)
    }

    /// 获取配置的key
    fn get_config_key(&self, identifier: &str) -> String {
        format!("{}:{}", self.config_key_prefix, identifier)
    }

    /// 获取暂停key
    fn get_suspended_key(&self, identifier: &str) -> String {
        format!("{}:suspended:{}", self.key_prefix, identifier)
    }

    /// 暂停指定标识符的请求（Circuit Breaker）
    pub async fn suspend(&self, identifier: &str, duration: Duration) -> Result<()> {
        let current_time = self.get_current_timestamp().await?;
        let suspend_until = current_time + duration.as_millis() as u64;

        if let Some(pool) = &self.pool {
            let mut conn = pool.get().await.map_err(|e| RateLimitError::RedisError(e.into()))?;
            let key = self.get_suspended_key(identifier);
            // Set with TTL
            let _: () = conn
                .set_ex(&key, suspend_until, duration.as_secs())
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
        } else {
            self.local_suspended.insert(identifier.to_string(), suspend_until);
        }
        // Invalidate cache
        self.suspend_cache.remove(identifier);
        Ok(())
    }

    /// 检查是否被暂停
    async fn check_suspended(&self, identifier: &str) -> Result<Option<u64>> {
        // Check short-lived cache first
        if let Some(entry) = self.suspend_cache.get(identifier) {
            let (checked_at, suspend_until_opt) = entry.value();
            if checked_at.elapsed() < Duration::from_millis(500) {
                 let current_time = self.get_current_timestamp().await?;
                 if let Some(suspend_until) = suspend_until_opt {
                     if *suspend_until > current_time {
                         return Ok(Some(*suspend_until - current_time));
                     }
                 } else {
                     return Ok(None);
                 }
            }
        }

        let current_time = self.get_current_timestamp().await?;
        let mut found_suspend_until: Option<u64> = None;
        
        if let Some(pool) = &self.pool {
             let mut conn = pool.get().await.map_err(|e| RateLimitError::RedisError(e.into()))?;
             let key = self.get_suspended_key(identifier);
             if let Some(suspend_until) = conn.get::<_, Option<u64>>(&key).await.map_err(|e| RateLimitError::RedisError(e.into()))? {
                  if suspend_until > current_time {
                      found_suspend_until = Some(suspend_until);
                  }
             }
        } else if let Some(suspend_until) = self.local_suspended.get(identifier) {
             if *suspend_until > current_time {
                 found_suspend_until = Some(*suspend_until);
             } else {
                 // Lazy cleanup
                 drop(suspend_until); // release lock
                 self.local_suspended.remove(identifier);
             }
        }

        // Update cache
        self.suspend_cache.insert(identifier.to_string(), (Instant::now(), found_suspend_until));

        if let Some(suspend_until) = found_suspend_until {
            Ok(Some(suspend_until - current_time))
        } else {
            Ok(None)
        }
    }

    /// 获取当前时间戳（毫秒），优先使用Redis时间以避免时钟偏差
    async fn get_current_timestamp(&self) -> Result<u64> {
        if let Some(pool) = &self.pool {
            // Try to use cached offset
            if let Some((offset, last_refresh)) = *self.time_offset.read().await {
                if last_refresh.elapsed() < Duration::from_secs(TIME_OFFSET_REFRESH_SECS) {
                    let local_now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64;
                    // redis_time = local_time - offset
                    let redis_time = local_now - offset;
                    return Ok(redis_time as u64);
                }
            }

            let mut conn = pool.get().await.map_err(|e| RateLimitError::RedisError(e.into()))?;
            // TIME command returns (seconds, microseconds)
            let time: (u64, u64) = deadpool_redis::redis::cmd("TIME")
                .query_async(&mut conn)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            
            let redis_millis = time.0 * 1000 + time.1 / 1000;
            let local_now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            
            // Calculate and cache offset: offset = local - redis
            let offset = local_now - redis_millis as i64;
            *self.time_offset.write().await = Some((offset, Instant::now()));

            Ok(redis_millis)
        } else {
            Ok(SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64)
        }
    }

    /// 为特定key设置限流配置
    ///
    /// # 参数
    /// * `key` - 要配置的key
    /// * `config` - 限流配置
    pub async fn set_key_config(&self, key: &str, config: RateLimitConfig) -> Result<()> {
        // Check cache first to avoid redundant Redis writes
        if let Some(existing) = self.local_configs.get(key) {
            if *existing == config {
                return Ok(());
            }
        }

        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError("redis connect error".into()))?;
            let config_key = self.get_config_key(key);
            let config_json = serde_json::to_string(&config)?;
            let _: () = conn
                .set(&config_key, &config_json)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            
            // Update cache
            self.local_configs.insert(key.to_string(), config);
        } else {
            self.local_configs.insert(key.to_string(), config);
        }
        Ok(())
    }

    pub async fn set_limit(&self, id: &str, limit: f32) -> Result<()> {
        let config = RateLimitConfig::new(limit);
        self.set_key_config(id, config).await
    }
    /// 设置全局限流配置（影响所有key）
    pub async fn set_all_limit(&self, limit: f32) -> Result<()> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            // 更新默认配置
            let mut default_config = self.default_config.clone();
            default_config.max_requests_per_second = limit;
            let config_json = serde_json::to_string(&default_config)?;
            let _: () = conn
                .set(&self.default_config_key, &config_json)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            // 更新所有已配置的key
            let pattern = format!("{}:*", self.config_key_prefix);
            let keys: Vec<String> = conn
                .keys(&pattern)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            for key in keys {
                let config_json: String = conn.get(&key).await.unwrap_or_default();
                if !config_json.is_empty()
                    && let Ok(mut config) = serde_json::from_str::<RateLimitConfig>(&config_json) {
                        config.max_requests_per_second = limit;
                        let updated_json = serde_json::to_string(&config)?;
                        let _: () = conn
                            .set(&key, &updated_json)
                            .await
                            .map_err(|e| RateLimitError::RedisError(e.into()))?;
                    }
            }
        } else {
            // 更新本地默认配置
            let mut default_config = self.default_config.clone();
            default_config.max_requests_per_second = limit;
            *self.local_default_config.write().await = Some(default_config);

            // 更新所有本地配置
            for mut entry in self.local_configs.iter_mut() {
                entry.value_mut().max_requests_per_second = limit;
            }
        }

        Ok(())
    }

    /// 批量设置多个key的限流配置
    ///
    /// # 参数
    /// * `configs` - key到配置的映射
    pub async fn set_key_configs(&self, configs: HashMap<String, RateLimitConfig>) -> Result<()> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let mut pipe = deadpool_redis::redis::pipe();
            
            let mut updates = Vec::new();

            for (key, config) in configs {
                 // Check cache
                if let Some(existing) = self.local_configs.get(&key) {
                    if *existing == config {
                        continue;
                    }
                }
                
                let config_key = self.get_config_key(&key);
                let config_json = serde_json::to_string(&config)?;
                pipe.set(&config_key, &config_json);
                updates.push((key, config));
            }

            if !updates.is_empty() {
                let _: () = pipe
                    .query_async(&mut *conn)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?;
                
                // Update cache
                for (key, config) in updates {
                    self.local_configs.insert(key, config);
                }
            }
        } else {
            for (key, config) in configs {
                self.local_configs.insert(key, config);
            }
        }
        Ok(())
    }

    /// 移除特定key的配置（将使用默认配置）
    ///
    /// # 参数
    /// * `key` - 要移除配置的key
    pub async fn remove_key_config(&self, key: &str) -> Result<()> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let config_key = self.get_config_key(key);
            let _: usize = conn
                .del(&config_key)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
        } else {
            self.local_configs.remove(key);
        }
        Ok(())
    }

    /// 获取特定key的配置
    ///
    /// # 参数
    /// * `key` - 要查询的key
    ///
    /// # 返回值
    /// 该key的配置，如果没有特定配置则返回默认配置
    pub async fn get_key_config(&self, key: &str) -> Result<RateLimitConfig> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let config_key = self.get_config_key(key);

            // 先尝试获取特定配置
            let config_json: Option<String> = conn
                .get(&config_key)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            if let Some(json) = config_json
                && let Ok(config) = serde_json::from_str::<RateLimitConfig>(&json) {
                    return Ok(config);
                }

            // 尝试获取默认配置
            let default_json: Option<String> = conn
                .get(&self.default_config_key)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            if let Some(json) = default_json
                && let Ok(config) = serde_json::from_str::<RateLimitConfig>(&json) {
                    return Ok(config);
                }
        } else {
            // 本地模式
            if let Some(config) = self.local_configs.get(key) {
                return Ok(config.clone());
            }
            if let Some(config) = self.local_default_config.read().await.as_ref() {
                return Ok(config.clone());
            }
        }

        // 返回硬编码的默认配置
        Ok(self.default_config.clone())
    }

    /// 获取所有已配置的key及其配置
    pub async fn get_all_key_configs(&self) -> Result<HashMap<String, RateLimitConfig>> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let pattern = format!("{}:*", self.config_key_prefix);
            let keys: Vec<String> = conn
                .keys(&pattern)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            let mut configs = HashMap::new();
            for key in keys {
                let config_json: Option<String> = conn
                    .get(&key)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?;
                if let Some(json) = config_json
                    && let Ok(config) = serde_json::from_str::<RateLimitConfig>(&json) {
                        // 提取实际的identifier（去掉前缀）
                        let identifier = key
                            .strip_prefix(&format!("{}:", self.config_key_prefix))
                            .unwrap_or(&key);
                        configs.insert(identifier.to_string(), config);
                    }
            }
            Ok(configs)
        } else {
            let mut configs = HashMap::new();
            for entry in self.local_configs.iter() {
                configs.insert(entry.key().clone(), entry.value().clone());
            }
            Ok(configs)
        }
    }

    /// 记录一次请求
    ///
    /// # 参数
    /// * `identifier` - 用于区分不同请求来源的标识符
    pub async fn record(&self, identifier: &str) -> Result<()> {
        let lock_key = self.get_lock_key(identifier);
        let last_request_key = self.get_last_request_key(identifier);

        // 获取分布式锁保护Redis操作
        if let Ok(acquired) = self
            .lock_manager
            .acquire_lock(&lock_key, 30, Duration::from_secs(5))
            .await
        {
            if !acquired {
                return Err(RateLimitError::RedisError("Failed to acquire lock".into()).into());
            }

            let current_time = self.get_current_timestamp().await?;

            if let Some(pool) = &self.pool {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| RateLimitError::RedisError("redis connect error".into()))?;

                // 设置TTL，基于实际请求间隔时间，确保下次验证时记录仍然存在
                let config = self.get_key_config(identifier).await?;
                let min_interval_millis = (config.window_size_millis as f64
                    / config.max_requests_per_second as f64)
                    as u64;
                let ttl_millis = min_interval_millis * 2;
                let ttl_seconds = (ttl_millis / 1000).max(1);
                // 更新最后请求时间
                let _: () = conn
                    .set_ex(&last_request_key, current_time, ttl_seconds)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?;
            } else {
                // 本地模式
                self.local_last_request
                    .insert(last_request_key, current_time);
            }

            // 释放锁
            let _ = self.lock_manager.release_lock(&lock_key).await;
        } else {
            return Err(RateLimitError::RedisError("Failed to acquire lock".into()).into());
        }

        Ok(())
    }

    /// 尝试获取许可 (原子操作)
    pub async fn acquire(&self, identifier: &str, _permits: f64) -> Result<()> {
        let wait_ms = self.check_and_update(identifier).await?;
        if wait_ms > 0 {
            let wait_secs = wait_ms.div_ceil(1000);
            return Err(RateLimitError::WaitTime(wait_secs).into());
        }
        Ok(())
    }

    /// 验证是否达到最大限制
    ///
    /// # 参数
    /// * `identifier` - 用于区分不同请求来源的标识符
    ///
    /// # 返回值
    /// * `Ok(())` - 未达到限制，可以继续
    /// * `Err(u64)` - 达到限制，返回需要等待的毫秒数
    pub async fn verify(&self, identifier: &str) -> Result<Option<u64>> {
        // Check suspension first
        if let Some(wait) = self.check_suspended(identifier).await? {
            return Ok(Some(wait));
        }

        let config = match self.get_key_config(identifier).await {
            Ok(cfg) => cfg,
            Err(e) => {
                error!("{e:?}");
                self.default_config.clone()
            }
        };

        let last_request_key = self.get_last_request_key(identifier);
        let min_interval_millis =
            (config.window_size_millis as f64 / config.max_requests_per_second as f64) as u64;
        
        let current_time = self.get_current_timestamp().await?;

        if let Some(pool) = &self.pool {
            let mut conn = pool.get().await.map_err(|e| RateLimitError::RedisError(e.into()))?;
            let script = r#"
            local key = KEYS[1]
            local current_time = tonumber(ARGV[1])
            local min_interval = tonumber(ARGV[2])

            local last_time = redis.call("GET", key)
            if last_time then
                local diff = current_time - tonumber(last_time)
                if diff < min_interval then
                    return min_interval - diff
                end
            end
            return 0
            "#;
            
            let wait_ms: u64 = deadpool_redis::redis::Script::new(script)
                .key(&last_request_key)
                .arg(current_time)
                .arg(min_interval_millis)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            
            if wait_ms > 0 {
                Ok(Some(wait_ms))
            } else {
                Ok(None)
            }
        } else {
             // Local mode
            if let Some(last_time) = self.local_last_request.get(&last_request_key).map(|v| *v) {
                let elapsed = current_time.saturating_sub(last_time);
                if elapsed < min_interval_millis {
                    return Ok(Some(min_interval_millis - elapsed));
                }
            }
            Ok(None)
        }
    }

    /// 原子检查并更新限流状态 (Atomic Check-and-Update)
    ///
    /// # 参数
    /// * `identifier` - 标识符
    ///
    /// # 返回值
    /// * `Ok(0)` - 成功获取许可
    /// * `Ok(wait_ms)` - 需要等待 wait_ms 毫秒
    pub async fn check_and_update(&self, identifier: &str) -> Result<u64> {
        // Check suspension first
        if let Some(wait) = self.check_suspended(identifier).await? {
            return Ok(wait);
        }

        let current_time = self.get_current_timestamp().await?;

        // Check local wait cache to reduce Redis pressure
        if let Some(entry) = self.local_wait_until.get(identifier) {
            let wait_until = *entry;
            if wait_until > current_time {
                return Ok(wait_until - current_time);
            }
            drop(entry);
            self.local_wait_until.remove(identifier);
        }

        let config = match self.get_key_config(identifier).await {
            Ok(cfg) => cfg,
            Err(e) => {
                error!("{e:?}");
                self.default_config.clone()
            }
        };

        let last_request_key = self.get_last_request_key(identifier);
        let min_interval_millis =
            (config.window_size_millis as f64 / config.max_requests_per_second as f64) as u64;
        let ttl_millis = min_interval_millis * 2;
        let ttl_seconds = (ttl_millis / 1000).max(1);

        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            let script = r#"
            local key = KEYS[1]
            local current_time = tonumber(ARGV[1])
            local min_interval = tonumber(ARGV[2])
            local ttl = tonumber(ARGV[3])

            local last_time = redis.call("GET", key)
            if last_time then
                local diff = current_time - tonumber(last_time)
                if diff < min_interval then
                    return min_interval - diff
                end
            end

            redis.call("SETEX", key, ttl, current_time)
            return 0
            "#;

            let result: u64 = deadpool_redis::redis::Script::new(script)
                .key(&last_request_key)
                .arg(current_time)
                .arg(min_interval_millis)
                .arg(ttl_seconds)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            if result > 0 {
                // Update local wait cache
                self.local_wait_until.insert(identifier.to_string(), current_time + result);
            }

            Ok(result)
        } else {
             // Simple implementation for local mode (Locking via DashMap entry)
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
    }

    /// 获取下次可以请求的时间间隔（毫秒）
    ///
    /// # 参数
    /// * `identifier` - 标识符
    ///
    /// # 返回值
    /// 需要等待的毫秒数，0表示可以立即请求
    pub async fn get_next_available_time(&self, identifier: &str) -> Result<u64> {
        self.verify(identifier).await.map(|res| res.unwrap_or(0))
    }

    /// 减少限流速率（Backpressure）
    ///
    /// # 参数
    /// * `identifier` - 标识符
    /// * `factor` - 减少因子 (0.0 - 1.0)，例如 0.5 表示减半
    pub async fn decrease_limit(&self, identifier: &str, factor: f32) -> Result<f32> {
        let mut config = self.get_key_config(identifier).await?;
        
        // 确保base_max_requests_per_second已设置
        if config.base_max_requests_per_second.is_none() {
            config.base_max_requests_per_second = Some(config.max_requests_per_second);
        }

        // 计算新的限制
        let new_limit = config.max_requests_per_second * factor;
        // 设置一个最小下限，避免变为0
        config.max_requests_per_second = new_limit.max(0.1);
        
        self.set_key_config(identifier, config.clone()).await?;
        Ok(config.max_requests_per_second)
    }

    /// 尝试恢复限流速率
    ///
    /// # 参数
    /// * `identifier` - 标识符
    /// * `step_factor` - 恢复步长因子 (> 1.0)，例如 1.1 表示增加 10%
    pub async fn try_restore_limit(&self, identifier: &str, step_factor: f32) -> Result<f32> {
        let mut config = self.get_key_config(identifier).await?;
        
        if let Some(base) = config.base_max_requests_per_second
            && config.max_requests_per_second < base {
                let new_limit = config.max_requests_per_second * step_factor;
                config.max_requests_per_second = new_limit.min(base);
                self.set_key_config(identifier, config.clone()).await?;
            }
        
        Ok(config.max_requests_per_second)
    }

    /// 清理所有过期的记录
    /// 这个方法可以定期调用来释放Redis内存
    pub async fn cleanup(&self) -> Result<u64> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let pattern = format!("{}:last_request:*", self.key_prefix);
            let keys: Vec<String> = conn
                .keys(&pattern)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            let mut total_cleaned = 0u64;
            let current_time = self.get_current_timestamp().await?;

            for key in keys {
                // 检查key是否过期（TTL <= 0）或者检查值是否过期
                let ttl: i64 = conn
                    .ttl(&key)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?;

                let should_delete = if ttl == -1 {
                    // 没有设置TTL，检查值是否过期
                    if let Ok(Some(last_time)) = conn.get::<_, Option<u64>>(&key).await {
                        // 基于默认配置计算最大间隔时间来判断是否过期
                        let min_interval_millis = (self.default_config.window_size_millis as f64
                            / self.default_config.max_requests_per_second as f64)
                            as u64;
                        let max_valid_age = min_interval_millis * 2; // 与TTL设置保持一致
                        current_time.saturating_sub(last_time) > max_valid_age
                    } else {
                        true // 获取不到值，删除key
                    }
                } else {
                    ttl <= 0 // TTL过期
                };

                if should_delete {
                    let deleted: usize = conn
                        .del(&key)
                        .await
                        .map_err(|e| RateLimitError::RedisError(e.into()))?;
                    if deleted > 0 {
                        total_cleaned += 1;
                    }
                }
            }

            Ok(total_cleaned)
        } else {
            // 本地模式清理
            let mut total_cleaned = 0u64;
            let current_time = self.get_current_timestamp().await?;

            // 收集需要删除的key，避免在迭代时修改
            let mut keys_to_remove = Vec::new();

            for entry in self.local_last_request.iter() {
                let last_time = *entry.value();

                // 尝试获取对应的配置来计算过期时间
                // 注意：这里简化处理，如果找不到特定配置就用默认配置
                // 实际上identifier需要从key中解析出来，但这里key就是last_request_key
                // 我们可以尝试解析identifier，或者简单地使用默认配置作为保守估计

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
    }

    /// 获取当前存储的标识符数量
    pub async fn get_identifier_count(&self) -> Result<usize> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let pattern = format!("{}:last_request:*", self.key_prefix);
            let keys: Vec<String> = conn
                .keys(&pattern)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            Ok(keys.len())
        } else {
            Ok(self.local_last_request.len())
        }
    }

    /// 重置指定标识符的记录
    pub async fn reset(&self, identifier: &str) -> Result<()> {
        let key = self.get_last_request_key(identifier);
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let _: usize = conn
                .del(&key)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
        } else {
            self.local_last_request.remove(&key);
        }
        Ok(())
    }

    /// 重置所有记录
    pub async fn reset_all(&self) -> Result<()> {
        if let Some(pool) = &self.pool {
            let mut conn = pool
                .get()
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            let pattern = format!("{}:last_request:*", self.key_prefix);
            let keys: Vec<String> = conn
                .keys(&pattern)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;

            if !keys.is_empty() {
                let _: usize = conn
                    .del(&keys)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?;
            }
        } else {
            self.local_last_request.clear();
        }

        Ok(())
    }

    /// 获取key前缀（用于调试）
    pub fn get_key_prefix(&self) -> &str {
        &self.key_prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_rate_limiter_local() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test"));
        let config = RateLimitConfig::new(1.0); // 1 request per second
        let limiter = DistributedSlidingWindowRateLimiter::new(None, lock_manager, "test", config);

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
         let lock_manager = Arc::new(DistributedLockManager::new(None, "test_cleanup"));
        let config = RateLimitConfig::new(10.0); // 10 req/s => 100ms interval => 200ms ttl
        let limiter = DistributedSlidingWindowRateLimiter::new(None, lock_manager, "test", config);
        
        limiter.record("user_cleanup").await.unwrap();
        // Check count (using internal method exposed via get_identifier_count)
        // Note: record inserts a key.
        assert!(limiter.get_identifier_count().await.unwrap() > 0);
        
        // Wait for expiration (TTL is roughly 200ms)
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        let cleaned = limiter.cleanup().await.unwrap();
        assert!(cleaned > 0);
        assert_eq!(limiter.get_identifier_count().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_adaptive_rate_limit() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_adaptive"));
        let config = RateLimitConfig::new(10.0);
        let limiter = DistributedSlidingWindowRateLimiter::new(None, lock_manager, "test", config);
        
        limiter.decrease_limit("adaptive", 0.5).await.unwrap();
        let limit = limiter.get_key_config("adaptive").await.unwrap().max_requests_per_second;
        assert_eq!(limit, 5.0);
        
        limiter.try_restore_limit("adaptive", 1.5).await.unwrap();
        let limit = limiter.get_key_config("adaptive").await.unwrap().max_requests_per_second;
        assert_eq!(limit, 7.5);
    }

    #[tokio::test]
    async fn test_suspend() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_suspend"));
        let config = RateLimitConfig::new(10.0);
        let limiter = DistributedSlidingWindowRateLimiter::new(None, lock_manager, "test", config);
        
        limiter.suspend("user_suspend", Duration::from_millis(200)).await.unwrap();
        
        let res = limiter.verify("user_suspend").await.unwrap();
        assert!(res.is_some());
        
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        // After wait, verify should pass (if no record exists, verify returns None)
        let res = limiter.verify("user_suspend").await.unwrap();
        assert!(res.is_none());
    }
}
