#![allow(unused)]
use crate::redis_lock::{AdvancedDistributedLock, DistributedLockManager};
use errors::Result;
use errors::error::RateLimitError;
use deadpool_redis::{
    Pool,
    redis::{AsyncCommands, cmd},
};
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use tokio::sync::RwLock;

/// 限流器配置项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// 每秒最大请求次数
    pub max_requests_per_second: f32,
    /// 窗口大小（默认1秒）
    pub window_size_millis: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 2.0,
            window_size_millis: 1000,
        }
    }
}

impl RateLimitConfig {
    pub fn new(max_requests_per_second: f32) -> Self {
        Self {
            max_requests_per_second,
            window_size_millis: 1000,
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
}

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

    /// 获取当前时间戳（毫秒）
    fn get_current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// 为特定key设置限流配置
    ///
    /// # 参数
    /// * `key` - 要配置的key
    /// * `config` - 限流配置
    pub async fn set_key_config(&self, key: &str, config: RateLimitConfig) -> Result<()> {
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
                if !config_json.is_empty() {
                    if let Ok(mut config) = serde_json::from_str::<RateLimitConfig>(&config_json) {
                        config.max_requests_per_second = limit;
                        let updated_json = serde_json::to_string(&config)?;
                        let _: () = conn
                            .set(&key, &updated_json)
                            .await
                            .map_err(|e| RateLimitError::RedisError(e.into()))?;
                    }
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
            let mut pipe = redis::pipe();

            for (key, config) in configs {
                let config_key = self.get_config_key(&key);
                let config_json = serde_json::to_string(&config)?;
                pipe.set(&config_key, &config_json);
            }

            let _: () = pipe
                .query_async(&mut *conn)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
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
            if let Some(json) = config_json {
                if let Ok(config) = serde_json::from_str::<RateLimitConfig>(&json) {
                    return Ok(config);
                }
            }

            // 尝试获取默认配置
            let default_json: Option<String> = conn
                .get(&self.default_config_key)
                .await
                .map_err(|e| RateLimitError::RedisError(e.into()))?;
            if let Some(json) = default_json {
                if let Ok(config) = serde_json::from_str::<RateLimitConfig>(&json) {
                    return Ok(config);
                }
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
                if let Some(json) = config_json {
                    if let Ok(config) = serde_json::from_str::<RateLimitConfig>(&json) {
                        // 提取实际的identifier（去掉前缀）
                        let identifier = key
                            .strip_prefix(&format!("{}:", self.config_key_prefix))
                            .unwrap_or(&key);
                        configs.insert(identifier.to_string(), config);
                    }
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

            let current_time = Self::get_current_timestamp();

            if let Some(pool) = &self.pool {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| RateLimitError::RedisError("redis connect error".into()))?;

                // 设置TTL，基于实际请求间隔时间，确保下次验证时记录仍然存在
                let config = self.get_key_config(identifier).await?;
                let min_interval_millis = (config.window_size_millis as f64
                    / config.max_requests_per_second as f64) as u64;
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

    /// 验证是否达到最大限制
    ///
    /// # 参数
    /// * `identifier` - 用于区分不同请求来源的标识符
    ///
    /// # 返回值
    /// * `Ok(())` - 未达到限制，可以继续
    /// * `Err(u64)` - 达到限制，返回需要等待的毫秒数
    pub async fn verify(&self, identifier: &str) -> Result<Option<u64>> {
        let config = match self.get_key_config(identifier).await {
            Ok(cfg) => cfg,
            Err(e) => {
                error!("{e:?}");
                self.default_config.clone()
            }
        };

        let lock_key = self.get_lock_key(identifier);
        let last_request_key = self.get_last_request_key(identifier);

        // 计算每个请求之间的最小间隔时间（平滑限流）
        let min_interval_millis =
            (config.window_size_millis as f64 / config.max_requests_per_second as f64) as u64;

        // 获取分布式锁保护Redis操作
        if let Ok(acquired) = self
            .lock_manager
            .acquire_lock(&lock_key, 30, Duration::from_secs(5))
            .await
        {
            if !acquired {
                return Err(RateLimitError::RedisError("Failed to acquire lock".into()).into());
            }

            let last_request_time = if let Some(pool) = &self.pool {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| RateLimitError::RedisError("redis connect error".into()))?;

                // 获取最后请求时间
                conn.get(&last_request_key)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?
            } else {
                // 本地模式
                self.local_last_request
                    .get(&last_request_key)
                    .map(|v| *v)
            };

            let result = if let Some(last_time) = last_request_time {
                let current_time = Self::get_current_timestamp();
                let elapsed_millis = current_time.saturating_sub(last_time);

                if elapsed_millis >= min_interval_millis {
                    None
                } else {
                    let wait_time = min_interval_millis - elapsed_millis;
                    Some(wait_time)
                }
            } else {
                // 没有记录，直接允许
                None
            };

            // 释放锁
            let _ = self.lock_manager.release_lock(&lock_key).await;
            Ok(result)
        } else {
            Err(RateLimitError::RedisError("Failed to acquire lock".into()).into())
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

            for key in keys {
                // 检查key是否过期（TTL <= 0）或者检查值是否过期
                let ttl: i64 = conn
                    .ttl(&key)
                    .await
                    .map_err(|e| RateLimitError::RedisError(e.into()))?;

                let should_delete = if ttl == -1 {
                    // 没有设置TTL，检查值是否过期
                    if let Ok(Some(last_time)) = conn.get::<_, Option<u64>>(&key).await {
                        let current_time = Self::get_current_timestamp();
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
            let current_time = Self::get_current_timestamp();
            
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
