#![allow(unused)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// 限流器配置项
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// 每秒最大请求次数
    pub max_requests_per_second: f32,
    /// 窗口大小（默认1秒）
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

/// 滑动窗口限流器
/// 使用滑动窗口算法来限制特定标识符的访问频率
/// 优化版本：
/// 1. 使用读写锁减少锁竞争，读操作可并发执行
/// 2. 直接存储时间戳减少内存开销
/// 3. 优化清理逻辑，减少不必要的遍历
/// 4. 支持为不同的key设置不同的速率限制
/// 5. 支持动态添加和修改key的速率配置
#[derive(Debug)]
pub struct SlidingWindowRateLimiter {
    /// 默认限流配置（用于未配置的key）
    default_config: Arc<RwLock<RateLimitConfig>>,
    /// 每个key的特定配置
    key_configs: Arc<RwLock<HashMap<String, RateLimitConfig>>>,
    /// 存储每个标识符对应的时间戳列表
    records: Arc<RwLock<HashMap<String, Vec<Instant>>>>,
}
impl Default for SlidingWindowRateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

impl SlidingWindowRateLimiter {
    /// 创建新的限流器实例
    ///
    /// # 参数
    /// * `default_config` - 默认的限流配置
    pub fn new(default_config: RateLimitConfig) -> Self {
        Self {
            default_config: Arc::new(RwLock::new(default_config)),
            key_configs: Arc::new(RwLock::new(HashMap::new())),
            records: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 创建一个简单的限流器（兼容旧接口）
    ///
    /// # 参数
    /// * `max_requests_per_second` - 每秒最大请求次数，支持小数
    pub fn simple(max_requests_per_second: f32) -> Self {
        Self::new(RateLimitConfig::new(max_requests_per_second))
    }

    /// 为特定key设置限流配置
    ///
    /// # 参数
    /// * `key` - 要配置的key
    /// * `config` - 限流配置
    pub async fn set_key_config(&self, key: String, config: RateLimitConfig) {
        let mut configs = self.key_configs.write().await;
        configs.insert(key, config);
    }
    /// 设置全局限流配置（影响所有key）
    pub async fn set_limit(&self, limit: f32) {
        self.default_config.write().await.max_requests_per_second = limit;
        let mut config = self.key_configs.write().await;
        config.iter_mut().for_each(|(_, config)| {
            config.max_requests_per_second = limit;
        })
    }

    /// 批量设置多个key的限流配置
    ///
    /// # 参数
    /// * `configs` - key到配置的映射
    pub async fn set_key_configs(&self, configs: HashMap<String, RateLimitConfig>) {
        let mut key_configs = self.key_configs.write().await;
        for (key, config) in configs {
            key_configs.insert(key, config);
        }
    }

    /// 移除特定key的配置（将使用默认配置）
    ///
    /// # 参数
    /// * `key` - 要移除配置的key
    pub async fn remove_key_config(&self, key: &str) {
        let mut configs = self.key_configs.write().await;
        configs.remove(key);
    }

    /// 获取特定key的配置
    ///
    /// # 参数
    /// * `key` - 要查询的key
    ///
    /// # 返回值
    /// 该key的配置，如果没有特定配置则返回默认配置
    pub async fn get_key_config(&self, key: &str) -> RateLimitConfig {
        let configs = self.key_configs.read().await;
        if let Some(config) = configs.get(key) {
            config.clone()
        } else {
            self.default_config.read().await.clone()
        }
    }

    /// 获取所有已配置的key及其配置
    pub async fn get_all_key_configs(&self) -> HashMap<String, RateLimitConfig> {
        let configs = self.key_configs.read().await;
        configs.clone()
    }

    /// 记录一次请求
    ///
    /// # 参数
    /// * `identifier` - 用于区分不同请求来源的标识符
    pub async fn record(&self, identifier: String) {
        let config = self.get_key_config(&identifier).await;
        let mut records = self.records.write().await;
        let entry = records.entry(identifier.clone()).or_insert_with(Vec::new);

        // 添加新的时间戳
        entry.push(Instant::now());

        // 清理过期的记录（这里忽略返回值，因为刚添加了新记录，不会为空）
        let _ = self.clean_expired_records_internal(entry, &config);

        // 顺便进行一次随机清理，避免长期未访问的identifier堆积
        self.opportunistic_cleanup(&mut records).await;
    }

    /// 验证是否达到最大限制
    ///
    /// # 参数
    /// * `identifier` - 用于区分不同请求来源的标识符
    ///
    /// # 返回值
    /// * `Ok(())` - 未达到限制，可以继续
    /// * `Err(u64)` - 达到限制，返回需要等待的毫秒数
    pub async fn verify(&self, identifier: &str) -> Result<(), u64> {
        let config = self.get_key_config(identifier).await;
        let records = self.records.read().await;

        if let Some(entry) = records.get(identifier) {
            // 如果没有记录，直接允许
            if entry.is_empty() {
                return Ok(());
            }

            // 计算相邻请求的最小间隔时间
            let min_interval = Duration::from_millis(
                (config.window_size.as_millis() as f64 / config.max_requests_per_second as f64)
                    as u64,
            );

            // 获取最后一次请求的时间戳
            let last_request_time = entry.last().unwrap();
            let elapsed_since_last = last_request_time.elapsed();

            // 检查间隔时间是否足够
            if elapsed_since_last >= min_interval {
                Ok(())
            } else {
                // 间隔时间不够，计算需要等待的时间
                let wait_time = min_interval - elapsed_since_last;
                Err(wait_time.as_millis() as u64)
            }
        } else {
            // 没有记录，直接允许
            Ok(())
        }
    }

    /// 验证并记录请求（原子操作）
    ///
    /// # 参数
    /// * `identifier` - 用于区分不同请求来源的标识符
    ///
    /// # 返回值
    /// * `Ok(())` - 未达到限制，已记录请求
    /// * `Err(u64)` - 达到限制，返回需要等待的毫秒数
    pub async fn verify_and_record(&self, identifier: String) -> Result<(), u64> {
        let config = self.get_key_config(&identifier).await;
        let mut records = self.records.write().await;
        // 趁有写锁的机会，进行一次机会性清理
        self.opportunistic_cleanup(&mut records).await;

        let entry = records.entry(identifier).or_insert_with(Vec::new);
        // 清理过期的记录
        self.clean_expired_records_internal(entry, &config);

        let now = Instant::now();

        // 如果没有记录，直接允许
        if entry.is_empty() {
            entry.push(now);
            return Ok(());
        }

        // 计算相邻请求的最小间隔时间
        // 间隔时间 = window_size / max_requests_per_second
        let min_interval = Duration::from_millis(
            (config.window_size.as_millis() as f64 / config.max_requests_per_second as f64) as u64,
        );

        // 获取最后一次请求的时间戳
        let last_request_time = entry.last().unwrap();
        let elapsed_since_last = now.duration_since(*last_request_time);

        // 检查间隔时间是否足够
        if elapsed_since_last >= min_interval {
            // 间隔时间足够，记录新请求
            entry.push(now);
            Ok(())
        } else {
            // 间隔时间不够，计算需要等待的时间
            let wait_time = min_interval - elapsed_since_last;
            Err(wait_time.as_millis() as u64)
        }
    }

    /// 内部方法：清理过期的时间记录
    ///
    /// # 参数
    /// * `records` - 要清理的时间记录列表
    /// * `config` - 该key的配置
    ///
    /// # 返回值
    /// * `true` - 清理后记录为空，应该删除该identifier
    /// * `false` - 清理后仍有有效记录
    fn clean_expired_records_internal(
        &self,
        records: &mut Vec<Instant>,
        config: &RateLimitConfig,
    ) -> bool {
        let now = Instant::now();
        let cutoff_time = now - config.window_size;

        // 保留在窗口期内的记录
        records.retain(|&timestamp| timestamp > cutoff_time);

        // 返回是否应该删除该identifier
        records.is_empty()
    }

    /// 机会性清理：在写锁期间随机清理一些过期的identifier
    /// 这样可以避免长期未访问的identifier永远留在内存中
    async fn opportunistic_cleanup(&self, records: &mut HashMap<String, Vec<Instant>>) {
        let total_identifiers = records.len();

        // 每次最多清理5个identifier，或者总数的10%，取较小值（至少1个）
        // 对于小规模映射也进行轻量清理，避免长时间未访问的键长期滞留。
        let max_cleanup_count = std::cmp::min(5, (total_identifiers / 10).max(1));

        let mut keys_to_remove = Vec::new();

        // 遍历部分identifier进行清理
        for (checked_count, (key, entry)) in records.iter_mut().enumerate() {
            if checked_count >= max_cleanup_count {
                break;
            }

            // 获取该key的配置
            let config = self.get_key_config(key).await;
            if self.clean_expired_records_internal(entry, &config) {
                keys_to_remove.push(key.clone());
            }
        }

        // 移除空的identifier
        for key in keys_to_remove {
            records.remove(&key);
        }
    }

    /// 计算有效记录数量（不修改原数据）
    fn count_valid_records(&self, records: &[Instant], config: &RateLimitConfig) -> f32 {
        let now = Instant::now();
        let cutoff_time = now - config.window_size;

        records
            .iter()
            .filter(|&&timestamp| timestamp > cutoff_time)
            .count() as f32
    }

    /// 计算需要等待的时间（毫秒）
    ///
    /// # 参数
    /// * `records` - 时间记录列表
    /// * `config` - 该key的配置
    ///
    /// # 返回值
    /// 需要等待的毫秒数
    fn calculate_wait_time(&self, records: &[Instant], config: &RateLimitConfig) -> u64 {
        if records.is_empty() {
            return 0;
        }

        // 找到最早的记录
        let oldest_record = match records.iter().min() {
            Some(record) => record,
            None => return 0, // 理论上不会到达这里，因为已经检查了 is_empty()
        };

        // 计算最早记录到现在的时间
        let elapsed = oldest_record.elapsed();

        // 如果还在窗口期内，计算剩余等待时间
        if elapsed < config.window_size {
            let remaining = config.window_size - elapsed;
            remaining.as_millis() as u64 + 1 // 加1毫秒确保下次检查时能通过
        } else {
            0
        }
    }

    /// 获取指定标识符的当前请求计数
    ///
    /// # 参数
    /// * `identifier` - 标识符
    ///
    /// # 返回值
    /// 当前窗口期内的请求数量
    pub async fn get_current_count(&self, identifier: &str) -> usize {
        let config = self.get_key_config(identifier).await;
        let records = self.records.read().await;
        if let Some(entry) = records.get(identifier) {
            self.count_valid_records(entry, &config) as usize
        } else {
            0
        }
    }

    /// 获取下次可以请求的时间间隔（毫秒）
    ///
    /// # 参数
    /// * `identifier` - 标识符
    ///
    /// # 返回值
    /// 需要等待的毫秒数，0表示可以立即请求
    pub async fn get_next_available_time(&self, identifier: &str) -> u64 {
        let config = self.get_key_config(identifier).await;
        let records = self.records.read().await;

        if let Some(entry) = records.get(identifier) {
            if entry.is_empty() {
                return 0;
            }

            // 计算相邻请求的最小间隔时间
            let min_interval = Duration::from_millis(
                (config.window_size.as_millis() as f64 / config.max_requests_per_second as f64)
                    as u64,
            );

            // 获取最后一次请求的时间戳
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

    /// 清理所有过期的记录
    /// 这个方法可以定期调用来释放内存
    /// 注：现在各个方法已经自动清理，这个方法主要用于手动清理
    pub async fn cleanup(&self) {
        let mut records = self.records.write().await;
        let mut keys_to_remove = Vec::new();

        for (key, entry) in records.iter_mut() {
            let config = self.get_key_config(key).await;
            if self.clean_expired_records_internal(entry, &config) {
                keys_to_remove.push(key.clone());
            }
        }

        // 移除空的条目
        for key in keys_to_remove {
            records.remove(&key);
        }
    }

    /// 获取当前存储的标识符数量
    pub async fn get_identifier_count(&self) -> usize {
        let records = self.records.read().await;
        records.len()
    }

    /// 重置指定标识符的记录
    pub async fn reset(&self, identifier: &str) {
        let mut records = self.records.write().await;
        records.remove(identifier);
    }

    /// 重置所有记录
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
