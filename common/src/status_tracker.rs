#![allow(unused)]

/// 多层级错误跟踪系统
///
/// 设计原则：
/// 1. Task 级别：控制整体任务的错误容忍度（10次）
/// 2. Module 级别：控制单个模块的错误容忍度（3次）
/// 3. Request 级别：控制单个请求的重试次数（3次）
///
/// 错误计数策略：
/// - 下载失败：Request +1, Module +1, Task +1
/// - 重试成功：Request -1（不影响 Module 和 Task）
/// - 解析失败：独立计数，不影响下载错误
///
/// 阈值到达后：
/// - Request 达到阈值：放弃该 Request，继续其他 Request
/// - Module 达到阈值：停止该 Module，Task 继续其他 Module
/// - Task 达到阈值：停止整个 Task
use cacheable::{CacheAble, CacheService};
use errors::{CacheError, Error, Result};
use log::warn;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use utils::redis_lock::DistributedLockManager;

/// 错误类型分类
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// 下载错误（网络、超时等）
    Download,
    /// 解析错误（内容解析失败）
    Parse,
    /// 认证错误（登录失效等）
    Auth,
    /// 限流错误
    RateLimit,
    /// 其他错误
    Other,
}

/// 错误严重程度
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorSeverity {
    /// 轻微错误（可重试）
    Minor,
    /// 严重错误（需要关注）
    Major,
    /// 致命错误（立即终止）
    Fatal,
}

/// 错误记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRecord {
    pub timestamp: u64,
    pub category: ErrorCategory,
    pub severity: ErrorSeverity,
    pub message: String,
    pub retry_count: usize,
}
impl CacheAble for ErrorRecord {
    fn field() -> impl AsRef<str> {
        "error_record".to_string()
    }
}
/// 错误统计
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ErrorStats {
    /// 总错误次数
    pub total_errors: usize,
    /// 成功次数
    pub success_count: usize,
    /// 当前连续错误次数
    pub consecutive_errors: usize,
    /// 按类别分类的错误次数
    pub errors_by_category: std::collections::HashMap<ErrorCategory, usize>,
    /// 最后一次错误时间
    pub last_error_time: Option<u64>,
    /// 最后一次成功时间
    pub last_success_time: Option<u64>,
    pub is_task_terminated: bool,
    pub is_module_terminated: bool,
}

impl CacheAble for ErrorStats {
    fn field() -> impl AsRef<str> {
        "error_stats".to_string()
    }
}

impl ErrorStats {
    pub fn error_rate(&self) -> f64 {
        let total = self.total_errors + self.success_count;
        if total == 0 {
            0.0
        } else {
            self.total_errors as f64 / total as f64
        }
    }

    pub fn health_score(&self) -> f64 {
        // 健康度评分：0.0（非常不健康）到 1.0（非常健康）
        let error_rate = self.error_rate();
        let consecutive_penalty = (self.consecutive_errors as f64 * 0.1).min(0.5);
        (1.0 - error_rate - consecutive_penalty).max(0.0)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModuleLocker {
    pub is_locker: bool,
    pub ts: u64,
}
impl CacheAble for ModuleLocker {
    fn field() -> impl AsRef<str> {
        "module_locker".to_string()
    }
}
/// 错误决策
#[derive(Debug, Clone)]
pub enum ErrorDecision {
    /// 继续执行
    Continue,
    /// 延迟后重试
    RetryAfter(Duration),
    /// 跳过当前项（Request）
    Skip,
    /// 终止当前层级（Module 或 Task）
    Terminate(String),
}

/// 错误跟踪配置
#[derive(Debug, Clone)]
pub struct ErrorTrackerConfig {
    /// Task 级别最大错误次数
    pub task_max_errors: usize,
    /// Module 级别最大错误次数
    pub module_max_errors: usize,
    /// Request 级别最大重试次数
    pub request_max_retries: usize,
    /// Parse 级别最大重试次数
    pub parse_max_retries: usize,

    /// 是否启用成功后的错误衰减
    pub enable_success_decay: bool,
    /// 成功后减少的错误计数
    pub success_decay_amount: usize,

    /// 是否启用时间窗口统计
    pub enable_time_window: bool,
    /// 时间窗口大小（秒）
    pub time_window_seconds: u64,

    /// 连续错误阈值（达到后降低健康度）
    pub consecutive_error_threshold: usize,

    /// 错误记录过期时间（秒），从配置的 cache_ttl 读取
    /// 用于自动清理过期的错误记录，避免历史错误永久影响判断
    pub error_ttl: u64,
}

impl Default for ErrorTrackerConfig {
    fn default() -> Self {
        Self {
            task_max_errors: 100,
            module_max_errors: 10,
            request_max_retries: 3,
            parse_max_retries: 3,
            enable_success_decay: true,
            success_decay_amount: 1,
            enable_time_window: false,
            time_window_seconds: 3600,
            consecutive_error_threshold: 3,
            error_ttl: 3600, // 默认 1 小时过期
        }
    }
}

use dashmap::DashMap;

/// 多层级错误跟踪器
#[derive(Clone)]
pub struct StatusTracker {
    cache_service: Arc<CacheService>,
    config: ErrorTrackerConfig,
    locker: Arc<DistributedLockManager>,
    cache: Arc<DashMap<String, (ErrorStats, Instant)>>,
}

impl StatusTracker {
    pub fn new(
        cache_service: Arc<CacheService>,
        config: ErrorTrackerConfig,
        locker: Arc<DistributedLockManager>,
    ) -> Self {
        Self {
            cache_service,
            config,
            locker,
            cache: Arc::new(DashMap::new()),
        }
    }

    /// 记录下载错误
    ///
    /// 影响范围：Request、Module、Task 三个层级都增加错误计数
    pub async fn record_download_error(
        &self,
        task_id: &str,
        module_id: &str,
        request_id: &str,
        error: &Error,
    ) -> Result<ErrorDecision> {
        let category = ErrorCategory::Download;
        let severity = self.classify_error_severity(error);

        let request_key = format!("request:{}:download", request_id);
        let module_key = format!("module:{}:download", module_id);
        let task_key = format!("task:{}:total", task_id);

        // Parallel update for performance
        let (request_res, module_res, task_res) = tokio::join!(
            self.increment_error(&request_key, category, severity),
            self.increment_error(&module_key, category, severity),
            self.increment_error(&task_key, category, severity)
        );

        let request_errors = request_res?;
        let module_errors = module_res?;
        let task_errors = task_res?;

        // 4. 决策
        if severity == ErrorSeverity::Fatal {
            return Ok(ErrorDecision::Terminate(format!(
                "Fatal error encountered: {}",
                error
            )));
        }

        if task_errors >= self.config.task_max_errors {
            return Ok(ErrorDecision::Terminate(format!(
                "Task {} reached max errors: {}/{}",
                task_id, task_errors, self.config.task_max_errors
            )));
        }

        if module_errors >= self.config.module_max_errors {
            // Module 达到错误阈值，释放锁
            self.release_module_locker(module_id).await;

            return Ok(ErrorDecision::Terminate(format!(
                "Module {} reached max errors: {}/{}",
                module_id, module_errors, self.config.module_max_errors
            )));
        }

        if request_errors >= self.config.request_max_retries {
            return Ok(ErrorDecision::Skip);
        }

        // 根据错误类型决定重试延迟
        let delay = match category {
            ErrorCategory::RateLimit => Duration::from_secs(60),
            ErrorCategory::Auth => Duration::from_secs(10),
            _ => Duration::from_secs(2u64.pow(request_errors as u32).min(30)),
        };

        Ok(ErrorDecision::RetryAfter(delay))
    }

    /// 记录下载成功（仅减少 Request 级别的错误计数）
    ///
    /// 设计理念：
    /// - Request 重试成功后，减少 Request 级别的错误（允许后续重试）
    /// - Module 和 Task 级别的错误不减少（因为确实发生过错误）
    pub async fn record_download_success(&self, request_id: &str) -> Result<()> {
        let request_key = format!("request:{}:download", request_id);
        
        // [OPTIMIZATION]
        // If the request succeeded, we don't need to load stats from Redis just to update an in-memory object we throw away.
        // We only need to clear any cached error state for this request.
        // If there were errors in Redis, they will expire naturally (TTL).
        
        if self.cache.contains_key(&request_key) {
            self.cache.remove(&request_key);
        }

        Ok(())
    }

    /// 记录解析错误（独立于下载错误）
    pub async fn record_parse_error(
        &self,
        task_id: &str,
        module_id: &str,
        request_id: &str,
        error: &Error,
    ) -> Result<ErrorDecision> {
        let category = ErrorCategory::Parse;
        let severity = self.classify_error_severity(error);

        log::debug!(
            "[ErrorTracker] record_parse_error: task_id={} module_id={} request_id={} error={}",
            task_id,
            module_id,
            request_id,
            error
        );

        // 解析错误也影响三个层级，但使用独立的计数器
        let request_key = format!("request:{}:parse", request_id);
        let module_key = format!("module:{}:parse", module_id);
        let task_key = format!("task:{}:total", task_id);

        let (request_res, module_res, task_res) = tokio::join!(
            self.increment_error(&request_key, category, severity),
            self.increment_error(&module_key, category, severity),
            self.increment_error(&task_key, category, severity)
        );

        let request_errors = request_res?;
        let module_errors = module_res?;
        let task_errors = task_res?;

        log::debug!(
            "[ErrorTracker] parse error counts: task={}/{} module={}/{} request={}/{}",
            task_errors,
            self.config.task_max_errors,
            module_errors,
            self.config.module_max_errors,
            request_errors,
            self.config.parse_max_retries
        );

        // 决策逻辑与下载类似
        if task_errors >= self.config.task_max_errors {
            return Ok(ErrorDecision::Terminate(format!(
                "Task {} reached max errors: {}/{}",
                task_id, task_errors, self.config.task_max_errors
            )));
        }

        if module_errors >= self.config.module_max_errors {
            // Module 达到错误阈值，释放锁
            self.release_module_locker(module_id).await;

            warn!(
                "[ErrorTracker] module parse errors exceeded threshold: module_id={} errors={}/{}",
                module_id, module_errors, self.config.module_max_errors
            );

            return Ok(ErrorDecision::Terminate(format!(
                "Module {} reached max errors: {}/{}",
                module_id, module_errors, self.config.module_max_errors
            )));
        }

        if request_errors >= self.config.parse_max_retries {
            return Ok(ErrorDecision::Skip);
        }

        Ok(ErrorDecision::RetryAfter(Duration::from_secs(1)))
    }

    /// 记录解析成功
    pub async fn record_parse_success(&self, request_id: &str) -> Result<()> {
        let request_key = format!("request:{}:parse", request_id);
        
        // [OPTIMIZATION] Skip loading stats from Redis. Just clear local cache.
        if self.cache.contains_key(&request_key) {
            self.cache.remove(&request_key);
        }

        Ok(())
    }

    /// 检查 Module 是否应该继续
    ///
    /// 如果 Module 达到错误阈值，会自动释放该 Module 的分布式锁
    pub async fn should_module_continue(&self, module_id: &str) -> Result<ErrorDecision> {
        // [OPTIMIZATION] Check cache first
        if let Some(entry) = self.cache.get(module_id) {
            let (stats, _) = entry.value();
            if stats.is_module_terminated {
                return Ok(ErrorDecision::Terminate(format!(
                    "Module {} has been terminated (cached)",
                    module_id
                )));
            }
        }

        let download_key = format!("module:{}:download", module_id);
        let parse_key = format!("module:{}:parse", module_id);

        let (terminated_res, download_res, parse_res) = tokio::join!(
            self.is_module_terminated(module_id),
            self.get_error_count(&download_key),
            self.get_error_count(&parse_key)
        );

        // 先检查是否已被标记为终止
        if terminated_res? {
            warn!(
                "[ErrorTracker] module already terminated: module_id={}",
                module_id
            );
            return Ok(ErrorDecision::Terminate(format!(
                "Module {} has been terminated",
                module_id
            )));
        }

        let download_errors = download_res?;
        let parse_errors = parse_res?;
        let total_errors = download_errors + parse_errors;

        log::debug!(
            "[ErrorTracker] should_module_continue: module_id={} total_errors={}/{} (download={}, parse={})",
            module_id,
            total_errors,
            self.config.module_max_errors,
            download_errors,
            parse_errors
        );

        if total_errors >= self.config.module_max_errors {
            // Module 达到错误阈值，标记为终止并释放锁
            self.mark_module_terminated(module_id).await?;
            self.release_module_locker(module_id).await;

            log::error!(
                "[ErrorTracker] module exceeded error threshold: module_id={} total_errors={}/{} (download={}, parse={})",
                module_id,
                total_errors,
                self.config.module_max_errors,
                download_errors,
                parse_errors
            );

            Ok(ErrorDecision::Terminate(format!(
                "Module {} total errors: {}/{} (download: {}, parse: {})",
                module_id,
                total_errors,
                self.config.module_max_errors,
                download_errors,
                parse_errors
            )))
        } else {
            Ok(ErrorDecision::Continue)
        }
    }

    /// 检查 Task 是否应该继续
    pub async fn should_task_continue(&self, task_id: &str) -> Result<ErrorDecision> {
        // [OPTIMIZATION] Check cache first
        if let Some(entry) = self.cache.get(task_id) {
            let (stats, _) = entry.value();
            if stats.is_task_terminated {
                return Ok(ErrorDecision::Terminate(format!(
                    "Task {} has been terminated (cached)",
                    task_id
                )));
            }
        }

        let task_key = format!("task:{}:total", task_id);
        
        let (terminated_res, count_res) = tokio::join!(
            self.is_task_terminated(task_id),
            self.get_error_count(&task_key)
        );

        // 先检查是否已被标记为终止
        if terminated_res? {
            return Ok(ErrorDecision::Terminate(format!(
                "Task {} has been terminated",
                task_id
            )));
        }

        let task_errors = count_res?;

        if task_errors >= self.config.task_max_errors {
            // Task 达到错误阈值，标记为终止
            self.mark_task_terminated(task_id).await?;

            Ok(ErrorDecision::Terminate(format!(
                "Task {} reached max errors: {}/{}",
                task_id, task_errors, self.config.task_max_errors
            )))
        } else {
            Ok(ErrorDecision::Continue)
        }
    }

    /// 获取统计信息
    pub async fn get_stats(&self, key: &str) -> Result<ErrorStats> {
         if let Some(entry) = self.cache.get(key) {
            let (stats, expires_at) = entry.value();
            if Instant::now() < *expires_at {
                // log::debug!("[StatusTracker] Cache HIT for {}", key);
                return Ok(stats.clone());
            } else {
                // log::debug!("[StatusTracker] Cache EXPIRED for {}", key);
            }
        } else {
            // log::debug!("[StatusTracker] Cache MISS for {}", key);
        }
        self.get_stats_no_cache(key).await
    }

    pub async fn get_stats_no_cache(&self, key: &str) -> Result<ErrorStats> {
        let stats_res = ErrorStats::sync(key, &self.cache_service).await;
        
        match stats_res {
            Ok(Some(stats)) => {
                self.cache.insert(key.to_string(), (stats.clone(), Instant::now() + Duration::from_secs(10)));
                Ok(stats)
            }
            Ok(None) => {
                // [OPTIMIZATION] Cache default (empty) stats to prevent Redis penetration for non-existent keys
                // This is critical for "is_terminated" checks which query random/empty keys frequently
                let stats = ErrorStats::default();
                self.cache.insert(key.to_string(), (stats.clone(), Instant::now() + Duration::from_secs(5)));
                Ok(stats)
            }
            Err(e) => Err(Error::from(e))
        }
    }

    /// 标记 Task 为已终止
    pub async fn mark_task_terminated(&self, task_id: &str) -> Result<()> {
        let cache_id = ErrorStats::cache_id(task_id, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut error_stats = self.get_stats_no_cache(task_id).await.unwrap_or_default();
                error_stats.is_task_terminated = true;
                error_stats.send(task_id, &self.cache_service).await
            })
            .await
            .ok();

        Ok(())
    }

    /// 检查 Task 是否已被标记为终止
    pub async fn is_task_terminated(&self, task_id: &str) -> Result<bool> {
        match self.get_stats(task_id).await {
            Ok(stats) => Ok(stats.is_task_terminated),
            Err(e) => {
                // 如果是 NotFound，说明还没有任何错误记录，肯定未终止
                if let Some(cache_err) = e
                    .inner
                    .source
                    .as_ref()
                    .and_then(|s| s.downcast_ref::<CacheError>())
                    && matches!(cache_err, CacheError::NotFound) {
                        return Ok(false);
                    }
                warn!(
                    "[ErrorTracker] task terminated check failed: task_id={}, error: {}",
                    task_id, e
                );
                Err(e)
            }
        }
    }

    /// 标记 Module 为已终止
    pub async fn mark_module_terminated(&self, module_id: &str) -> Result<()> {
        let cache_id = ErrorStats::cache_id(module_id, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut error_stats = self.get_stats_no_cache(module_id).await.unwrap_or_default();
                error_stats.is_module_terminated = true;
                error_stats.send(module_id, &self.cache_service).await
            })
            .await
            .ok();

        Ok(())
    }

    /// 检查 Module 是否已被标记为终止
    pub async fn is_module_terminated(&self, module_id: &str) -> Result<bool> {
        match self.get_stats(module_id).await {
            Ok(stats) => Ok(stats.is_module_terminated),
            Err(e) => {
                // 如果是 NotFound，说明还没有任何错误记录，肯定未终止
                if let Some(cache_err) = e
                    .inner
                    .source
                    .as_ref()
                    .and_then(|s| s.downcast_ref::<CacheError>())
                    && matches!(cache_err, CacheError::NotFound) {
                        return Ok(false);
                    }
                warn!(
                    "[ErrorTracker] module terminated check failed: module_id={}, error: {}",
                    module_id, e
                );
                Err(e)
            }
        }
    }

    // === 私有辅助方法 ===

    /// 递增错误计数 (使用 Redis 原子操作优化)
    async fn increment_error(
        &self,
        key: &str,
        category: ErrorCategory,
        _severity: ErrorSeverity,
    ) -> Result<usize> {
        // Request level: pure in-memory/cache simple update (as before)
        if key.starts_with("request:") {
            // For requests, we can just skip persistence for max performance or use short TTL
            // Simple INCR is fine
            let count_key = format!("{}:total_errors", key);
            // Use 1 hour TTL for request stats
            let total = self.cache_service.incr(&count_key, 1).await? as usize;
            return Ok(total);
        }

        // Global level (Task/Module): Use Atomic INCR to avoid locks
        // Optimization: Use separate keys for counters to avoid read-modify-write cycle of big JSON
        let count_key = format!("{}:total_errors", key);
        let total = self.cache_service.incr(&count_key, 1).await? as usize;
        
        let consecutive_key = format!("{}:consecutive_errors", key);
        let _ = self.cache_service.incr(&consecutive_key, 1).await;

        // SKIP UPDATING BIG JSON.
        // We sacrifice "errors_by_category" visibility in Redis for speed.
        
        Ok(total)
    }

    /// 递减错误计数
    async fn decrement_error(&self, key: &str, amount: usize) -> Result<()> {
        let count_key = format!("{}:total_errors", key);
        // Atomic Decrement
        let _ = self.cache_service.incr(&count_key, -(amount as i64)).await;

        let consecutive_key = format!("{}:consecutive_errors", key);
         // Reset consecutive errors
        let _ = self.cache_service.set(&consecutive_key, b"0", None).await;

        Ok(())
    }
    

    /// 递增成功计数
    async fn increment_success(&self, key: &str) -> Result<()> {
        let mut stats = self.get_stats(key).await.unwrap_or_default();
        stats.success_count += 1;
        stats.last_success_time = Some(Self::now());
        self.save_stats_with_ttl(key, &stats).await.ok();
        Ok(())
    }

    /// 重置连续错误计数
    async fn reset_consecutive_errors(&self, key: &str) -> Result<()> {
        let mut stats = self.get_stats(key).await.unwrap_or_default();
        stats.consecutive_errors = 0;
        self.save_stats_with_ttl(key, &stats).await.ok();
        Ok(())
    }

    /// 获取错误计数
    async fn get_error_count(&self, key: &str) -> Result<usize> {
        let count_key = format!("{}:total_errors", key);
        if let Some(val) = self.cache_service.get(&count_key).await? {
            // Redis returns string for INCR
            let s = String::from_utf8(val).unwrap_or_default();
            let count = s.parse::<usize>().unwrap_or(0);
            return Ok(count);
        }
        
        // Fallback: If not found in atomic counter, check maybe it's in old JSON format (migration support)
        // Or just return 0. Returning 0 is safer/faster.
        Ok(0)
    }

    /// 保存统计信息
    async fn save_stats(&self, key: &str, stats: &ErrorStats) -> Result<()> {
        self.save_stats_with_ttl(key, stats).await
    }

    /// 保存统计信息（带 TTL）
    async fn save_stats_with_ttl(&self, key: &str, stats: &ErrorStats) -> Result<()> {
        self.cache.insert(key.to_string(), (stats.clone(), Instant::now() + Duration::from_secs(10)));
        stats
            .send(key, &self.cache_service)
            .await
            .map_err(Error::from)
    }

    /// 错误严重程度分类
    fn classify_error_severity(&self, error: &Error) -> ErrorSeverity {
        let error_str = error.to_string().to_lowercase();

        if error_str.contains("auth") || error_str.contains("unauthorized") {
            ErrorSeverity::Fatal
        } else if error_str.contains("rate limit") || error_str.contains("too many") {
            ErrorSeverity::Major
        } else {
            ErrorSeverity::Minor
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    pub async fn lock_module(&self, module_id: &str) {
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let module_locker = ModuleLocker {
            is_locker: true,
            ts,
        };
        module_locker
            .send(module_id, &self.cache_service)
            .await
            .ok();
    }
    pub async fn is_module_locker(&self, module_id: &str, ttl: u64) -> bool {
        ModuleLocker::sync(module_id, &self.cache_service)
            .await
            .ok()
            .flatten()
            .is_some_and(|locker| {
                if locker.is_locker {
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    now - locker.ts <= ttl
                } else {
                    false
                }
            })
    }
    pub async fn release_module_locker(&self, module_id: &str) {
        ModuleLocker::delete(module_id, &self.cache_service)
            .await
            .ok();
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_stats_calculations() {
        let mut stats = ErrorStats::default();
        stats.total_errors = 3;
        stats.success_count = 7;

        assert_eq!(stats.error_rate(), 0.3);
        assert!(stats.health_score() > 0.6);

        stats.consecutive_errors = 5;
        assert!(stats.health_score() < 0.5);
    }
}
