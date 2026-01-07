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
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
pub struct ModuleLocker{
    pub is_locker:bool,
    pub ts:u64
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

/// 多层级错误跟踪器
pub struct StatusTracker {
    cache_service: Arc<CacheService>,
    config: ErrorTrackerConfig,
    locker: Arc<DistributedLockManager>,
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

        // 1. 记录 Request 级别错误
        let request_key = format!("request:{}:download", request_id);
        self.increment_error(&request_key, category, severity)
            .await?;
        let request_errors = self.get_error_count(&request_key).await?;

        // 2. 记录 Module 级别错误
        let module_key = format!("module:{}:download", module_id);
        self.increment_error(&module_key, category, severity)
            .await?;
        let module_errors = self.get_error_count(&module_key).await?;

        // 3. 记录 Task 级别错误
        let task_key = format!("task:{}:total", task_id);
        self.increment_error(&task_key, category, severity).await?;
        let task_errors = self.get_error_count(&task_key).await?;

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

        // 1. 记录成功
        self.increment_success(&request_key).await?;

        // 2. 重置连续错误计数
        self.reset_consecutive_errors(&request_key).await?;

        // 3. 如果启用了衰减，减少 Request 级别的错误计数
        if self.config.enable_success_decay {
            self.decrement_error(&request_key, self.config.success_decay_amount)
                .await?;
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

        log::info!(
            "[ErrorTracker] record_parse_error: task_id={} module_id={} request_id={} error={}",
            task_id,
            module_id,
            request_id,
            error
        );

        // 解析错误也影响三个层级，但使用独立的计数器
        let request_key = format!("request:{}:parse", request_id);
        self.increment_error(&request_key, category, severity)
            .await?;
        let request_errors = self.get_error_count(&request_key).await?;

        let module_key = format!("module:{}:parse", module_id);
        self.increment_error(&module_key, category, severity)
            .await?;
        let module_errors = self.get_error_count(&module_key).await?;

        let task_key = format!("task:{}:total", task_id);
        self.increment_error(&task_key, category, severity).await?;
        let task_errors = self.get_error_count(&task_key).await?;

        log::info!(
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

            log::warn!(
                "[ErrorTracker] module parse errors exceeded threshold: module_id={} errors={}/{}",
                module_id,
                module_errors,
                self.config.module_max_errors
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
        self.increment_success(&request_key).await?;
        self.reset_consecutive_errors(&request_key).await?;

        if self.config.enable_success_decay {
            self.decrement_error(&request_key, self.config.success_decay_amount)
                .await?;
        }

        Ok(())
    }

    /// 检查 Module 是否应该继续
    ///
    /// 如果 Module 达到错误阈值，会自动释放该 Module 的分布式锁
    pub async fn should_module_continue(&self, module_id: &str) -> Result<ErrorDecision> {
        // 先检查是否已被标记为终止
        if self.is_module_terminated(module_id).await? {
            log::warn!(
                "[ErrorTracker] module already terminated: module_id={}",
                module_id
            );
            return Ok(ErrorDecision::Terminate(format!(
                "Module {} has been terminated",
                module_id
            )));
        }

        let download_key = format!("module:{}:download", module_id);
        let parse_key = format!("module:{}:parse", module_id);

        let download_errors = self.get_error_count(&download_key).await?;
        let parse_errors = self.get_error_count(&parse_key).await?;
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
        // 先检查是否已被标记为终止
        if self.is_task_terminated(task_id).await? {
            return Ok(ErrorDecision::Terminate(format!(
                "Task {} has been terminated",
                task_id
            )));
        }

        let task_key = format!("task:{}:total", task_id);
        let task_errors = self.get_error_count(&task_key).await?;

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
        ErrorStats::sync(key, &self.cache_service)
            .await
            .and_then(|v| v.ok_or(CacheError::NotFound))
            .map_err(Error::from)
    }

    /// 标记 Task 为已终止
    pub async fn mark_task_terminated(&self, task_id: &str) -> Result<()> {
        let cache_id = ErrorStats::cache_id(task_id, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut error_stats = self.get_stats(task_id).await.unwrap_or_default();
                error_stats.is_task_terminated = true;
                error_stats.send(task_id, &self.cache_service).await
            })
            .await
            .ok();

        Ok(())
    }

    /// 检查 Task 是否已被标记为终止
    pub async fn is_task_terminated(&self, task_id: &str) -> Result<bool> {

        let  error_stats = self.get_stats(task_id).await?;
        Ok(error_stats.is_task_terminated)
    }

    /// 标记 Module 为已终止
    pub async fn mark_module_terminated(&self, module_id: &str) -> Result<()> {
        let cache_id = ErrorStats::cache_id(module_id, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut error_stats = self.get_stats(module_id).await.unwrap_or_default();
                error_stats.is_module_terminated = true;
                error_stats.send(module_id, &self.cache_service).await
            })
            .await
            .ok();

        Ok(())
    }

    /// 检查 Module 是否已被标记为终止
    pub async fn is_module_terminated(&self, module_id: &str) -> Result<bool> {
        let  error_stats = self.get_stats(module_id).await?;
        Ok(error_stats.is_module_terminated)
    }

    // === 私有辅助方法 ===

    /// 递增错误计数
    async fn increment_error(
        &self,
        key: &str,
        category: ErrorCategory,
        _severity: ErrorSeverity,
    ) -> Result<()> {
        let cache_id = ErrorStats::cache_id(key, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut stats = self.get_stats(key).await.unwrap_or_default();
                stats.total_errors += 1;
                stats.consecutive_errors += 1;
                stats.last_error_time = Some(Self::now());

                *stats.errors_by_category.entry(category).or_insert(0) += 1;
                self.save_stats_with_ttl(key, &stats).await.ok();
            })
            .await
            .ok();
        Ok(())
    }

    /// 递减错误计数
    async fn decrement_error(&self, key: &str, amount: usize) -> Result<()> {
        let cache_id = ErrorStats::cache_id(key, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut stats = self.get_stats(key).await.unwrap_or_default();
                // Safely decrease total errors without underflow
                stats.total_errors = stats.total_errors.saturating_sub(amount);
                // A successful decay should reset consecutive errors
                stats.consecutive_errors = 0;
                stats.last_success_time = Some(Self::now());

                self.save_stats_with_ttl(key, &stats).await.ok();
            })
            .await
            .ok();
        Ok(())
    }

    /// 递增成功计数
    async fn increment_success(&self, key: &str) -> Result<()> {
        let cache_id = ErrorStats::cache_id(key, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut stats = self.get_stats(key).await.unwrap_or_default();
                stats.success_count += 1;
                stats.last_success_time = Some(Self::now());
                self.save_stats_with_ttl(key, &stats).await.ok();
            })
            .await
            .ok();

        Ok(())
    }

    /// 重置连续错误计数
    async fn reset_consecutive_errors(&self, key: &str) -> Result<()> {
        let cache_id = ErrorStats::cache_id(key, &self.cache_service);
        self.locker
            .with_lock(&cache_id, 10, Duration::from_secs(10), async {
                let mut stats = self.get_stats(key).await.unwrap_or_default();
                stats.consecutive_errors = 0;
                self.save_stats_with_ttl(key, &stats).await.ok();
            })
            .await
            .ok();

        Ok(())
    }

    /// 获取错误计数
    async fn get_error_count(&self, key: &str) -> Result<usize> {
        match self.get_stats(key).await {
            Ok(stats) => Ok(stats.total_errors),
            Err(_) => Ok(0),
        }
    }

    /// 保存统计信息
    async fn save_stats(&self, key: &str, stats: &ErrorStats) -> Result<()> {
        self.save_stats_with_ttl(key, stats).await
    }

    /// 保存统计信息（带 TTL）
    async fn save_stats_with_ttl(&self, key: &str, stats: &ErrorStats) -> Result<()> {
        stats.send(key, &self.cache_service).await.map_err(|e|Error::from(e))
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
        let module_locker = ModuleLocker{
            is_locker:true,
            ts
        };
        module_locker.send(module_id, &self.cache_service).await.ok();
    }
    pub async fn is_module_locker(&self, module_id: &str, ttl: u64) -> bool {
        ModuleLocker::sync(module_id,&self.cache_service).await.ok().map_or(false,|locker|{
            if let Some(locker) = locker && locker.is_locker{
                let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
                if now - locker.ts <= ttl{
                    true
                }else{
                    false
                }
            }else{
                false
            }
        })
    }
    pub async fn release_module_locker(&self, module_id: &str) {
        ModuleLocker::delete(module_id,&self.cache_service).await.ok();
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
