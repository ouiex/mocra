#![allow(unused)]

use crate::common::model::{PipelineStage, StatusEntry, TaskStatus};
use crate::engine::api::profile_store::ProfileControlPlaneStore;
/// Multi-level error tracking used by task, module, and request workflows.
///
/// Core semantics:
/// - Download failures increment request/module/task counters.
/// - Parse failures use independent counters.
/// - Success clears request-local cached state and does not rewrite historical task/module failures.
/// - Thresholds can lead to `Skip` (request) or `Terminate` (module/task).
use crate::errors::{Error, Result};
use log::warn;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// High-level category assigned to an error record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// Download and transport failures.
    Download,
    /// Parsing and extraction failures.
    Parse,
    /// Authentication or authorization failures.
    Auth,
    /// Quota and throttling failures.
    RateLimit,
    /// Any category not explicitly covered above.
    Other,
}

/// Severity level used for decisioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorSeverity {
    /// Low impact and usually retryable.
    Minor,
    /// Significant but not immediately fatal.
    Major,
    /// Fatal condition that should terminate execution.
    Fatal,
}

/// Aggregated counters and state flags for an error key.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ErrorStats {
    /// Total error count.
    pub total_errors: usize,
    /// Total success count.
    pub success_count: usize,
    /// Current streak of consecutive errors.
    pub consecutive_errors: usize,
    /// Error counts grouped by category.
    pub errors_by_category: std::collections::HashMap<ErrorCategory, usize>,
    /// UNIX timestamp of the latest error.
    pub last_error_time: Option<u64>,
    /// UNIX timestamp of the latest success.
    pub last_success_time: Option<u64>,
    pub is_task_terminated: bool,
    pub is_module_terminated: bool,
}

impl ErrorStats {
    /// Returns error ratio in `[0.0, 1.0]`.
    pub fn error_rate(&self) -> f64 {
        let total = self.total_errors + self.success_count;
        if total == 0 {
            0.0
        } else {
            self.total_errors as f64 / total as f64
        }
    }

    /// Computes a bounded health score where `1.0` is healthiest.
    ///
    /// # Examples
    ///
    /// ```
    /// use mocra::common::status_tracker::ErrorStats;
    ///
    /// let mut stats = ErrorStats::default();
    /// stats.total_errors = 3;
    /// stats.success_count = 7;
    /// assert_eq!(stats.error_rate(), 0.3);
    /// assert!(stats.health_score() > 0.6);
    ///
    /// stats.consecutive_errors = 5;
    /// assert!(stats.health_score() < 0.5);
    /// ```
    pub fn health_score(&self) -> f64 {
        // Health score in [0.0, 1.0].
        let error_rate = self.error_rate();
        let consecutive_penalty = (self.consecutive_errors as f64 * 0.1).min(0.5);
        (1.0 - error_rate - consecutive_penalty).max(0.0)
    }
}
/// Execution decision made after recording or checking errors.
#[derive(Debug, Clone)]
pub enum ErrorDecision {
    /// Continue normal execution.
    Continue,
    /// Retry after a delay.
    RetryAfter(Duration),
    /// Skip current item (typically request-level).
    Skip,
    /// Terminate the current scope (module or task).
    Terminate(String),
}

/// Runtime thresholds and behaviors for [`StatusTracker`].
#[derive(Debug, Clone)]
pub struct ErrorTrackerConfig {
    /// Max task-level errors before termination.
    pub task_max_errors: usize,
    /// Max module-level errors before termination.
    pub module_max_errors: usize,
    /// Max request-level retries for download errors.
    pub request_max_retries: usize,
    /// Max request-level retries for parse errors.
    pub parse_max_retries: usize,

    /// Whether to apply decay after successful execution.
    pub enable_success_decay: bool,
    /// Error decrement amount when decay is enabled.
    pub success_decay_amount: usize,

    /// Whether rolling time-window logic is enabled.
    pub enable_time_window: bool,
    /// Time-window size in seconds.
    pub time_window_seconds: u64,

    /// Consecutive-error threshold used by health scoring.
    pub consecutive_error_threshold: usize,

    /// Error TTL in seconds for persisted tracker data.
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
            error_ttl: 3600,
        }
    }
}

use dashmap::DashMap;

/// Tracks and evaluates failures across request/module/task levels.
#[derive(Clone)]
pub struct StatusTracker {
    config: ErrorTrackerConfig,
    profile_store: Arc<ProfileControlPlaneStore>,
    cache: Arc<DashMap<String, (ErrorStats, Instant)>>,
}

impl StatusTracker {
    pub fn new(config: ErrorTrackerConfig, profile_store: Arc<ProfileControlPlaneStore>) -> Self {
        Self {
            config,
            profile_store,
            cache: Arc::new(DashMap::new()),
        }
    }

    /// Records a download error and returns the next execution decision.
    ///
    /// Affects request, module, and task counters.
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

        // Decision stage.
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
            // Release lock when module threshold is reached.
            self.release_module_locker(module_id).await;

            return Ok(ErrorDecision::Terminate(format!(
                "Module {} reached max errors: {}/{}",
                module_id, module_errors, self.config.module_max_errors
            )));
        }

        if request_errors >= self.config.request_max_retries {
            return Ok(ErrorDecision::Skip);
        }

        // Delay strategy by category.
        let delay = match category {
            ErrorCategory::RateLimit => Duration::from_secs(60),
            ErrorCategory::Auth => Duration::from_secs(10),
            _ => Duration::from_secs(2u64.pow(request_errors as u32).min(30)),
        };

        Ok(ErrorDecision::RetryAfter(delay))
    }

    /// Records a download success for request-local state.
    ///
    /// This intentionally does not decrement historical module/task counters.
    pub async fn record_download_success(&self, request_id: &str) -> Result<()> {
        let request_key = format!("request:{}:download", request_id);

        // [OPTIMIZATION]
        // If the request succeeded, we don't need to load persisted stats just to update an in-memory object we throw away.
        // We only need to clear any cached error state for this request.
        // Persisted error counters expire naturally by TTL.

        if self.cache.contains_key(&request_key) {
            self.cache.remove(&request_key);
        }

        Ok(())
    }

    /// Records a parse error using counters independent from download errors.
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

        // Parse counters use separate keys while still updating three levels.
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

        // Decision logic mirrors download handling.
        if task_errors >= self.config.task_max_errors {
            return Ok(ErrorDecision::Terminate(format!(
                "Task {} reached max errors: {}/{}",
                task_id, task_errors, self.config.task_max_errors
            )));
        }

        if module_errors >= self.config.module_max_errors {
            // Release lock when module threshold is reached.
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

    /// Records a parse success by clearing request-local cached parse state.
    pub async fn record_parse_success(&self, request_id: &str) -> Result<()> {
        let request_key = format!("request:{}:parse", request_id);

        // [OPTIMIZATION] Skip loading persisted stats. Just clear local cache.
        if self.cache.contains_key(&request_key) {
            self.cache.remove(&request_key);
        }

        Ok(())
    }

    /// Checks whether a module should continue execution.
    ///
    /// Automatically releases module lock on termination threshold.
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

        // Check explicit termination marker first.
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
            // Mark terminated and release lock when threshold is reached.
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

    /// Checks whether a task should continue execution.
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

        // Check explicit termination marker first.
        if terminated_res? {
            return Ok(ErrorDecision::Terminate(format!(
                "Task {} has been terminated",
                task_id
            )));
        }

        let task_errors = count_res?;

        if task_errors >= self.config.task_max_errors {
            // Mark terminated when threshold is reached.
            self.mark_task_terminated(task_id).await?;

            Ok(ErrorDecision::Terminate(format!(
                "Task {} reached max errors: {}/{}",
                task_id, task_errors, self.config.task_max_errors
            )))
        } else {
            Ok(ErrorDecision::Continue)
        }
    }

    /// Marks a task as terminated.
    pub async fn mark_task_terminated(&self, task_id: &str) -> Result<()> {
        self.profile_store
            .mark_task_terminated(task_id, Self::now())
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))?;

        let mut stats = self.cached_stats(task_id).unwrap_or_default();
        stats.is_task_terminated = true;
        self.cache.insert(
            task_id.to_string(),
            (stats, Instant::now() + Duration::from_secs(10)),
        );

        Ok(())
    }

    /// Returns whether a task is marked as terminated.
    pub async fn is_task_terminated(&self, task_id: &str) -> Result<bool> {
        let terminated = self.profile_store.is_task_terminated(task_id);
        if terminated {
            let mut stats = self.cached_stats(task_id).unwrap_or_default();
            stats.is_task_terminated = true;
            self.cache.insert(
                task_id.to_string(),
                (stats, Instant::now() + Duration::from_secs(10)),
            );
        }
        Ok(terminated)
    }

    /// Marks a module as terminated.
    pub async fn mark_module_terminated(&self, module_id: &str) -> Result<()> {
        self.profile_store
            .mark_module_terminated(module_id, Self::now())
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))?;

        let mut stats = self.cached_stats(module_id).unwrap_or_default();
        stats.is_module_terminated = true;
        self.cache.insert(
            module_id.to_string(),
            (stats, Instant::now() + Duration::from_secs(10)),
        );

        Ok(())
    }

    /// Returns whether a module is marked as terminated.
    pub async fn is_module_terminated(&self, module_id: &str) -> Result<bool> {
        let terminated = self.profile_store.is_module_terminated(module_id);
        if terminated {
            let mut stats = self.cached_stats(module_id).unwrap_or_default();
            stats.is_module_terminated = true;
            self.cache.insert(
                module_id.to_string(),
                (stats, Instant::now() + Duration::from_secs(10)),
            );
        }
        Ok(terminated)
    }

    // Private helpers.

    /// Increments error counters using atomic cache operations.
    async fn increment_error(
        &self,
        key: &str,
        category: ErrorCategory,
        _severity: ErrorSeverity,
    ) -> Result<usize> {
        let total = self
            .profile_store
            .increment_status_counter(&format!("{}:total_errors", key), 1)
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))?
            as usize;

        let _ = self
            .profile_store
            .increment_status_counter(&format!("{}:consecutive_errors", key), 1)
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)));

        // SKIP UPDATING BIG JSON.
        // We sacrifice detailed "errors_by_category" visibility for speed.

        Ok(total)
    }

    /// Decrements total error counters.
    async fn decrement_error(&self, key: &str, amount: usize) -> Result<()> {
        let _ = self
            .profile_store
            .increment_status_counter(&format!("{}:total_errors", key), -(amount as i64))
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))?;
        self.profile_store
            .set_status_counter(&format!("{}:consecutive_errors", key), 0)
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))?;

        Ok(())
    }

    /// Resets consecutive error counters.
    async fn reset_consecutive_errors(&self, key: &str) -> Result<()> {
        self.profile_store
            .set_status_counter(&format!("{}:consecutive_errors", key), 0)
            .await
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))?;
        Ok(())
    }

    /// Reads the total error count for a key.
    async fn get_error_count(&self, key: &str) -> Result<usize> {
        Ok(self
            .profile_store
            .get_status_counter(&format!("{}:total_errors", key)) as usize)
    }

    /// Classifies raw error text into severity tiers.
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
        self.profile_store.set_module_lock(module_id, ts).await.ok();
    }

    pub async fn is_module_locker(&self, module_id: &str, ttl: u64) -> bool {
        self.profile_store
            .get_module_lock(module_id)
            .is_some_and(|locker| {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                now.saturating_sub(locker.locked_at) <= ttl
            })
    }

    pub async fn release_module_locker(&self, module_id: &str) {
        self.profile_store.release_module_lock(module_id).await.ok();
    }

    pub async fn update_status(
        &self,
        task_id: &str,
        stage: PipelineStage,
        status: TaskStatus,
        retry_count: u32,
        node_id: &str,
        error_msg: Option<String>,
    ) -> Result<()> {
        let mut cached = self.cached_stats(task_id).unwrap_or_default();
        cached.is_task_terminated = self.profile_store.is_task_terminated(task_id);
        self.cache.insert(
            task_id.to_string(),
            (cached, Instant::now() + Duration::from_secs(10)),
        );

        self.profile_store
            .upsert_status_entry(StatusEntry {
                task_id: task_id.to_string(),
                stage,
                status,
                retry_count,
                node_id: node_id.to_string(),
                updated_at: 0,
                error_msg,
            })
            .await
            .map(|_| ())
            .map_err(|e| Error::new(crate::errors::ErrorKind::CacheService, Some(e)))
    }

    fn cached_stats(&self, key: &str) -> Option<ErrorStats> {
        self.cache.get(key).and_then(|entry| {
            let (stats, expires_at) = entry.value();
            if Instant::now() < *expires_at {
                Some(stats.clone())
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use crate::errors::ErrorKind;
    use std::sync::Arc;

    #[test]
    fn test_error_stats_calculations() {
        let mut stats = ErrorStats {
            total_errors: 3,
            success_count: 7,
            ..ErrorStats::default()
        };

        assert_eq!(stats.error_rate(), 0.3);
        assert!(stats.health_score() > 0.6);

        stats.consecutive_errors = 5;
        assert!(stats.health_score() < 0.5);
    }

    #[tokio::test]
    async fn request_module_and_task_errors_persist_to_control_plane_store() {
        let store = Arc::new(ProfileControlPlaneStore::open_temp("demo").expect("open temp store"));
        let tracker = StatusTracker::new(
            ErrorTrackerConfig {
                request_max_retries: 5,
                parse_max_retries: 5,
                ..ErrorTrackerConfig::default()
            },
            store.clone(),
        );

        let error = Error::new(
            ErrorKind::Download,
            Some(std::io::Error::other("request failed")),
        );

        let decision = tracker
            .record_download_error("task-1", "module-1", "request-1", &error)
            .await
            .expect("record error");

        assert!(matches!(decision, ErrorDecision::RetryAfter(_)));
        assert_eq!(
            store.get_status_counter("request:request-1:download:total_errors"),
            1
        );
        assert_eq!(
            store.get_status_counter("module:module-1:download:total_errors"),
            1
        );
        assert_eq!(
            store.get_status_counter("task:task-1:total:total_errors"),
            1
        );
    }
}
