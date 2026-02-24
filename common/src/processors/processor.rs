#![allow(unused)]

use async_trait::async_trait;
use errors::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Retry policy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum retry attempts.
    pub max_retries: u32,
    /// Initial retry delay in milliseconds.
    pub retry_delay: u64,
    /// Current retry count.
    pub current_retry: u32,
    /// Retry reason.
    pub reason: Option<String>,
    /// Extra metadata.
    pub meta: serde_json::Value,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: 2000,
            current_retry: 0,
            reason: None,
            meta: Default::default(),
        }
    }
}

impl RetryPolicy {
    /// Returns `true` when another retry should be attempted.
    pub fn should_retry(&self) -> bool {
        self.current_retry < self.max_retries
    }
    /// Calculates delay before the next retry.
    pub fn next_retry_delay(&self) -> u64 {
        self.retry_delay * (self.current_retry + 1) as u64
    }
    /// Increments retry count.
    pub fn increment_retry(&mut self) {
        self.current_retry += 1;
    }
    /// Resets retry state.
    pub fn reset(&mut self) {
        self.current_retry = 0;
        self.retry_delay = 2000;
    }
    /// Advances retry state and returns the delay for the next retry.
    pub async fn delay_next_retry(&mut self) -> Option<u64> {
        if !self.should_retry() {
            return None;
        }
        self.increment_retry();
        let delay = self.next_retry_delay();
        Some(delay)
    }

    /// Adds a metadata entry.
    pub fn with_meta(mut self, key: &str, value: serde_json::Value) -> Self {
        if let serde_json::Value::Object(ref mut map) = self.meta {
            map.insert(key.to_string(), value);
        }
        self
    }
    /// Sets retry reason.
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }
}

/// Processor execution result.
#[derive(Debug)]
pub enum ProcessorResult<T> {
    /// Processing succeeded.
    Success(T),
    /// Processing failed, but can be retried.
    RetryableFailure(RetryPolicy),
    /// Processing failed and is not retryable.
    FatalFailure(Error),
}

impl<T> ProcessorResult<T> {
    pub fn is_success(&self) -> bool {
        matches!(self, ProcessorResult::Success(_))
    }

    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            ProcessorResult::RetryableFailure(_) | ProcessorResult::FatalFailure(_)
        )
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, ProcessorResult::RetryableFailure(_))
    }
}

/// Processor execution context.
#[derive(Debug, Clone)]
pub struct ProcessorContext {
    /// Thread-safe metadata.
    pub metadata: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    /// Retry policy.
    pub retry_policy: Option<RetryPolicy>,
    /// Creation timestamp.
    pub created_at: u64,
    /// Step timeout in milliseconds.
    pub step_timeout_ms: Option<u64>,
    /// Whether execution has been cancelled.
    pub cancelled: bool,
}

impl Default for ProcessorContext {
    fn default() -> Self {
        Self {
            metadata: Default::default(),
            retry_policy: None,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            step_timeout_ms: None,
            cancelled: false,
        }
    }
}

impl ProcessorContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn with_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        self.metadata.write().await.insert(key, value);
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    pub fn with_step_timeout_ms(mut self, ms: u64) -> Self {
        self.step_timeout_ms = Some(ms);
        self
    }

    pub fn cancel(mut self) -> Self {
        self.cancelled = true;
        self
    }
}

/// Base processor trait.
#[async_trait]
pub trait ProcessorTrait<Input, Output>: Send + Sync {
    /// Processor name.
    fn name(&self) -> &'static str;

    /// Processes input data.
    async fn process(&self, input: Input, context: ProcessorContext) -> ProcessorResult<Output>;

    /// Optional pre-processing hook.
    async fn pre_process(&self, _input: &Input, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }

    /// Optional post-processing hook.
    async fn post_process(
        &self,
        _input: &Input,
        _output: &Output,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }

    /// Optional error handling hook.
    async fn handle_error(
        &self,
        _input: &Input,
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Output> {
        ProcessorResult::FatalFailure(_error)
    }

    /// Optional predicate to decide whether this input should be processed.
    async fn should_process(&self, _input: &Input, _context: &ProcessorContext) -> bool {
        true
    }
}

/// Configurable processor trait.
#[async_trait]
pub trait ConfigurableProcessor<Input, Output, Config>: ProcessorTrait<Input, Output> {
    /// Applies configuration.
    async fn apply_config(&mut self, config: Config) -> Result<()>;

    /// Returns current configuration.
    fn get_config(&self) -> Option<&Config>;
}
