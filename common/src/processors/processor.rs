#![allow(unused)]

use async_trait::async_trait;
use errors::error::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub retry_delay: u64,
    pub current_retry: u32,
    pub reason: Option<String>,
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
    pub fn should_retry(&self) -> bool {
        self.current_retry < self.max_retries
    }
    pub fn next_retry_delay(&self) -> u64 {
        self.retry_delay * (self.current_retry + 1) as u64
    }
    pub fn increment_retry(&mut self) {
        self.current_retry += 1;
    }
    pub fn reset(&mut self) {
        self.current_retry = 0;
        self.retry_delay = 2000;
    }
    pub async fn delay_next_retry(&mut self) -> Option<u64> {
        if !self.should_retry() {
            return None;
        }
        self.increment_retry();
        let delay = self.next_retry_delay();
        Some(delay)
    }

    pub fn with_meta(mut self, key: &str, value: serde_json::Value) -> Self {
        if let serde_json::Value::Object(ref mut map) = self.meta {
            map.insert(key.to_string(), value);
        }
        self
    }
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }
}

/// 处理器结果
#[derive(Debug)]
pub enum ProcessorResult<T> {
    /// 处理成功
    Success(T),
    /// 处理失败，但可以重试
    RetryableFailure(RetryPolicy),
    /// 处理失败，不可重试
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

/// 处理器上下文
#[derive(Debug, Clone)]
pub struct ProcessorContext {
    pub metadata: Arc<RwLock<HashMap<String, serde_json::Value>>>,
    pub retry_policy: Option<RetryPolicy>,
    pub created_at: u64,
    // Optional overall timeout in milliseconds for a single processor step
    pub step_timeout_ms: Option<u64>,
    // Cooperative cancellation flag
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

/// 基础处理器trait
#[async_trait]
pub trait ProcessorTrait<Input, Output>: Send + Sync {
    /// 处理器名称
    fn name(&self) -> &'static str;

    /// 处理输入数据
    async fn process(&self, input: Input, context: ProcessorContext) -> ProcessorResult<Output>;

    /// 前置处理（可选）
    async fn pre_process(&self, _input: &Input, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }

    /// 后置处理（可选）
    async fn post_process(
        &self,
        _input: &Input,
        _output: &Output,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }

    /// 错误处理（可选）
    async fn handle_error(
        &self,
        _input: &Input,
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Output> {
        ProcessorResult::FatalFailure(_error)
    }

    /// 检查是否应该处理该输入
    async fn should_process(&self, _input: &Input, _context: &ProcessorContext) -> bool {
        true
    }
}

/// 可配置的处理器trait
#[async_trait]
pub trait ConfigurableProcessor<Input, Output, Config>: ProcessorTrait<Input, Output> {
    /// 应用配置
    async fn apply_config(&mut self, config: Config) -> Result<()>;

    /// 获取当前配置
    fn get_config(&self) -> Option<&Config>;
}
