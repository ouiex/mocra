use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use deadpool_redis::redis::{from_redis_value, FromRedisValue, Value as RedisValue};
use metrics::{counter, histogram};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, Instant};
use uuid::Uuid;

use crate::cacheable::CacheService;

use super::types::{
    DagError, DagNodeDispatcher, DagNodeTrait, NodeExecutionContext, NodePlacement, TaskPayload,
};

#[derive(Debug, Clone)]
pub struct RedisRemoteDispatcherOptions {
    pub response_timeout_ms: u64,
    pub poll_interval_ms: u64,
    pub result_ttl_secs: u64,
}

impl Default for RedisRemoteDispatcherOptions {
    fn default() -> Self {
        Self {
            response_timeout_ms: 15_000,
            poll_interval_ms: 20,
            result_ttl_secs: 120,
        }
    }
}

#[derive(Clone)]
pub struct RedisRemoteDispatcher {
    cache: Arc<CacheService>,
    key_prefix: String,
    options: RedisRemoteDispatcherOptions,
}

impl RedisRemoteDispatcher {
    pub fn new(
        cache: Arc<CacheService>,
        key_prefix: impl AsRef<str>,
        options: RedisRemoteDispatcherOptions,
    ) -> Self {
        Self {
            cache,
            key_prefix: key_prefix.as_ref().to_string(),
            options,
        }
    }

    fn queue_key(&self, worker_group: &str) -> String {
        format!("{}:remote:queue:{}", self.key_prefix, worker_group)
    }

    fn result_key(&self, request_id: &str) -> String {
        format!("{}:remote:result:{}", self.key_prefix, request_id)
    }

    fn cancel_key(&self, request_id: &str) -> String {
        format!("{}:remote:cancel:{}", self.key_prefix, request_id)
    }

    fn request_map_key(
        &self,
        worker_group: &str,
        run_id: &str,
        node_id: &str,
        attempt: usize,
    ) -> String {
        format!(
            "{}:remote:reqmap:{}:{}:{}:{}",
            self.key_prefix, worker_group, run_id, node_id, attempt
        )
    }

    fn ttl_secs(&self) -> u64 {
        self.options.result_ttl_secs.max(1)
    }

    fn request_map_ttl_secs(&self) -> u64 {
        let response_timeout_secs = self.options.response_timeout_ms.div_ceil(1000).max(1);
        self.ttl_secs().max(response_timeout_secs.saturating_add(2))
    }

    fn cache_err(node_id: &str, action: &str, err: impl std::fmt::Display) -> DagError {
        DagError::NodeExecutionFailed {
            node_id: node_id.to_string(),
            reason: format!("{action}: {err}"),
        }
    }

    fn parse_value<T: FromRedisValue>(
        node_id: &str,
        value: &RedisValue,
        action: &str,
    ) -> Result<T, DagError> {
        from_redis_value(value).map_err(|e| Self::cache_err(node_id, action, e))
    }

    fn next_poll_delay_ms(current_ms: u64, base_ms: u64, max_ms: u64) -> u64 {
        let base = base_ms.max(1);
        let max = max_ms.max(base);
        let current = current_ms.max(base);
        current.saturating_mul(2).min(max)
    }
}

#[async_trait]
impl DagNodeDispatcher for RedisRemoteDispatcher {
    async fn dispatch(
        &self,
        node_id: &str,
        placement: &NodePlacement,
        executor: Arc<dyn DagNodeTrait>,
        context: NodeExecutionContext,
    ) -> Result<TaskPayload, DagError> {
        match placement {
            NodePlacement::Local => executor.start(context).await,
            NodePlacement::Remote { worker_group } => {
                let dispatch_started = Instant::now();
                counter!(
                    "dag_remote_dispatch_total",
                    "worker_group" => worker_group.clone(),
                    "result" => "started".to_string()
                )
                .increment(1);
                let queue_key = self.queue_key(worker_group);
                let request_map_key =
                    self.request_map_key(worker_group, &context.run_id, node_id, context.attempt);
                let requested_id = Uuid::now_v7().to_string();

                let request = WireDispatchRequest {
                    request_id: requested_id.clone(),
                    node_id: node_id.to_string(),
                    worker_group: worker_group.clone(),
                    context: WireNodeExecutionContext::from_runtime(&context)?,
                };
                let req_json = serde_json::to_string(&request)
                    .map_err(|e| Self::cache_err(node_id, "serialize remote request", e))?;

                let request_id: String = {
                    let ttl_arg = self.request_map_ttl_secs().to_string();
                    let keys = [&request_map_key[..], &queue_key[..]];
                    let args = [&requested_id[..], &req_json[..], &ttl_arg[..]];
                    let script = r#"
                    local existing = redis.call('GET', KEYS[1])
                    if existing then
                        return existing
                    end
                    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
                    redis.call('LPUSH', KEYS[2], ARGV[2])
                    return ARGV[1]
                    "#;
                    let value = self
                        .cache
                        .eval_lua(script, &keys, &args)
                        .await
                        .map_err(|e| Self::cache_err(node_id, "enqueue remote request", e))?;
                    Self::parse_value(node_id, &value, "parse enqueue response")?
                };

                let result_key = self.result_key(&request_id);
                let cancel_key = self.cancel_key(&request_id);
                let timeout = Duration::from_millis(self.options.response_timeout_ms);
                let base_poll_ms = self.options.poll_interval_ms.max(1);
                let max_poll_ms = base_poll_ms.saturating_mul(16).min(1_000);
                let mut current_poll_ms = base_poll_ms;
                let started = Instant::now();
                let mut last_poll_error: Option<String> = None;

                loop {
                    if started.elapsed() >= timeout {
                        counter!(
                            "dag_remote_dispatch_total",
                            "worker_group" => worker_group.clone(),
                            "result" => "timeout".to_string()
                        )
                        .increment(1);
                        histogram!(
                            "dag_remote_dispatch_latency_seconds",
                            "worker_group" => worker_group.clone(),
                            "result" => "timeout".to_string()
                        )
                        .record(dispatch_started.elapsed().as_secs_f64());
                        counter!(
                            "dag_alert_event_total",
                            "source" => "dag_remote_dispatcher".to_string(),
                            "severity" => "warning".to_string(),
                            "event" => "timeout".to_string(),
                            "worker_group" => worker_group.clone()
                        )
                        .increment(1);
                        let _ = self
                            .cache
                            .set(
                                &cancel_key,
                                b"1",
                                Some(Duration::from_secs(self.ttl_secs())),
                            )
                            .await;
                        let _ = self.cache.del(&result_key).await;
                        let _ = self.cache.del(&request_map_key).await;
                        let reason = if let Some(err) = last_poll_error.as_ref() {
                            format!(
                                "remote dispatch response timeout: request_id={} worker_group={} last_poll_error={}",
                                request_id, worker_group, err
                            )
                        } else {
                            format!(
                                "remote dispatch response timeout: request_id={} worker_group={}",
                                request_id, worker_group
                            )
                        };
                        return Err(DagError::NodeExecutionFailed {
                            node_id: node_id.to_string(),
                            reason,
                        });
                    }

                    let raw = match self.cache.get(&result_key).await {
                        Ok(value) => {
                            last_poll_error = None;
                            value
                        }
                        Err(e) => {
                            counter!(
                                "dag_remote_dispatch_total",
                                "worker_group" => worker_group.clone(),
                                "result" => "poll_error".to_string()
                            )
                            .increment(1);
                            last_poll_error = Some(e.to_string());
                            tokio::time::sleep(Duration::from_millis(current_poll_ms)).await;
                            current_poll_ms = Self::next_poll_delay_ms(
                                current_poll_ms,
                                base_poll_ms,
                                max_poll_ms,
                            );
                            continue;
                        }
                    };

                    if let Some(raw_resp) = raw {
                        let _ = self.cache.del(&result_key).await;
                        let _ = self.cache.del(&cancel_key).await;
                        let _ = self.cache.del(&request_map_key).await;

                        let raw_resp = String::from_utf8(raw_resp)
                            .map_err(|e| Self::cache_err(node_id, "decode remote response utf8", e))?;
                        let resp: WireDispatchResponse = serde_json::from_str(&raw_resp)
                            .map_err(|e| Self::cache_err(node_id, "deserialize remote response", e))?;

                        if resp.ok {
                            let payload_bytes = resp.payload_envelope.ok_or_else(|| {
                                DagError::NodeExecutionFailed {
                                    node_id: node_id.to_string(),
                                    reason: "remote response missing payload".to_string(),
                                }
                            })?;
                            counter!(
                                "dag_remote_dispatch_total",
                                "worker_group" => worker_group.clone(),
                                "result" => "success".to_string()
                            )
                            .increment(1);
                            histogram!(
                                "dag_remote_dispatch_latency_seconds",
                                "worker_group" => worker_group.clone(),
                                "result" => "success".to_string()
                            )
                            .record(dispatch_started.elapsed().as_secs_f64());
                            return TaskPayload::from_envelope_bytes(&payload_bytes);
                        }

                        counter!(
                            "dag_remote_dispatch_total",
                            "worker_group" => worker_group.clone(),
                            "result" => "failed".to_string()
                        )
                        .increment(1);
                        histogram!(
                            "dag_remote_dispatch_latency_seconds",
                            "worker_group" => worker_group.clone(),
                            "result" => "failed".to_string()
                        )
                        .record(dispatch_started.elapsed().as_secs_f64());
                        counter!(
                            "dag_alert_event_total",
                            "source" => "dag_remote_dispatcher".to_string(),
                            "severity" => "warning".to_string(),
                            "event" => "remote_execute_failed".to_string(),
                            "worker_group" => worker_group.clone()
                        )
                        .increment(1);

                        return Err(DagError::NodeExecutionFailed {
                            node_id: node_id.to_string(),
                            reason: resp
                                .error
                                .unwrap_or_else(|| "remote worker execution failed".to_string()),
                        });
                    }

                    tokio::time::sleep(Duration::from_millis(current_poll_ms)).await;
                    current_poll_ms = Self::next_poll_delay_ms(
                        current_poll_ms,
                        base_poll_ms,
                        max_poll_ms,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RedisRemoteDispatcher;

    #[test]
    fn next_poll_delay_ms_grows_and_caps() {
        let base = 5;
        let max = 80;

        let p1 = RedisRemoteDispatcher::next_poll_delay_ms(base, base, max);
        let p2 = RedisRemoteDispatcher::next_poll_delay_ms(p1, base, max);
        let p3 = RedisRemoteDispatcher::next_poll_delay_ms(p2, base, max);
        let p4 = RedisRemoteDispatcher::next_poll_delay_ms(p3, base, max);
        let p5 = RedisRemoteDispatcher::next_poll_delay_ms(p4, base, max);

        assert_eq!(p1, 10);
        assert_eq!(p2, 20);
        assert_eq!(p3, 40);
        assert_eq!(p4, 80);
        assert_eq!(p5, 80);
    }
}

#[derive(Clone)]
pub struct RedisDagWorker {
    cache: Arc<CacheService>,
    key_prefix: String,
    worker_group: String,
    worker_id: String,
    handlers: HashMap<String, Arc<dyn DagNodeTrait>>,
    idle_sleep_ms: u64,
    result_ttl_secs: u64,
    handler_max_retries: usize,
    handler_retry_backoff_ms: u64,
    heartbeat_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    reclaim_interval_ms: u64,
    max_delivery_attempts: usize,
    delivery_ttl_secs: u64,
}

impl RedisDagWorker {
    pub fn new(
        cache: Arc<CacheService>,
        key_prefix: impl AsRef<str>,
        worker_group: impl AsRef<str>,
    ) -> Self {
        Self {
            cache,
            key_prefix: key_prefix.as_ref().to_string(),
            worker_group: worker_group.as_ref().to_string(),
            worker_id: Uuid::now_v7().to_string(),
            handlers: HashMap::new(),
            idle_sleep_ms: 20,
            result_ttl_secs: 120,
            handler_max_retries: 0,
            handler_retry_backoff_ms: 10,
            heartbeat_ttl_ms: 5_000,
            heartbeat_interval_ms: 1_000,
            reclaim_interval_ms: 1_000,
            max_delivery_attempts: 3,
            delivery_ttl_secs: 600,
        }
    }

    pub fn with_worker_id(mut self, worker_id: impl AsRef<str>) -> Self {
        self.worker_id = worker_id.as_ref().to_string();
        self
    }

    pub fn with_idle_sleep_ms(mut self, idle_sleep_ms: u64) -> Self {
        self.idle_sleep_ms = idle_sleep_ms.max(1);
        self
    }

    pub fn with_result_ttl_secs(mut self, result_ttl_secs: u64) -> Self {
        self.result_ttl_secs = result_ttl_secs.max(1);
        self
    }

    pub fn with_handler_retry(mut self, max_retries: usize, retry_backoff_ms: u64) -> Self {
        self.handler_max_retries = max_retries;
        self.handler_retry_backoff_ms = retry_backoff_ms.max(1);
        self
    }

    pub fn with_heartbeat(mut self, heartbeat_ttl_ms: u64, heartbeat_interval_ms: u64) -> Self {
        self.heartbeat_ttl_ms = heartbeat_ttl_ms.max(200);
        self.heartbeat_interval_ms = heartbeat_interval_ms.max(50);
        self
    }

    pub fn with_reclaim_interval_ms(mut self, reclaim_interval_ms: u64) -> Self {
        self.reclaim_interval_ms = reclaim_interval_ms.max(50);
        self
    }

    pub fn with_max_delivery_attempts(mut self, max_delivery_attempts: usize) -> Self {
        self.max_delivery_attempts = max_delivery_attempts.max(1);
        self
    }

    pub fn register_handler(
        mut self,
        node_id: impl AsRef<str>,
        handler: Arc<dyn DagNodeTrait>,
    ) -> Self {
        self.handlers.insert(node_id.as_ref().to_string(), handler);
        self
    }

    fn queue_key(&self) -> String {
        format!("{}:remote:queue:{}", self.key_prefix, self.worker_group)
    }

    fn workers_set_key(&self) -> String {
        format!("{}:remote:workers:{}", self.key_prefix, self.worker_group)
    }

    fn processing_key_for(&self, worker_id: &str) -> String {
        format!(
            "{}:remote:processing:{}:{}",
            self.key_prefix, self.worker_group, worker_id
        )
    }

    fn processing_key(&self) -> String {
        self.processing_key_for(&self.worker_id)
    }

    fn worker_alive_key_for(&self, worker_id: &str) -> String {
        format!(
            "{}:remote:worker_alive:{}:{}",
            self.key_prefix, self.worker_group, worker_id
        )
    }

    fn worker_alive_key(&self) -> String {
        self.worker_alive_key_for(&self.worker_id)
    }

    fn result_key(&self, request_id: &str) -> String {
        format!("{}:remote:result:{}", self.key_prefix, request_id)
    }

    fn cancel_key(&self, request_id: &str) -> String {
        format!("{}:remote:cancel:{}", self.key_prefix, request_id)
    }

    fn delivery_count_key(&self, request_id: &str) -> String {
        format!("{}:remote:delivery:{}", self.key_prefix, request_id)
    }

    fn dlq_key(&self) -> String {
        format!("{}:remote:dlq:{}", self.key_prefix, self.worker_group)
    }

    fn ms_to_ttl_secs(ms: u64) -> u64 {
        ms.div_ceil(1000).max(1)
    }

    fn parse_value<T: FromRedisValue>(value: &RedisValue, action: &str) -> Result<T, DagError> {
        from_redis_value(value).map_err(|e| DagError::NodeExecutionFailed {
            node_id: "remote-worker".to_string(),
            reason: format!("{action}: {e}"),
        })
    }

    async fn heartbeat(&self) {
        let workers_key = self.workers_set_key();
        let alive_key = self.worker_alive_key();
        let script = "return redis.call('SADD', KEYS[1], ARGV[1])";
        let keys = [&workers_key[..]];
        let args = [&self.worker_id[..]];
        let _ = self.cache.eval_lua(script, &keys, &args).await;
        let _ = self
            .cache
            .set(
                &alive_key,
                b"1",
                Some(Duration::from_secs(Self::ms_to_ttl_secs(self.heartbeat_ttl_ms))),
            )
            .await;
        counter!(
            "dag_remote_worker_heartbeat_total",
            "worker_group" => self.worker_group.clone(),
            "worker_id" => self.worker_id.clone()
        )
        .increment(1);
    }

    async fn pop_move_to_processing(
        &self,
        queue_key: &str,
        processing_key: &str,
    ) -> Option<String> {
        let script = "return redis.call('RPOPLPUSH', KEYS[1], KEYS[2])";
        let keys = [queue_key, processing_key];
        let value = self.cache.eval_lua(script, &keys, &[]).await.ok()?;
        let moved = Self::parse_value::<Option<String>>(&value, "parse pop-move")
            .ok()
            .flatten();
        if moved.is_some() {
            counter!(
                "dag_remote_worker_pop_total",
                "worker_group" => self.worker_group.clone(),
                "worker_id" => self.worker_id.clone(),
                "result" => "hit".to_string()
            )
            .increment(1);
        }
        moved
    }

    async fn lrem_raw_job(&self, processing_key: &str, raw_job: &str) {
        let script = "return redis.call('LREM', KEYS[1], 1, ARGV[1])";
        let keys = [processing_key];
        let args = [raw_job];
        let _ = self.cache.eval_lua(script, &keys, &args).await;
    }

    async fn push_dlq(&self, dlq_key: &str, raw_job: &str) {
        let script = "return redis.call('RPUSH', KEYS[1], ARGV[1])";
        let keys = [dlq_key];
        let args = [raw_job];
        let _ = self.cache.eval_lua(script, &keys, &args).await;
    }

    async fn expire_key(&self, key: &str, ttl_secs: u64) {
        let script = "return redis.call('EXPIRE', KEYS[1], ARGV[1])";
        let ttl = ttl_secs.to_string();
        let keys = [key];
        let args = [ttl.as_str()];
        let _ = self.cache.eval_lua(script, &keys, &args).await;
    }

    async fn smembers_workers(&self, workers_key: &str) -> Vec<String> {
        let script = "return redis.call('SMEMBERS', KEYS[1])";
        let keys = [workers_key];
        let value = match self.cache.eval_lua(script, &keys, &[]).await {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };
        Self::parse_value::<Vec<String>>(&value, "parse workers set").unwrap_or_default()
    }

    async fn reclaim_stale_workers(&self, queue_key: &str) {
        let workers_key = self.workers_set_key();
        let worker_ids = self.smembers_workers(&workers_key).await;

        for worker_id in worker_ids {
            if worker_id == self.worker_id {
                continue;
            }
            let alive_key = self.worker_alive_key_for(&worker_id);
            let alive = self.cache.get(&alive_key).await.ok().flatten().is_some();
            if alive {
                continue;
            }

            let stale_processing_key = self.processing_key_for(&worker_id);
            for _ in 0..256 {
                let moved = self
                    .pop_move_to_processing(&stale_processing_key, queue_key)
                    .await;
                if moved.is_none() {
                    break;
                }
                counter!(
                    "dag_remote_worker_reclaim_total",
                    "worker_group" => self.worker_group.clone(),
                    "worker_id" => self.worker_id.clone()
                )
                .increment(1);
            }
        }
    }

    pub async fn run_until_stopped(self, stop_flag: Arc<AtomicBool>) {
        let queue_key = self.queue_key();
        let processing_key = self.processing_key();
        let dlq_key = self.dlq_key();

        self.heartbeat().await;
        self.reclaim_stale_workers(&queue_key).await;

        let mut last_heartbeat = Instant::now();
        let mut last_reclaim = Instant::now();

        while !stop_flag.load(Ordering::SeqCst) {
            if last_heartbeat.elapsed() >= Duration::from_millis(self.heartbeat_interval_ms) {
                self.heartbeat().await;
                last_heartbeat = Instant::now();
            }
            if last_reclaim.elapsed() >= Duration::from_millis(self.reclaim_interval_ms) {
                self.reclaim_stale_workers(&queue_key).await;
                last_reclaim = Instant::now();
            }

            let raw_job = self.pop_move_to_processing(&queue_key, &processing_key).await;
            let Some(raw_job) = raw_job else {
                tokio::time::sleep(Duration::from_millis(self.idle_sleep_ms)).await;
                continue;
            };

            let request: WireDispatchRequest = match serde_json::from_str(&raw_job) {
                Ok(v) => v,
                Err(_) => {
                    counter!(
                        "dag_remote_worker_dlq_total",
                        "worker_group" => self.worker_group.clone(),
                        "worker_id" => self.worker_id.clone(),
                        "reason" => "invalid_json".to_string()
                    )
                    .increment(1);
                    self.push_dlq(&dlq_key, &raw_job).await;
                    self.lrem_raw_job(&processing_key, &raw_job).await;
                    continue;
                }
            };

            let execute_started = Instant::now();

            let result_key = self.result_key(&request.request_id);
            let cancel_key = self.cancel_key(&request.request_id);
            let delivery_count_key = self.delivery_count_key(&request.request_id);

            let exists = self.cache.get(&result_key).await.ok().flatten().is_some();
            if exists {
                self.lrem_raw_job(&processing_key, &raw_job).await;
                continue;
            }

            let canceled_before_execute = self.cache.get(&cancel_key).await.ok().flatten().is_some();
            if canceled_before_execute {
                counter!(
                    "dag_remote_worker_cancel_total",
                    "worker_group" => self.worker_group.clone(),
                    "worker_id" => self.worker_id.clone(),
                    "stage" => "before_execute".to_string()
                )
                .increment(1);
                let _ = self.cache.del(&cancel_key).await;
                self.lrem_raw_job(&processing_key, &raw_job).await;
                continue;
            }

            let delivery_attempt = self.cache.incr(&delivery_count_key, 1).await.unwrap_or(1);
            self.expire_key(&delivery_count_key, self.delivery_ttl_secs).await;

            if delivery_attempt as usize > self.max_delivery_attempts {
                let response = WireDispatchResponse {
                    ok: false,
                    payload_envelope: None,
                    error: Some(format!(
                        "remote worker delivery attempts exhausted: request_id={} attempts={} max_attempts={}",
                        request.request_id, delivery_attempt, self.max_delivery_attempts
                    )),
                };
                if let Ok(resp_json) = serde_json::to_string(&response) {
                    let _ = self
                        .cache
                        .set(
                            &result_key,
                            resp_json.as_bytes(),
                            Some(Duration::from_secs(self.result_ttl_secs)),
                        )
                        .await;
                }
                counter!(
                    "dag_remote_worker_dlq_total",
                    "worker_group" => self.worker_group.clone(),
                    "worker_id" => self.worker_id.clone(),
                    "reason" => "delivery_exhausted".to_string()
                )
                .increment(1);
                counter!(
                    "dag_alert_event_total",
                    "source" => "dag_remote_worker".to_string(),
                    "severity" => "warning".to_string(),
                    "event" => "delivery_exhausted".to_string(),
                    "worker_group" => self.worker_group.clone(),
                    "worker_id" => self.worker_id.clone()
                )
                .increment(1);
                self.push_dlq(&dlq_key, &raw_job).await;
                self.lrem_raw_job(&processing_key, &raw_job).await;
                continue;
            }

            let response = self.execute_request(request).await;

            let canceled = self.cache.get(&cancel_key).await.ok().flatten().is_some();
            if canceled {
                counter!(
                    "dag_remote_worker_cancel_total",
                    "worker_group" => self.worker_group.clone(),
                    "worker_id" => self.worker_id.clone(),
                    "stage" => "after_execute".to_string()
                )
                .increment(1);
                let _ = self.cache.del(&cancel_key).await;
                self.lrem_raw_job(&processing_key, &raw_job).await;
                continue;
            }

            if let Ok(resp_json) = serde_json::to_string(&response) {
                let _ = self
                    .cache
                    .set(
                        &result_key,
                        resp_json.as_bytes(),
                        Some(Duration::from_secs(self.result_ttl_secs)),
                    )
                    .await;
            }

            let exec_result = if response.ok { "success" } else { "failed" };
            counter!(
                "dag_remote_worker_execute_total",
                "worker_group" => self.worker_group.clone(),
                "worker_id" => self.worker_id.clone(),
                "result" => exec_result.to_string()
            )
            .increment(1);
            if !response.ok {
                counter!(
                    "dag_alert_event_total",
                    "source" => "dag_remote_worker".to_string(),
                    "severity" => "warning".to_string(),
                    "event" => "handler_failed".to_string(),
                    "worker_group" => self.worker_group.clone(),
                    "worker_id" => self.worker_id.clone()
                )
                .increment(1);
            }
            histogram!(
                "dag_remote_worker_execute_latency_seconds",
                "worker_group" => self.worker_group.clone(),
                "worker_id" => self.worker_id.clone(),
                "result" => exec_result.to_string()
            )
            .record(execute_started.elapsed().as_secs_f64());
            self.lrem_raw_job(&processing_key, &raw_job).await;
        }

        let _ = self.cache.del(&self.worker_alive_key()).await;
    }

    async fn execute_request(&self, request: WireDispatchRequest) -> WireDispatchResponse {
        let Some(handler) = self.handlers.get(&request.node_id).cloned() else {
            return WireDispatchResponse {
                ok: false,
                payload_envelope: None,
                error: Some(format!("remote worker handler missing for node {}", request.node_id)),
            };
        };

        let context = match request.context.into_runtime() {
            Ok(c) => c,
            Err(e) => {
                return WireDispatchResponse {
                    ok: false,
                    payload_envelope: None,
                    error: Some(e.to_string()),
                }
            }
        };

        let mut last_error: Option<String> = None;
        for retry in 0..=self.handler_max_retries {
            match handler.start(context.clone()).await {
                Ok(payload) => {
                    return match payload.to_envelope_bytes() {
                        Ok(bytes) => WireDispatchResponse {
                            ok: true,
                            payload_envelope: Some(bytes),
                            error: None,
                        },
                        Err(e) => WireDispatchResponse {
                            ok: false,
                            payload_envelope: None,
                            error: Some(e.to_string()),
                        },
                    }
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    if retry < self.handler_max_retries {
                        tokio::time::sleep(Duration::from_millis(self.handler_retry_backoff_ms)).await;
                        continue;
                    }
                }
            }
        }

        WireDispatchResponse {
            ok: false,
            payload_envelope: None,
            error: Some(last_error.unwrap_or_else(|| "remote worker execution failed".to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireDispatchRequest {
    request_id: String,
    node_id: String,
    worker_group: String,
    context: WireNodeExecutionContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireDispatchResponse {
    ok: bool,
    payload_envelope: Option<Vec<u8>>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireNodeExecutionContext {
    run_id: String,
    run_fencing_token: Option<u64>,
    node_id: String,
    attempt: usize,
    upstream_nodes: Vec<String>,
    upstream_outputs_envelope: HashMap<String, Vec<u8>>,
    layer_index: usize,
}

impl WireNodeExecutionContext {
    fn from_runtime(context: &NodeExecutionContext) -> Result<Self, DagError> {
        let mut upstream_outputs_envelope = HashMap::with_capacity(context.upstream_outputs.len());
        for (k, v) in &context.upstream_outputs {
            upstream_outputs_envelope.insert(k.clone(), v.to_envelope_bytes()?);
        }

        Ok(Self {
            run_id: context.run_id.clone(),
            run_fencing_token: context.run_fencing_token,
            node_id: context.node_id.clone(),
            attempt: context.attempt,
            upstream_nodes: context.upstream_nodes.clone(),
            upstream_outputs_envelope,
            layer_index: context.layer_index,
        })
    }

    fn into_runtime(self) -> Result<NodeExecutionContext, DagError> {
        let mut upstream_outputs = HashMap::with_capacity(self.upstream_outputs_envelope.len());
        for (k, bytes) in self.upstream_outputs_envelope {
            upstream_outputs.insert(k, TaskPayload::from_envelope_bytes(&bytes)?);
        }

        Ok(NodeExecutionContext {
            run_id: self.run_id,
            run_fencing_token: self.run_fencing_token,
            node_id: self.node_id,
            attempt: self.attempt,
            upstream_nodes: self.upstream_nodes,
            upstream_outputs,
            layer_index: self.layer_index,
        })
    }
}
