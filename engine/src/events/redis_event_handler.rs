use super::SystemEvent;
use super::event_bus::EventHandler;
use async_trait::async_trait;
use log::{error, warn};
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use deadpool_redis::redis;

/// Redis事件处理器，用于将事件保存到Redis供其他程序监控
pub struct RedisEventHandler {
    redis_pool: Arc<deadpool_redis::Pool>,
    key_prefix: String,
    max_retries: u32,
}

impl RedisEventHandler {
    pub fn new(redis_pool: Arc<deadpool_redis::Pool>, key_prefix: String, _ttl: u64) -> Self {
        Self {
            redis_pool,
            key_prefix,
            max_retries: 3,
        }
    }

    /// 生成Redis键名
    fn generate_key(&self, event_type: &str) -> String {
        format!("{}:events:{}", self.key_prefix, event_type)
    }

    /// 生成时间序列键名
    fn generate_timeseries_key(&self, event_type: &str) -> String {
        format!("{}:timeseries:{}", self.key_prefix, event_type)
    }

    /// 启动后台清理任务
    ///
    /// # Arguments
    /// * `interval` - 清理任务执行间隔
    /// * `retention` - 数据保留时间
    pub fn start_cleanup_task(&self, interval: Duration, retention: Duration) {
        let pool = self.redis_pool.clone();
        let key_pattern = format!("{}:timeseries:*", self.key_prefix);

        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            loop {
                timer.tick().await;
                if let Err(e) = Self::perform_cleanup(&pool, &key_pattern, retention).await {
                    error!("Redis cleanup task failed: {}", e);
                }
            }
        });
    }

    async fn perform_cleanup(
        pool: &deadpool_redis::Pool,
        key_pattern: &str,
        retention: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = pool
            .get()
            .await
            .map_err(|e| format!("Redis connection failed: {e}"))?;

        // 1. 扫描所有相关 Key
        let mut keys: Vec<String> = Vec::new();
        {
            let mut iter: redis::AsyncIter<String> = redis::cmd("SCAN")
                .cursor_arg(0)
                .arg("MATCH")
                .arg(key_pattern)
                .arg("COUNT")
                .arg(100)
                .clone()
                .iter_async(&mut conn)
                .await?;

            while let Some(key) = iter.next_item().await {
                keys.push(key);
            }
        } // iter dropped here, releasing borrow on conn

        // 2. 计算截止时间
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs_f64();
        let cutoff = now - retention.as_secs_f64();

        // 3. 批量清理
        for key in keys {
            let mut pipe = redis::pipe();
            // 移除旧数据
            pipe.cmd("ZREMRANGEBYSCORE")
                .arg(&key)
                .arg("-inf")
                .arg(cutoff)
                // 重置过期时间（确保 Key 在不再更新后最终会过期）
                .expire(&key, retention.as_secs() as i64);

            let _: () = pipe.query_async(&mut conn).await?;
        }

        Ok(())
    }



    /// 序列化事件
    fn serialize_event(&self, event: &SystemEvent) -> String {
        json!({
            "event_type": event.event_type(),
            "timestamp": event.timestamp(),
            "data": event,
        })
        .to_string()
    }

    /// 重试机制
    async fn retry_operation<F, Fut>(
        &self,
        mut operation: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        let mut last_error = None;

        for attempt in 0..self.max_retries {
            match operation().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!("Redis operation failed on attempt {}: {}", attempt + 1, e);
                    last_error = Some(e);

                    if attempt < self.max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "All retry attempts failed".into()))
    }
}

#[async_trait]
impl EventHandler for RedisEventHandler {
    async fn handle(
        &self,
        event: &SystemEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // debug!("Processing event for Redis storage: {}", event.event_type());

        let event_type = event.event_type();
        let timestamp = event.timestamp();

        // 1. 序列化一次，复用
        let event_data = self.serialize_event(event);

        // 2. 准备 Key
        let key = self.generate_key(event_type);
        let ts_key = self.generate_timeseries_key(event_type);

        // 3. 准备时间序列数据 (复用 event_data)
        let score = timestamp as f64;
        // 注意：原代码中 save_timeseries_data 的 member 结构与 serialize_event 是一致的
        let member = &event_data;

        // 4. 使用 Pipeline 执行写入，并带有重试机制
        // 注意：这里移除了每次写入时的 ZREMRANGEBYSCORE 和 EXPIRE 操作，应由后台任务处理
        // 将连接获取移入闭包内部，既解决了生命周期问题，又能在重试时获取新连接（提高健壮性）
        // 在 Happy Path 下，仍然只获取一次连接。
        let operation = || async {
            let mut conn = self
                .redis_pool
                .get()
                .await
                .map_err(|e| format!("Redis connection failed: {e}"))?;

            let mut pipe = redis::pipe();
            pipe.rpush(&key, &event_data)
                .zadd(&ts_key, member, score);

            let _: () = timeout(Duration::from_secs(5), async {
                pipe.query_async(&mut conn).await
            })
            .await??;
            Ok(())
        };

        if let Err(e) = self.retry_operation(operation).await {
            error!("Failed to save event to Redis (pipeline): {}", e);
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "RedisEventHandler"
    }
}
