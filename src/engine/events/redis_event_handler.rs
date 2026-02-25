use super::EventEnvelope;
// use super::event_bus::EventHandler;
// use async_trait::async_trait;
use log::{error, warn};
use serde_json::json;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use deadpool_redis::redis;

/// Persists runtime events to Redis for downstream monitoring consumers.
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

    /// Builds the Redis list key used to append raw events.
    fn generate_key(&self, event_type: &str) -> String {
        format!("{}:events:{}", self.key_prefix, event_type)
    }

    /// Builds the Redis sorted-set key used for time-series indexing.
    fn generate_timeseries_key(&self, event_type: &str) -> String {
        format!("{}:timeseries:{}", self.key_prefix, event_type)
    }

    /// Starts a background cleanup task for time-series retention.
    ///
    /// # Arguments
    /// * `interval` - Cleanup execution interval.
    /// * `retention` - Retention window used to prune stale points.
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

        // 1) Scan matching time-series keys.
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
                keys.push(key?);
            }
        } // iter dropped here, releasing borrow on conn

        // 2) Compute retention cutoff.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs_f64();
        let cutoff = now - retention.as_secs_f64();

        // 3) Remove old points and refresh key TTL.
        for key in keys {
            let mut pipe = redis::pipe();
            pipe.cmd("ZREMRANGEBYSCORE")
                .arg(&key)
                .arg("-inf")
                .arg(cutoff)
                .expire(&key, retention.as_secs() as i64);

            let _: () = pipe.query_async(&mut conn).await?;
        }

        Ok(())
    }



    /// Serializes an event envelope into a compact JSON payload.
    fn serialize_event(&self, event: &EventEnvelope) -> String {
        json!({
            "event_key": event.event_key(),
            "timestamp_ms": event.timestamp_ms,
            "data": event,
        })
        .to_string()
    }

    /// Executes an async Redis operation with bounded exponential backoff.
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

    pub async fn start(self, mut rx: tokio::sync::mpsc::Receiver<EventEnvelope>) {
        let handler = Arc::new(self);
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                    let event_type = event.event_key();
                    let timestamp = event.timestamp_ms;

                    let event_data = handler.serialize_event(&event);
                    let key = handler.generate_key(&event_type);
                    let ts_key = handler.generate_timeseries_key(&event_type);
                    let score = timestamp as f64;
                    let member = &event_data;

                    // clone handler for the closure? no, closure captures handler (Arc)
                    let h_clone = handler.clone();
                    
                    let operation = || async {
                        let mut conn = h_clone.redis_pool
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

                    if let Err(e) = handler.retry_operation(operation).await {
                        error!("Failed to save event to Redis (pipeline): {}", e);
                    }
            }
        });
    }
}
