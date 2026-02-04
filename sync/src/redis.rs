use crate::CoordinationBackend;
use deadpool_redis::redis::streams::{StreamReadOptions, StreamReadReply};
use deadpool_redis::redis::{AsyncCommands, RedisResult};
use deadpool_redis::Pool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

/// Multiplexed stream router for sync subscriptions.
/// All subscriptions share a single Redis connection to avoid pool exhaustion.
struct StreamRouter {
    routes: HashMap<String, (mpsc::Sender<Vec<u8>>, String)>, // topic -> (sender, last_id)
}

#[derive(Clone)]
pub struct RedisBackend {
    pool: Pool,
    router: Arc<RwLock<StreamRouter>>,
    listener_started: Arc<std::sync::atomic::AtomicBool>,
}

impl RedisBackend {
    pub fn new(pool: Pool) -> Self {
        let backend = Self { 
            pool,
            router: Arc::new(RwLock::new(StreamRouter { routes: HashMap::new() })),
            listener_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        backend
    }

    /// Spawn a single multiplexed listener that handles all subscriptions.
    /// Uses only ONE connection from the pool, regardless of subscription count.
    fn ensure_listener_started(&self) {
        use std::sync::atomic::Ordering;
        
        // Only start once
        if self.listener_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let pool = self.pool.clone();
        let router = self.router.clone();

        tokio::spawn(async move {
            info!("Starting multiplexed sync listener (uses 1 connection for all subscriptions)");
            
            // Get a dedicated connection (not from pool, to avoid blocking pool)
            let client = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to get connection for sync listener: {}", e);
                    return;
                }
            };
            
            // We need to hold the connection and keep reusing it
            // But deadpool connections have timeouts, so we use a raw connection approach
            let mut conn: Option<deadpool_redis::Connection> = Some(client);

            loop {
                // 1. Get current routes snapshot
                let (topics, last_ids): (Vec<String>, Vec<String>) = {
                    let r = router.read().await;
                    if r.routes.is_empty() {
                        drop(r);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        continue;
                    }
                    r.routes.iter()
                        .map(|(topic, (_, last_id))| (topic.clone(), last_id.clone()))
                        .unzip()
                };

                if topics.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }

                // 2. Ensure connection
                if conn.is_none() {
                    match pool.get().await {
                        Ok(c) => conn = Some(c),
                        Err(e) => {
                            error!("Sync listener reconnect failed: {}", e);
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }

                let active_conn = match conn.as_mut() {
                    Some(c) => c,
                    None => continue,
                };

                // 3. XREAD multiple streams with short block time (2s) to allow dynamic updates
                let opts = StreamReadOptions::default()
                    .count(100)
                    .block(2000);

                let ids_refs: Vec<&str> = last_ids.iter().map(|s| s.as_str()).collect();
                let topics_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();

                let result: RedisResult<StreamReadReply> = 
                    active_conn.xread_options(&topics_refs, &ids_refs, &opts).await;

                match result {
                    Ok(reply) => {
                        let mut router_guard = router.write().await;
                        for stream_key in reply.keys {
                            let topic = &stream_key.key;
                            if let Some((sender, last_id)) = router_guard.routes.get_mut(topic) {
                                for element in stream_key.ids {
                                    *last_id = element.id.clone();
                                    
                                    let payload_val = element.map.get("payload")
                                        .unwrap_or(&deadpool_redis::redis::Value::Nil);
                                    let payload_vec: Vec<u8> = match deadpool_redis::redis::FromRedisValue::from_redis_value(payload_val) {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    };

                                    if sender.send(payload_vec).await.is_err() {
                                        warn!("Sync subscriber for {} dropped, removing route", topic);
                                        // Will be cleaned up next iteration
                                    }
                                }
                            }
                        }
                        // Clean up dead routes
                        router_guard.routes.retain(|_, (sender, _)| !sender.is_closed());
                    }
                    Err(e) => {
                        error!("Sync listener XREAD error: {}. Reconnecting...", e);
                        conn = None;
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl CoordinationBackend for RedisBackend {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        conn.xadd::<_, _, _, _, ()>(topic, "*", &[("payload", payload)])
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        // Ensure the multiplexed listener is running
        self.ensure_listener_started();

        let (tx, rx) = mpsc::channel(1000);
        
        // Register this topic in the router
        {
            let mut router = self.router.write().await;
            // Start from new messages ($)
            router.routes.insert(topic.to_string(), (tx, "$".to_string()));
        }

        info!("Registered sync subscription for topic: {} (multiplexed)", topic);
        Ok(rx)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        conn.set(key, value).await.map_err(|e| e.to_string())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        conn.get(key).await.map_err(|e| e.to_string())
    }

    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        // Lua script for atomic CAS
        // Return 1 for success, 0 for failure
        let script = if let Some(_old) = old_val {
            deadpool_redis::redis::Script::new(
                r"
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    redis.call('set', KEYS[1], ARGV[2])
                    return 1
                else
                    return 0
                end
            ",
            )
        } else {
            deadpool_redis::redis::Script::new(
                r"
                if redis.call('exists', KEYS[1]) == 0 then
                    redis.call('set', KEYS[1], ARGV[1])
                    return 1
                else
                    return 0
                end
            ",
            )
        };

        let mut invocation = script.key(key);
        if let Some(old) = old_val {
            invocation.arg(old).arg(new_val);
        } else {
            invocation.arg(new_val);
        }

        let result: bool = invocation
            .invoke_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    }

    async fn acquire_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        let result: Option<String> = deadpool_redis::redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms)
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(result.is_some())
    }

    async fn renew_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        let script = deadpool_redis::redis::Script::new(
            r"
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('pexpire', KEYS[1], ARGV[2])
            else
                return 0
            end
            ",
        );
        let result: bool = script
            .key(key)
            .arg(value)
            .arg(ttl_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    }
}
