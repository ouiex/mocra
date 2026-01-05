
use tokio::sync::mpsc;
use crate::MqBackend;
use redis::{AsyncCommands};
use redis::streams::{StreamReadOptions, StreamReadReply};
use uuid::Uuid;
use deadpool_redis::{Config, Runtime, Pool};

#[derive(Clone)]
pub struct RedisBackend {
    pool: Pool,
}

impl RedisBackend {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl MqBackend for RedisBackend {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        let mut conn = self.pool.get().await.map_err(|e| e.to_string())?;
        conn.xadd::<_, _, _, _, ()>(topic, "*", &[("payload", payload)]).await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        // For subscription, we need a dedicated connection or at least one that blocks.
        // deadpool connections are meant to be returned.
        // However, for XREAD BLOCK 0, we hold the connection forever.
        // We can just take one from the pool and never return it (it will be dropped when task ends).
        // Or better, create a standalone client for subscription if we want to avoid exhausting the pool.
        // But for simplicity, let's use the pool.
        let pool = self.pool.clone();
        let topic = topic.to_string();
        let group_name = format!("sync_group_{}", Uuid::new_v4());
        let (tx, rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            let mut conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Redis connection failed: {}", e);
                    return;
                }
            };

            let _: redis::RedisResult<()> = conn.xgroup_create_mkstream(&topic, &group_name, "$").await;

            let opts = StreamReadOptions::default()
                .group(&group_name, "consumer_1")
                .count(1)
                .block(0);

            loop {
                let result: redis::RedisResult<StreamReadReply> = conn.xread_options(&[&topic], &[">"], &opts).await;
                match result {
                    Ok(reply) => {
                        for stream_key in reply.keys {
                            for element in stream_key.ids {
                                let payload_val = element.map.get("payload").unwrap_or(&redis::Value::Nil);
                                let payload_vec: Vec<u8> = match redis::FromRedisValue::from_redis_value(payload_val) {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };

                                if tx.send(payload_vec).await.is_err() { return; }

                                let _: redis::RedisResult<()> = conn.xack(&topic, &group_name, &[element.id.as_str()]).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Redis Stream read error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        // If connection is broken, we might need to re-acquire from pool?
                        // deadpool handles some of this, but if the error is fatal, we might need to break and restart task.
                        // For now, simple sleep.
                    }
                }
            }
        });

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
            redis::Script::new(r"
                if redis.call('get', KEYS[1]) == ARGV[1] then
                    redis.call('set', KEYS[1], ARGV[2])
                    return 1
                else
                    return 0
                end
            ")
        } else {
            redis::Script::new(r"
                if redis.call('exists', KEYS[1]) == 0 then
                    redis.call('set', KEYS[1], ARGV[1])
                    return 1
                else
                    return 0
                end
            ")
        };

        let mut invocation = script.key(key);
        if let Some(old) = old_val {
            invocation.arg(old).arg(new_val);
        } else {
            invocation.arg(new_val);
        }

        let result: bool = invocation.invoke_async(&mut conn).await.map_err(|e| e.to_string())?;
        Ok(result)
    }
}