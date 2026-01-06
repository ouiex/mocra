use crate::{Message, MqBackend};
use async_trait::async_trait;
use common::model::config::RedisConfig;
use errors::Result;
use errors::error::QueueError;
use log::{error, info, warn};
use redis::{AsyncCommands, FromRedisValue};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub struct RedisQueue {
    pool: deadpool_redis::Pool,
    group_id: String,
    consumer_name: String,
    minid_time: u64,
    namespace: String,
}

impl RedisQueue {
    pub fn new(redis_config: &RedisConfig, minid_time: u64, namespace: &str) -> Result<Self> {
        let pool = utils::connector::create_redis_pool(
            &redis_config.redis_host,
            redis_config.redis_port,
            redis_config.redis_db,
            &redis_config.redis_username,
            &redis_config.redis_password,
        )
        .ok_or(QueueError::ConnectionFailed)?;

        let consumer_name = uuid::Uuid::new_v4().to_string();
        // Use a default group ID or maybe pass it in?
        // The previous implementation didn't use group ID for lists, but streams need it.
        // I'll use a default one or maybe "crawler_group".
        let group_id = format!("{}:crawler_group", namespace);

        Ok(Self {
            pool,
            group_id,
            consumer_name,
            minid_time,
            namespace: namespace.to_string(),
        })
    }
}

#[async_trait]
impl MqBackend for RedisQueue {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| QueueError::ConnectionFailed)?;

        let topic_key = format!("{}:{}", self.namespace, topic);

        let _: String = redis::cmd("XADD")
            .arg(topic_key)
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async(&mut conn)
            .await
            .map_err(|e| QueueError::PushFailed(Box::new(e)))?;
        Ok(())
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()> {
        let pool = self.pool.clone();
        let topic = format!("{}:{}", self.namespace, topic);
        let group_id = self.group_id.clone();
        let consumer_name = self.consumer_name.clone();

        tokio::spawn(async move {
            info!(
                "Starting Redis stream listener for topic: {} (Group: {}, Consumer: {})",
                topic, group_id, consumer_name
            );

            let mut conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to connect to Redis for subscription: {}", e);
                    return;
                }
            };

            // Ensure group exists
            match conn
                .xgroup_create_mkstream::<&str, &str, &str, ()>(&topic, &group_id, "$")
                .await
            {
                Ok(_) => info!("Created consumer group {} for topic {}", group_id, topic),
                Err(e) => {
                    if e.code() == Some("BUSYGROUP") {
                        // Group already exists, which is fine.
                    } else {
                        error!("Failed to create consumer group: {}", e);
                    }
                }
            }

            loop {
                // XREADGROUP GROUP group consumer BLOCK 0 COUNT 10 STREAMS topic >
                let opts = redis::streams::StreamReadOptions::default()
                    .group(&group_id, &consumer_name)
                    .block(0)
                    .count(10);

                let result: redis::RedisResult<redis::streams::StreamReadReply> =
                    conn.xread_options(&[&topic], &[">"], &opts).await;

                match result {
                    Ok(reply) => {
                        for stream_key in reply.keys {
                            for stream_id in stream_key.ids {
                                let id = stream_id.id.clone();
                                if let Some(val) = stream_id.map.get("payload")
                                    && let Ok(data) = Vec::<u8>::from_redis_value(val)
                                {
                                    // Create an ACK channel for this message
                                    let (ack_tx, mut ack_rx) = mpsc::channel(1);

                                    let msg = Message {
                                        payload: data,
                                        ack_tx,
                                    };

                                    if sender.send(msg).await.is_ok() {
                                        let pool_clone = pool.clone();
                                        let topic_clone = topic.clone();
                                        let group_clone = group_id.clone();
                                        let id_clone = id.clone();

                                        tokio::spawn(async move {
                                            // Wait for the ack signal
                                            if ack_rx.recv().await.is_some() {
                                                if let Ok(mut ack_conn) = pool_clone.get().await {
                                                    let _: redis::RedisResult<()> = ack_conn
                                                        .xack(
                                                            &topic_clone,
                                                            &group_clone,
                                                            &[&id_clone],
                                                        )
                                                        .await;
                                                }
                                            }
                                        });
                                    } else {
                                        warn!(
                                            "No active subscribers for topic {}, stopping listener",
                                            topic
                                        );
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error reading from Redis stream: {}. Retrying in 5s...", e);
                        // If the error is NOGROUP, we should try to recreate the group
                        if let Some(code) = e.code()
                            && code == "NOGROUP"
                        {
                            warn!(
                                "Consumer group missing for topic {}, attempting to recreate...",
                                topic
                            );
                            match conn
                                .xgroup_create_mkstream::<&str, &str, &str, ()>(
                                    &topic, &group_id, "$",
                                )
                                .await
                            {
                                Ok(_) => info!(
                                    "Recreated consumer group {} for topic {}",
                                    group_id, topic
                                ),
                                Err(e) => error!("Failed to recreate consumer group: {}", e),
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn clean_storage(&self) -> Result<()> {
        if self.minid_time == 0 {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| QueueError::ConnectionFailed)?;
        let retention_period = self.minid_time as u128 * 60 * 60 * 1000;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();
        let min_id = now.saturating_sub(retention_period);

        info!(
            "Starting Redis storage cleanup for namespace {}, min_id: {}",
            self.namespace, min_id
        );

        let pattern = format!("{}:*", self.namespace);
        let mut keys: Vec<String> = Vec::new();

        {
            let mut cursor = 0;
            loop {
                let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                    .cursor_arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| QueueError::OperationFailed(Box::new(e)))?;

                keys.extend(batch);
                cursor = next_cursor;

                if cursor == 0 {
                    break;
                }
            }
        }

        for key in keys {
            let key_type: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap_or("none".to_string());
            if key_type == "stream" {
                let _: () = redis::cmd("XTRIM")
                    .arg(&key)
                    .arg("MINID")
                    .arg("~")
                    .arg(min_id.to_string())
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| warn!("Failed to trim stream {}: {}", key, e))
                    .unwrap_or(());
            }
        }

        Ok(())
    }
}
