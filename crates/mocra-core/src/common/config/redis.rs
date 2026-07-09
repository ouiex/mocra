use async_trait::async_trait;
use crate::common::model::config::Config;
use super::ConfigProvider;
use tokio::sync::watch;
use redis::AsyncCommands;
use log::{error, info};
use std::time::Duration;

/// Redis-backed configuration provider.
///
/// Loads configuration JSON from a Redis key and supports change watching via polling.
pub struct RedisConfigProvider {
    client: redis::Client,
    key: String,
    // channel: String, // Not used for polling
}

impl RedisConfigProvider {
    /// Creates a new Redis config provider.
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection string.
    /// * `key` - Key where config is stored.
    /// * `_channel` - (unused) Pub/Sub channel.
    pub fn new(redis_url: &str, key: &str, _channel: &str) -> Result<Self, String> {
        let client = redis::Client::open(redis_url).map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            key: key.to_string(),
        })
    }
    
    /// Fetches config from Redis.
    async fn fetch_config(&self) -> Result<Config, String> {
        let mut con = self.client.get_multiplexed_async_connection().await.map_err(|e| e.to_string())?;
        let config_str: String = con.get(&self.key).await.map_err(|e| e.to_string())?;
        serde_json::from_str(&config_str).map_err(|e| e.to_string())
    }
}

#[async_trait]
impl ConfigProvider for RedisConfigProvider {
    async fn load_config(&self) -> Result<Config, String> {
        self.fetch_config().await
    }

    async fn watch(&self) -> Result<watch::Receiver<Config>, String> {
        let initial_config = self.load_config().await?;
        let (tx, rx) = watch::channel(initial_config);
        
        let client = self.client.clone();
        let key = self.key.clone();

        tokio::spawn(async move {
            let mut last_config_str = String::new();
            
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                
                let mut con = match client.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                         error!("Failed to get redis connection: {}", e);
                         continue;
                    }
                };

                let config_str: String = match con.get(&key).await {
                    Ok(s) => s,
                    Err(e) => {
                         error!("Failed to fetch config from Redis: {}", e);
                         continue;
                    }
                };

                if config_str != last_config_str {
                    match serde_json::from_str::<Config>(&config_str) {
                        Ok(config) => {
                             info!("Config changed in Redis, reloading...");
                             if let Err(e) = tx.send(config) {
                                 error!("Failed to send config update: {}", e);
                                 break;
                             }
                             last_config_str = config_str;
                        },
                        Err(e) => error!("Failed to parse config: {}", e),
                    }
                }
            }
        });

        Ok(rx)
    }
}
