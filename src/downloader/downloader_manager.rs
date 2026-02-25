use crate::downloader::request_downloader::RequestDownloader;
use crate::downloader::{Downloader, WebSocketDownloader};
use crate::common::model::Request;
use crate::common::model::download_config::DownloadConfig;
use crate::common::state::State;
use dashmap::DashMap;
use deadpool_redis::redis::Script;
use std::sync::Arc;
use std::time::SystemTime;

pub struct DownloaderManager {
    pub state: Arc<State>,
    pub default_downloader: Box<dyn Downloader>,
    // Registered downloader factories.
    pub downloader: Arc<DashMap<String, Box<dyn Downloader>>>,
    // Task downloader configuration.
    pub config: Arc<DashMap<String, DownloadConfig>>,
    // Task downloader instances.
    pub task_downloader: Arc<DashMap<String, Box<dyn Downloader>>>,
    pub wss_downloader: Arc<WebSocketDownloader>,
    // Records the last expiration update timestamp to reduce Redis write frequency.
    pub expire_update_cache: Arc<DashMap<String, u64>>,
}

impl DownloaderManager {
    /// Create a new DownloaderManager instance.
    ///
    /// # Arguments
    /// * `state` - Shared application state containing configuration, rate limiter, etc.
    pub async fn new(state: Arc<State>) -> Self {
        let (pool_size, max_response_size) = {
            let cfg = state.config.read().await;
            (
                cfg.download_config.pool_size.unwrap_or(200),
                cfg.download_config.max_response_size.unwrap_or(10 * 1024 * 1024),
            )
        };

        DownloaderManager {
            state: state.clone(),
            default_downloader: Box::new(RequestDownloader::new(
                Arc::clone(&state.limiter),
                Arc::clone(&state.locker),
                Arc::clone(&state.cache_service),
                pool_size,
                max_response_size,
            )),
            // Downloader factory list.
            downloader: Arc::new(DashMap::new()),
            // Task downloader configuration.
            config: Arc::new(DashMap::new()),
            // Task downloader instances.
            task_downloader: Arc::new(DashMap::new()),
            wss_downloader: Arc::new(WebSocketDownloader::new()),
            expire_update_cache: Arc::new(DashMap::new()),
        }
    }

    /// Register a custom downloader implementation.
    ///
    /// The downloader will be available to use by its `name()`.
    pub async fn register(&self, downloader: Box<dyn Downloader>) {
        self.downloader.insert(downloader.name(), downloader);
    }

    /// Set rate limit for a specific limit_id dynamically.
    ///
    /// This updates the rate limit configuration for an active downloader instance.
    pub async fn set_limit(&self, limit_id: &str, limit: f32) {
        if let Some(downloader) = self.task_downloader.get(limit_id) {
            downloader.set_limit(limit_id, limit).await;
        }
    }

    // Helper function to get the current timestamp.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // Cleans up expired downloaders.
    async fn cleanup_expired_downloader(&self) {
        let downloader_expire_time = self
            .state
            .config
            .read()
            .await
            .download_config
            .downloader_expire;

        let current_time = Self::current_timestamp();
        let max_score = current_time.saturating_sub(downloader_expire_time);

        let config_name = self.state.config.read().await.name.clone();
        let key = format!("{}:downloader_expire", config_name);

        // Lua script to get and remove expired keys
        let script = Script::new(
            r#"
            local key = KEYS[1]
            local max_score = ARGV[1]
            local expired = redis.call('ZRANGEBYSCORE', key, '-inf', max_score)
            if #expired > 0 then
                redis.call('ZREM', key, unpack(expired))
            end
            return expired
        "#,
        );

        let mut expired_keys: Vec<String> = Vec::new();

        // Execute Lua script
        let pool = self.state.locker.get_pool();
        if let Some(pool) = pool
            && let Ok(mut conn) = pool.get().await {
                let result: Result<Vec<String>, _> = script
                    .key(&key)
                    .arg(max_score)
                    .invoke_async(&mut conn)
                    .await;

                if let Ok(keys) = result {
                    expired_keys = keys;
                }
            }

        // Remove expired keys from local map
        for key in &expired_keys {
            self.task_downloader.remove(key);
        }

        // Check health status.
        let check_list: Vec<(String, Box<dyn Downloader>)> = self
            .task_downloader
            .iter()
            .map(|r| (r.key().clone(), dyn_clone::clone_box(r.value().as_ref())))
            .collect();

        for (key, downloader) in check_list {
            if downloader.health_check().await.is_err() {
                self.task_downloader.remove(&key);
                if let Some(pool) = pool
                    && let Ok(mut conn) = pool.get().await {
                        let _: () = deadpool_redis::redis::cmd("ZREM")
                            .arg(&key)
                            .arg(&key)
                            .query_async(&mut conn)
                            .await
                            .unwrap_or(());
                    }
            }
        }
    }

    /// Start the background cleanup task.
    /// Should be called once at startup.
    pub fn start_background_cleaner(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                self.cleanup_expired_downloader().await;
            }
        });
    }

    /// Get or create a downloader for the given request.
    ///
    /// This method manages the lifecycle of downloader instances for specific tasks (modules).
    /// It handles creation, configuration updates, and expiration of idle downloaders.
    ///
    /// # Arguments
    /// * `request` - The request containing module ID and limit ID.
    /// * `download_config` - Configuration for the downloader.
    pub async fn get_downloader(
        &self,
        request: &Request,
        download_config: DownloadConfig,
    ) -> Box<dyn Downloader> {
        let current_time = Self::current_timestamp();
        let module_id = request.module_id();

        // Check whether expiration time needs to be updated (once every 60 seconds).
        let should_update = if let Some(last_update) = self.expire_update_cache.get(&module_id) {
            current_time > *last_update + 60
        } else {
            true
        };

        if should_update {
            // Optimistically update cache to prevent spamming the spawn
            self.expire_update_cache
                .insert(module_id.clone(), current_time);

            // Update expiration time (Redis ZSET).
            let config_name = self.state.config.read().await.name.clone();
            let key = format!("{}:downloader_expire", config_name);
            let pool = self.state.locker.get_pool().map(|p| p.clone());
            let module_id_clone = module_id.clone();
            let current_time_clone = current_time;

            tokio::spawn(async move {
                if let Some(pool) = pool
                    && let Ok(mut conn) = pool.get().await {
                        let _: () = deadpool_redis::redis::cmd("ZADD")
                            .arg(&key)
                            .arg(current_time_clone)
                            .arg(&module_id_clone)
                            .query_async(&mut conn)
                            .await
                            .unwrap_or(());
                    }
            });
        }

        // Get or insert configuration.
        // Use entry API to only insert if needed, but we also want to return the config.
        // If we just use get() then insert(), we reduce lock contention on write?
        // DashMap entry() locks until released.
        // Let's use get first.
        let config = if let Some(existing) = self.config.get(&module_id) {
            let cached = existing.clone();
            if cached != download_config {
                self.config.insert(module_id.clone(), download_config.clone());
                download_config.clone()
            } else {
                cached
            }
        } else {
            self.config.insert(module_id.clone(), download_config.clone());
            download_config.clone()
        };

        // Determine effective limit_id here to ensure rate limiter gets correct key
        let limit_id = if request.limit_id.is_empty() {
             request.module_id()
        } else {
             request.limit_id.clone()
        };

        // Get or create downloader.
        if let Some(downloader) = self.task_downloader.get(&module_id) {
             // Check if we really need to update config.
             // We can optimistically skip calling set_config if we assume config in self.config hasn't changed 
             // and set_config was called when it was inserted.
             // However, `download_config` passed in `input` might be different from `self.config`?
             // The logic above `entry().or_insert(download_config)` implies we trust the cached config over the input one?
             // Or should we update `self.config` if input is different?
             // The original code was: .entry(..).or_insert(download_config).clone() -> favoring cached one.
             
             // If we favor the cached one, and we assume the downloader already has it set (from creation or previous update),
             // then we ONLY need to call set_config if the cached config changed (which it doesn't here).
             // BUT `set_config` also sets limit on `request.limit_id`. If `limit_id` varies per request for the same module,
             // we MUST call set_config (or set_limit) for that new limit_id.
             
              let d = dyn_clone::clone_box(downloader.value().as_ref());
              d.set_config(&limit_id, config).await;
              return d;
        }

        let new_downloader = {
            if let Some(registered) = self.downloader.get(&config.downloader) {
                dyn_clone::clone_box(registered.value().as_ref())
            } else {
                dyn_clone::clone_box(self.default_downloader.as_ref())
            }
        };

        // Ensure the configuration is up to date.
        new_downloader.set_config(&limit_id, config).await;
        self.task_downloader.insert(
            module_id.clone(),
            dyn_clone::clone_box(new_downloader.as_ref()),
        );
        new_downloader
    }
    /// Clear all configurations and downloaders.
    ///
    /// This removes all registered task downloaders and their configurations.
    pub async fn clear(&self) {
        self.config.clear();
        self.task_downloader.clear();
    }
}
