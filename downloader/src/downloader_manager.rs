use crate::request_downloader::RequestDownloader;
use crate::{Downloader, WebSocketDownloader};
use common::model::Request;
use common::model::download_config::DownloadConfig;
use common::state::State;
use dashmap::DashMap;
use deadpool_redis::{redis::{AsyncCommands, Script}};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

pub struct DownloaderManager {
    pub state: Arc<State>,
    pub default_downloader: Box<dyn Downloader>,
    // 注册的下载器工厂
    pub downloader: Arc<DashMap<String, Box<dyn Downloader>>>,
    // task下载器配置
    pub config: Arc<DashMap<String, DownloadConfig>>,
    // task下载器实例
    pub task_downloader: Arc<DashMap<String, Box<dyn Downloader>>>,
    pub wss_downloader: Arc<WebSocketDownloader>,
    // 记录上次更新过期时间的时间戳，减少Redis写入频率
    pub expire_update_cache: Arc<DashMap<String, u64>>,
}

impl DownloaderManager {
    pub fn new(state: Arc<State>) -> Self {
        DownloaderManager {
            state: state.clone(),
            default_downloader: Box::new(RequestDownloader::new(
                Arc::clone(&state.limiter),
                Arc::clone(&state.locker),
                Arc::clone(&state.cache_service),
            )),
            //下载器工厂列表
            downloader: Arc::new(DashMap::new()),
            //task下载器配置
            config: Arc::new(DashMap::new()),
            //task下载器实例
            task_downloader: Arc::new(DashMap::new()),
            wss_downloader: Arc::new(WebSocketDownloader::new()),
            expire_update_cache: Arc::new(DashMap::new()),
        }
    }

    /// 注册下载器工厂
    pub async fn register(&self, downloader: Box<dyn Downloader>) {
        self.downloader.insert(downloader.name(), downloader);
    }

    pub async fn set_limit(&self, limit_id: &str, limit: f32) {
        if let Some(downloader) = self.task_downloader.get(limit_id) {
            downloader.set_limit(limit_id, limit).await;
        }
    }

    // 获取当前时间戳的辅助函数
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // 清理过期的下载器
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
        let mut pool = self.state.locker.get_pool();
        if let Some(pool) = pool
            && let Ok(mut conn) = pool.get().await
        {
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

        // 检查健康状态
        let check_list: Vec<(String, Box<dyn Downloader>)> = self
            .task_downloader
            .iter()
            .map(|r| (r.key().clone(), dyn_clone::clone_box(r.value().as_ref())))
            .collect();

        for (key, downloader) in check_list {
            if downloader.health_check().await.is_err() {
                self.task_downloader.remove(&key);
                if let Some(pool) = pool
                    && let Ok(mut conn) = pool.get().await
                {
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

    pub async fn get_downloader(
        &self,
        request: &Request,
        download_config: DownloadConfig,
    ) -> Box<dyn Downloader> {
        // 清理过期的下载器
        self.cleanup_expired_downloader().await;

        let current_time = Self::current_timestamp();
        let module_id = request.module_id();

        // 检查是否需要更新过期时间（每60秒更新一次）
        let should_update = if let Some(last_update) = self.expire_update_cache.get(&module_id) {
            current_time > *last_update + 60
        } else {
            true
        };

        if should_update {
            // 更新过期时间 (Redis ZSET)
            let config_name = self.state.config.read().await.name.clone();
            let key = format!("{}:downloader_expire", config_name);
            let pool = self.state.locker.get_pool();

            if let Some(pool) = pool
                && let Ok(mut conn) = pool.get().await
            {
                let _: () = deadpool_redis::redis::cmd("ZADD")
                    .arg(&key)
                    .arg(current_time)
                    .arg(&module_id)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(());

                self.expire_update_cache
                    .insert(module_id.clone(), current_time);
            }
        }

        // 获取或插入配置
        let config = self
            .config
            .entry(module_id.clone())
            .or_insert(download_config)
            .clone();

        // 获取或创建下载器
        if let Some(downloader) = self.task_downloader.get(&module_id) {
            let d = dyn_clone::clone_box(downloader.value().as_ref());
            d.set_config(&request.limit_id, config).await;
            return d;
        }

        let new_downloader = {
            if let Some(registered) = self.downloader.get(&config.downloader) {
                dyn_clone::clone_box(registered.value().as_ref())
            } else {
                dyn_clone::clone_box(self.default_downloader.as_ref())
            }
        };

        self.task_downloader.insert(
            module_id.clone(),
            dyn_clone::clone_box(new_downloader.as_ref()),
        );

        // 确保配置是最新的
        new_downloader.set_config(&request.limit_id, config).await;
        new_downloader
    }
    // pub async fn get_downloader_with_config(
    //     &self,
    //     id: &str,
    //     config: DownloadConfig,
    // ) -> Box<RequestDownloader> {
    //
    //     self.get_downloader(id, config).await
    // }
    pub async fn clear(&self) {
        self.config.clear();
        self.task_downloader.clear();
    }
}
