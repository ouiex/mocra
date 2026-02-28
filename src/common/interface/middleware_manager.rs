#![allow(unused)]
use crate::errors::Result;
use crate::common::interface::middleware::{
    DataMiddlewareHandle, DataStoreMiddlewareHandle, DownloadMiddlewareHandle,
};
use crate::common::model::data::Data;
use crate::common::state::State;
use crate::common::model::ModuleConfig;
use crate::common::model::{Request, Response};
use futures::future::join_all;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Manages download/data/store middlewares.
///
/// All middlewares should be registered before scheduler startup.
/// Dynamic registration at runtime is not supported.
/// Use `register_download_middleware` / `register_data_middleware` for single registration,
/// and `register_*_from_vec` for batch registration.
///
/// Weight resolution:
/// - Effective weight comes from `ModuleConfig` override when available,
///   otherwise from middleware's own `weight()`.
/// - `ModuleConfig` overrides have higher priority than middleware defaults.
/// - For download middleware, overrides come from
///   `download_middleware_config` and `rel_module_download_middleware_config`.
/// - Lower weight executes earlier.
///
pub struct MiddlewareManager {
    pub download_middleware: Arc<RwLock<Vec<DownloadMiddlewareHandle>>>,
    pub data_middleware: Arc<RwLock<Vec<DataMiddlewareHandle>>>,
    pub store_middleware: Arc<RwLock<Vec<DataStoreMiddlewareHandle>>>,
    pub state: Arc<State>,
}
impl MiddlewareManager {
    pub fn new(state: Arc<State>) -> Self {
        MiddlewareManager {
            download_middleware: Default::default(),
            data_middleware: Default::default(),
            store_middleware: Default::default(),
            state,
        }
    }
    pub async fn register_download_middleware(&self, middleware: DownloadMiddlewareHandle) {
        let mut middlewares = self.download_middleware.write().await;
        middlewares.push(middleware);
    }

    pub async fn register_download_middleware_from_vec(
        &self,
        middlewares: Vec<DownloadMiddlewareHandle>,
    ) {
        let mut existing_middlewares = self.download_middleware.write().await;
        for middleware in middlewares {
            existing_middlewares.push(middleware);
        }
    }

    pub async fn register_data_middleware(&self, middleware: DataMiddlewareHandle) {
        let mut middlewares = self.data_middleware.write().await;
        middlewares.push(middleware);
    }
    pub async fn register_data_middleware_from_vec(
        &self,
        middlewares: Vec<DataMiddlewareHandle>,
    ) {
        let mut existing_middlewares = self.data_middleware.write().await;
        for middleware in middlewares {
            existing_middlewares.push(middleware);
        }
    }

    pub async fn register_store_middleware(&self, middleware: DataStoreMiddlewareHandle) {
        let mut middlewares = self.store_middleware.write().await;
        middlewares.push(middleware);
    }
    pub async fn register_store_middleware_from_vec(
        &self,
        middlewares: Vec<DataStoreMiddlewareHandle>,
    ) {
        let mut existing_middlewares = self.store_middleware.write().await;
        for middleware in middlewares {
            existing_middlewares.push(middleware);
        }
    }

    async fn get_download_middleware(
        &self,
        middleware_name: &[String],
        config: &Option<ModuleConfig>,
    ) -> Vec<(DownloadMiddlewareHandle, u32)> {
        let middlewares = self.download_middleware.read().await.clone();
        let mut out = Vec::new();
        for middleware in middlewares {
            let mut middleware_guard = middleware.lock().await;
            let middleware_actual_name = middleware_guard.name();
            if middleware_name.contains(&middleware_actual_name) {
                let middleware_weight = config
                    .as_ref()
                    .and_then(|m| m.get_middleware_weight(&middleware_actual_name))
                    .unwrap_or_else(|| middleware_guard.weight());
                out.push((middleware.clone(), middleware_weight));
            }
        }
        out
    }
    async fn get_data_middleware(
        &self,
        middleware_name: &[String],
        config: &Option<ModuleConfig>,
    ) -> Vec<(DataMiddlewareHandle, u32)> {
        let middlewares = self.data_middleware.read().await.clone();
        let mut out = Vec::new();
        for middleware in middlewares {
            let mut middleware_guard = middleware.lock().await;
            let middleware_actual_name = middleware_guard.name();
            if middleware_name.contains(&middleware_actual_name) {
                let middleware_weight = config
                    .as_ref()
                    .and_then(|m| m.get_middleware_weight(&middleware_actual_name))
                    .unwrap_or_else(|| middleware_guard.weight());
                out.push((middleware.clone(), middleware_weight));
            }
        }
        out
    }

    async fn get_store_middleware(
        &self,
        middleware_name: &[String],
    ) -> Vec<DataStoreMiddlewareHandle> {
        let middlewares = self.store_middleware.read().await.clone();
        let mut out = Vec::new();
        for middleware in middlewares {
            let mut middleware_guard = middleware.lock().await;
            let middleware_actual_name = middleware_guard.name();
            if middleware_name.contains(&middleware_actual_name) {
                out.push(middleware.clone());
            }
        }
        out
    }

    pub async fn handle_request(&self, request: Request, config: &Option<ModuleConfig>) -> Option<Request> {
        let mut req = request;
        let mut middleware: Vec<(DownloadMiddlewareHandle, u32)> = self
            .get_download_middleware(&req.download_middleware, config)
            .await;
        middleware.sort_by(|x, y| x.1.cmp(&y.1));
        for (middleware, _) in middleware {
            let mut middleware = middleware.lock().await;
            match middleware.before_request(req, config).await {
                Some(next_req) => req = next_req,
                None => return None,
            }
        }
        Some(req)
    }
    pub async fn handle_response(
        &self,
        response: Response,
        config: &Option<ModuleConfig>,
    ) -> Option<Response> {
        let mut resp = response;
        let mut middleware: Vec<(DownloadMiddlewareHandle, u32)> = self
            .get_download_middleware(&resp.download_middleware, config)
            .await;
        middleware.sort_by(|x, y| y.1.cmp(&x.1));
        for (middleware, _) in middleware {
            let mut middleware = middleware.lock().await;
            match middleware.after_response(resp, config).await {
                Some(next_resp) => resp = next_resp,
                None => return None,
            }
        }
        Some(resp)
    }
    pub async fn handle_data(&self, data: Data, config: &Option<ModuleConfig>) -> Option<Data> {
        let mut data = data;
        let mut middleware: Vec<(DataMiddlewareHandle, u32)> = self
            .get_data_middleware(&data.data_middleware, config)
            .await;
        middleware.sort_by(|x, y| x.1.cmp(&y.1));
        for (middleware, _) in middleware {
            let mut middleware = middleware.lock().await;
            match middleware.handle_data(data, config).await {
                Some(next_data) => data = next_data,
                None => return None,
            }
        }
        Some(data)
    }
    /// Returns a map of storage results (`middleware_name -> result`), filtered to errors only.
    pub async fn handle_store_data(
        &self,
        data: Data,
        config: &Option<ModuleConfig>,
    ) -> HashMap<String, Result<()>> {
        let middleware = self.get_store_middleware(&data.data_middleware).await;

        // Run all store operations concurrently and collect (name, result)
        let tasks = middleware.into_iter().map(|m| {
            let data_cloned = data.clone();
            let module_name = data.module.clone();
            async move {
                let mut middleware = m.lock().await;
                let middleware_name = middleware.name();
                let name = format!(
                    "{}, schema: {}, table: {}",
                    middleware_name, module_name, middleware_name
                );
                let result = match middleware.before_store(config).await {
                    Ok(()) => match middleware.store_data(data_cloned, config).await {
                        Ok(()) => middleware.after_store(config).await,
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                };
                (name, result)
            }
        });

        let mut results: Vec<(String, Result<()>)> = join_all(tasks).await;
        results
            .into_iter()
            .filter(|x| x.1.is_err())
            .collect()
    }
    pub async fn handle_store_data_with_middleware(
        &self,
        data: Data,
        middleware: Vec<String>,
        config: &Option<ModuleConfig>,
    ) -> HashMap<String, Result<()>> {
        let middleware = self.get_store_middleware(&middleware).await;

        // Run all store operations concurrently and collect (name, result)
        let tasks = middleware.into_iter().map(|m| {
            let data_cloned = data.clone();
            async move {
                let mut middleware = m.lock().await;
                let name = middleware.name();
                let result = match middleware.before_store(config).await {
                    Ok(()) => match middleware.store_data(data_cloned, config).await {
                        Ok(()) => middleware.after_store(config).await,
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                };
                (name, result)
            }
        });

        let mut results: Vec<(String, Result<()>)> = join_all(tasks).await;
        results
            .into_iter()
            .filter(|x| x.1.is_err())
            .collect()
    }
}
