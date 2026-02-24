#![allow(unused)]
use errors::Result;
use crate::interface::middleware::DataStoreMiddleware;
use crate::interface::middleware::{DataMiddleware, DownloadMiddleware};
use crate::model::data::Data;
use crate::state::State;
use crate::model::ModuleConfig;
use crate::model::{Request, Response};
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
    pub download_middleware: Arc<RwLock<Vec<Arc<dyn DownloadMiddleware>>>>,
    pub data_middleware: Arc<RwLock<Vec<Arc<dyn DataMiddleware>>>>,
    pub store_middleware: Arc<RwLock<Vec<Arc<dyn DataStoreMiddleware>>>>,
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
    pub async fn register_download_middleware(&self, middleware: Arc<dyn DownloadMiddleware>) {
        let mut middlewares = self.download_middleware.write().await;
        middlewares.push(middleware);
        middlewares.sort_by_key(|a| a.weight());
    }

    pub async fn register_download_middleware_from_vec(
        &self,
        middlewares: Vec<Arc<dyn DownloadMiddleware>>,
    ) {
        let mut existing_middlewares = self.download_middleware.write().await;
        for middleware in middlewares {
            existing_middlewares.push(middleware);
        }
        existing_middlewares.sort_by_key(|a| a.weight());
    }

    pub async fn register_data_middleware(&self, middleware: Arc<dyn DataMiddleware>) {
        let mut middlewares = self.data_middleware.write().await;
        middlewares.push(middleware);
        middlewares.sort_by_key(|a| a.weight());
    }
    pub async fn register_data_middleware_from_vec(
        &self,
        middlewares: Vec<Arc<dyn DataMiddleware>>,
    ) {
        let mut existing_middlewares = self.data_middleware.write().await;
        for middleware in middlewares {
            existing_middlewares.push(middleware);
        }
        existing_middlewares.sort_by_key(|a| a.weight());
    }

    pub async fn register_store_middleware(&self, middleware: Arc<dyn DataStoreMiddleware>) {
        let mut middlewares = self.store_middleware.write().await;
        middlewares.push(middleware);
        middlewares.sort_by_key(|a| a.weight());
    }
    pub async fn register_store_middleware_from_vec(
        &self,
        middlewares: Vec<Arc<dyn DataStoreMiddleware>>,
    ) {
        let mut existing_middlewares = self.store_middleware.write().await;
        for middleware in middlewares {
            existing_middlewares.push(middleware);
        }
        existing_middlewares.sort_by_key(|a| a.weight());
    }

    async fn get_download_middleware(
        &self,
        middleware_name: &[String],
        config: &Option<ModuleConfig>,
    ) -> Vec<(Arc<dyn DownloadMiddleware>, u32)> {
        self.download_middleware
            .read()
            .await
            .iter()
            .filter(|x| middleware_name.contains(&x.name()))
            .map(|x| {
                (
                    x.clone(),
                    config
                        .as_ref()
                        .map(|m| m.get_middleware_weight(&x.name()).unwrap_or(x.weight()))
                        .unwrap_or(x.weight()),
                )
            })
            .collect()
    }
    async fn get_data_middleware(
        &self,
        middleware_name: &[String],
        config: &Option<ModuleConfig>,
    ) -> Vec<(Arc<dyn DataMiddleware>, u32)> {
        self.data_middleware
            .read()
            .await
            .iter()
            .filter(|x| middleware_name.contains(&x.name()))
            .map(|x| {
                (
                    x.clone(),
                    config
                        .as_ref()
                        .map(|m| m.get_middleware_weight(&x.name()).unwrap_or(x.weight()))
                        .unwrap_or(x.weight()),
                )
            })
            .collect()
    }

    async fn get_store_middleware(
        &self,
        middleware_name: &[String],
    ) -> Vec<Arc<dyn DataStoreMiddleware>> {
        self.store_middleware
            .read()
            .await
            .iter()
            .filter(|x| middleware_name.contains(&x.name())).cloned()
            .collect()
    }

    pub async fn handle_request(&self, request: Request, config: &Option<ModuleConfig>) -> Option<Request> {
        let mut req = request;
        let mut middleware: Vec<(Arc<dyn DownloadMiddleware>, u32)> = self
            .get_download_middleware(&req.download_middleware, config)
            .await;
        middleware.sort_by(|x, y| x.1.cmp(&y.1));
        for (middleware, _) in middleware {
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
        let mut middleware: Vec<(Arc<dyn DownloadMiddleware>, u32)> = self
            .get_download_middleware(&resp.download_middleware, config)
            .await;
        middleware.sort_by(|x, y| y.1.cmp(&x.1));
        for (middleware, _) in middleware {
            if resp.download_middleware.contains(&middleware.name()) {
                match middleware.after_response(resp, config).await {
                    Some(next_resp) => resp = next_resp,
                    None => return None,
                }
            }
        }
        Some(resp)
    }
    pub async fn handle_data(&self, data: Data, config: &Option<ModuleConfig>) -> Option<Data> {
        let mut data = data;
        let mut middleware: Vec<(Arc<dyn DataMiddleware>, u32)> = self
            .get_data_middleware(&data.data_middleware, config)
            .await;
        middleware.sort_by(|x, y| x.1.cmp(&y.1));
        for (middleware, _) in middleware {
            if data.data_middleware.contains(&middleware.name()) {
                match middleware.handle_data(data, config).await {
                    Some(next_data) => data = next_data,
                    None => return None,
                }
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
        let tasks = middleware.iter().map(|m| {
            let name = format!("{}, schema: {}, table: {}", m.name(), data.module,m.name());
            let data_cloned = data.clone();
            async move { (name, m.store_data(data_cloned, config).await) }
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
        let tasks = middleware.iter().map(|m| {
            let name = m.name();
            let data_cloned = data.clone();
            async move { (name, m.store_data(data_cloned, config).await) }
        });

        let mut results: Vec<(String, Result<()>)> = join_all(tasks).await;
        results
            .into_iter()
            .filter(|x| x.1.is_err())
            .collect()
    }
}
