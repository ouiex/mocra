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

/// MiddlewareManager 用于管理下载和数据中间件
/// 在schedule启动前需要将所有的中间件注册到此管理器中
/// 不支持动态注册中间件，所有中间件在启动时就需要注册完成
/// 通过 `register_download_middleware` 和 `register_data_middleware` 方法注册中
/// 通过 `register_download_middleware_from_vec` 和 `register_data_middleware_from_vec` 方法批量注册中间件
/// 目前只用于处理下载请求和响应的中间件，暂不处理数据处理的中间件
/// 数据中间件需要在解析器模块中处理
/// 中间件的权重来自于 `ModuleConfig` 中的配置或 `DownloadMiddleware.weight` 和 `DataMiddleware.weight` 方法
/// `ModuleConfig` 中的配置优先级高于中间件自身的权重
/// `ModuleConfig` 中的权重来源于 `download_middleware_config` 和 `rel_module_download_middleware_config` 这两个配置里
/// 其他配置中的weight字段不会影响中间件的权重
/// 数据库表中的weight字段暂时不参与权重，后续可以删除该字段
/// 中间件的执行顺序由权重决定，权重越小优先
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
    /// 返回存储结果的map，key为中间件名称，value为存储结果，只返回错误的结果
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
