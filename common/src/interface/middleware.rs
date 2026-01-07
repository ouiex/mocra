use crate::model::data::Data;
use crate::model::{ModuleConfig, Request, Response};
use async_trait::async_trait;
use errors::Result;
use std::sync::Arc;

#[async_trait]
pub trait DownloadMiddleware: Send + Sync {
    fn name(&self) -> String;
    fn weight(&self) -> u32 {
        0
    }
    async fn handle_request(&self, request: Request, config: &Option<ModuleConfig>) -> Request;
    async fn handle_response(&self, response: Response, config: &Option<ModuleConfig>) -> Response;
    fn default_arc() -> Arc<dyn DownloadMiddleware>
    where
        Self: Sized;
}
#[async_trait]
pub trait DataMiddleware: Send + Sync {
    fn name(&self) -> String;
    fn weight(&self) -> u32 {
        0
    }
    async fn handle_data(&self, data: Data, config: &Option<ModuleConfig>) -> Data;
    fn default_arc() -> Arc<dyn DataMiddleware>
    where
        Self: Sized;
}

#[async_trait]
pub trait DataStoreMiddleware: DataMiddleware {
    async fn store_data(&self, data: Data, config: &Option<ModuleConfig>) -> Result<()>;
    fn default_arc() -> Arc<dyn DataStoreMiddleware>
    where
        Self: Sized;
}
