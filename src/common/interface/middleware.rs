use crate::common::model::data::Data;
use crate::common::model::{ModuleConfig, Request, Response};
use async_trait::async_trait;
use crate::errors::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type DownloadMiddlewareHandle = Arc<Mutex<Box<dyn DownloadMiddleware>>>;
pub type DataMiddlewareHandle = Arc<Mutex<Box<dyn DataMiddleware>>>;
pub type DataStoreMiddlewareHandle = Arc<Mutex<Box<dyn DataStoreMiddleware>>>;

#[async_trait]
/// Middleware for intercepting download requests and responses.
pub trait DownloadMiddleware: Send + Sync {
    /// Returns the unique name of the middleware instance.
    fn name(&mut self) -> String;
    
    /// Returns the execution priority weight. Higher weights may execute earlier/later depending on implementation.
    fn weight(&mut self) -> u32 {
        0
    }
    
    /// Hook executed before a request is sent.
    /// 
    /// Can be used to modify headers, URL, or validation.
    /// Returning `None` will skip this request.
    async fn before_request(&mut self, request: Request, _config: &Option<ModuleConfig>) -> Option<Request> {
        Some(request)
    }
    
    /// Hook executed after a response is received.
    /// 
    /// Can be used for validation, logging, or error handling.
    /// Returning `None` will skip subsequent response middleware and publishing.
    async fn after_response(&mut self, response: Response, _config: &Option<ModuleConfig>) -> Option<Response> {
        Some(response)
    }
    
    /// Returns a default shared instance of the middleware.
    fn default_arc() -> DownloadMiddlewareHandle
    where
        Self: Sized;
}

#[async_trait]
/// Middleware for processing extracted data.
pub trait DataMiddleware: Send + Sync {
    /// Returns the unique name of the middleware instance.
    fn name(&mut self) -> String;
    
    /// Returns the execution priority weight.
    fn weight(&mut self) -> u32 {
        0
    }
    
    /// Hook to process data items.
    /// 
    /// Can be used for cleaning, transformation, or enrichment.
    /// Returning `None` will skip subsequent data middleware and storage.
    async fn handle_data(&mut self, data: Data, _config: &Option<ModuleConfig>) -> Option<Data> {
        Some(data)
    }
    
    /// Returns a default shared instance of the middleware.
    fn default_arc() -> DataMiddlewareHandle
    where
        Self: Sized;
}

#[async_trait]
/// Middleware for persisting data to external storage.
pub trait DataStoreMiddleware: DataMiddleware {
    /// Hook executed before persisting data.
    async fn before_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }

    /// Stores the data item.
    async fn store_data(&mut self, data: Data, config: &Option<ModuleConfig>) -> Result<()>;

    /// Hook executed after persisting data.
    async fn after_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }
    
    /// Returns a default shared instance of the middleware.
    fn default_arc() -> DataStoreMiddlewareHandle
    where
        Self: Sized;
}
