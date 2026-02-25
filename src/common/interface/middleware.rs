use crate::common::model::data::Data;
use crate::common::model::{ModuleConfig, Request, Response};
use async_trait::async_trait;
use crate::errors::Result;
use std::sync::Arc;

#[async_trait]
/// Middleware for intercepting download requests and responses.
pub trait DownloadMiddleware: Send + Sync {
    /// Returns the unique name of the middleware instance.
    fn name(&self) -> String;
    
    /// Returns the execution priority weight. Higher weights may execute earlier/later depending on implementation.
    fn weight(&self) -> u32 {
        0
    }
    
    /// Hook executed before a request is sent.
    /// 
    /// Can be used to modify headers, URL, or validation.
    /// Returning `None` will skip this request.
    async fn before_request(&self, request: Request, _config: &Option<ModuleConfig>) -> Option<Request> {
        Some(request)
    }
    
    /// Hook executed after a response is received.
    /// 
    /// Can be used for validation, logging, or error handling.
    /// Returning `None` will skip subsequent response middleware and publishing.
    async fn after_response(&self, response: Response, _config: &Option<ModuleConfig>) -> Option<Response> {
        Some(response)
    }
    
    /// Returns a default shared instance of the middleware.
    fn default_arc() -> Arc<dyn DownloadMiddleware>
    where
        Self: Sized;
}

#[async_trait]
/// Middleware for processing extracted data.
pub trait DataMiddleware: Send + Sync {
    /// Returns the unique name of the middleware instance.
    fn name(&self) -> String;
    
    /// Returns the execution priority weight.
    fn weight(&self) -> u32 {
        0
    }
    
    /// Hook to process data items.
    /// 
    /// Can be used for cleaning, transformation, or enrichment.
    /// Returning `None` will skip subsequent data middleware and storage.
    async fn handle_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Option<Data> {
        Some(data)
    }
    
    /// Returns a default shared instance of the middleware.
    fn default_arc() -> Arc<dyn DataMiddleware>
    where
        Self: Sized;
}

#[async_trait]
/// Middleware for persisting data to external storage.
pub trait DataStoreMiddleware: DataMiddleware {
    /// Stores the data item.
    async fn store_data(&self, data: Data, config: &Option<ModuleConfig>) -> Result<()>;
    
    /// Returns a default shared instance of the middleware.
    fn default_arc() -> Arc<dyn DataStoreMiddleware>
    where
        Self: Sized;
}
