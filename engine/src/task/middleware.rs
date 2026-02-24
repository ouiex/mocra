#![allow(unused)]


use std::sync::Arc;
use common::interface::DownloadMiddleware;
use common::model::ModuleConfig;

/// Download middleware wrapper with bound config.
pub struct DownloaderMiddleware {
    /// Middleware name.
    pub name: String,
    /// Module configuration snapshot.
    pub config: ModuleConfig,
    /// Middleware implementation.
    pub work: Arc<dyn DownloadMiddleware>,
}

/// Data middleware wrapper.
pub struct DataMiddleware {
    /// Middleware name.
    pub name: String,
    /// Module configuration snapshot.
    pub config: ModuleConfig,
    /// Middleware implementation.
    pub work: Arc<dyn DownloadMiddleware>,
}
