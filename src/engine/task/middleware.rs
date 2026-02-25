#![allow(unused)]


use std::sync::Arc;
use crate::common::interface::DownloadMiddleware;
use crate::common::model::ModuleConfig;

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
