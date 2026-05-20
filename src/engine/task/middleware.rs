#![allow(unused)]

use crate::common::interface::DownloadMiddlewareHandle;
use crate::common::model::ModuleConfig;
use std::sync::Arc;

/// Download middleware wrapper with bound config.
pub struct DownloaderMiddleware {
    /// Middleware name.
    pub name: String,
    /// Module configuration snapshot.
    pub config: ModuleConfig,
    /// Middleware implementation.
    pub work: DownloadMiddlewareHandle,
}

/// Data middleware wrapper.
pub struct DataMiddleware {
    /// Middleware name.
    pub name: String,
    /// Module configuration snapshot.
    pub config: ModuleConfig,
    /// Middleware implementation.
    pub work: DownloadMiddlewareHandle,
}
