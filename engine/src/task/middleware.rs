#![allow(unused)]


use std::sync::Arc;
use common::interface::DownloadMiddleware;
use common::model::ModuleConfig;

/// 下载中间件包装器
///
/// 包含中间件名称、配置以及具体的 `DownloadMiddleware` 实现。
pub struct DownloaderMiddleware {
    /// 中间件名称
    pub name: String,
    /// 模块配置
    pub config: ModuleConfig,
    /// 中间件实现
    pub work: Arc<dyn DownloadMiddleware>,
}

/// 数据中间件包装器
///
/// 包含中间件名称、配置以及具体的 `DataMiddleware` 实现。
pub struct DataMiddleware {
    /// 中间件名称
    pub name: String,
    /// 模块配置
    pub config: ModuleConfig,
    /// 中间件实现
    pub work: Arc<dyn DownloadMiddleware>,
}
