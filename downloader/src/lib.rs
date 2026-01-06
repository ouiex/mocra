#![allow(unused)]
pub mod request_downloader;
use common::model::download_config::DownloadConfig;
use common::model::{Request, Response};
use errors::{Error, Result};
use semver::Version;
pub mod downloader_manager;
mod remote_downloader;
pub mod websocket_downloader;
pub use websocket_downloader::WebSocketDownloader;

pub use downloader_manager::DownloaderManager;
#[async_trait::async_trait]
pub trait Downloader: dyn_clone::DynClone + Send + Sync + 'static {
    async fn set_config(&self, id: &str, config: DownloadConfig);
    async fn set_limit(&self, id: &str, limit: f32);
    /// 下载器的名称
    fn name(&self) -> String;

    /// 下载器的版本
    fn version(&self) -> Version;

    /// 执行下载任务
    async fn download(&self, request: Request) -> Result<Response>;

    /// 健康检查
    async fn health_check(&self) -> Result<()>;
    async fn close(&self) -> Result<()> {
        // 默认实现，子类可以覆盖
        Ok(())
    }
}

// 为 trait 对象启用克隆能力（基于实现类型的 Clone），用于 Box<dyn Downloader> 的 clone
dyn_clone::clone_trait_object!(Downloader);
