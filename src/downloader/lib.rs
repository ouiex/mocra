pub mod request_downloader;
use crate::common::model::download_config::DownloadConfig;
use crate::common::model::{Request, Response};
use crate::errors::Result;
use semver::Version;
pub mod downloader_manager;
pub mod websocket_downloader;
pub use websocket_downloader::WebSocketDownloader;

pub use downloader_manager::DownloaderManager;
#[async_trait::async_trait]
pub trait Downloader: dyn_clone::DynClone + Send + Sync + 'static {
    /// Sets downloader configuration.
    async fn set_config(&self, id: &str, config: DownloadConfig);
    /// Sets rate limit.
    async fn set_limit(&self, id: &str, limit: f32);
    /// Downloader name.
    fn name(&self) -> String;

    /// Downloader version.
    fn version(&self) -> Version;

    /// Executes download task.
    async fn download(&self, request: Request) -> Result<Response>;

    /// Health check.
    async fn health_check(&self) -> Result<()>;
    async fn close(&self) -> Result<()> {
        // Default implementation; concrete implementations may override.
        Ok(())
    }
}

// Enables clone support for trait objects (based on implementor `Clone`) used by
// `Box<dyn Downloader>`.
dyn_clone::clone_trait_object!(Downloader);
