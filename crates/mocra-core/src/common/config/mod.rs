use async_trait::async_trait;
use crate::common::model::config::Config;
use tokio::sync::watch;

#[async_trait]
pub trait ConfigProvider: Send + Sync {
    /// Load the initial configuration
    async fn load_config(&self) -> Result<Config, String>;
    
    /// Watch for configuration changes. 
    /// Returns a watch::Receiver that yields the latest Config.
    async fn watch(&self) -> Result<watch::Receiver<Config>, String>;
}

pub mod file;
pub mod redis;
