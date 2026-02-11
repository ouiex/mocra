use async_trait::async_trait;
use crate::model::config::Config;
use super::ConfigProvider;
use tokio::sync::watch;
use std::path::PathBuf;
use std::time::Duration;
use log::{error, info};

/// 基于文件的配置提供者
///
/// 从本地文件系统加载配置，并支持文件变更监听（热重载）。
pub struct FileConfigProvider {
    path: PathBuf,
}

impl FileConfigProvider {
    /// 创建新的文件配置提供者
    ///
    /// # Arguments
    /// * `path` - 配置文件路径
    pub fn new(path: &str) -> Self {
        Self {
            path: PathBuf::from(path),
        }
    }
}

#[async_trait]
impl ConfigProvider for FileConfigProvider {
    async fn load_config(&self) -> Result<Config, String> {
        let path_str = self.path.to_str().ok_or("Invalid path")?;
        Config::load(path_str)
    }

    async fn watch(&self) -> Result<watch::Receiver<Config>, String> {
        let initial_config = self.load_config().await?;
        let (tx, rx) = watch::channel(initial_config);
        let path = self.path.clone();

        tokio::spawn(async move {
            let mut last_modified = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if let Ok(metadata) = std::fs::metadata(&path)
                    && let Ok(modified) = metadata.modified()
                        && last_modified != Some(modified) {
                            last_modified = Some(modified);
                            info!("Config file changed, reloading...");
                            match Config::load(path.to_str().unwrap_or_default()) {
                                Ok(config) => {
                                    if let Err(e) = tx.send(config) {
                                        error!("Failed to send config update: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => error!("Failed to reload config: {}", e),
                            }
                        }
            }
        });

        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_load_config_success() {
        // Create a temp file with valid config
        let mut file = NamedTempFile::new().unwrap();
        let config_content = r#"
            name = "test_app"
            
            [db]
            url = "postgres://user:password@localhost:5432/db"
            database_schema = "public"
            
            [download_config]
            downloader_expire = 3600
            timeout = 30
            rate_limit = 10.0
            enable_cache = true
            enable_locker = false
            enable_rate_limit = true
            cache_ttl = 600
            wss_timeout = 60
            
            [cache]
            ttl = 3600
            
            [crawler]
            request_max_retries = 3
            task_max_errors = 5
            module_max_errors = 10
            module_locker_ttl = 60
            
            [channel_config]
            minid_time = 0
            capacity = 1000
            compression_threshold = 1024
        "#;
        write!(file, "{}", config_content).unwrap();
        
        let provider = FileConfigProvider::new(file.path().to_str().unwrap());
        let config = provider.load_config().await;
        assert!(config.is_ok(), "Config load failed: {:?}", config.err());
    }

    #[tokio::test]
    async fn test_load_config_not_found() {
        let provider = FileConfigProvider::new("non_existent_file.toml");
        let result = provider.load_config().await;
        assert!(result.is_err());
    }
}
