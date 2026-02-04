use async_trait::async_trait;
use errors::Result;
use std::sync::Arc;

#[async_trait]
pub trait BlobStorage: Send + Sync {
    /// Upload data to storage, returns the access path/key/url
    async fn put(&self, key: &str, data: &[u8]) -> Result<String>;
    /// Download data from storage
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
}

#[async_trait]
pub trait Offloadable {
    fn should_offload(&self, threshold: usize) -> bool;
    async fn offload(&mut self, storage: &Arc<dyn BlobStorage>) -> Result<()>;
    async fn reload(&mut self, storage: &Arc<dyn BlobStorage>) -> Result<()>;
}

pub struct FileSystemBlobStorage {
    root_path: std::path::PathBuf,
}

impl FileSystemBlobStorage {
    pub fn new(root_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            root_path: root_path.into(),
        }
    }
}

#[async_trait]
impl BlobStorage for FileSystemBlobStorage {
    async fn put(&self, key: &str, data: &[u8]) -> Result<String> {
        let path = self.root_path.join(key);
        if let Some(parent) = path.parent() {
             tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(path.to_string_lossy().to_string())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let path = std::path::PathBuf::from(key);
        let data = tokio::fs::read(path).await?;
        Ok(data)
    }
}
