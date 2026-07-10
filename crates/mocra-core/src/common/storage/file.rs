use crate::common::interface::storage::BlobStorage;
use async_trait::async_trait;
use crate::errors::Result;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use log::debug;

pub struct FileBlobStorage {
    base_path: PathBuf,
}

impl FileBlobStorage {
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    async fn ensure_dir(&self, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
             if !parent.exists() {
                 fs::create_dir_all(parent).await.map_err(|e| {
                     crate::errors::Error::from(crate::errors::error::DataStoreError::SaveFailed(Box::new(e)))
                 })?;
             }
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStorage for FileBlobStorage {
    async fn put(&self, key: &str, data: &[u8]) -> Result<String> {
        let file_path = self.base_path.join(key);
        self.ensure_dir(&file_path).await?;
        
        let mut file = fs::File::create(&file_path).await.map_err(|e| {
            crate::errors::Error::from(crate::errors::error::DataStoreError::SaveFailed(Box::new(e)))
        })?;
        
        file.write_all(data).await.map_err(|e| {
            crate::errors::Error::from(crate::errors::error::DataStoreError::SaveFailed(Box::new(e)))
        })?;
        
        file.flush().await.map_err(|e| {
            crate::errors::Error::from(crate::errors::error::DataStoreError::SaveFailed(Box::new(e)))
        })?;

        debug!("Offloaded {} bytes to {}", data.len(), file_path.display());
        
        Ok(key.to_string())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let file_path = self.base_path.join(key);
        let data = fs::read(&file_path).await.map_err(|e| {
            crate::errors::Error::from(crate::errors::error::DataStoreError::InvalidData(Box::new(e)))
        })?;
        Ok(data)
    }
}
