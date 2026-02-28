use async_trait::async_trait;
use mocra::common::interface::{
    DataMiddleware, DataMiddlewareHandle, DataStoreMiddleware, DataStoreMiddlewareHandle,
};
use mocra::common::model::data::{Data, DataType};
use mocra::common::model::ModuleConfig;
use mocra::errors::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ObjectStoreMiddleware;

#[async_trait]
impl DataMiddleware for ObjectStoreMiddleware {
    fn name(&mut self) -> String {
        "object_store".to_string()
    }

    async fn handle_data(&mut self, data: Data, _config: &Option<ModuleConfig>) -> Option<Data> {
        Some(data)
    }

    fn default_arc() -> DataMiddlewareHandle
    where
        Self: Sized,
    {
        Arc::new(Mutex::new(Box::new(ObjectStoreMiddleware)))
    }
}

#[async_trait]
impl DataStoreMiddleware for ObjectStoreMiddleware {
    async fn store_data(&mut self, data: Data, _config: &Option<ModuleConfig>) -> Result<()> {
        if let DataType::File(file_data) = data.data {
            let path = std::path::Path::new(&file_data.file_path);
            if !path.exists() {
                tokio::fs::create_dir_all(path).await?;
            }
            let full_path = path.join(&file_data.file_name);
            tokio::fs::write(full_path, file_data.content).await?;
        }
        Ok(())
    }

    fn default_arc() -> DataStoreMiddlewareHandle
    where
        Self: Sized,
    {
        Arc::new(Mutex::new(Box::new(ObjectStoreMiddleware)))
    }
}
