use async_trait::async_trait;
use common::interface::{DataMiddleware, DataStoreMiddleware};
use common::model::data::{Data, DataType};
use common::model::ModuleConfig;
use errors::Result;
use std::sync::Arc;

pub struct ObjectStoreMiddleware;

#[async_trait]
impl DataMiddleware for ObjectStoreMiddleware {
    fn name(&self) -> String {
        "object_store".to_string()
    }

    async fn handle_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Option<Data> {
        Some(data)
    }

    fn default_arc() -> Arc<dyn DataMiddleware>
    where
        Self: Sized,
    {
        Arc::new(ObjectStoreMiddleware)
    }
}

#[async_trait]
impl DataStoreMiddleware for ObjectStoreMiddleware {
    async fn store_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Result<()> {
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

    fn default_arc() -> Arc<dyn DataStoreMiddleware>
    where
        Self: Sized,
    {
        Arc::new(ObjectStoreMiddleware)
    }
}
