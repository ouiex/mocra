use std::sync::Arc;
use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use common::interface::DownloadMiddleware;
use common::model::{Request, Response, ModuleConfig};
pub struct BinaryStoreMiddleware;
#[async_trait]
impl DownloadMiddleware for BinaryStoreMiddleware {
    fn name(&self) -> String {
        "binary_store".to_string()
    }

    async fn handle_request(&self, request: Request, _config: &Option<ModuleConfig>) -> Request {
        request
    }

    async fn handle_response(&self, response: Response, _config: &Option<ModuleConfig>) -> Response {
        let out_dir = std::path::Path::new("download");
        if !out_dir.exists() {
            std::fs::create_dir_all(out_dir).unwrap();
        }
        let mut file = File::create(format!("download/{}-{}.bin", response.module_id(),response.id)).await.unwrap();
        file.write_all(serde_json::to_string_pretty(&response).unwrap().as_ref()).await.unwrap();
        file.flush().await.unwrap();

        response
    }

    fn default_arc() -> Arc<dyn DownloadMiddleware>
    where
        Self: Sized
    {
        Arc::new(BinaryStoreMiddleware)
    }
}