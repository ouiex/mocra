use crate::common::model::Cookies;
use crate::common::model::ExecutionMark;
use crate::common::model::meta::MetaData;
use crate::cacheable::CacheAble;
use uuid::Uuid;

use serde::{Deserialize, Serialize};
#[derive(
    Debug, Clone, Serialize, Deserialize
)]
pub struct Response {
    pub id: Uuid,
    pub platform: String,
    pub account: String,
    pub module: String,
    pub status_code: u16,
    pub cookies: Cookies,
    pub content: Vec<u8>,
    pub storage_path: Option<String>,
    pub headers: Vec<(String, String)>,
    pub task_retry_times: usize,
    pub metadata: MetaData,
    pub download_middleware: Vec<String>,
    pub data_middleware: Vec<String>,
    pub task_finished: bool,
    pub context: ExecutionMark,
    pub run_id: Uuid,
    pub prefix_request: Uuid,
    pub request_hash: Option<String>,
    #[serde(default)]
    pub priority: crate::common::model::Priority,
}
impl Response {
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.account, self.platform)
    }
    pub fn module_id(&self) -> String {
        format!("{}-{}-{}", self.account, self.platform, self.module)
    }
    pub fn get_trait_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        self.metadata.get_trait_config::<T>(key)
    }
    pub fn get_login_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        self.metadata.get_login_config::<T>(key)
    }
    pub fn get_module_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        self.metadata.get_module_config::<T>(key)
    }
    pub fn get_task_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        self.metadata.get_task_config::<T>(key)
    }
    pub fn get_context(&self) -> &ExecutionMark {
        &self.context
    }
    pub fn with_context(mut self, ctx: ExecutionMark) -> Self {
        self.context = ctx;
        self
    }
}

impl CacheAble for Response {
    fn field() -> impl AsRef<str> {
        "response"
    }

    fn serialized_size_hint(&self) -> Option<usize> {
        Some(self.content.len() + self.headers.len() * 64)
    }

    fn clone_for_serialize(&self) -> Option<Self> {
        Some(self.clone())
    }
}

impl crate::common::model::priority::Prioritizable for Response {
    fn get_priority(&self) -> crate::common::model::Priority {
        self.priority
    }
}

use async_trait::async_trait;
#[async_trait]
impl crate::common::interface::storage::Offloadable for Response {
    fn should_offload(&self, threshold: usize) -> bool {
        self.content.len() > threshold && self.storage_path.is_none()
    }

    async fn offload(&mut self, storage: &std::sync::Arc<dyn crate::common::interface::storage::BlobStorage>) -> crate::errors::Result<()> {
        if self.content.is_empty() {
             return Ok(());
        }
        let key = format!("response/{}/{}.bin", self.run_id, self.id);
        let path = storage.put(&key, &self.content).await?;
        self.storage_path = Some(path);
        self.content.clear();
        self.content.shrink_to_fit();
        Ok(())
    }

    async fn reload(&mut self, storage: &std::sync::Arc<dyn crate::common::interface::storage::BlobStorage>) -> crate::errors::Result<()> {
        if let Some(path) = &self.storage_path {
            // If content is already present (e.g. reloaded twice), skip? 
            // But if we want to ensure full content, we should check if empty.
            if self.content.is_empty() {
                let data = storage.get(path).await?;
                self.content = data;
            }
        }
        Ok(())
    }
}
