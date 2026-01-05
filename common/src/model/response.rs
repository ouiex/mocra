use crate::model::Cookies;
use crate::model::ExecutionMark;
use crate::model::meta::MetaData;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: Uuid,
    pub platform: String,
    pub account: String,
    pub module: String,
    pub status_code: u16,
    pub cookies: Cookies,
    pub content: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub task_retry_times: usize,
    pub metadata: MetaData,
    pub download_middleware: Vec<String>,
    pub data_middleware: Vec<String>,
    pub task_finished: bool,
    pub context: ExecutionMark,
    pub run_id:Uuid,
    pub prefix_request:Uuid,
    pub request_hash:Option<String>
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
    pub fn get_context(&self) -> &ExecutionMark { &self.context }
    pub fn with_context(mut self, ctx: ExecutionMark) -> Self { self.context = ctx; self}
}
