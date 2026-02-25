#![allow(unused)]
use crate::common::model::entity::{AccountModel, PlatformModel};
use crate::common::model::{Request, login_info::LoginInfo};
use crate::engine::task::module::Module;
use crate::errors::Result;
use uuid::Uuid;
use futures::StreamExt;

/// Runtime task aggregate for one account-platform execution scope.
#[derive(Clone)]
pub struct Task {
    pub account: AccountModel,
    pub platform: PlatformModel,
    pub login_info: Option<LoginInfo>,
    pub modules: Vec<Module>,
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub run_id:Uuid,
    pub prefix_request:Uuid
}


impl Task {
    /// Returns stable task id in account-platform format.
    pub fn id(&self) -> String {
        format!("{}-{}", self.account.name, self.platform.name)
    }

    /// Returns runtime module ids included in this task.
    pub fn get_module_names(&self) -> Vec<String> {
        self.modules.iter().map(|module| module.id()).collect()
    }

    /// Concurrently generates requests from all modules and merges results.
    pub async fn build_requests(&self) -> Result<Vec<Request>> {
        use futures::future::join_all;
        // 1) Generate request streams concurrently.
        let futures = self
            .modules
            .iter()
            .map(|module| module.generate(self.metadata.clone(), self.login_info.clone()));

        let results = join_all(futures).await;

        // 2) Collect and merge all produced request streams.
        let mut all_requests = Vec::new();
        for result in results {
            match result {
                Ok(stream) => {
                    let mut reqs: Vec<Request> = stream.collect().await;
                    all_requests.append(&mut reqs);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(all_requests)
    }

    /// Returns true when task has no runnable modules.
    pub fn is_empty(&self)->bool{
        self.modules.is_empty()
    }
}
