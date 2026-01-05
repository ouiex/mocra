#![allow(unused)]
use crate::model::login_info::LoginInfo;
use crate::model::{Cookies, ExecutionMark, Headers, ModuleConfig, Request, Response};
// use crate::parser::ParserTrait;
use crate::model::message::ParserData;
use async_trait::async_trait;
use dyn_clone::DynClone;
use errors::RequestError;
use errors::Result;
use futures::Stream;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;

#[async_trait]
pub trait Module: Send+Sync {
    fn should_login(&self) -> bool {
        true
    }
    fn name(&self) -> String;
    fn version(&self) -> i32;
    async fn headers(&self) -> Headers {
        Headers::default()
    }
    async fn cookies(&self) -> Cookies {
        Cookies::default()
    }

    /// 可选：携带 StateHandle 的生成方法，默认回落到不带 state 的实现
    // async fn generate(
    //     &self,
    //     config: ModuleConfig,
    //     task_meta: serde_json::Value,
    //     login_info: Option<LoginInfo>,
    //     _state: Option<Arc<StateHandle>>,
    // ) -> Result<Vec<Request>> ;
    fn default() -> Arc<dyn Module>
    where
        Self: Sized;
    async fn add_step(&self) -> Vec<Box<dyn ModuleNode>> {
        vec![]
    }
    async fn pre_process(&self, _config: Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }
    // 这个在ModuleProcessorWithChain执行完所有节点后调用，用于做一些收尾工作
    // 注意此时所以的Response可能还未进入DataMiddleware阶段，所以不能依赖最终数据处理结果
    async fn post_process(&self, _config: Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }
}



/// 目前ModuleStepNodeTrait负责每个节点的请求生成和响应解析，每个请求对应一个解析
/// 问题1 多节点之间如何进行数据传递，目前是使用Meta字段进行传递 Request->Response->ParserData->Request
/// 问题2 每个执行节点是一个单独的struct,理想情况下struct中的所有字段都是只读的，不允许在执行过程中进行修改
///      但是实际情况下无法保证这种情况，例如记录页码进行翻页，这种情况只能通过Meta进行传递
#[async_trait]
pub trait ModuleNode: Send+Sync {
    async fn generate(
        &self,
        _config: ModuleConfig,
        _params: Map<String, serde_json::Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>>;
    async fn parser(&self, response: Response, _config: Option<ModuleConfig>)
    -> Result<ParserData>;
    fn retryable(&self) -> bool {
        true
    }
}
