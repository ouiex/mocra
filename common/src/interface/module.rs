use crate::model::login_info::LoginInfo;
use crate::model::{Cookies, CronConfig, Headers, ModuleConfig, Request, Response};
// use crate::parser::ParserTrait;
use crate::model::message::ParserData;
use async_trait::async_trait;
use errors::Result;
use futures::Stream;
use serde_json::Map;
use std::pin::Pin;
use std::sync::Arc;


pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;

#[async_trait]
pub trait ModuleTrait: Send + Sync {
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


    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized;
    async fn add_step(&self)->Vec<Arc<dyn ModuleNodeTrait>> {
        vec![]
    }
    async fn pre_process(&self, _config: Option<Arc<ModuleConfig>>,) -> Result<()> {
        Ok(())
    }
    // 这个在ModuleProcessorWithChain执行完所有节点后调用，用于做一些收尾工作
    // 注意此时所以的Response可能还未进入DataMiddleware阶段，所以不能依赖最终数据处理结果
    async fn post_process(&self, _config: Option<Arc<ModuleConfig>>,) -> Result<()> {
        Ok(())
    }
    /// 返回该模块的定时调度配置
    /// 默认为 None (不启用定时启动)
    fn cron(&self) -> Option<CronConfig> {
        None
    }
}

/// 目前ModuleStepNodeTrait负责每个节点的请求生成和响应解析，每个请求对应一个解析
/// 问题1 多节点之间如何进行数据传递，目前是使用Meta字段进行传递 Request->Response->ParserData->Request
/// 问题2 每个执行节点是一个单独的struct,理想情况下struct中的所有字段都是只读的，不允许在执行过程中进行修改
///      但是实际情况下无法保证这种情况，例如记录页码进行翻页，这种情况只能通过Meta进行传递
#[async_trait]
pub trait ModuleNodeTrait: Send + Sync {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, serde_json::Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>>;
    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<ParserData>;
    fn retryable(&self) -> bool{
        true
    }
}

pub trait ToSyncBoxStream<T> {
    fn to_stream(self) -> SyncBoxStream<'static, T>;
    fn into_stream_ok(self) -> Result<SyncBoxStream<'static, T>>;
}

impl<T> ToSyncBoxStream<T> for Vec<T>
where
    T: Send + Sync + 'static,
{
    fn to_stream(self) -> SyncBoxStream<'static, T> {
        Box::pin(futures::stream::iter(self))
    }
    
    fn into_stream_ok(self) -> Result<SyncBoxStream<'static, T>> {
        Ok(Box::pin(futures::stream::iter(self)))
    }
}
