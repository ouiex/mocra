use crate::common::model::login_info::LoginInfo;
use crate::common::model::{Cookies, CronConfig, Headers, ModuleConfig, Request, Response};
use crate::engine::task::module_dag_compiler::ModuleDagDefinition;
// use crate::common::parser::ParserTrait;
use crate::common::model::message::TaskOutputEvent;
use async_trait::async_trait;
use crate::errors::Result;
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
    /// Optional custom DAG definition built from ModuleNodeTrait nodes.
    ///
    /// - `Some(definition)`: use custom DAG path.
    /// - `None`: fallback to legacy `add_step` linear-compat DAG.
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
        None
    }
    async fn add_step(&self)->Vec<Arc<dyn ModuleNodeTrait>> {
        vec![]
    }
    async fn pre_process(&self, _config: Option<Arc<ModuleConfig>>,) -> Result<()> {
        Ok(())
    }
    // Called after `ModuleProcessorWithChain` finishes all nodes, for finalization logic.
    // Responses may not yet pass through `DataMiddleware`, so do not depend on final processed output.
    async fn post_process(&self, _config: Option<Arc<ModuleConfig>>,) -> Result<()> {
        Ok(())
    }
    /// Returns cron schedule config for this module.
    /// Defaults to `None` (scheduled startup disabled).
    fn cron(&self) -> Option<CronConfig> {
        None
    }
}

/// `ModuleNodeTrait` defines per-node request generation and response parsing.
/// Data across nodes is currently propagated via metadata:
/// `Request -> Response -> ParserData -> Request`.
/// Ideally node structs are immutable during execution; when mutable progression state
/// (e.g. pagination) is needed, it should be carried in metadata.
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
    ) -> Result<TaskOutputEvent>;
    fn retryable(&self) -> bool{
        true
    }
    /// Returns a stable, unique string key for this node type within the DAG.
    ///
    /// When non-empty, `ModuleDagNodeDef` uses this value directly as the node ID
    /// instead of a randomly generated UUID. This ensures node IDs remain consistent
    /// across DAG rebuilds (e.g. after error-task retries cause the factory to
    /// reconstruct the module instance), so error retry routing works correctly.
    ///
    /// Override this and return a short constant string that is unique among all
    /// nodes in the same module's DAG. Default is `""` (random UUID, existing
    /// behavior preserved for backward compatibility).
    fn stable_node_key(&self) -> &'static str {
        ""
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
