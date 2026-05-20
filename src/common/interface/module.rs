use crate::common::model::login_info::LoginInfo;
use crate::common::model::{
    Cookies, CronConfig, ExecutionMeta, Headers, NodeInput, NodeParseOutput, Priority, Request,
    ResolvedCommonConfig, ResolvedNodeConfig, Response, RoutingMeta, TaskProfileSnapshot,
};
use crate::engine::task::module_dag_compiler::ModuleDagDefinition;
// use crate::common::parser::ParserTrait;
use crate::errors::Result;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;

#[async_trait]
pub trait ModuleTrait: Send + Sync {
    fn should_login(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str;
    fn version(&self) -> i32;
    fn rate_limit(&self) -> Option<f32> {
        None
    }
    fn proxy_pool(&self) -> Option<&str> {
        None
    }
    fn timeout_secs(&self) -> Option<u64> {
        None
    }
    fn priority(&self) -> Priority {
        Priority::default()
    }
    fn serial_execution(&self) -> bool {
        false
    }
    fn module_locker(&self) -> bool {
        false
    }
    fn enable_session(&self) -> bool {
        false
    }
    fn downloader(&self) -> &str {
        "request_downloader"
    }
    fn rate_limit_group(&self) -> Option<&str> {
        None
    }
    fn response_cache_enabled(&self) -> bool {
        false
    }
    fn response_cache_ttl_secs(&self) -> Option<u64> {
        None
    }
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
    /// - `None`: build a linear DAG from `add_step`.
    ///
    /// When both `dag_definition()` and `add_step()` are implemented, the custom DAG
    /// takes precedence and `add_step()` is ignored.
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
        None
    }
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![]
    }
    fn default_common_config(&self) -> ResolvedCommonConfig {
        ResolvedCommonConfig {
            timeout_secs: self.timeout_secs().unwrap_or(30),
            rate_limit: self.rate_limit(),
            priority: self.priority(),
            proxy_pool: self.proxy_pool().map(str::to_string),
            downloader: self.downloader().to_string(),
            enable_session: self.enable_session(),
            serial_execution: self.serial_execution(),
            module_locker: self.module_locker(),
            rate_limit_group: self.rate_limit_group().map(str::to_string),
            response_cache_enabled: self.response_cache_enabled(),
            response_cache_ttl_secs: self.response_cache_ttl_secs(),
        }
    }
    async fn pre_process(&self, _profile: Option<Arc<TaskProfileSnapshot>>) -> Result<()> {
        Ok(())
    }
    // Called after `ModuleProcessorWithChain` finishes all nodes, for finalization logic.
    // Responses may not yet pass through `DataMiddleware`, so do not depend on final processed output.
    async fn post_process(&self, _profile: Option<Arc<TaskProfileSnapshot>>) -> Result<()> {
        Ok(())
    }
    /// Returns cron schedule config for this module.
    /// Defaults to `None` (scheduled startup disabled).
    fn cron(&self) -> Option<CronConfig> {
        None
    }
}

/// `ModuleNodeTrait` defines per-node request generation and response parsing.
/// Data across nodes is propagated through typed parser output:
/// `Request -> Response -> NodeParseOutput -> NodeDispatch`.
/// Ideally node structs are immutable during execution; when mutable progression state
/// (e.g. pagination) is needed, it should be carried in metadata.
pub struct NodeGenerateContext<'a> {
    pub routing: &'a RoutingMeta,
    pub exec: &'a ExecutionMeta,
    pub config: &'a ResolvedNodeConfig,
    pub input: &'a NodeInput,
    pub login_info: Option<&'a LoginInfo>,
}

pub struct NodeParseContext<'a> {
    pub routing: &'a RoutingMeta,
    pub exec: &'a ExecutionMeta,
    pub config: &'a ResolvedNodeConfig,
    pub login_info: Option<&'a LoginInfo>,
}

#[async_trait]
pub trait ModuleNodeTrait: Send + Sync {
    async fn generate(
        &self,
        _ctx: NodeGenerateContext<'_>,
    ) -> Result<SyncBoxStream<'static, Request>>;
    async fn parser(
        &self,
        response: Response,
        _ctx: NodeParseContext<'_>,
    ) -> Result<NodeParseOutput>;
    fn retryable(&self) -> bool {
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
    /// nodes in the same module's DAG. Default is `""` (random UUID).
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

#[cfg(test)]
mod tests {
    use super::*;

    struct DefaultConfigModule;

    #[async_trait]
    impl ModuleTrait for DefaultConfigModule {
        fn name(&self) -> &'static str {
            "default-config-module"
        }

        fn version(&self) -> i32 {
            1
        }

        fn default_arc() -> Arc<dyn ModuleTrait>
        where
            Self: Sized,
        {
            Arc::new(Self)
        }

        fn timeout_secs(&self) -> Option<u64> {
            Some(45)
        }

        fn rate_limit(&self) -> Option<f32> {
            Some(2.5)
        }

        fn priority(&self) -> Priority {
            Priority::High
        }

        fn proxy_pool(&self) -> Option<&str> {
            Some("pool-a")
        }

        fn serial_execution(&self) -> bool {
            true
        }

        fn module_locker(&self) -> bool {
            true
        }

        fn enable_session(&self) -> bool {
            true
        }

        fn rate_limit_group(&self) -> Option<&str> {
            Some("group-a")
        }

        fn response_cache_enabled(&self) -> bool {
            true
        }

        fn response_cache_ttl_secs(&self) -> Option<u64> {
            Some(60)
        }
    }

    #[test]
    fn module_trait_default_common_config_assembles_static_defaults() {
        let module = DefaultConfigModule;
        let config = module.default_common_config();

        assert_eq!(config.timeout_secs, 45);
        assert_eq!(config.rate_limit, Some(2.5));
        assert_eq!(config.priority, Priority::High);
        assert_eq!(config.proxy_pool.as_deref(), Some("pool-a"));
        assert_eq!(config.downloader, "request_downloader");
        assert!(config.enable_session);
        assert!(config.serial_execution);
        assert_eq!(config.rate_limit_group.as_deref(), Some("group-a"));
        assert!(config.response_cache_enabled);
        assert_eq!(config.response_cache_ttl_secs, Some(60));
    }
}
