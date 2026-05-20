use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::common::interface::ModuleTrait;
use crate::common::model::{
    MiddlewareBinding, MiddlewareType, ModuleConfig, NodeSpec, PayloadCodec, ResolvedCommonConfig,
    ResolvedNodeConfig, TaskProfileSnapshot, TypedEnvelope, WorkflowDefinition,
};
use crate::engine::task::module_dag_compiler::ModuleDagDefinition;
use crate::engine::task::node_context_adapter::apply_module_config_common_overrides;
use crate::engine::task::workflow_compiler::{WorkflowCompileError, WorkflowCompiler};

#[derive(Debug, Error)]
pub enum ProfileLoadError {
    #[error(transparent)]
    WorkflowCompile(#[from] WorkflowCompileError),
    #[error("workflow node `{0}` has no resolved node config")]
    MissingNodeConfig(String),
}

#[derive(Debug, Clone)]
pub struct LoadedProfile {
    pub snapshot: TaskProfileSnapshot,
    pub workflow: WorkflowDefinition,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NodeConfigResolver;

impl NodeConfigResolver {
    pub fn resolve(
        &self,
        snapshot: &TaskProfileSnapshot,
        node_key: &str,
    ) -> Result<ResolvedNodeConfig, ProfileLoadError> {
        snapshot
            .resolve_node_config(node_key)
            .ok_or_else(|| ProfileLoadError::MissingNodeConfig(node_key.to_string()))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProfileLoader {
    workflow_compiler: WorkflowCompiler,
    node_config_resolver: NodeConfigResolver,
}

impl ProfileLoader {
    pub async fn load(
        &self,
        namespace: &str,
        account: &str,
        platform: &str,
        module_name: &str,
        updated_by: &str,
        module_impl: Arc<dyn ModuleTrait>,
        module_config: &ModuleConfig,
    ) -> Result<LoadedProfile, ProfileLoadError> {
        let default_common = module_impl.default_common_config();
        let definition = self
            .workflow_compiler
            .build_module_definition(module_impl)
            .await;
        let snapshot = self.build_snapshot(
            namespace,
            account,
            platform,
            module_name,
            updated_by,
            default_common,
            module_config,
            &definition,
        );
        let workflow = self
            .workflow_compiler
            .compile_definition(&snapshot, definition)?;

        for NodeSpec { node_key, .. } in &workflow.nodes {
            self.node_config_resolver.resolve(&snapshot, node_key)?;
        }

        Ok(LoadedProfile { snapshot, workflow })
    }

    fn build_snapshot(
        &self,
        namespace: &str,
        account: &str,
        platform: &str,
        module_name: &str,
        updated_by: &str,
        default_common: ResolvedCommonConfig,
        module_config: &ModuleConfig,
        definition: &ModuleDagDefinition,
    ) -> TaskProfileSnapshot {
        let merged_config = module_config.get_merged_config();
        let merged_bytes = serde_json::to_vec(&merged_config).unwrap_or_default();
        let common = apply_module_config_common_overrides(default_common, Some(module_config));

        let node_configs = definition
            .nodes
            .iter()
            .map(|node| {
                (
                    node.node_id.clone(),
                    TypedEnvelope::new(
                        format!("mocra.node_config.v1.{}", node.node_id),
                        1,
                        PayloadCodec::Json,
                        merged_bytes.clone(),
                    ),
                )
            })
            .collect();

        let download_middleware: Vec<MiddlewareBinding> = module_config
            .download_middleware_config
            .keys()
            .map(|name| MiddlewareBinding {
                name: name.clone(),
                middleware_type: MiddlewareType::Download,
                weight: module_config
                    .get_middleware_weight(name)
                    .map(|weight| weight as i32)
                    .unwrap_or_default(),
            })
            .collect();
        let data_middleware: Vec<MiddlewareBinding> = module_config
            .data_middleware_config
            .keys()
            .map(|name| MiddlewareBinding {
                name: name.clone(),
                middleware_type: MiddlewareType::Data,
                weight: 0,
            })
            .collect();
        let middleware_configs = collect_middleware_configs(module_config);
        let version = stable_profile_version(
            namespace,
            account,
            platform,
            module_name,
            &common,
            &node_configs,
            &download_middleware,
            &data_middleware,
            &middleware_configs,
        );

        TaskProfileSnapshot {
            namespace: namespace.to_string(),
            account: account.to_string(),
            platform: platform.to_string(),
            module: module_name.to_string(),
            version,
            enabled: true,
            common,
            node_configs,
            download_middleware,
            data_middleware,
            middleware_configs,
            debug_layers_json: None,
            updated_at: now_ms(),
            updated_by: updated_by.to_string(),
        }
    }
}

fn stable_profile_version(
    namespace: &str,
    account: &str,
    platform: &str,
    module_name: &str,
    common: &ResolvedCommonConfig,
    node_configs: &BTreeMap<String, TypedEnvelope>,
    download_middleware: &[MiddlewareBinding],
    data_middleware: &[MiddlewareBinding],
    middleware_configs: &BTreeMap<String, TypedEnvelope>,
) -> u64 {
    #[derive(serde::Serialize)]
    struct ProfileFingerprint<'a> {
        namespace: &'a str,
        account: &'a str,
        platform: &'a str,
        module_name: &'a str,
        common: &'a ResolvedCommonConfig,
        node_configs: &'a BTreeMap<String, TypedEnvelope>,
        download_middleware: &'a [MiddlewareBinding],
        data_middleware: &'a [MiddlewareBinding],
        middleware_configs: &'a BTreeMap<String, TypedEnvelope>,
    }

    let fingerprint = ProfileFingerprint {
        namespace,
        account,
        platform,
        module_name,
        common,
        node_configs,
        download_middleware,
        data_middleware,
        middleware_configs,
    };
    let digest = md5::compute(serde_json::to_vec(&fingerprint).unwrap_or_default());
    u64::from_be_bytes(digest.0[..8].try_into().unwrap_or([0; 8]))
}

fn collect_middleware_configs(module_config: &ModuleConfig) -> BTreeMap<String, TypedEnvelope> {
    let mut middleware_configs = BTreeMap::new();

    for (name, config) in &module_config.download_middleware_config {
        middleware_configs.insert(
            name.clone(),
            TypedEnvelope::new(
                format!("mocra.middleware.v1.download.{name}"),
                1,
                PayloadCodec::Json,
                serde_json::to_vec(config).unwrap_or_default(),
            ),
        );
    }
    for (name, config) in &module_config.data_middleware_config {
        middleware_configs.insert(
            name.clone(),
            TypedEnvelope::new(
                format!("mocra.middleware.v1.data.{name}"),
                1,
                PayloadCodec::Json,
                serde_json::to_vec(config).unwrap_or_default(),
            ),
        );
    }
    for (name, config) in &module_config.rel_module_download_middleware_config {
        middleware_configs.insert(
            name.clone(),
            TypedEnvelope::new(
                format!("mocra.middleware.v1.download.{name}"),
                1,
                PayloadCodec::Json,
                serde_json::to_vec(config).unwrap_or_default(),
            ),
        );
    }
    for (name, config) in &module_config.rel_module_data_middleware_config {
        middleware_configs.insert(
            name.clone(),
            TypedEnvelope::new(
                format!("mocra.middleware.v1.data.{name}"),
                1,
                PayloadCodec::Json,
                serde_json::to_vec(config).unwrap_or_default(),
            ),
        );
    }

    middleware_configs
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::common::interface::{
        ModuleNodeTrait, NodeGenerateContext, NodeParseContext, SyncBoxStream, ToSyncBoxStream,
    };
    use crate::common::model::{NodeParseOutput, Request, Response};

    struct DummyNode {
        stable_key: &'static str,
    }

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(&self, _ctx: NodeGenerateContext<'_>) -> crate::errors::Result<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(&self, _response: Response, _ctx: NodeParseContext<'_>) -> crate::errors::Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            self.stable_key
        }
    }

    struct DummyModule;

    #[async_trait]
    impl ModuleTrait for DummyModule {
        fn name(&self) -> &'static str {
            "dummy_module"
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

        fn enable_session(&self) -> bool {
            true
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![
                Arc::new(DummyNode {
                    stable_key: "entry",
                }),
                Arc::new(DummyNode {
                    stable_key: "detail",
                }),
            ]
        }
    }

    fn module_config() -> ModuleConfig {
        ModuleConfig {
            module_config: serde_json::json!({
                "downloader": "custom_downloader",
                "serial_execution": true
            }),
            download_middleware_config: std::collections::HashMap::from([(
                "download-cache".to_string(),
                serde_json::json!({"weight": 7}),
            )]),
            ..ModuleConfig::default()
        }
    }

    #[tokio::test]
    async fn profile_loader_builds_snapshot_and_workflow() {
        let loader = ProfileLoader::default();
        let loaded = loader
            .load(
                "demo",
                "account-a",
                "platform-x",
                "dummy_module",
                "task_factory",
                Arc::new(DummyModule),
                &module_config(),
            )
            .await
            .expect("profile should load");

        assert_eq!(
            loaded.snapshot.profile_key(),
            "demo:profile:account-a:platform-x:dummy_module"
        );
        assert_eq!(loaded.snapshot.common.timeout_secs, 45);
        assert_eq!(loaded.snapshot.common.downloader, "custom_downloader");
        assert!(loaded.snapshot.common.serial_execution);
        assert!(loaded.snapshot.common.enable_session);
        assert!(loaded.workflow.node("entry").is_some());
        assert!(loaded.workflow.node("detail").is_some());
    }

    #[tokio::test]
    async fn node_config_resolver_returns_per_node_config() {
        let loader = ProfileLoader::default();
        let loaded = loader
            .load(
                "demo",
                "account-a",
                "platform-x",
                "dummy_module",
                "task_factory",
                Arc::new(DummyModule),
                &module_config(),
            )
            .await
            .expect("profile should load");

        let resolved = NodeConfigResolver
            .resolve(&loaded.snapshot, "entry")
            .expect("entry config should exist");

        assert_eq!(
            resolved.profile_key,
            "demo:profile:account-a:platform-x:dummy_module"
        );
        assert_eq!(resolved.node_config.schema_id, "mocra.node_config.v1.entry");
    }

    #[tokio::test]
    async fn profile_loader_assigns_stable_non_zero_profile_version() {
        let loader = ProfileLoader::default();
        let first = loader
            .load(
                "demo",
                "account-a",
                "platform-x",
                "dummy_module",
                "task_factory",
                Arc::new(DummyModule),
                &module_config(),
            )
            .await
            .expect("profile should load");
        let second = loader
            .load(
                "demo",
                "account-a",
                "platform-x",
                "dummy_module",
                "task_factory",
                Arc::new(DummyModule),
                &module_config(),
            )
            .await
            .expect("profile should load");

        assert_ne!(first.snapshot.version, 0);
        assert_eq!(first.snapshot.version, second.snapshot.version);
        assert_eq!(
            first.workflow.metadata.get("profile_version"),
            Some(&first.snapshot.version.to_string())
        );
    }
}
