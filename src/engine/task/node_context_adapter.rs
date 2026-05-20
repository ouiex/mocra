use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::common::interface::module::{NodeGenerateContext, NodeParseContext};
use crate::common::model::login_info::LoginInfo;
use crate::common::model::{
    ExecutionMeta, ModuleConfig, NodeInput, PayloadCodec, Priority,
    ResolvedCommonConfig, ResolvedNodeConfig, Response, RoutingMeta, TypedEnvelope,
};

pub(crate) struct OwnedNodeGenerateContext {
    pub(crate) routing: RoutingMeta,
    pub(crate) exec: ExecutionMeta,
    pub(crate) config: ResolvedNodeConfig,
    pub(crate) input: NodeInput,
    pub(crate) login_info: Option<LoginInfo>,
}

impl OwnedNodeGenerateContext {
    pub(crate) fn borrowed(&self) -> NodeGenerateContext<'_> {
        NodeGenerateContext {
            routing: &self.routing,
            exec: &self.exec,
            config: &self.config,
            input: &self.input,
            login_info: self.login_info.as_ref(),
        }
    }
}

pub(crate) struct OwnedNodeParseContext {
    pub(crate) routing: RoutingMeta,
    pub(crate) exec: ExecutionMeta,
    pub(crate) config: ResolvedNodeConfig,
    pub(crate) login_info: Option<LoginInfo>,
}

impl OwnedNodeParseContext {
    pub(crate) fn borrowed(&self) -> NodeParseContext<'_> {
        NodeParseContext {
            routing: &self.routing,
            exec: &self.exec,
            config: &self.config,
            login_info: self.login_info.as_ref(),
        }
    }
}

pub(crate) fn build_module_config_generate_context(
    module_id: &str,
    run_id: Uuid,
    node_key: &str,
    base_common: ResolvedCommonConfig,
    config: &ModuleConfig,
    params: Map<String, Value>,
    login_info: Option<LoginInfo>,
    parent_request_id: Option<Uuid>,
) -> OwnedNodeGenerateContext {
    let (account, platform, module) = split_module_id(module_id);
    let now_ms = now_ms();

    OwnedNodeGenerateContext {
        routing: RoutingMeta {
            namespace: String::new(),
            account,
            platform,
            module,
            node_key: node_key.to_string(),
            run_id,
            request_id: Uuid::now_v7(),
            parent_request_id: parent_request_id.filter(|id| !id.is_nil()),
            priority: Priority::default(),
        },
        exec: ExecutionMeta {
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            ..ExecutionMeta::default()
        },
        config: build_module_config_node_config(module_id, node_key, base_common, Some(config)),
        input: build_module_config_input(node_key, params),
        login_info,
    }
}

pub(crate) fn build_module_config_parse_context(
    module_id: &str,
    node_key: &str,
    base_common: ResolvedCommonConfig,
    config: Option<&ModuleConfig>,
    response: &Response,
) -> OwnedNodeParseContext {
    let now_ms = now_ms();

    OwnedNodeParseContext {
        routing: RoutingMeta {
            namespace: String::new(),
            account: response.account.clone(),
            platform: response.platform.clone(),
            module: response.module.clone(),
            node_key: node_key.to_string(),
            run_id: response.run_id,
            request_id: response.id,
            parent_request_id: (!response.prefix_request.is_nil())
                .then_some(response.prefix_request),
            priority: response.priority,
        },
        exec: ExecutionMeta {
            task_retry_count: response.task_retry_times as u32,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            ..ExecutionMeta::default()
        },
        config: build_module_config_node_config(module_id, node_key, base_common, config),
        login_info: None,
    }
}

fn build_module_config_node_config(
    module_id: &str,
    node_key: &str,
    base_common: ResolvedCommonConfig,
    config: Option<&ModuleConfig>,
) -> ResolvedNodeConfig {
    let (account, platform, module) = split_module_id(module_id);
    let merged_value = config
        .map(ModuleConfig::get_merged_config)
        .unwrap_or_else(|| json!({}));

    ResolvedNodeConfig {
        profile_key: format!("mocra.profile.v1:{}:{}:{}", account, platform, module),
        profile_version: 0,
        common: build_module_config_common_config(base_common, config),
        node_config: TypedEnvelope::new(
            format!("mocra.node_config.v1.{}", node_key),
            1,
            PayloadCodec::Json,
            serde_json::to_vec(&merged_value).unwrap_or_default(),
        ),
    }
}

pub(crate) fn apply_module_config_common_overrides(
    mut common: ResolvedCommonConfig,
    config: Option<&ModuleConfig>,
) -> ResolvedCommonConfig {
    if let Some(config) = config {
        common.timeout_secs = config
            .get_config::<u64>("timeout_secs")
            .or_else(|| config.get_config::<u64>("timeout"))
            .unwrap_or(common.timeout_secs);
        common.rate_limit = config.get_config::<f32>("rate_limit").or(common.rate_limit);
        common.proxy_pool = config
            .get_config::<String>("proxy_pool")
            .or(common.proxy_pool.clone());
        common.downloader = config
            .get_config::<String>("downloader")
            .unwrap_or_else(|| common.downloader.clone());
        common.enable_session = config
            .get_config::<bool>("enable_session")
            .unwrap_or(common.enable_session);
        common.serial_execution = config
            .get_config::<bool>("serial_execution")
            .unwrap_or(common.serial_execution);
        common.rate_limit_group = config
            .get_config::<String>("rate_limit_group")
            .or(common.rate_limit_group.clone());
        common.response_cache_enabled = config
            .get_config::<bool>("response_cache_enabled")
            .or_else(|| config.get_config::<bool>("enable_response_cache"))
            .unwrap_or(common.response_cache_enabled);
        common.response_cache_ttl_secs = config
            .get_config::<u64>("response_cache_ttl_secs")
            .or(common.response_cache_ttl_secs);
        if let Some(priority) = config.get_config::<Priority>("priority") {
            common.priority = priority;
        }
        common.module_locker = config
            .get_config::<bool>("module_locker")
            .unwrap_or(common.module_locker);
    }

    common
}

fn build_module_config_common_config(
    base_common: ResolvedCommonConfig,
    config: Option<&ModuleConfig>,
) -> ResolvedCommonConfig {
    apply_module_config_common_overrides(base_common, config)
}

fn build_module_config_input(target_node: &str, params: Map<String, Value>) -> NodeInput {
    NodeInput::new(
        target_node,
        TypedEnvelope::new(
            "mocra.node_input.v1",
            1,
            PayloadCodec::Json,
            serde_json::to_vec(&Value::Object(params)).unwrap_or_default(),
        ),
    )
}

fn split_module_id(module_id: &str) -> (String, String, String) {
    let mut parts = module_id.splitn(3, '-');
    let account = parts.next().unwrap_or_default().to_string();
    let platform = parts.next().unwrap_or_default().to_string();
    let module = parts.next().unwrap_or(module_id).to_string();
    (account, platform, module)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::Priority;
    use crate::common::interface::ModuleTrait;
    use std::sync::Arc;

    struct CommonDefaultsTestModule;

    #[async_trait::async_trait]
    impl ModuleTrait for CommonDefaultsTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "common_defaults_test"
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

        fn rate_limit(&self) -> Option<f32> {
            Some(2.5)
        }

        fn proxy_pool(&self) -> Option<&str> {
            Some("pool-a")
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

        fn priority(&self) -> Priority {
            Priority::High
        }
    }

    #[test]
    fn apply_module_config_common_overrides_preserves_inherited_option_defaults() {
        let default_common = CommonDefaultsTestModule.default_common_config();
        let resolved = apply_module_config_common_overrides(default_common, Some(&ModuleConfig::default()));

        assert_eq!(resolved.rate_limit, Some(2.5));
        assert_eq!(resolved.proxy_pool.as_deref(), Some("pool-a"));
        assert_eq!(resolved.rate_limit_group.as_deref(), Some("group-a"));
        assert!(resolved.response_cache_enabled);
        assert_eq!(resolved.response_cache_ttl_secs, Some(60));
        assert_eq!(resolved.priority, Priority::High);
    }
}
