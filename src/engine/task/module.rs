use crate::errors::Result;
use log::warn;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Map;
use std::sync::Arc;
use uuid::Uuid;
use futures::StreamExt;
use crate::common::interface::{
    DataMiddlewareHandle, DataStoreMiddlewareHandle, ModuleNodeTrait, ModuleTrait, SyncBoxStream,
};
use crate::common::model::entity::{AccountModel, PlatformModel};
use crate::common::model::login_info::LoginInfo;
use crate::common::model::{Cookies, Headers, ModuleConfig, Response,Request};
use crate::common::model::message::TaskOutputEvent;
use crate::errors::RequestError;
use crate::engine::task::module_dag_compiler::{ModuleDagCompiler, ModuleDagDefinition};
use crate::engine::task::module_processor_with_chain::ModuleProcessorWithChain;
use crate::schedule::dag::{Dag, DagError};

#[derive(Debug, Clone, Copy, Default)]
pub struct ModuleDagRuntimeFlags {
    pub enabled: bool,
    pub dry_run: bool,
    pub shadow_execute: bool,
    pub primary_preview: bool,
    pub cutover_requested: bool,
    pub cutover_supported: bool,
    pub primary_cutover: bool,
}

/// Runtime module instance bound to account/platform context.
///
/// A Module aggregates static module behavior, resolved configuration,
/// middleware bindings, and chain-runtime metadata.
#[derive(Clone)]
pub struct Module {
    /// Resolved module configuration.
    pub config: Arc<ModuleConfig>,
    /// Bound account model.
    pub account: AccountModel,
    /// Bound platform model.
    pub platform: PlatformModel,
    /// In-memory error counter snapshot.
    pub error_times: u32,
    /// Completion flag at module level.
    pub finished: bool,
    /// Data middleware names.
    pub data_middleware: Vec<String>,
    /// Download middleware names.
    pub download_middleware: Vec<String>,
    /// Module behavior implementation.
    pub module: Arc<dyn ModuleTrait>,
    /// Whether distributed locking is enabled.
    pub locker: bool,
    /// Lock TTL in seconds.
    pub locker_ttl: u64,
    /// Chain processor for step generation and parsing.
    pub processor: ModuleProcessorWithChain,
    /// Run identifier for cross-stage scoping.
    pub run_id: Uuid,
    /// Prefix request for fallback tracing.
    pub prefix_request: Uuid,
    /// Optional execution context for precise step targeting.
    pub pending_ctx: Option<crate::common::model::ExecutionMark>,
    /// Task metadata injected by TaskModuleProcessor.
    pub bound_task_meta: Option<Map<String, serde_json::Value>>,
    /// Login context injected by TaskModuleProcessor.
    pub bound_login_info: Option<LoginInfo>,
}
impl Module {
    fn module_dag_enabled(&self) -> bool {
        self.config
            .get_config_value("module_dag_enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    fn module_dag_whitelist_match(&self) -> bool {
        let module_name = self.module.name();
        let Some(value) = self.config.get_config_value("module_dag_whitelist") else {
            return true;
        };

        if let Some(list) = value.as_array() {
            return list
                .iter()
                .filter_map(|v| v.as_str())
                .any(|name| name == module_name);
        }

        if let Some(csv) = value.as_str() {
            return csv
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .any(|name| name == module_name);
        }

        true
    }

    fn module_dag_cutover_whitelist_match(&self) -> bool {
        let module_name = self.module.name();
        let Some(value) = self
            .config
            .get_config_value("module_dag_cutover_whitelist")
        else {
            // Cutover must be explicitly allowlisted for production safety.
            return false;
        };

        if let Some(list) = value.as_array() {
            return list
                .iter()
                .filter_map(|v| v.as_str())
                .any(|name| name == module_name);
        }

        if let Some(csv) = value.as_str() {
            return csv
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .any(|name| name == module_name);
        }

        false
    }

    fn module_dag_force_linear_compat(&self) -> bool {
        self.config
            .get_config_value("module_dag_force_linear_compat")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    }

    fn should_validate_linear_dag_compat(&self) -> bool {
        self.module_dag_enabled()
            && self.module_dag_force_linear_compat()
            && self.module_dag_whitelist_match()
    }

    pub fn module_dag_dry_run(&self) -> bool {
        self.config
            .get_config_value("module_dag_dry_run")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn should_run_dag_dry_run(&self) -> bool {
        self.module_dag_enabled()
            && self.module_dag_dry_run()
            && self.module_dag_force_linear_compat()
            && self.module_dag_whitelist_match()
    }

    pub fn dag_runtime_flags(&self) -> ModuleDagRuntimeFlags {
        let enabled = self.module_dag_enabled()
            && self.module_dag_force_linear_compat()
            && self.module_dag_whitelist_match();
        let dry_run = enabled && self.module_dag_dry_run();
        let shadow_execute = dry_run && self.module_dag_shadow_execute();
        let primary_preview = enabled && self.module_dag_execute_primary();
        let cutover_requested = primary_preview && self.module_dag_execute_primary_cutover();
        let cutover_supported = cutover_requested && self.module_dag_cutover_whitelist_match();
        let primary_cutover = cutover_supported;

        ModuleDagRuntimeFlags {
            enabled,
            dry_run,
            shadow_execute,
            primary_preview,
            cutover_requested,
            cutover_supported,
            primary_cutover,
        }
    }

    pub fn module_dag_shadow_execute(&self) -> bool {
        self.config
            .get_config_value("module_dag_shadow_execute")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn should_run_dag_shadow_execute(&self) -> bool {
        self.dag_runtime_flags().shadow_execute
    }

    pub fn module_dag_execute_primary(&self) -> bool {
        self.config
            .get_config_value("module_dag_execute_primary")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn module_dag_execute_primary_cutover(&self) -> bool {
        self.config
            .get_config_value("module_dag_execute_primary_cutover")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn should_run_dag_execute_primary(&self) -> bool {
        self.dag_runtime_flags().primary_preview
    }

    fn validate_linear_dag_from_nodes(&self, nodes: &[Arc<dyn ModuleNodeTrait>]) {
        if !self.should_validate_linear_dag_compat() {
            return;
        }

        let definition = ModuleDagDefinition::from_linear_steps(nodes.to_vec());
        if let Err(err) = ModuleDagCompiler::compile(definition) {
            warn!(
                "module dag compile validation failed, fallback to chain mode: module={} err={}",
                self.module.name(),
                err
            );
        }
    }

    /// Binds task metadata and optional login context.
    pub fn bind_task_context(
        &mut self,
        task_meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
    ) {
        self.bound_task_meta = Some(task_meta);
        self.bound_login_info = login_info;
    }

    /// Returns task metadata and login info used by generate.
    pub fn runtime_task_context(&self) -> (Map<String, serde_json::Value>, Option<LoginInfo>) {
        (
            self.bound_task_meta.clone().unwrap_or_default(),
            self.bound_login_info.clone(),
        )
    }

    /// Generates request stream for the current chain step.
    ///
    /// Besides delegating to ModuleProcessorWithChain, this method enriches each request
    /// with module/account/platform identity, middleware, config payloads, and run markers.
    pub async fn generate(
        &self,
        task_meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        if self.module.should_login() && login_info.is_none() {
            return Err(RequestError::NotLogin("module need login".into()).into());
        }
        let request_stream = self
            .processor
            .execute_request(
                self.config.clone(),
                task_meta.clone(),
                login_info.clone(),
                self.pending_ctx.clone(),
                Some(self.prefix_request),
            )
            .await?;

        // let request_list = self
        //     .module
        //     .generate(
        //         self.config.clone(),
        //         task_meta.clone(),
        //         login_info.clone(),
        //         self.state_handle.clone(),
        //     )
        //     .await?;

        // Capture needed values for the mapping closure.
        let module_name = self.module.name().clone();
        let platform_name = self.platform.name.clone();
        let download_middleware = self.download_middleware.clone();
        let data_middleware = self.data_middleware.clone();
        let account_name = self.account.name.clone();
        let finished = self.finished;
        let limit_id = self
            .config
            .get_config_value("limit_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let headers = self.module.headers().await;
        let cookies = self.module.cookies().await;
        let run_id = self.run_id;
        let prefix_request = self.prefix_request;
        let config = self.config.clone();
        let stream = request_stream
            .map(move |mut request| {
                if request.id.is_nil() {
                    request.id = Uuid::now_v7();
                }
                request.module = module_name.clone();
                request.platform = platform_name.clone();
                let mut merged_download_middleware = request.download_middleware.clone();
                merged_download_middleware.extend(download_middleware.clone());
                merged_download_middleware.sort();
                merged_download_middleware.dedup();
                request.download_middleware = merged_download_middleware;

                let mut merged_data_middleware = request.data_middleware.clone();
                merged_data_middleware.extend(data_middleware.clone());
                merged_data_middleware.sort();
                merged_data_middleware.dedup();
                request.data_middleware = merged_data_middleware;
                request.account = account_name.clone();
                request.task_finished = finished;
                // inherit run_id from Module/Task so ModuleProcessor and downstream can isolate state per run
                request.run_id = run_id;
                // Set chain prefix from Task/Module level; used to recover previous request on failures
                request.prefix_request = prefix_request;

                if request.headers.is_empty() && !headers.is_empty() {
                    request = request.with_headers(headers.clone());
                }
                if !cookies.is_empty() {
                    request = request.with_cookies(cookies.clone());
                }

                // Merge login-provided headers and cookies.
                if let Some(ref info) = login_info {
                    let cookies = Cookies::from(info);
                    let headers = Headers::from(info);
                    request.headers.merge(&headers);
                    request.cookies.merge(&cookies);
                    request = request.with_login_info(info);
                }
                request.limit_id = limit_id.clone().unwrap_or(request.module_id());
                // request.meta = request.meta.add_module_config(&self.config);
                request = request
                    .with_module_config(&config)
                    .with_task_config(task_meta.clone());
                // add downloader from config
                if let Some(downloader) = config.get_config::<String>("downloader") {
                    request.downloader = downloader;
                }
                else {
                    request.downloader = "request_downloader".to_string();
                }
                log::debug!("[Module] request prepared: request_id={}", request.id);
                request
            });
        Ok(Box::pin(stream))
    }

    /// Loads module step nodes and appends them to the chain processor.
    pub async fn add_step(&self) {
        // Run module-level pre_process once before registering steps
        if let Err(e) = self
            .module
            .pre_process(Some(self.config.clone()))
            .await
        {
            // Keep non-breaking behavior for default/no-op modules.
            warn!(
                "module pre_process failed for {}: {}",
                self.module.name(),
                e
            );
        }
        let nodes = self.module.add_step().await;

        self.validate_linear_dag_from_nodes(&nodes);

        for n in nodes {
            self.processor.add_step_node(n).await;
        }
    }

    /// Builds a linear-compatible DAG definition from module steps.
    pub async fn build_dag_definition_linear_compat(&self) -> ModuleDagDefinition {
        let steps = self.module.add_step().await;
        ModuleDagDefinition::from_linear_steps(steps)
    }

    /// Builds a DAG graph from module steps in compatibility mode.
    pub async fn prepare_execution_graph(&self) -> std::result::Result<Dag, DagError> {
        let definition = self.build_dag_definition_linear_compat().await;
        ModuleDagCompiler::compile(definition)
    }

    /// Runs DAG preflight validation in dry-run mode.
    ///
    /// Returns `Ok(())` when dry-run is disabled, so callers can invoke it safely
    /// without duplicating config checks.
    pub async fn run_dag_preflight_dry_run(&self) -> std::result::Result<(), DagError> {
        if !self.should_run_dag_dry_run() {
            return Ok(());
        }
        self.prepare_execution_graph().await.map(|_| ())
    }
    
    /// Parses response at the routed step and handles terminal lifecycle hook.
    pub async fn parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        // Capture current step to determine if this is the last step
        let current_step = response.context.step_idx.unwrap_or(0) as usize;
        // Clone config for potential post_process
        let cfg_for_post = config.clone();

        let mut data = self.processor.execute_parser(response, config).await?;
        // Enrich returned Data with module/account/platform identifiers
        for d in data.data.iter_mut() {
            d.module = self.module.name();
            d.account = self.account.name.clone();
            d.platform = self.platform.name.clone();
        }


        // Run post_process only when chain reaches terminal step without next task.
        let total_steps = self.processor.get_total_steps().await;
        let is_last_step = current_step + 1 >= total_steps && total_steps > 0;
        let no_next_task = data.parser_task.is_empty();
        if is_last_step && no_next_task {
            self.module.post_process(cfg_for_post).await?;
            
        }
        Ok(data)
    }

    /// Returns stable module runtime id in account-platform-module format.
    pub fn id(&self) -> String {
        format!(
            "{}-{}-{}",
            self.account.name,
            self.platform.name,
            self.module.name()
        )
    }

}

impl Serialize for Module {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Module", 8)?;
        state.serialize_field("config", &self.config)?;
        state.serialize_field("account", &self.account)?;
        state.serialize_field("platform", &self.platform)?;
        state.serialize_field("error_times", &self.error_times)?;
        state.serialize_field("data_middleware", &self.data_middleware)?;
        state.serialize_field("download_middleware", &self.download_middleware)?;
        state.serialize_field("module", &self.module.name())?;
        state.end()
    }
}


/// Assembly helper for creating Module runtime instances.
pub struct ModuleEntity{
    pub module_work:Arc<dyn ModuleTrait>,
    pub download_middleware:Vec<Arc<dyn ModuleTrait>>,
    pub data_middleware:Vec<DataMiddlewareHandle>,
    pub store_middleware:Vec<DataStoreMiddlewareHandle>,
}

impl From<Arc<dyn ModuleTrait>> for ModuleEntity {
    fn from(module: Arc<dyn ModuleTrait>) -> Self {
        ModuleEntity {
            module_work: module,
            download_middleware: vec![],
            data_middleware: vec![],
            store_middleware: vec![],
        }
    }
}
impl ModuleEntity {
    /// Adds a download middleware module.
    pub fn add_download_middleware(mut self, middleware: Arc<dyn ModuleTrait>)->Self {
        self.download_middleware.push(middleware);
        self
    }

    /// Adds a data middleware implementation.
    pub fn add_data_middleware(mut self, middleware: DataMiddlewareHandle) ->Self{
        self.data_middleware.push(middleware);
        self
    }

    /// Adds a data store middleware implementation.
    pub fn add_store_middleware(mut self, middleware: DataStoreMiddlewareHandle)->Self {
        self.store_middleware.push(middleware);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::cacheable::CacheService;
    use crate::common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream, ToSyncBoxStream};
    use crate::common::model::entity::{AccountModel, PlatformModel};
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{Request, Response};
    use crate::engine::task::module_processor_with_chain::ModuleProcessorWithChain;

    struct LoginRequiredTestModule;

    struct DummyNode;

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            _config: Arc<ModuleConfig>,
            _params: Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> Result<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> Result<TaskOutputEvent> {
            Ok(TaskOutputEvent::default())
        }
    }

    struct DryRunModuleWithStep;

    #[async_trait]
    impl ModuleTrait for LoginRequiredTestModule {
        fn should_login(&self) -> bool {
            true
        }

        fn name(&self) -> String {
            "login_required_test".to_string()
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
    }

    #[async_trait]
    impl ModuleTrait for DryRunModuleWithStep {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> String {
            "dry_run_with_step".to_string()
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

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(DummyNode)]
        }
    }

    fn build_test_module(module_impl: Arc<dyn ModuleTrait>) -> Module {
        let now = chrono::Utc::now().naive_utc();
        Module {
            config: Arc::new(ModuleConfig::default()),
            account: AccountModel {
                id: 1,
                name: "acc".to_string(),
                modules: vec![],
                enabled: true,
                config: serde_json::json!({}),
                priority: 1,
                created_at: now,
                updated_at: now,
            },
            platform: PlatformModel {
                id: 1,
                name: "pf".to_string(),
                description: None,
                base_url: None,
                enabled: true,
                config: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            },
            error_times: 0,
            finished: false,
            data_middleware: vec![],
            download_middleware: vec![],
            module: module_impl,
            locker: false,
            locker_ttl: 0,
            processor: ModuleProcessorWithChain::new(
                "acc-pf-login_required_test",
                Arc::new(CacheService::new(None, "test".to_string(), None, None)),
                Uuid::now_v7(),
                60,
            ),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            pending_ctx: None,
            bound_task_meta: None,
            bound_login_info: None,
        }
    }

    #[test]
    fn bind_task_context_roundtrip() {
        let mut module = build_test_module(Arc::new(LoginRequiredTestModule));
        let mut meta = Map::new();
        meta.insert("k".to_string(), serde_json::json!("v"));
        let login = LoginInfo::default();
        module.bind_task_context(meta.clone(), Some(login.clone()));

        let (bound_meta, bound_login) = module.runtime_task_context();
        assert_eq!(bound_meta.get("k"), Some(&serde_json::json!("v")));
        assert_eq!(bound_login.as_ref().map(|x| x.useragent.clone()), Some(login.useragent));
    }

    #[tokio::test]
    async fn generate_returns_not_login_error_when_login_required_and_missing() {
        let module = build_test_module(Arc::new(LoginRequiredTestModule));
        match module.generate(Map::new(), None).await {
            Ok(_) => panic!("should fail without login info"),
            Err(err) => {
                let msg = err.to_string();
                assert!(msg.contains("module need login"), "unexpected error message: {msg}");
            }
        }
    }

    #[tokio::test]
    async fn dag_dry_run_disabled_returns_ok() {
        let module = build_test_module(Arc::new(LoginRequiredTestModule));
        let result = module.run_dag_preflight_dry_run().await;
        assert!(result.is_ok());
        assert!(!module.should_run_dag_dry_run());
    }

    #[tokio::test]
    async fn dag_dry_run_enabled_with_empty_steps_returns_error() {
        let mut module = build_test_module(Arc::new(LoginRequiredTestModule));
        let mut cfg = (*module.config).clone();
        cfg.module_config = serde_json::json!({
            "module_dag_enabled": true,
            "module_dag_dry_run": true,
            "module_dag_force_linear_compat": true
        });
        module.config = Arc::new(cfg);

        assert!(module.should_run_dag_dry_run());
        let result = module.run_dag_preflight_dry_run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn dag_dry_run_enabled_with_steps_returns_ok() {
        let mut module = build_test_module(Arc::new(DryRunModuleWithStep));
        let mut cfg = (*module.config).clone();
        cfg.module_config = serde_json::json!({
            "module_dag_enabled": true,
            "module_dag_dry_run": true,
            "module_dag_force_linear_compat": true
        });
        module.config = Arc::new(cfg);

        assert!(module.should_run_dag_dry_run());
        let result = module.run_dag_preflight_dry_run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn dag_dry_run_enabled_but_not_in_whitelist_returns_ok_without_running() {
        let mut module = build_test_module(Arc::new(DryRunModuleWithStep));
        let mut cfg = (*module.config).clone();
        cfg.module_config = serde_json::json!({
            "module_dag_enabled": true,
            "module_dag_dry_run": true,
            "module_dag_force_linear_compat": true,
            "module_dag_whitelist": ["other_module"]
        });
        module.config = Arc::new(cfg);

        assert!(!module.should_run_dag_dry_run());
        let result = module.run_dag_preflight_dry_run().await;
        assert!(result.is_ok());
    }

    #[test]
    fn dag_cutover_requested_without_cutover_whitelist_is_not_supported() {
        let mut module = build_test_module(Arc::new(DryRunModuleWithStep));
        let mut cfg = (*module.config).clone();
        cfg.module_config = serde_json::json!({
            "module_dag_enabled": true,
            "module_dag_force_linear_compat": true,
            "module_dag_whitelist": ["dry_run_with_step"],
            "module_dag_execute_primary": true,
            "module_dag_execute_primary_cutover": true
        });
        module.config = Arc::new(cfg);

        let flags = module.dag_runtime_flags();
        assert!(flags.primary_preview);
        assert!(flags.cutover_requested);
        assert!(!flags.cutover_supported);
        assert!(!flags.primary_cutover);
    }

    #[test]
    fn dag_cutover_whitelist_enables_supported_module_cutover() {
        let mut module = build_test_module(Arc::new(DryRunModuleWithStep));
        let mut cfg = (*module.config).clone();
        cfg.module_config = serde_json::json!({
            "module_dag_enabled": true,
            "module_dag_force_linear_compat": true,
            "module_dag_whitelist": ["dry_run_with_step"],
            "module_dag_execute_primary": true,
            "module_dag_execute_primary_cutover": true,
            "module_dag_cutover_whitelist": ["dry_run_with_step"]
        });
        module.config = Arc::new(cfg);

        let flags = module.dag_runtime_flags();
        assert!(flags.primary_preview);
        assert!(flags.cutover_requested);
        assert!(flags.cutover_supported);
        assert!(flags.primary_cutover);
    }
}
