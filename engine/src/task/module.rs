use errors::Result;
use log::warn;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Map;
use std::sync::Arc;
use uuid::Uuid;
use futures::StreamExt;
use common::interface::{DataMiddleware, DataStoreMiddleware, ModuleTrait, SyncBoxStream};
use common::model::entity::{AccountModel, PlatformModel};
use common::model::login_info::LoginInfo;
use common::model::{Cookies, Headers, ModuleConfig, Response,Request};
use common::model::message::ParserData;
use errors::RequestError;
use crate::task::module_processor_with_chain::ModuleProcessorWithChain;

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
    pub pending_ctx: Option<common::model::ExecutionMark>,
    /// Task metadata injected by TaskModuleProcessor.
    pub bound_task_meta: Option<Map<String, serde_json::Value>>,
    /// Login context injected by TaskModuleProcessor.
    pub bound_login_info: Option<LoginInfo>,
}
impl Module {
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
        let node = self.module.add_step().await;
        for n in node {
            self.processor.add_step_node(n).await;
        }
    }
    
    /// Parses response at the routed step and handles terminal lifecycle hook.
    pub async fn parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<ParserData> {
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
        let no_next_task = data.parser_task.is_none();
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
    pub data_middleware:Vec<Arc<dyn DataMiddleware>>,
    pub store_middleware:Vec<Arc<dyn DataStoreMiddleware>>,
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
    pub fn add_data_middleware(mut self, middleware: Arc<dyn DataMiddleware>) ->Self{
        self.data_middleware.push(middleware);
        self
    }

    /// Adds a data store middleware implementation.
    pub fn add_store_middleware(mut self, middleware: Arc<dyn DataStoreMiddleware>)->Self {
        self.store_middleware.push(middleware);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cacheable::CacheService;
    use common::interface::ModuleTrait;
    use common::model::entity::{AccountModel, PlatformModel};
    use crate::task::module_processor_with_chain::ModuleProcessorWithChain;

    struct LoginRequiredTestModule;

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
}
