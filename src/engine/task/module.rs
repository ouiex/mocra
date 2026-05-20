use crate::common::interface::{
    DataMiddlewareHandle, DataStoreMiddlewareHandle, ModuleTrait, SyncBoxStream,
};
use crate::common::model::entity::{AccountModel, PlatformModel};
use crate::common::model::login_info::LoginInfo;
use crate::common::model::{
    Cookies, ExecutionMeta, Headers, ModuleConfig, NodeInput, PayloadCodec, Request, Response,
    RoutingMeta, TaskProfileSnapshot, TypedEnvelope, WorkflowDefinition,
};
use crate::common::response_cache::apply_request_response_cache_policy;
use crate::engine::task::module_dag_orchestrator::ModuleDagOrchestrator;
use crate::engine::task::module_dag_processor::ModuleDagProcessor;
use crate::engine::task::module_node_dag_adapter::scheduler_parser_error_message;
use crate::engine::task::module_node_runtime_bridge::{
    decode_parser_output_payload, decode_request_batch_payload,
    SchedulerNodeGenerateRuntimeInput, SchedulerNodeParserRuntimeInput,
};
use crate::engine::task::parser_error_adapter::TypedParserOutput;
use crate::errors::ModuleError;
use crate::errors::RequestError;
use crate::errors::Result;
use futures::StreamExt;
use log::warn;
use crate::schedule::dag::DagNodeDispatcher;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Map;
use std::sync::Arc;
use uuid::Uuid;

/// Runtime module instance bound to account/platform context.
///
/// A Module aggregates static module behavior, resolved configuration,
/// middleware bindings, and DAG-runtime metadata.
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
    /// Queue-backed DAG processor for node generation and parsing.
    pub processor: ModuleDagProcessor,
    /// Optional scheduler dispatcher used by the generate bridge.
    pub dag_dispatcher: Option<Arc<dyn DagNodeDispatcher>>,
    /// Run identifier for cross-stage scoping.
    pub run_id: Uuid,
    /// Prefix request for fallback tracing.
    pub prefix_request: Uuid,
    /// Optional execution context for precise node targeting.
    pub pending_ctx: Option<crate::common::model::ExecutionMark>,
    /// Task metadata injected by TaskModuleProcessor.
    pub bound_task_meta: Option<Map<String, serde_json::Value>>,
    /// Login context injected by TaskModuleProcessor.
    pub bound_login_info: Option<LoginInfo>,
    /// Loaded target-state profile snapshot for lifecycle hooks and future runtime cutover.
    pub profile: Option<Arc<TaskProfileSnapshot>>,
    /// Compiled workflow definition aligned with the loaded profile.
    pub workflow: Option<Arc<WorkflowDefinition>>,
}
impl Module {
    fn profile_version(&self) -> u64 {
        self.profile
            .as_ref()
            .map(|profile| profile.version)
            .unwrap_or_default()
    }

    fn dag_version(&self) -> String {
        self.workflow
            .as_ref()
            .and_then(|workflow| workflow.metadata.get("dag_version").cloned())
            .unwrap_or_default()
    }

    fn bind_execution_task_meta(
        &self,
        mut task_meta: Map<String, serde_json::Value>,
    ) -> Map<String, serde_json::Value> {
        task_meta.insert(
            "profile_version".to_string(),
            serde_json::Value::from(self.profile_version()),
        );
        task_meta.insert(
            "dag_version".to_string(),
            serde_json::Value::String(self.dag_version()),
        );
        task_meta.insert(
            "run_id".to_string(),
            serde_json::Value::String(self.run_id.to_string()),
        );
        task_meta
    }

    fn build_scheduler_generate_runtime_input(
        &self,
        node_id: &str,
        task_meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SchedulerNodeGenerateRuntimeInput> {
        let profile = self.profile.as_ref().ok_or_else(|| {
            ModuleError::Model(
                std::io::Error::other(format!(
                    "scheduler generate requires loaded profile for node '{}' in module '{}'",
                    node_id,
                    self.module.name()
                ))
                .into(),
            )
        })?;
        let Some(resolved_config) = profile.resolve_node_config(node_id) else {
            return Err(ModuleError::Model(
                std::io::Error::other(format!(
                    "profile node config missing for scheduler generate node '{}'",
                    node_id
                ))
                .into(),
            )
            .into());
        };
        let now_ms = chrono::Utc::now().timestamp_millis();
        Ok(SchedulerNodeGenerateRuntimeInput {
            routing: RoutingMeta {
                namespace: String::new(),
                account: self.account.name.clone(),
                platform: self.platform.name.clone(),
                module: self.module.name().to_string(),
                node_key: node_id.to_string(),
                run_id: self.run_id,
                request_id: Uuid::now_v7(),
                parent_request_id: (!self.prefix_request.is_nil()).then_some(self.prefix_request),
                priority: Default::default(),
            },
            exec: ExecutionMeta {
                profile_version: resolved_config.profile_version,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                ..ExecutionMeta::default()
            },
            config: resolved_config,
            input: NodeInput::new(
                node_id,
                TypedEnvelope::new(
                    "mocra.node_input.v1",
                    1,
                    PayloadCodec::Json,
                    serde_json::to_vec(&serde_json::Value::Object(task_meta)).unwrap_or_default(),
                ),
            ),
            login_info,
        })
    }

    fn build_scheduler_parse_runtime_input(
        &self,
        node_id: &str,
        response: &Response,
    ) -> Result<SchedulerNodeParserRuntimeInput> {
        let profile = self.profile.as_ref().ok_or_else(|| {
            ModuleError::Model(
                std::io::Error::other(format!(
                    "scheduler parser requires loaded profile for node '{}' in module '{}'",
                    node_id,
                    self.module.name()
                ))
                .into(),
            )
        })?;
        let Some(resolved_config) = profile.resolve_node_config(node_id) else {
            return Err(ModuleError::Model(
                std::io::Error::other(format!(
                    "profile node config missing for scheduler parser node '{}'",
                    node_id
                ))
                .into(),
            )
            .into());
        };
        let now_ms = chrono::Utc::now().timestamp_millis();
        Ok(SchedulerNodeParserRuntimeInput {
            routing: RoutingMeta {
                namespace: String::new(),
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: response.module.clone(),
                node_key: node_id.to_string(),
                run_id: response.run_id,
                request_id: response.id,
                parent_request_id: (!response.prefix_request.is_nil())
                    .then_some(response.prefix_request),
                priority: response.priority,
            },
            exec: ExecutionMeta {
                task_retry_count: response.task_retry_times as u32,
                profile_version: resolved_config.profile_version,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                ..ExecutionMeta::default()
            },
            config: resolved_config,
            response: response.clone(),
            login_info: None,
        })
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
    /// Runs the target DAG node through the scheduler bridge, then enriches each request
    /// with module/account/platform identity, middleware, config payloads, and run markers.
    pub async fn generate(
        &self,
        task_meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let bound_task_meta = self.bind_execution_task_meta(task_meta);

        if self.module.should_login() && login_info.is_none() {
            return Err(RequestError::NotLogin("module need login".into()).into());
        }
        let (request_stream, generated_context) = if let Some((dag, node_id, hint)) = self
            .processor
            .compile_generate_node_dag(&self.pending_ctx)
            .await
            .map_err(|err| ModuleError::Model(err.into()))?
        {
            let requires_remote_dispatch = matches!(
                hint.as_ref().and_then(|value| value.placement.clone()),
                Some(crate::schedule::dag::NodePlacement::Remote { .. })
            );
            if requires_remote_dispatch && self.dag_dispatcher.is_none() {
                return Err(ModuleError::Model(
                    std::io::Error::other(format!(
                        "remote node '{}' requires dag_dispatcher but none configured",
                        node_id
                    ))
                    .into(),
                )
                .into());
            } else {
                let generated_context = {
                    let mut mark = self.pending_ctx.clone().unwrap_or_default();
                    mark.node_id = Some(node_id.clone());
                    if mark.module_id.is_none() {
                        mark.module_id = Some(self.id());
                    }
                    mark
                };
                let runtime_input = self.build_scheduler_generate_runtime_input(
                    &node_id,
                    bound_task_meta.clone(),
                    login_info.clone(),
                )?;
                let bridge_result = ModuleDagOrchestrator::default()
                    .execute_dag_with_generate_runtime_input_and_dispatcher(
                        dag,
                        &node_id,
                        &runtime_input,
                        Vec::new(),
                        self.dag_dispatcher.clone(),
                    )
                    .await
                    .map_err(|err| ModuleError::Model(err.into()))
                    .and_then(|report| {
                        decode_request_batch_payload(
                            report.outputs.get(&node_id).ok_or_else(|| {
                                ModuleError::Model(
                                    std::io::Error::other(format!(
                                        "scheduler generate output missing for node '{}'",
                                        node_id
                                    ))
                                    .into(),
                                )
                            })?,
                        )
                        .map_err(|err| ModuleError::Model(err.into()))
                    });

                match bridge_result {
                    Ok(requests) => (
                        Box::pin(futures::stream::iter(requests))
                            as SyncBoxStream<'static, Request>,
                        Some(generated_context),
                    ),
                    Err(err) => return Err(err.into()),
                }
            }
        } else if self.pending_ctx.is_some() {
            let unresolved_node_id = self
                .pending_ctx
                .as_ref()
                .and_then(|value| value.node_id.clone())
                .unwrap_or_else(|| "<none>".to_string());
            let unresolved_step_idx = self
                .pending_ctx
                .as_ref()
                .and_then(|value| value.step_idx)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string());
            return Err(ModuleError::Model(
                std::io::Error::other(format!(
                    "scheduler generate cutover could not resolve target node (node_id='{}', step_idx={})",
                    unresolved_node_id, unresolved_step_idx
                ))
                .into(),
            )
            .into());
        } else {
            (
                Box::pin(futures::stream::empty::<Request>())
                    as SyncBoxStream<'static, Request>,
                None,
            )
        };

        let module_name = self.module.name().to_string();
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
        let profile = self.profile.clone();
        let trait_common = self.module.default_common_config();
        let generated_context = generated_context.clone();
        let stream = request_stream.map(move |mut request| {
            if request.context.node_id.is_none()
                && let Some(context) = generated_context.clone()
            {
                request.context = context;
            }
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
            request.run_id = run_id;
            request.prefix_request = prefix_request;

            if request.headers.is_empty() && !headers.is_empty() {
                request = request.with_headers(headers.clone());
            }
            if !cookies.is_empty() {
                request = request.with_cookies(cookies.clone());
            }

            if let Some(ref info) = login_info {
                let cookies = Cookies::from(info);
                let headers = Headers::from(info);
                request.headers.merge(&headers);
                request.cookies.merge(&cookies);
                request = request.with_login_info(info);
            }
            request.limit_id = limit_id.clone().unwrap_or(request.module_id());
            request = request
                .with_module_config(&config)
                .with_task_config(bound_task_meta.clone());
            if let Some(downloader) = config.get_config::<String>("downloader") {
                request.downloader = downloader;
            } else {
                request.downloader = "request_downloader".to_string();
            }
            let response_cache_common = request
                .context
                .node_id
                .as_deref()
                .and_then(|node_id| {
                    profile
                        .as_ref()
                        .and_then(|profile| profile.resolve_node_config(node_id))
                        .map(|resolved| resolved.common)
                })
                .unwrap_or_else(|| trait_common.clone());
            request = apply_request_response_cache_policy(request, &response_cache_common);
            log::debug!(
                "[Module] request prepared: account={} platform={} module={} url={} request_id={}",
                request.account,
                request.platform,
                request.module,
                request.url,
                request.id
            );
            request
        });
        Ok(Box::pin(stream))
    }

    /// Builds the merged DAG definition and initializes the `ModuleDagProcessor`.
    ///
    /// Merges `dag_definition()` (custom graph) with `add_step()` (linear steps) when both
    /// are provided, following `ModuleDagOrchestrator::compile_module` priority rules.
    pub async fn add_step(&self) {
        if let Err(e) = self.module.pre_process(self.profile.clone()).await {
            warn!(
                "module pre_process failed: account={} platform={} module={} error={}",
                self.account.name,
                self.platform.name,
                self.module.name(),
                e
            );
        }

        use crate::engine::task::module_dag_orchestrator::ModuleDagOrchestrator;
        let orchestrator = ModuleDagOrchestrator::default();
        let definition = orchestrator.build_definition(self.module.clone()).await;
        self.processor
            .set_default_common_config(self.module.default_common_config())
            .await;
        self.processor.init_from_definition(&definition).await;
    }

    /// Parses response at the routed DAG node and handles terminal lifecycle hook.
    pub async fn parser(
        &self,
        response: Response,
    ) -> Result<TypedParserOutput> {
        let mut data = if let Some((dag, node_id, hint)) = self
            .processor
            .compile_parse_node_dag(&response)
            .await
            .map_err(|err| ModuleError::Model(err.into()))?
        {
            let requires_remote_dispatch = matches!(
                hint.as_ref().and_then(|value| value.placement.clone()),
                Some(crate::schedule::dag::NodePlacement::Remote { .. })
            );
            if requires_remote_dispatch && self.dag_dispatcher.is_none() {
                return Err(ModuleError::Model(
                    std::io::Error::other(format!(
                        "remote node '{}' requires dag_dispatcher but none configured",
                        node_id
                    ))
                    .into(),
                )
                .into());
            }

            let runtime_input = self.build_scheduler_parse_runtime_input(&node_id, &response)?;

            match ModuleDagOrchestrator::default()
                .execute_dag_with_parser_runtime_input_and_dispatcher(
                    dag,
                    &node_id,
                    &runtime_input,
                    Vec::new(),
                    self.dag_dispatcher.clone(),
                )
                .await
            {
                Ok(report) => {
                    let parsed = decode_parser_output_payload(
                        report.outputs.get(&node_id).ok_or_else(|| {
                            ModuleError::Model(
                                std::io::Error::other(format!(
                                    "scheduler parser output missing for node '{}'",
                                    node_id
                                ))
                                .into(),
                            )
                        })?,
                    )
                    .map_err(|err| ModuleError::Model(err.into()))?;

                    if self.processor.check_stop().await? {
                        return Ok(TypedParserOutput::default());
                    }

                    self.processor
                        .route_parsed_output(
                            response,
                            node_id,
                            parsed.into_node_parse_output(),
                        )
                        .await?
                }
                Err(err) => {
                    if let Some((_, error_message)) = scheduler_parser_error_message(&err) {
                        if self.processor.check_stop().await? {
                            return Ok(TypedParserOutput::default());
                        }
                        self.processor
                            .execute_parse_with_node_error(response, &node_id, error_message)
                            .await?
                    } else {
                        return Err(ModuleError::Model(err.into()).into());
                    }
                }
            }
        } else {
            let unresolved_node_id = response
                .context
                .node_id
                .clone()
                .unwrap_or_else(|| "<none>".to_string());
            let unresolved_step_idx = response
                .context
                .step_idx
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string());
            return Err(ModuleError::Model(
                std::io::Error::other(format!(
                    "scheduler parser cutover could not resolve target node (node_id='{}', step_idx={})",
                    unresolved_node_id, unresolved_step_idx
                ))
                .into(),
            )
            .into());
        };

        // Enrich returned data with module/account/platform identifiers.
        for d in data.data.iter_mut() {
            d.module = self.module.name().to_string();
            d.account = self.account.name.clone();
            d.platform = self.platform.name.clone();
        }

        // Run post_process when at a leaf node with no pending next tasks.
        let no_next_task = data.next_dispatches.is_empty();
        if no_next_task {
            self.module.post_process(self.profile.clone()).await?;
        }

        // When the DAG signals an explicit stop, clean up the session using Module.run_id
        // (the correctly-patched run_id from the task event).
        // NOTE: ModuleDagProcessor.run_id may be stale when loaded from the factory cache
        // since factory.load_parser_model / load_error_model update m.run_id but not
        // m.processor.run_id. Using self.run_id here ensures the correct session key.
        if data.stop {
            self.processor.delete_session_for_run(self.run_id).await;
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
pub struct ModuleEntity {
    pub module_work: Arc<dyn ModuleTrait>,
    pub download_middleware: Vec<Arc<dyn ModuleTrait>>,
    pub data_middleware: Vec<DataMiddlewareHandle>,
    pub store_middleware: Vec<DataStoreMiddlewareHandle>,
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
    pub fn add_download_middleware(mut self, middleware: Arc<dyn ModuleTrait>) -> Self {
        self.download_middleware.push(middleware);
        self
    }

    /// Adds a data middleware implementation.
    pub fn add_data_middleware(mut self, middleware: DataMiddlewareHandle) -> Self {
        self.data_middleware.push(middleware);
        self
    }

    /// Adds a data store middleware implementation.
    pub fn add_store_middleware(mut self, middleware: DataStoreMiddlewareHandle) -> Self {
        self.store_middleware.push(middleware);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use super::*;
    use crate::cacheable::CacheService;
    use crate::common::interface::{
        ModuleNodeTrait, ModuleTrait, NodeGenerateContext, NodeParseContext, ToSyncBoxStream,
    };
    use crate::common::model::data::DataEvent;
    use crate::common::model::entity::{AccountModel, PlatformModel};
    use crate::common::model::{
        ExecutionMark, NodeParseOutput, Priority, Request, ResolvedCommonConfig, Response,
    };
    use crate::common::response_cache::{RESPONSE_CACHE_EXPIRES_AT_KEY, current_time_ms};
    use crate::engine::task::module_dag_compiler::{ModuleDagDefinition, ModuleDagNodeDef};
    use crate::engine::task::module_dag_processor::ModuleDagProcessor;
    use crate::schedule::dag::{
        DagError, DagNodeDispatcher, DagNodeTrait, NodeExecutionContext, NodePlacement,
        TaskPayload,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct LoginRequiredTestModule;
    struct SingleRequestTestNode;
    struct CommonConfigEchoNode;
    struct EmptyDagTestModule;
    struct SingleRequestTestModule;
    struct CacheEnabledSingleRequestTestModule;
    struct CommonConfigEchoModule;
    struct RemoteRequestTestModule;
    struct ParseOutputTestNode;
    struct ParseOutputTestModule;
    struct FailingParseTestNode;
    struct FailingParseTestModule;
    struct RemoteFailingParseTestModule;
    struct FanoutPageTestNode;
    struct FanoutLeafTestNode(&'static str);
    struct FanoutParseTestModule;
    struct FanoutFailingParseTestModule;
    struct RemoteFanoutParseTestModule;
    struct RemoteFanoutFailingParseTestModule;
    struct StopParseTestNode;
    struct StopParseTestModule;
    struct RemoteStopParseTestModule;
    struct FailingDispatcher;
    struct NetworkFailingDispatcher;

    #[derive(Default)]
    struct RecordingRemoteDispatcher {
        remote_dispatches: AtomicUsize,
    }

    #[async_trait]
    impl DagNodeDispatcher for RecordingRemoteDispatcher {
        fn backend_name(&self) -> &'static str {
            "recording_remote"
        }

        fn supports_placement(&self, _placement: &NodePlacement) -> bool {
            true
        }

        async fn dispatch(
            &self,
            _node_id: &str,
            placement: &NodePlacement,
            executor: Arc<dyn DagNodeTrait>,
            context: NodeExecutionContext,
        ) -> std::result::Result<TaskPayload, DagError> {
            if matches!(placement, NodePlacement::Remote { .. }) {
                self.remote_dispatches.fetch_add(1, Ordering::SeqCst);
            }
            executor.start(context).await
        }
    }

    #[async_trait]
    impl DagNodeDispatcher for FailingDispatcher {
        fn backend_name(&self) -> &'static str {
            "failing_dispatcher"
        }

        fn supports_placement(&self, _placement: &NodePlacement) -> bool {
            true
        }

        async fn dispatch(
            &self,
            _node_id: &str,
            _placement: &NodePlacement,
            _executor: Arc<dyn DagNodeTrait>,
            _context: NodeExecutionContext,
        ) -> std::result::Result<TaskPayload, DagError> {
            Err(DagError::NodeExecutionFailed {
                node_id: "page".to_string(),
                reason: "forced scheduler bridge failure".to_string(),
            })
        }
    }

    #[async_trait]
    impl DagNodeDispatcher for NetworkFailingDispatcher {
        fn backend_name(&self) -> &'static str {
            "network_failing_dispatcher"
        }

        fn supports_placement(&self, _placement: &NodePlacement) -> bool {
            true
        }

        async fn dispatch(
            &self,
            _node_id: &str,
            _placement: &NodePlacement,
            _executor: Arc<dyn DagNodeTrait>,
            _context: NodeExecutionContext,
        ) -> std::result::Result<TaskPayload, DagError> {
            Err(DagError::NodeExecutionFailed {
                node_id: "page".to_string(),
                reason: "network timeout while dispatching remote node".to_string(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for EmptyDagTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "empty_dag_test"
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
    impl ModuleNodeTrait for SingleRequestTestNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            Ok(vec![Request::new("https://example.com/page", "GET")].to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for ParseOutputTestNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default().with_data(DataEvent::default()))
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for FailingParseTestNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Err(RequestError::InvalidParams(Box::new(std::io::Error::other(
                "forced parser node error",
            )))
            .into())
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for FanoutPageTestNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for FanoutLeafTestNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            self.0
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for StopParseTestNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default().finish())
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for CommonConfigEchoNode {
        async fn generate(
            &self,
            ctx: NodeGenerateContext<'_>,
        ) -> Result<crate::common::interface::SyncBoxStream<'static, Request>> {
            let request = Request::new("https://example.com/common", "GET")
                .add_meta("ctx_cache_enabled", ctx.config.common.response_cache_enabled)
                .add_meta(
                    "ctx_cache_ttl_secs",
                    ctx.config.common.response_cache_ttl_secs,
                );
            Ok(vec![request].to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    #[async_trait]
    impl ModuleTrait for LoginRequiredTestModule {
        fn should_login(&self) -> bool {
            true
        }

        fn name(&self) -> &'static str {
            "login_required_test"
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
    impl ModuleTrait for SingleRequestTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "single_request_test"
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
            vec![Arc::new(SingleRequestTestNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for CacheEnabledSingleRequestTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "cache_enabled_single_request_test"
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

        fn response_cache_enabled(&self) -> bool {
            true
        }

        fn response_cache_ttl_secs(&self) -> Option<u64> {
            Some(60)
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(SingleRequestTestNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for CommonConfigEchoModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "common_config_echo_test"
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

        fn response_cache_enabled(&self) -> bool {
            true
        }

        fn response_cache_ttl_secs(&self) -> Option<u64> {
            Some(60)
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(CommonConfigEchoNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for RemoteRequestTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "remote_request_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![ModuleDagNodeDef {
                    node_id: "page".to_string(),
                    node: Arc::new(SingleRequestTestNode),
                    placement_override: Some(NodePlacement::remote("wg-test")),
                    policy_override: None,
                    tags: vec![],
                }],
                edges: vec![],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for ParseOutputTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "parser_data_test"
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
            vec![Arc::new(ParseOutputTestNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for FailingParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "failing_parser_test"
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
            vec![Arc::new(FailingParseTestNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for RemoteFailingParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "remote_failing_parser_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![ModuleDagNodeDef {
                    node_id: "page".to_string(),
                    node: Arc::new(FailingParseTestNode),
                    placement_override: Some(NodePlacement::remote("wg-test")),
                    policy_override: None,
                    tags: vec![],
                }],
                edges: vec![],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for FanoutParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "fanout_parser_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![
                    ModuleDagNodeDef {
                        node_id: "page".to_string(),
                        node: Arc::new(FanoutPageTestNode),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "detail".to_string(),
                        node: Arc::new(FanoutLeafTestNode("detail")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "reviews".to_string(),
                        node: Arc::new(FanoutLeafTestNode("reviews")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                ],
                edges: vec![
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "detail".to_string(),
                    },
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "reviews".to_string(),
                    },
                ],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for FanoutFailingParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "fanout_failing_parser_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![
                    ModuleDagNodeDef {
                        node_id: "page".to_string(),
                        node: Arc::new(FailingParseTestNode),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "detail".to_string(),
                        node: Arc::new(FanoutLeafTestNode("detail")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "reviews".to_string(),
                        node: Arc::new(FanoutLeafTestNode("reviews")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                ],
                edges: vec![
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "detail".to_string(),
                    },
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "reviews".to_string(),
                    },
                ],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for RemoteFanoutParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "remote_fanout_parser_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![
                    ModuleDagNodeDef {
                        node_id: "page".to_string(),
                        node: Arc::new(FanoutPageTestNode),
                        placement_override: Some(NodePlacement::remote("wg-test")),
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "detail".to_string(),
                        node: Arc::new(FanoutLeafTestNode("detail")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "reviews".to_string(),
                        node: Arc::new(FanoutLeafTestNode("reviews")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                ],
                edges: vec![
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "detail".to_string(),
                    },
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "reviews".to_string(),
                    },
                ],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for RemoteFanoutFailingParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "remote_fanout_failing_parser_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![
                    ModuleDagNodeDef {
                        node_id: "page".to_string(),
                        node: Arc::new(FailingParseTestNode),
                        placement_override: Some(NodePlacement::remote("wg-test")),
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "detail".to_string(),
                        node: Arc::new(FanoutLeafTestNode("detail")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "reviews".to_string(),
                        node: Arc::new(FanoutLeafTestNode("reviews")),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                ],
                edges: vec![
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "detail".to_string(),
                    },
                    crate::engine::task::module_dag_compiler::ModuleDagEdgeDef {
                        from: "page".to_string(),
                        to: "reviews".to_string(),
                    },
                ],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for StopParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "stop_parser_test"
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
            vec![Arc::new(StopParseTestNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for RemoteStopParseTestModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "remote_stop_parser_test"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![ModuleDagNodeDef {
                    node_id: "page".to_string(),
                    node: Arc::new(StopParseTestNode),
                    placement_override: Some(NodePlacement::remote("wg-test")),
                    policy_override: None,
                    tags: vec![],
                }],
                edges: vec![],
                entry_nodes: vec!["page".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    fn build_test_module(
        module_impl: Arc<dyn ModuleTrait>,
        dag_dispatcher: Option<Arc<dyn DagNodeDispatcher>>,
    ) -> Module {
        let now = chrono::Utc::now().naive_utc();
        let module_name = module_impl.name().to_string();
        let default_common = module_impl.default_common_config();
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
            processor: ModuleDagProcessor::new(
                "acc-pf-login_required_test".to_string(),
                Arc::new(CacheService::new(None, "test".to_string(), None, None)),
                Uuid::now_v7(),
                60,
            ),
            dag_dispatcher,
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            pending_ctx: None,
            bound_task_meta: None,
            bound_login_info: None,
            profile: Some(build_test_profile_for_nodes(
                &module_name,
                &["page", "detail", "reviews", "step_0"],
                1,
                default_common,
            )),
            workflow: Some(build_test_workflow()),
        }
    }

    fn build_test_response(module_name: &str, node_id: &str) -> Response {
        Response {
            id: Uuid::now_v7(),
            platform: "pf".to_string(),
            account: "acc".to_string(),
            module: module_name.to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: vec![],
            storage_path: None,
            headers: vec![],
            task_retry_times: 0,
            metadata: Default::default(),
            download_middleware: vec![],
            data_middleware: vec![],
            task_finished: false,
            context: ExecutionMark::default().with_node_id(node_id),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            request_hash: None,
            priority: Priority::Normal,
        }
    }

    fn build_test_workflow() -> Arc<WorkflowDefinition> {
        Arc::new(WorkflowDefinition {
            metadata: BTreeMap::from([(
                "dag_version".to_string(),
                "test-dag-version".to_string(),
            )]),
            ..WorkflowDefinition::default()
        })
    }

    fn build_test_profile_for_nodes(
        module_name: &str,
        node_ids: &[&str],
        version: u64,
        common: ResolvedCommonConfig,
    ) -> Arc<TaskProfileSnapshot> {
        Arc::new(TaskProfileSnapshot {
            namespace: "test".to_string(),
            account: "acc".to_string(),
            platform: "pf".to_string(),
            module: module_name.to_string(),
            version,
            common,
            node_configs: node_ids
                .iter()
                .map(|node_id| {
                    (
                        (*node_id).to_string(),
                        TypedEnvelope::new(
                            "typed.node_config",
                            1,
                            PayloadCodec::Json,
                            b"{}".to_vec(),
                        ),
                    )
                })
                .collect(),
            ..TaskProfileSnapshot::default()
        })
    }

    fn build_test_profile(module_name: &str, node_id: &str, version: u64) -> Arc<TaskProfileSnapshot> {
        build_test_profile_for_nodes(
            module_name,
            &[node_id],
            version,
            ResolvedCommonConfig::default(),
        )
    }

    #[test]
    fn bind_task_context_roundtrip() {
        let mut module = build_test_module(Arc::new(LoginRequiredTestModule), None);
        let mut meta = Map::new();
        meta.insert("k".to_string(), serde_json::json!("v"));
        let login = LoginInfo::default();
        module.bind_task_context(meta.clone(), Some(login.clone()));

        let (bound_meta, bound_login) = module.runtime_task_context();
        assert_eq!(bound_meta.get("k"), Some(&serde_json::json!("v")));
        assert_eq!(
            bound_login.as_ref().map(|x| x.useragent.clone()),
            Some(login.useragent)
        );
    }

    #[test]
    fn scheduler_generate_runtime_input_prefers_profile_node_config() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.profile = Some(build_test_profile(module.module.name(), "page", 9));

        let runtime_input = module.build_scheduler_generate_runtime_input("page", Map::new(), None).expect("typed generate runtime input should build");

        assert_eq!(runtime_input.config.profile_version, 9);
        assert_eq!(runtime_input.config.node_config.schema_id, "typed.node_config");
        assert_eq!(runtime_input.exec.profile_version, 9);
    }

    #[test]
    fn scheduler_parse_runtime_input_prefers_profile_node_config() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.profile = Some(build_test_profile(module.module.name(), "page", 11));
        let response = build_test_response(module.module.name(), "page");

        let runtime_input = module.build_scheduler_parse_runtime_input("page", &response).expect("typed parser runtime input should build");

        assert_eq!(runtime_input.config.profile_version, 11);
        assert_eq!(runtime_input.config.node_config.schema_id, "typed.node_config");
        assert_eq!(runtime_input.exec.profile_version, 11);
    }

    #[test]
    fn scheduler_generate_runtime_input_errors_when_profile_missing() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.profile = None;

        let err = module
            .build_scheduler_generate_runtime_input("page", Map::new(), None)
            .expect_err("missing generate profile should fail closed");

        assert!(
            err.to_string()
                .contains("scheduler generate requires loaded profile for node 'page'"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn scheduler_parse_runtime_input_errors_when_profile_missing() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.profile = None;
        let response = build_test_response(module.module.name(), "page");

        let err = module
            .build_scheduler_parse_runtime_input("page", &response)
            .expect_err("missing parser profile should fail closed");

        assert!(
            err.to_string()
                .contains("scheduler parser requires loaded profile for node 'page'"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn scheduler_generate_runtime_input_errors_when_profile_node_config_missing() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.profile = Some(build_test_profile(module.module.name(), "other", 9));

        let err = module
            .build_scheduler_generate_runtime_input("page", Map::new(), None)
            .expect_err("missing typed generate node config should fail");

        assert!(
            err.to_string()
                .contains("profile node config missing for scheduler generate node 'page'"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn scheduler_parse_runtime_input_errors_when_profile_node_config_missing() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.profile = Some(build_test_profile(module.module.name(), "other", 11));
        let response = build_test_response(module.module.name(), "page");

        let err = module
            .build_scheduler_parse_runtime_input("page", &response)
            .expect_err("missing typed parser node config should fail");

        assert!(
            err.to_string()
                .contains("profile node config missing for scheduler parser node 'page'"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn generate_returns_not_login_error_when_login_required_and_missing() {
        let module = build_test_module(Arc::new(LoginRequiredTestModule), None);
        match module.generate(Map::new(), None).await {
            Ok(_) => panic!("should fail without login info"),
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("module need login"),
                    "unexpected error message: {msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn generate_returns_requests_for_single_node_module() {
        let module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.add_step().await;

        let requests: Vec<Request> = module
            .generate(Map::new(), None)
            .await
            .expect("generate should succeed")
            .collect()
            .await;

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://example.com/page");
        assert_eq!(requests[0].context.node_id.as_deref(), Some("page"));
    }

    #[tokio::test]
    async fn generate_with_empty_dag_uses_scheduler_bridge_only() {
        let module = build_test_module(Arc::new(EmptyDagTestModule), None);
        module.add_step().await;

        let requests: Vec<Request> = module
            .generate(Map::new(), None)
            .await
            .expect("empty dag should not fail")
            .collect()
            .await;

        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn generate_applies_response_cache_policy_from_module_trait_defaults() {
        let module = build_test_module(Arc::new(CacheEnabledSingleRequestTestModule), None);
        module.add_step().await;

        let requests: Vec<Request> = module
            .generate(Map::new(), None)
            .await
            .expect("generate should succeed")
            .collect()
            .await;

        assert_eq!(requests.len(), 1);
        assert!(requests[0].enable_response_cache);
        let expires_at = requests[0]
            .meta
            .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY)
            .expect("request should carry explicit cache expiry from module defaults");
        assert!(expires_at > current_time_ms());
    }

    #[tokio::test]
    async fn generate_context_common_preserves_module_trait_defaults_through_profile_path() {
        let module = build_test_module(Arc::new(CommonConfigEchoModule), None);
        module.add_step().await;

        let requests: Vec<Request> = module
            .generate(Map::new(), None)
            .await
            .expect("generate should succeed")
            .collect()
            .await;

        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].meta.get_trait_config::<bool>("ctx_cache_enabled"),
            Some(true)
        );
        assert_eq!(
            requests[0].meta.get_trait_config::<Option<u64>>("ctx_cache_ttl_secs"),
            Some(Some(60))
        );
    }

    #[tokio::test]
    async fn generate_uses_remote_dispatcher_for_remote_placement() {
        let dispatcher = Arc::new(RecordingRemoteDispatcher::default());
        let module = build_test_module(
            Arc::new(RemoteRequestTestModule),
            Some(dispatcher.clone() as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let requests: Vec<Request> = module
            .generate(Map::new(), None)
            .await
            .expect("generate should succeed through remote dispatcher")
            .collect()
            .await;

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://example.com/page");
        assert_eq!(requests[0].context.node_id.as_deref(), Some("page"));
        assert_eq!(dispatcher.remote_dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn parser_uses_remote_dispatcher_for_remote_placement() {
        let dispatcher = Arc::new(RecordingRemoteDispatcher::default());
        let module = build_test_module(
            Arc::new(RemoteRequestTestModule),
            Some(dispatcher.clone() as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let _ = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("parser should succeed through remote dispatcher");

        assert_eq!(dispatcher.remote_dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn generate_fails_when_remote_placement_has_no_dispatcher() {
        let module = build_test_module(Arc::new(RemoteRequestTestModule), None);
        module.add_step().await;

        let err = match module.generate(Map::new(), None).await {
            Ok(_) => panic!("generate should fail when remote placement has no dispatcher"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("requires dag_dispatcher but none configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn parser_fails_when_remote_placement_has_no_dispatcher() {
        let module = build_test_module(Arc::new(RemoteRequestTestModule), None);
        module.add_step().await;

        let err = match module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
        {
            Ok(_) => panic!("parser should fail when remote placement has no dispatcher"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("requires dag_dispatcher but none configured"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn generate_fails_when_remote_dispatcher_execution_fails() {
        let module = build_test_module(
            Arc::new(RemoteRequestTestModule),
            Some(Arc::new(FailingDispatcher) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let err = match module.generate(Map::new(), None).await {
            Ok(_) => panic!("generate should fail-closed when remote dispatcher execution fails"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("forced scheduler bridge failure"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn parser_fails_when_remote_dispatcher_execution_fails() {
        let module = build_test_module(
            Arc::new(RemoteRequestTestModule),
            Some(Arc::new(FailingDispatcher) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let err = match module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
        {
            Ok(_) => panic!("parser should fail-closed when remote dispatcher execution fails"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("forced scheduler bridge failure"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn parser_fails_when_remote_dispatcher_network_fails() {
        let module = build_test_module(
            Arc::new(RemoteRequestTestModule),
            Some(Arc::new(NetworkFailingDispatcher) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let err = match module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
        {
            Ok(_) => panic!("parser should fail-closed when remote network dispatch fails"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("network timeout while dispatching remote node"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn generate_fails_when_remote_dispatcher_network_fails() {
        let module = build_test_module(
            Arc::new(RemoteRequestTestModule),
            Some(Arc::new(NetworkFailingDispatcher) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let err = match module.generate(Map::new(), None).await {
            Ok(_) => panic!("generate should fail-closed when remote network dispatch fails"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("network timeout while dispatching remote node"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn parser_succeeds_without_dispatcher_for_local_node_when_output_is_bridge_supported() {
        let module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("parser should succeed through local scheduler execution");

        assert!(result.data.is_empty());
        assert!(result.next_dispatches.is_empty());
    }

    #[tokio::test]
    async fn parser_succeeds_without_dispatcher_for_local_node_when_output_contains_data() {
        let module = build_test_module(Arc::new(ParseOutputTestModule), None);
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("parser with data should succeed through local scheduler execution");

        assert_eq!(result.data.len(), 1);
        assert!(result.next_dispatches.is_empty());
    }

    #[tokio::test]
    async fn parser_fails_closed_when_scheduler_bridge_fails_for_local_node() {
        let module = build_test_module(
            Arc::new(ParseOutputTestModule),
            Some(Arc::new(FailingDispatcher) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let err = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect_err("parser should fail-closed when scheduler bridge fails");

        assert!(
            err.to_string().contains("forced scheduler bridge failure"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn parser_node_error_yields_same_node_error_seed_through_local_scheduler_bridge() {
        let module = build_test_module(Arc::new(FailingParseTestModule), None);
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("parser node error should be converted into error seed");

        let error = result.error.expect("error seed should be present");
        assert!(result.next_dispatches.is_empty());
        assert_eq!(error.context.node_id.as_deref(), Some("page"));
        assert_eq!(error.context.step_idx, Some(0));
        assert!(error.context.stay_current_step);
        assert!(error.error_message.contains("forced parser node error"));
    }

    #[tokio::test]
    async fn parser_node_error_yields_same_node_error_seed_through_remote_scheduler_bridge() {
        let module = build_test_module(
            Arc::new(RemoteFailingParseTestModule),
            Some(Arc::new(RecordingRemoteDispatcher::default()) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("remote parser node error should be converted into error seed");

        let error = result.error.expect("error seed should be present");
        assert!(result.next_dispatches.is_empty());
        assert_eq!(error.context.node_id.as_deref(), Some("page"));
        assert_eq!(error.context.step_idx, Some(0));
        assert!(error.context.stay_current_step);
        assert!(error.error_message.contains("forced parser node error"));
    }

    #[tokio::test]
    async fn parser_bridge_preserves_fanout_placeholder_routing() {
        let module = build_test_module(Arc::new(FanoutParseTestModule), None);
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("fanout parser should succeed through scheduler bridge");

        let mut targets = result
            .next_dispatches
            .iter()
            .map(|dispatch| dispatch.context.node_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        targets.sort();

        assert_eq!(targets, vec!["detail".to_string(), "reviews".to_string()]);
        assert!(result.error.is_none());
        assert!(result.data.is_empty());
    }

    #[tokio::test]
    async fn parser_bridge_preserves_stop_signal() {
        let module = build_test_module(Arc::new(StopParseTestModule), None);
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("stop parser should succeed through scheduler bridge");

        assert!(result.stop);
        assert!(result.next_dispatches.is_empty());
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn parser_remote_bridge_preserves_stop_signal() {
        let dispatcher = Arc::new(RecordingRemoteDispatcher::default());
        let module = build_test_module(
            Arc::new(RemoteStopParseTestModule),
            Some(dispatcher.clone() as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("remote stop parser should succeed through scheduler bridge");

        assert_eq!(dispatcher.remote_dispatches.load(Ordering::SeqCst), 1);
        assert!(result.stop);
        assert!(result.next_dispatches.is_empty());
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn parser_bridge_preserves_advance_gate_across_repeated_parse_calls() {
        let module = build_test_module(Arc::new(FanoutParseTestModule), None);
        module.add_step().await;

        let first = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("first fanout parser call should succeed through scheduler bridge");

        let second = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("second fanout parser call should also succeed through scheduler bridge");

        assert_eq!(first.next_dispatches.len(), 2);
        assert!(first.error.is_none());
        assert!(second.next_dispatches.is_empty());
        assert!(second.error.is_none());
        assert!(second.data.is_empty());
        assert!(!second.stop);
    }

    #[tokio::test]
    async fn parser_bridge_error_recovery_does_not_advance_fanout_successors() {
        let module = build_test_module(Arc::new(FanoutFailingParseTestModule), None);
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("failing fanout parser should return same-node error seed through scheduler bridge");

        let error = result.error.expect("error seed should be present");
        assert!(result.next_dispatches.is_empty());
        assert!(result.data.is_empty());
        assert_eq!(error.context.node_id.as_deref(), Some("page"));
        assert!(error.context.stay_current_step);
        assert!(error.error_message.contains("forced parser node error"));
    }

    #[tokio::test]
    async fn parser_remote_bridge_preserves_fanout_placeholder_routing() {
        let dispatcher = Arc::new(RecordingRemoteDispatcher::default());
        let module = build_test_module(
            Arc::new(RemoteFanoutParseTestModule),
            Some(dispatcher.clone() as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("remote fanout parser should succeed through scheduler bridge");

        let mut targets = result
            .next_dispatches
            .iter()
            .map(|dispatch| dispatch.context.node_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        targets.sort();

        assert_eq!(dispatcher.remote_dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(targets, vec!["detail".to_string(), "reviews".to_string()]);
        assert!(result.error.is_none());
        assert!(result.data.is_empty());
    }

    #[tokio::test]
    async fn parser_remote_bridge_error_recovery_does_not_advance_fanout_successors() {
        let dispatcher = Arc::new(RecordingRemoteDispatcher::default());
        let module = build_test_module(
            Arc::new(RemoteFanoutFailingParseTestModule),
            Some(dispatcher.clone() as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let result = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("remote failing fanout parser should return same-node error seed through scheduler bridge");

        let error = result.error.expect("error seed should be present");
        assert_eq!(dispatcher.remote_dispatches.load(Ordering::SeqCst), 1);
        assert!(result.next_dispatches.is_empty());
        assert!(result.data.is_empty());
        assert_eq!(error.context.node_id.as_deref(), Some("page"));
        assert!(error.context.stay_current_step);
        assert!(error.error_message.contains("forced parser node error"));
    }

    #[tokio::test]
    async fn parser_remote_bridge_preserves_advance_gate_across_repeated_parse_calls() {
        let dispatcher = Arc::new(RecordingRemoteDispatcher::default());
        let module = build_test_module(
            Arc::new(RemoteFanoutParseTestModule),
            Some(dispatcher.clone() as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let first = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("first remote fanout parser call should succeed through scheduler bridge");

        let second = module
            .parser(
                build_test_response(module.module.name(), "page"),
            )
            .await
            .expect("second remote fanout parser call should also succeed through scheduler bridge");

        assert_eq!(dispatcher.remote_dispatches.load(Ordering::SeqCst), 2);
        assert_eq!(first.next_dispatches.len(), 2);
        assert!(first.error.is_none());
        assert!(second.next_dispatches.is_empty());
        assert!(second.error.is_none());
        assert!(second.data.is_empty());
        assert!(!second.stop);
    }

    #[tokio::test]
    async fn parser_fails_closed_when_scheduler_cutover_cannot_resolve_target_node() {
        let module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.add_step().await;

        let mut response = build_test_response(module.module.name(), "missing-node");
        response.context = response.context.with_step_idx(99);

        let err = module
            .parser(response)
            .await
            .expect_err("parser should fail-closed when cutover cannot resolve the target node");

        assert!(
            err.to_string()
                .contains("scheduler parser cutover could not resolve target node"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn generate_fails_closed_when_scheduler_cutover_cannot_resolve_target_node() {
        let mut module = build_test_module(Arc::new(SingleRequestTestModule), None);
        module.pending_ctx = Some(
            ExecutionMark::default()
                .with_node_id("missing-node")
                .with_step_idx(99),
        );
        module.add_step().await;

        let err = match module.generate(Map::new(), None).await {
            Ok(_) => panic!("generate should fail-closed when cutover cannot resolve the target node"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("scheduler generate cutover could not resolve target node"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn generate_fails_closed_when_scheduler_bridge_fails_for_local_node() {
        let module = build_test_module(
            Arc::new(SingleRequestTestModule),
            Some(Arc::new(FailingDispatcher) as Arc<dyn DagNodeDispatcher>),
        );
        module.add_step().await;

        let err = match module.generate(Map::new(), None).await {
            Ok(_) => panic!("generate should fail-closed when scheduler bridge fails"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("forced scheduler bridge failure"),
            "unexpected error: {err}"
        );
    }
}
