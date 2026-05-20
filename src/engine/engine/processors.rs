use super::*;
use crate::common::model::{PipelineStage, TaskStatus};
use crate::engine::task::task_dispatch_adapter::{
    decode_task_dispatch, processor_context_from_dispatch,
};
use crate::engine::task::request_response_adapter::{
    decode_request_dispatch, decode_response_dispatch,
};
use crate::engine::task::parser_error_adapter::{
    extract_error_envelope_seed, extract_parser_dispatch_seed,
};

impl Engine {
    /// Sends periodic cluster heartbeat updates until shutdown.
    pub(super) async fn start_node_heartbeat(&self) {
        info!("Starting control-plane node heartbeat");
        let profile_store = self.state.profile_store.clone();
        let mut shutdown = self.shutdown_tx.subscribe();
        let mut interval =
            tokio::time::interval(Duration::from_secs(Self::NODE_HEARTBEAT_INTERVAL_SECS));

        let hostname = std::env::var("COMPUTERNAME")
            .or(std::env::var("HOSTNAME"))
            .unwrap_or("unknown".to_string());
        let ip = get_primary_local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "127.0.0.1".to_string());
        let api_port = self.state.config.read().await.api.as_ref().map(|api| api.port);

        loop {
            tokio::select! {
               _ = shutdown.recv() => {
                   info!("Node heartbeat received shutdown signal");
                   break;
               }
               _ = interval.tick() => {
                   let now = std::time::SystemTime::now()
                       .duration_since(std::time::UNIX_EPOCH)
                       .unwrap_or_default()
                       .as_secs();
                    if let Err(e) = profile_store.heartbeat_node(crate::common::registry::NodeInfo {
                        id: self.node_id.clone(),
                        ip: ip.clone(),
                        hostname: hostname.clone(),
                        api_port,
                        last_heartbeat: now,
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    }).await {
                        error!("Failed to send heartbeat: {}", e);
                   }
               }
            }
        }
    }

    /// Shared runner for queue-backed processors with pause/shutdown awareness.
    async fn run_processor_loop<T, F, Fut>(
        &self,
        name: &str,
        receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<T>>>,
        concurrency: usize,
        execute_fn: F,
    ) where
        T: Identifiable + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = ()> + Send,
    {
        let runner = ProcessorRunner::new(
            name,
            self.shutdown_tx.subscribe(),
            self.pause_tx.subscribe(),
            concurrency,
            self.inflight_counter.clone(),
        );

        runner.run(receiver, execute_fn).await;
    }

    /// Starts task ingestion workers (`TaskModel` / unified ingress path).
    pub(super) async fn start_task_processor(
        &self,
        unified_task_ingress: Arc<UnifiedTaskIngressChain>,
    ) {
        let concurrency = self
            .state
            .config
            .read()
            .await
            .crawler
            .task_concurrency
            .unwrap_or(2048);
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());
        let status_tracker = self.state.status_tracker.clone();
        let node_id = self.node_id.clone();

        self.run_processor_loop(
            "Task",
            self.queue_manager.get_task_pop_channel(),
            concurrency,
            move |task_item| {
                let ingress = unified_task_ingress.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                let status_tracker = status_tracker.clone();
                let node_id = node_id.clone();
                async move {
                    let (dispatch, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task = match decode_task_dispatch(dispatch.clone()) {
                        Ok(task) => task,
                        Err(e) => {
                            error!("Failed to decode task dispatch envelope: {}", e);
                            if let Some(f) = nack_fn.take() {
                                let _ = f(format!("task dispatch decode failed: {e}")).await;
                            }
                            return;
                        }
                    };
                    let task_for_dlq = task.clone();
                    let id = task.get_id();
                    let status_task_id = task.run_id.to_string();
                    let _ = status_tracker
                        .update_status(
                            &status_task_id,
                            PipelineStage::Task,
                            TaskStatus::Running,
                            0,
                            &node_id,
                            None,
                        )
                        .await;
                    let processor_context = processor_context_from_dispatch(&dispatch);
                    let result = ingress
                        .execute(UnifiedTaskInput::Task(task), processor_context)
                        .await;
                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(
                            mut stream,
                        ) => {
                            while stream.next().await.is_some() {}
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Task,
                                    TaskStatus::Done,
                                    0,
                                    &node_id,
                                    None,
                                )
                                .await;
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("task", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack task {}: {}", id, e);
                                }
                            }
                        }
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(
                            retry_policy,
                        ) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Task,
                                    TaskStatus::Retrying,
                                    retry_policy.current_retry,
                                    &node_id,
                                    retry_policy.reason.clone(),
                                )
                                .await;
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "task",
                                "task_model",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        crate::common::processors::processor::ProcessorResult::FatalFailure(
                            err,
                        ) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Task,
                                    TaskStatus::Failed,
                                    0,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "task",
                                "task_model",
                                &task_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        )
        .await;
    }

    /// Starts request download workers (HTTP and WebSocket variants).
    pub(super) async fn start_download_processor(&self) {
        let concurrency = self
            .state
            .config
            .read()
            .await
            .crawler
            .task_concurrency
            .unwrap_or(2048);
        info!("Starting download processor");
        let download_chain = Arc::new(
            create_download_chain(
                self.state.clone(),
                self.downloader_manager.clone(),
                self.queue_manager.clone(),
                self.middleware_manager.clone(),
                self.event_bus.clone(),
                self.proxy_manager.clone(),
            )
            .await,
        );
        let wss_download_chain = Arc::new(
            create_wss_download_chain(
                self.state.clone(),
                self.downloader_manager.clone(),
                self.queue_manager.clone(),
                self.middleware_manager.clone(),
                self.state.cache_service.clone(),
                self.event_bus.clone(),
                self.proxy_manager.clone(),
            )
            .await,
        );
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());
        let status_tracker = self.state.status_tracker.clone();
        let node_id = self.node_id.clone();

        self.run_processor_loop(
            "Download",
            self.queue_manager.get_request_pop_channel(),
            concurrency,
            move |request_item| {
                let download_chain = download_chain.clone();
                let wss_chain = wss_download_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                let status_tracker = status_tracker.clone();
                let node_id = node_id.clone();
                async move {
                    let (request_dispatch, mut ack_fn, mut nack_fn) = request_item.into_parts();
                    let request_dispatch_for_dlq = request_dispatch.clone();
                    let dispatch_id = request_dispatch.routing.request_id.to_string();
                    let dispatch_run_id = request_dispatch.routing.run_id.to_string();
                    let dispatch_retry_count = request_dispatch.exec.retry_count;
                    let request = match decode_request_dispatch(request_dispatch) {
                        Ok(request) => request,
                        Err(err) => {
                            error!(
                                "Failed to decode request envelope {}: {}",
                                dispatch_id, err
                            );
                            let _ = status_tracker
                                .update_status(
                                    &dispatch_run_id,
                                    PipelineStage::Request,
                                    TaskStatus::Failed,
                                    dispatch_retry_count,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            if let Some(f) = nack_fn.take() {
                                let _ = f(format!("request envelope decode failed: {err}")).await;
                            }
                            return;
                        }
                    };
                    let id = request.get_id();
                    let status_task_id = request.run_id.to_string();
                    let retry_count = request.retry_times as u32;
                    let _ = status_tracker
                        .update_status(
                            &status_task_id,
                            PipelineStage::Request,
                            TaskStatus::Running,
                            retry_count,
                            &node_id,
                            None,
                        )
                        .await;
                    info!("[DownloadExecuteFn] starting chain for request_id={} module={} url={}", id, request.module_id(), request.url);

                    let chain_start = std::time::Instant::now();
                    let result = if request.downloader.eq_ignore_ascii_case("wss_downloader") {
                        wss_chain.execute(request, ProcessorContext::default()).await
                    } else {
                        download_chain.execute(request, ProcessorContext::default()).await
                    };
                    let chain_elapsed = chain_start.elapsed();
                    info!("[DownloadExecuteFn] chain returned for request_id={} elapsed={:?} result={}", id, chain_elapsed,
                        match &result {
                            crate::common::processors::processor::ProcessorResult::Success(_) => "Success",
                            crate::common::processors::processor::ProcessorResult::RetryableFailure(_) => "RetryableFailure",
                            crate::common::processors::processor::ProcessorResult::FatalFailure(_) => "FatalFailure",
                        });

                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(_) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Request,
                                    TaskStatus::Done,
                                    retry_count,
                                    &node_id,
                                    None,
                                )
                                .await;
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("request", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack request {}: {}", id, e);
                                }
                            }
                        }
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Request,
                                    TaskStatus::Retrying,
                                    retry_policy.current_retry,
                                    &node_id,
                                    retry_policy.reason.clone(),
                                )
                                .await;
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "request",
                                "download",
                                &request_dispatch_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        crate::common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Request,
                                    TaskStatus::Failed,
                                    retry_count,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "request",
                                "download",
                                &request_dispatch_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        )
        .await;
    }

    /// Starts parser-task workers that continue chain progression after parser outcomes.
    pub(super) async fn start_parser_model_processor(
        &self,
        unified_task_ingress: Arc<UnifiedTaskIngressChain>,
    ) {
        let concurrency = {
            let cfg = self.state.config.read().await;
            cfg.crawler
                .parser_concurrency
                .or(cfg.crawler.task_concurrency)
                .unwrap_or(2048)
        };
        info!("Starting parser processor");
        let queue_manager = self.queue_manager.clone();
        let state = self.state.clone();
        let lua_registry = self.lua_registry.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());
        let status_tracker = self.state.status_tracker.clone();
        let node_id = self.node_id.clone();
        let use_cache_redis_lua = self.state.has_cache_redis_backend();

        self.run_processor_loop(
            "Parser",
            self.queue_manager.get_parser_task_pop_channel(),
            concurrency,
            move |task_item| {
                let ingress = unified_task_ingress.clone();
                let queue_manager = queue_manager.clone();
                let state = state.clone();
                let lua_registry = lua_registry.clone();
                let policy_resolver = policy_resolver.clone();
                let status_tracker = status_tracker.clone();
                let node_id = node_id.clone();
                let use_cache_redis_lua = use_cache_redis_lua;
                async move {
                    let (task_dispatch, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let dispatch_id = task_dispatch.routing.request_id.to_string();
                    let dispatch_run_id = task_dispatch.routing.run_id.to_string();
                    let parser_seed = match extract_parser_dispatch_seed(&task_dispatch) {
                        Ok(seed) => seed,
                        Err(err) => {
                            error!(
                                "Failed to extract parser task seed {}: {}",
                                dispatch_id, err
                            );
                            let _ = status_tracker
                                .update_status(
                                    &dispatch_run_id,
                                    PipelineStage::ParserTask,
                                    TaskStatus::Failed,
                                    0,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            if let Some(f) = nack_fn.take() {
                                let _ = f(format!("parser task envelope seed extraction failed: {err}")).await;
                            }
                            return;
                        }
                    };
                    let task_for_dlq = task_dispatch.clone();
                    let id = task_for_dlq.get_id();
                    let status_task_id = task_dispatch.routing.run_id.to_string();

                    let module_id = parser_seed
                        .context
                        .module_id
                        .clone()
                        .or_else(|| {
                            parser_seed
                                .task_model
                                .module
                                .as_ref()
                                .and_then(|modules| {
                                    modules.first().map(|m| {
                                        chain_key::module_runtime_id(
                                            &parser_seed.task_model.account,
                                            &parser_seed.task_model.platform,
                                            m,
                                        )
                                    })
                                })
                        })
                        .unwrap_or_else(|| {
                            chain_key::module_runtime_id(
                                &parser_seed.task_model.account,
                                &parser_seed.task_model.platform,
                                "unknown",
                            )
                        });
                    let step_idx = parser_seed.context.step_idx.unwrap_or(0);
                    let expected_step = step_idx.saturating_add(1);
                    let ptm_key = chain_key::ptm_key(
                        parser_seed.run_id,
                        &parser_seed.task_model.account,
                        &parser_seed.task_model.platform,
                        &module_id,
                        step_idx,
                        parser_seed.prefix_request,
                    );
                    let dedup_key = chain_key::dedup_key(&ptm_key);
                    let exec_key = chain_key::execution_state_key(parser_seed.run_id, &module_id);
                    let lease_owner = state
                        .config
                        .read()
                        .await
                        .crawler
                        .node_id
                        .clone()
                        .unwrap_or_else(|| format!("parser-node-{}", uuid::Uuid::now_v7()));
                    let now_epoch = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        .to_string();

                    let claim_keys = [dedup_key.as_str(), exec_key.as_str()];
                    let expected_step_s = expected_step.to_string();
                    let lease_ttl_s = "300".to_string();
                    let claim_args = [
                        expected_step_s.as_str(),
                        lease_owner.as_str(),
                        lease_ttl_s.as_str(),
                        now_epoch.as_str(),
                    ];

                    if use_cache_redis_lua {
                        match lua_registry
                            .eval_triplet_with_fallback(
                                state.cache_service.as_ref(),
                                "ptm_claim.lua",
                                include_str!("../../lua/ptm_claim.lua"),
                                &claim_keys,
                                &claim_args,
                            )
                            .await
                        {
                            Ok((0, _, _)) => {}
                            Ok((1, _, _)) | Ok((2, _, _)) => {
                                info!("ParserTask {} already handled or stale, ack", id);
                                if let Some(f) = ack_fn.take() {
                                    let _ = f().await;
                                }
                                return;
                            }
                            Ok((3, msg, _)) | Ok((4, msg, _)) => {
                                let retry_policy = RetryPolicy::default()
                                    .with_reason(format!("ptm_claim deferred: {}", msg));
                                let _ = status_tracker
                                    .update_status(
                                        &status_task_id,
                                        PipelineStage::ParserTask,
                                        TaskStatus::Retrying,
                                        retry_policy.current_retry,
                                        &node_id,
                                        retry_policy.reason.clone(),
                                    )
                                    .await;
                                Self::handle_policy_retry(
                                    &policy_resolver,
                                    &queue_manager,
                                    "parser_task",
                                    "parser_dispatch",
                                    &task_for_dlq,
                                    &retry_policy,
                                    &mut ack_fn,
                                    &mut nack_fn,
                                )
                                .await;
                                return;
                            }
                            Ok((code, msg, _)) => {
                                warn!(
                                    "Unexpected ptm_claim result code={} msg={}, fallback to retry",
                                    code, msg
                                );
                            }
                            Err(err) => {
                                warn!(
                                    "ptm_claim failed for parser task {}, fallback to retry: {}",
                                    id, err
                                );
                            }
                        }
                    }

                    let _ = status_tracker
                        .update_status(
                            &status_task_id,
                            PipelineStage::ParserTask,
                            TaskStatus::Running,
                            0,
                            &node_id,
                            None,
                        )
                        .await;

                    let result = ingress
                        .execute(
                            UnifiedTaskInput::ParserDispatch(task_dispatch),
                            ProcessorContext::default(),
                        )
                        .await;
                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::ParserTask,
                                    TaskStatus::Done,
                                    0,
                                    &node_id,
                                    None,
                                )
                                .await;
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("parser_task", &id).await;
                            }

                            if use_cache_redis_lua {
                                let done_ttl_s = "86400".to_string();
                                let commit_args = [
                                    expected_step_s.as_str(),
                                    lease_owner.as_str(),
                                    now_epoch.as_str(),
                                    done_ttl_s.as_str(),
                                ];
                                let commit_result = lua_registry
                                    .eval_triplet_with_fallback(
                                        state.cache_service.as_ref(),
                                        "ptm_commit_success.lua",
                                        include_str!("../../lua/ptm_commit_success.lua"),
                                        &claim_keys,
                                        &commit_args,
                                    )
                                    .await;

                                match commit_result {
                                    Ok((0, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "success").increment(1);
                                    }
                                    Ok((3, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "already_committed").increment(1);
                                    }
                                    Ok((1, msg, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "fencing_reject").increment(1);
                                        let retry_policy = RetryPolicy::default()
                                            .with_reason(format!(
                                                "ptm_commit_success fencing reject: {}",
                                                msg
                                            ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_dispatch",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Ok((2, msg, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "cas_conflict").increment(1);
                                        let retry_policy = RetryPolicy::default()
                                            .with_reason(format!("ptm_commit_success cas conflict: {}", msg));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_dispatch",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Ok((code, msg, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "unknown").increment(1);
                                        warn!(
                                            "Unexpected ptm_commit_success code={} msg={}, fallback retry",
                                            code, msg
                                        );
                                        let retry_policy = RetryPolicy::default().with_reason(format!(
                                            "ptm_commit_success unexpected code {}",
                                            code
                                        ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_dispatch",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Err(err) => {
                                        counter!("mocra_ptm_commit_total", "result" => "redis_error").increment(1);
                                        warn!("ptm_commit_success failed: {}", err);
                                        let retry_policy = RetryPolicy::default().with_reason(format!(
                                            "ptm_commit_success redis error: {}",
                                            err
                                        ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_dispatch",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                }
                            }

                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack parser task {}: {}", id, e);
                                }
                            }
                        }
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::ParserTask,
                                    TaskStatus::Retrying,
                                    retry_policy.current_retry,
                                    &node_id,
                                    retry_policy.reason.clone(),
                                )
                                .await;
                            if use_cache_redis_lua {
                                let error_hash = format!("retry:{}", id);
                                let error_emit_key = chain_key::error_emit_key(
                                    parser_seed.run_id,
                                    &module_id,
                                    step_idx as usize,
                                    parser_seed.prefix_request,
                                    &error_hash,
                                );
                                let fail_ttl_s = "300".to_string();
                                let emit_ttl_s = "86400".to_string();
                                let commit_err_keys = [
                                    dedup_key.as_str(),
                                    exec_key.as_str(),
                                    error_emit_key.as_str(),
                                ];
                                let commit_err_args = [
                                    lease_owner.as_str(),
                                    fail_ttl_s.as_str(),
                                    emit_ttl_s.as_str(),
                                ];
                                let commit_err_res = lua_registry
                                    .eval_triplet_with_fallback(
                                        state.cache_service.as_ref(),
                                        "ptm_commit_error.lua",
                                        include_str!("../../lua/ptm_commit_error.lua"),
                                        &commit_err_keys,
                                        &commit_err_args,
                                    )
                                    .await;
                                match commit_err_res {
                                    Ok((0, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "error_emit_ok").increment(1);
                                    }
                                    Ok((1, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "error_already_emitted")
                                            .increment(1);
                                    }
                                    Ok((2, msg, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "fencing_reject").increment(1);
                                        let retry_policy = RetryPolicy::default().with_reason(format!(
                                            "ptm_commit_error fencing reject: {}",
                                            msg
                                        ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_dispatch",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Ok((code, msg, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "unknown").increment(1);
                                        warn!(
                                            "Unexpected ptm_commit_error code={} msg={}, continue retry flow",
                                            code, msg
                                        );
                                    }
                                    Err(err) => {
                                        counter!("mocra_ptm_commit_total", "result" => "redis_error").increment(1);
                                        warn!("ptm_commit_error failed on retryable branch: {}", err);
                                    }
                                }
                            }
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "parser_task",
                                "parser_dispatch",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        crate::common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::ParserTask,
                                    TaskStatus::Failed,
                                    0,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            if use_cache_redis_lua {
                                let error_hash = format!("fatal:{}", id);
                                let error_emit_key = chain_key::error_emit_key(
                                    parser_seed.run_id,
                                    &module_id,
                                    step_idx as usize,
                                    parser_seed.prefix_request,
                                    &error_hash,
                                );
                                let fail_ttl_s = "300".to_string();
                                let emit_ttl_s = "86400".to_string();
                                let commit_err_keys = [
                                    dedup_key.as_str(),
                                    exec_key.as_str(),
                                    error_emit_key.as_str(),
                                ];
                                let commit_err_args = [
                                    lease_owner.as_str(),
                                    fail_ttl_s.as_str(),
                                    emit_ttl_s.as_str(),
                                ];
                                let commit_err_res = lua_registry
                                    .eval_triplet_with_fallback(
                                        state.cache_service.as_ref(),
                                        "ptm_commit_error.lua",
                                        include_str!("../../lua/ptm_commit_error.lua"),
                                        &commit_err_keys,
                                        &commit_err_args,
                                    )
                                    .await;
                                match commit_err_res {
                                    Ok((0, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "error_emit_ok").increment(1);
                                    }
                                    Ok((1, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "error_already_emitted")
                                            .increment(1);
                                    }
                                    Ok((2, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "fencing_reject").increment(1);
                                    }
                                    Ok((_, _, _)) => {
                                        counter!("mocra_ptm_commit_total", "result" => "unknown").increment(1);
                                    }
                                    Err(_) => {
                                        counter!("mocra_ptm_commit_total", "result" => "redis_error").increment(1);
                                    }
                                }
                            }
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "parser_task",
                                "parser_dispatch",
                                &task_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        )
        .await;
    }

    pub(super) async fn start_error_processor(
        &self,
        unified_task_ingress: Arc<UnifiedTaskIngressChain>,
    ) {
        let concurrency = {
            let cfg = self.state.config.read().await;
            cfg.crawler
                .error_task_concurrency
                .or(cfg.crawler.parser_concurrency)
                .or(cfg.crawler.task_concurrency)
                .unwrap_or(2048)
        };
        info!("Starting error processor");
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());
        let status_tracker = self.state.status_tracker.clone();
        let node_id = self.node_id.clone();

        self.run_processor_loop(
            "Error",
            self.queue_manager.get_error_pop_channel(),
            concurrency,
            move |task_item| {
                let ingress = unified_task_ingress.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                let status_tracker = status_tracker.clone();
                let node_id = node_id.clone();
                async move {
                    let (task_envelope, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let dispatch_id = task_envelope.routing.request_id.to_string();
                    let dispatch_run_id = task_envelope.routing.run_id.to_string();
                    let error_seed = match extract_error_envelope_seed(&task_envelope) {
                        Ok(seed) => seed,
                        Err(err) => {
                            error!(
                                "Failed to extract error task seed {}: {}",
                                dispatch_id, err
                            );
                            let _ = status_tracker
                                .update_status(
                                    &dispatch_run_id,
                                    PipelineStage::Error,
                                    TaskStatus::Failed,
                                    0,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            if let Some(f) = nack_fn.take() {
                                let _ = f(format!("error task envelope seed extraction failed: {err}")).await;
                            }
                            return;
                        }
                    };
                    let task_for_dlq = task_envelope.clone();
                    let id = task_for_dlq.get_id();
                    let status_task_id = task_envelope.routing.run_id.to_string();
                    let _ = status_tracker
                        .update_status(
                            &status_task_id,
                            PipelineStage::Error,
                            TaskStatus::Running,
                            0,
                            &node_id,
                            Some(error_seed.error_message.clone()),
                        )
                        .await;
                    let result = ingress
                        .execute(
                            UnifiedTaskInput::ErrorEnvelope(task_envelope),
                            ProcessorContext::default(),
                        )
                        .await;
                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(
                            mut stream,
                        ) => {
                            while stream.next().await.is_some() {}
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Error,
                                    TaskStatus::Done,
                                    0,
                                    &node_id,
                                    None,
                                )
                                .await;
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("error_task", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack error task {}: {}", id, e);
                                }
                            }
                        }
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(
                            retry_policy,
                        ) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Error,
                                    TaskStatus::Retrying,
                                    retry_policy.current_retry,
                                    &node_id,
                                    retry_policy.reason.clone(),
                                )
                                .await;
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "error_task",
                                "system_error",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        crate::common::processors::processor::ProcessorResult::FatalFailure(
                            err,
                        ) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Error,
                                    TaskStatus::Failed,
                                    0,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "error_task",
                                "system_error",
                                &task_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        )
        .await;
    }

    pub(super) async fn start_response_parser_processor(&self) {
        let concurrency = {
            let cfg = self.state.config.read().await;
            cfg.crawler
                .parser_concurrency
                .or(cfg.crawler.task_concurrency)
                .unwrap_or(2048)
        };
        info!("Starting response processor");
        let parser_chain = Arc::new(
            create_parser_chain(
                self.state.clone(),
                self.task_manager.clone(),
                self.middleware_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.cache_service.clone(),
            )
            .await,
        );
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());
        let status_tracker = self.state.status_tracker.clone();
        let node_id = self.node_id.clone();

        self.run_processor_loop(
            "Response",
            self.queue_manager.get_response_pop_channel(),
            concurrency,
            move |response_item| {
                let chain = parser_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                let status_tracker = status_tracker.clone();
                let node_id = node_id.clone();
                async move {
                    let (response_dispatch, mut ack_fn, mut nack_fn) = response_item.into_parts();
                    let response_dispatch_for_dlq = response_dispatch.clone();
                    let dispatch_id = response_dispatch.routing.request_id.to_string();
                    let dispatch_run_id = response_dispatch.routing.run_id.to_string();
                    let dispatch_retry_count = response_dispatch.exec.task_retry_count;
                    let response = match decode_response_dispatch(response_dispatch) {
                        Ok(response) => response,
                        Err(err) => {
                            error!(
                                "Failed to decode response envelope {}: {}",
                                dispatch_id, err
                            );
                            let _ = status_tracker
                                .update_status(
                                    &dispatch_run_id,
                                    PipelineStage::Response,
                                    TaskStatus::Failed,
                                    dispatch_retry_count,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            if let Some(f) = nack_fn.take() {
                                let _ = f(format!("response envelope decode failed: {err}")).await;
                            }
                            return;
                        }
                    };
                    let id = response.get_id();
                    let status_task_id = response.run_id.to_string();
                    let retry_count = response.task_retry_times as u32;
                    let _ = status_tracker
                        .update_status(
                            &status_task_id,
                            PipelineStage::Response,
                            TaskStatus::Running,
                            retry_count,
                            &node_id,
                            None,
                        )
                        .await;
                    let result = chain.execute(response, ProcessorContext::default()).await;
                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(_) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Response,
                                    TaskStatus::Done,
                                    retry_count,
                                    &node_id,
                                    None,
                                )
                                .await;
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("response", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack response {}: {}", id, e);
                                }
                            }
                        }
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(
                            retry_policy,
                        ) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Response,
                                    TaskStatus::Retrying,
                                    retry_policy.current_retry,
                                    &node_id,
                                    retry_policy.reason.clone(),
                                )
                                .await;
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "response",
                                "parser",
                                &response_dispatch_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        crate::common::processors::processor::ProcessorResult::FatalFailure(
                            err,
                        ) => {
                            let _ = status_tracker
                                .update_status(
                                    &status_task_id,
                                    PipelineStage::Response,
                                    TaskStatus::Failed,
                                    retry_count,
                                    &node_id,
                                    Some(err.to_string()),
                                )
                                .await;
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "response",
                                "parser",
                                &response_dispatch_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        )
        .await;
    }

    pub(super) async fn start_health_monitor(&self) {
        info!("Starting health monitor");

        let event_bus = self.event_bus.clone();
        let mut shutdown = self.shutdown_tx.subscribe();

        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Health monitor received shutdown signal, exiting loop");
                    break;
                }
                _ = interval.tick() => {
                    match self.state.limiter.cleanup().await {
                         Ok(count) => if count > 0 { info!("Cleaned up {} expired rate limit keys", count); },
                         Err(e) => error!("Failed to cleanup rate limit keys: {:?}", e),
                    }

                    let health_event = EventEnvelope::system_health(
                        HealthCheckEvent {
                            component: "schedule".to_string(),
                            status: "healthy".to_string(),
                            metrics: serde_json::json!({
                                "uptime_seconds": std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs(),
                                "processors": {
                                    "task": "running",
                                    "download": "running",
                                    "parser": "running",
                                    "error": "running"
                                }
                            }),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        },
                        EventPhase::Completed,
                    );

                    if let Some(event_bus) = &event_bus {
                        if let Err(e) = event_bus.publish(health_event).await {
                            error!("Failed to publish health check event: {e}");
                        }
                    }
                }
            }
        }
    }
}
