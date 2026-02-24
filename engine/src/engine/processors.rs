use super::*;

impl Engine {
    /// Sends periodic cluster heartbeat updates until shutdown.
    pub(super) async fn start_node_registry(&self) {
        info!("Starting node registry heartbeat");
        let registry = self.node_registry.clone();
        let mut shutdown = self.shutdown_tx.subscribe();
        let mut interval = tokio::time::interval(Duration::from_secs(Self::NODE_HEARTBEAT_INTERVAL_SECS));

        let hostname = std::env::var("COMPUTERNAME").or(std::env::var("HOSTNAME")).unwrap_or("unknown".to_string());
        let ip = get_primary_local_ip().map(|ip| ip.to_string()).unwrap_or_else(|_| "127.0.0.1".to_string());

        loop {
            tokio::select! {
               _ = shutdown.recv() => {
                   info!("Node registry heartbeat received shutdown signal");
                   break;
               }
               _ = interval.tick() => {
                   if let Err(e) = registry.heartbeat(&ip, &hostname, env!("CARGO_PKG_VERSION")).await {
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
    )
    where
        T: Identifiable + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = ()> + Send,
    {
        let runner = ProcessorRunner::new(
            name,
            self.shutdown_tx.subscribe(),
            self.pause_tx.subscribe(),
            concurrency,
        );

        runner.run(receiver, execute_fn).await;
    }

    /// Starts task ingestion workers (`TaskModel` / unified ingress path).
    pub(super) async fn start_task_processor(&self, unified_task_ingress: Arc<UnifiedTaskIngressChain>) {
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

        self.run_processor_loop(
            "Task",
            self.queue_manager.get_task_pop_channel(),
            concurrency,
            move |task_item| {
                let ingress = unified_task_ingress.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (task, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task_for_dlq = task.clone();
                    let id = task.get_id();
                    let result = ingress
                        .execute(UnifiedTaskInput::Task(task), ProcessorContext::default())
                        .await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("task", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack task {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
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
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
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

        self.run_processor_loop(
            "Download",
            self.queue_manager.get_request_pop_channel(),
            concurrency,
            move |request_item| {
                let download_chain = download_chain.clone();
                let wss_chain = wss_download_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (request, mut ack_fn, mut nack_fn) = request_item.into_parts();
                    let request_for_dlq = request.clone();
                    let id = request.get_id();

                    let result = if request.downloader.eq_ignore_ascii_case("wss_downloader") {
                        wss_chain.execute(request, ProcessorContext::default()).await
                    } else {
                        download_chain.execute(request, ProcessorContext::default()).await
                    };

                    match result {
                        common::processors::processor::ProcessorResult::Success(_) => {
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("request", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack request {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "request",
                                "download",
                                &request_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "request",
                                "download",
                                &request_for_dlq,
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
    pub(super) async fn start_parser_model_processor(&self, unified_task_ingress: Arc<UnifiedTaskIngressChain>) {
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
        let single_node_mode = {
            let cfg = self.state.config.read().await;
            cfg.is_single_node_mode()
        };

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
                let single_node_mode = single_node_mode;
                async move {
                    let (task, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task_for_dlq = task.clone();
                    let id = task.get_id();

                    let module_id = task
                        .context
                        .module_id
                        .clone()
                        .or_else(|| {
                            task.account_task
                                .module
                                .as_ref()
                                .and_then(|modules| {
                                    modules.first().map(|m| {
                                        chain_key::module_runtime_id(
                                            &task.account_task.account,
                                            &task.account_task.platform,
                                            m,
                                        )
                                    })
                                })
                        })
                        .unwrap_or_else(|| {
                            chain_key::module_runtime_id(
                                &task.account_task.account,
                                &task.account_task.platform,
                                "unknown",
                            )
                        });
                    let step_idx = task.context.step_idx.unwrap_or(0);
                    let expected_step = step_idx.saturating_add(1);
                    let ptm_key = chain_key::ptm_key(
                        task.run_id,
                        &task.account_task.account,
                        &task.account_task.platform,
                        &module_id,
                        step_idx,
                        task.prefix_request,
                    );
                    let dedup_key = chain_key::dedup_key(&ptm_key);
                    let exec_key = chain_key::execution_state_key(task.run_id, &module_id);
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

                    if !single_node_mode {
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
                                Self::handle_policy_retry(
                                    &policy_resolver,
                                    &queue_manager,
                                    "parser_task",
                                    "parser_task_model",
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

                    let result = ingress
                        .execute(
                            UnifiedTaskInput::ParserTask(task.clone()),
                            ProcessorContext::default(),
                        )
                        .await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("parser_task", &id).await;
                            }

                            if !single_node_mode {
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
                                        counter!("ptm_commit_total", "result" => "success").increment(1);
                                    }
                                    Ok((3, _, _)) => {
                                        counter!("ptm_commit_total", "result" => "already_committed").increment(1);
                                    }
                                    Ok((1, msg, _)) => {
                                        counter!("ptm_commit_total", "result" => "fencing_reject").increment(1);
                                        let retry_policy = RetryPolicy::default().with_reason(format!(
                                            "ptm_commit_success fencing reject: {}",
                                            msg
                                        ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_task_model",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Ok((2, msg, _)) => {
                                        counter!("ptm_commit_total", "result" => "cas_conflict").increment(1);
                                        let retry_policy = RetryPolicy::default()
                                            .with_reason(format!("ptm_commit_success cas conflict: {}", msg));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_task_model",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Ok((code, msg, _)) => {
                                        counter!("ptm_commit_total", "result" => "unknown").increment(1);
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
                                            "parser_task_model",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Err(err) => {
                                        counter!("ptm_commit_total", "result" => "redis_error").increment(1);
                                        warn!("ptm_commit_success failed: {}", err);
                                        let retry_policy = RetryPolicy::default().with_reason(format!(
                                            "ptm_commit_success redis error: {}",
                                            err
                                        ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_task_model",
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
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            if !single_node_mode {
                                let error_hash = format!("retry:{}", id);
                                let error_emit_key = chain_key::error_emit_key(
                                    task.run_id,
                                    &module_id,
                                    step_idx as usize,
                                    task.prefix_request,
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
                                        counter!("ptm_commit_total", "result" => "error_emit_ok").increment(1);
                                    }
                                    Ok((1, _, _)) => {
                                        counter!("ptm_commit_total", "result" => "error_already_emitted")
                                            .increment(1);
                                    }
                                    Ok((2, msg, _)) => {
                                        counter!("ptm_commit_total", "result" => "fencing_reject").increment(1);
                                        let retry_policy = RetryPolicy::default().with_reason(format!(
                                            "ptm_commit_error fencing reject: {}",
                                            msg
                                        ));
                                        Self::handle_policy_retry(
                                            &policy_resolver,
                                            &queue_manager,
                                            "parser_task",
                                            "parser_task_model",
                                            &task_for_dlq,
                                            &retry_policy,
                                            &mut ack_fn,
                                            &mut nack_fn,
                                        )
                                        .await;
                                        return;
                                    }
                                    Ok((code, msg, _)) => {
                                        counter!("ptm_commit_total", "result" => "unknown").increment(1);
                                        warn!(
                                            "Unexpected ptm_commit_error code={} msg={}, continue retry flow",
                                            code, msg
                                        );
                                    }
                                    Err(err) => {
                                        counter!("ptm_commit_total", "result" => "redis_error").increment(1);
                                        warn!("ptm_commit_error failed on retryable branch: {}", err);
                                    }
                                }
                            }
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "parser_task",
                                "parser_task_model",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            if !single_node_mode {
                                let error_hash = format!("fatal:{}", id);
                                let error_emit_key = chain_key::error_emit_key(
                                    task.run_id,
                                    &module_id,
                                    step_idx as usize,
                                    task.prefix_request,
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
                                        counter!("ptm_commit_total", "result" => "error_emit_ok").increment(1);
                                    }
                                    Ok((1, _, _)) => {
                                        counter!("ptm_commit_total", "result" => "error_already_emitted")
                                            .increment(1);
                                    }
                                    Ok((2, _, _)) => {
                                        counter!("ptm_commit_total", "result" => "fencing_reject").increment(1);
                                    }
                                    Ok((_, _, _)) => {
                                        counter!("ptm_commit_total", "result" => "unknown").increment(1);
                                    }
                                    Err(_) => {
                                        counter!("ptm_commit_total", "result" => "redis_error").increment(1);
                                    }
                                }
                            }
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "parser_task",
                                "parser_task_model",
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

    pub(super) async fn start_error_processor(&self, unified_task_ingress: Arc<UnifiedTaskIngressChain>) {
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

        self.run_processor_loop(
            "Error",
            self.queue_manager.get_error_pop_channel(),
            concurrency,
            move |task_item| {
                let ingress = unified_task_ingress.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (task, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task_for_dlq = task.clone();
                    let id = task.get_id();
                    let result = ingress
                        .execute(UnifiedTaskInput::ErrorTask(task), ProcessorContext::default())
                        .await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("error_task", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack error task {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
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
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
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

        self.run_processor_loop(
            "Response",
            self.queue_manager.get_response_pop_channel(),
            concurrency,
            move |response_item| {
                let chain = parser_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (response, mut ack_fn, mut nack_fn) = response_item.into_parts();
                    let response_for_dlq = response.clone();
                    let id = response.get_id();
                    let result = chain.execute(response, ProcessorContext::default()).await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(_) => {
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("response", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack response {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "response",
                                "parser",
                                &response_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "response",
                                "parser",
                                &response_for_dlq,
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
