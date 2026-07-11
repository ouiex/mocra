use super::*;

impl Engine {
    /// Sends periodic cluster heartbeat updates until shutdown.
    pub(super) async fn start_node_registry(&self) {
        info!("Starting node registry heartbeat");
        let registry = self.node_registry.clone();
        let mut shutdown = self.shutdown_tx.subscribe();
        let mut interval =
            tokio::time::interval(Duration::from_secs(Self::NODE_HEARTBEAT_INTERVAL_SECS));

        let hostname = std::env::var("COMPUTERNAME")
            .or(std::env::var("HOSTNAME"))
            .unwrap_or("unknown".to_string());
        let ip = get_primary_local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "127.0.0.1".to_string());

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
                        crate::common::processors::processor::ProcessorResult::Success(
                            mut stream,
                        ) => {
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
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(
                            retry_policy,
                        ) => {
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
                self.state.pipeline_ctx(),
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
                self.state.pipeline_ctx(),
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
                        crate::common::processors::processor::ProcessorResult::FatalFailure(err) => {
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
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());

        self.run_processor_loop(
            "Parser",
            self.queue_manager.get_parser_task_pop_channel(),
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
                        .execute(
                            UnifiedTaskInput::ParserTask(task.clone()),
                            ProcessorContext::default(),
                        )
                        .await;
                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("parser_task", &id).await;
                            }

                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack parser task {}: {}", id, e);
                                }
                            }
                        }
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
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
                        crate::common::processors::processor::ProcessorResult::FatalFailure(err) => {
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
                        .execute(
                            UnifiedTaskInput::ErrorTask(task),
                            ProcessorContext::default(),
                        )
                        .await;
                    match result {
                        crate::common::processors::processor::ProcessorResult::Success(
                            mut stream,
                        ) => {
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
                        crate::common::processors::processor::ProcessorResult::RetryableFailure(
                            retry_policy,
                        ) => {
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
                self.state.pipeline_ctx(),
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
                        crate::common::processors::processor::ProcessorResult::Success(_) => {
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
                        crate::common::processors::processor::ProcessorResult::FatalFailure(
                            err,
                        ) => {
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
