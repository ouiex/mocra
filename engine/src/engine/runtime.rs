use super::*;

impl Engine {
    /// Registers optional event consumers (console + Redis) and binds subscriptions.
    async fn setup_event_handlers(&self) {
        let Some(event_bus) = &self.event_bus else {
            info!("EventBus disabled; skipping event handlers");
            return;
        };

        let log_rx = event_bus.subscribe("*".to_string()).await;
        crate::events::handlers::console_handler::ConsoleLogHandler::start(log_rx, "INFO".to_string()).await;

        let config = self.state.config.read().await;
        if let Some(redis_config) = &config.cookie {
            if let Some(pool) = create_redis_pool(
                &redis_config.redis_host,
                redis_config.redis_port,
                redis_config.redis_db,
                &redis_config.redis_username,
                &redis_config.redis_password,
                redis_config.pool_size,
                redis_config.tls.unwrap_or(false),
            ) {
                let redis_rx = event_bus.subscribe("*".to_string()).await;
                let redis_handler = RedisEventHandler::new(
                    Arc::new(pool),
                    config.name.clone(),
                    3600,
                );
                redis_handler.start(redis_rx).await;

                info!(
                    "Redis event handler registered successfully (TLS: {})",
                    redis_config.tls.unwrap_or(false)
                );
            } else {
                info!("Redis pool creation failed");
            }
        } else {
            info!("Redis not configured, skipping Redis event handler");
        }

        info!("Event handlers registered successfully");
    }

    /// Starts the full engine runtime.
    ///
    /// Startup phases:
    /// 1. Optional Lua preload for distributed mode.
    /// 2. Optional API startup.
    /// 3. Event bus initialization.
    /// 4. Background services (cleaners/monitor/idle-stop watcher).
    /// 5. Chain construction and processor loops.
    pub async fn start(&self) {
        info!("Starting Schedule with event-driven architecture");
        let use_distributed_lua = {
            let config = self.state.config.read().await;
            !config.is_single_node_mode()
        };
        if use_distributed_lua {
            self.lua_registry
                .preload(self.state.cache_service.as_ref())
                .await;
        } else {
            info!("Single-node mode detected, skipping Lua script preload");
        }
        let api_config = self.state.config.read().await.api.clone();
        if let Some(api) = api_config {
            self.start_api(api.port).await;
            info!("API server started on host:  http://127.0.0.1:{}", api.port);
            if api.api_key.is_none() {
                warn!("No API Key configured; API requests will be rejected. Set 'api.api_key' in config to enable access.");
            }
        }

        if self.event_bus.is_some() {
            self.setup_event_handlers().await;

            if let Some(event_bus) = &self.event_bus {
                event_bus.start().await;
            }
        } else {
            info!("EventBus disabled; skipping setup and start");
        }

        self.downloader_manager.clone().start_background_cleaner();

        if let Some(event_bus) = &self.event_bus {
            if let Err(e) = event_bus
                .publish(EventEnvelope::engine(
                    EventType::SystemHealth,
                    EventPhase::Started,
                    serde_json::json!({ "event": "system_started" }),
                ))
                .await
            {
                error!("Failed to publish system started event: {e}");
            }
        }

        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                info!("Received Ctrl+C, initiating shutdown...");
                let _ = shutdown_tx.send(());
            }
        });

        let state_for_zombie = self.state.clone();
        tokio::spawn(async move {
            zombie::start_zombie_cleaner(state_for_zombie, 600).await;
        });

        let state_for_monitor = self.state.clone();
        tokio::spawn(async move {
            SystemMonitor::new(15).run(state_for_monitor).await;
        });

        let idle_stop_secs = self
            .state
            .config
            .read()
            .await
            .crawler
            .idle_stop_secs
            .unwrap_or(0);
        if idle_stop_secs > 0 {
            let queue_manager = self.queue_manager.clone();
            let cron_scheduler = self.cron_scheduler.clone();
            let shutdown_tx = self.shutdown_tx.clone();
            let mut shutdown_rx = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                let mut last_active = Instant::now();

                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Idle stop monitor shutting down");
                            break;
                        }
                        _ = interval.tick() => {
                            let (task, download, response, parser, error, remote_task) =
                                queue_manager.local_pending_breakdown().await;
                            let pending = task + download + response + parser + error + remote_task;
                            let has_running_cron_tasks = cron_scheduler.has_running_tasks();

                            if pending > 0 || has_running_cron_tasks {
                                last_active = Instant::now();
                                continue;
                            }

                            if last_active.elapsed().as_secs() >= idle_stop_secs {
                                info!("No local queue data for {}s, initiating shutdown", idle_stop_secs);
                                let _ = shutdown_tx.send(());
                                break;
                            }
                        }
                    }
                }
            });
        }

        self.start_cron_scheduler();

        info!("Starting all processors concurrently...");

        let unified_task_ingress = Arc::new(
            create_unified_task_ingress_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
                if use_distributed_lua {
                    Some(self.lua_registry.clone())
                } else {
                    None
                },
            )
            .await,
        );

        macro_rules! run_processor {
            ($name:expr, $fut:expr) => {
                async {
                    while let Err(e) = std::panic::AssertUnwindSafe($fut).catch_unwind().await {
                        error!("{} panicked: {:?}. Restarting...", $name, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            };
        }

        tokio::join!(
            run_processor!(
                "TaskProcessor",
                self.start_task_processor(unified_task_ingress.clone())
            ),
            run_processor!("DownloadProcessor", self.start_download_processor()),
            run_processor!(
                "ParserProcessor",
                self.start_parser_model_processor(unified_task_ingress.clone())
            ),
            run_processor!(
                "ErrorProcessor",
                self.start_error_processor(unified_task_ingress.clone())
            ),
            run_processor!("HealthMonitor", self.start_health_monitor()),
            run_processor!("ResponseProcessor", self.start_response_parser_processor()),
            run_processor!("NodeRegistry", self.start_node_registry()),
        );
    }

    /// Triggers graceful shutdown for processors and optional event infrastructure.
    pub async fn shutdown(&self) {
        info!("Shutting down Schedule");

        if let Err(e) = self.node_registry.deregister().await {
            warn!("Failed to deregister node: {}", e);
        }
        let _ = self.shutdown_tx.send(());

        if let Some(event_bus) = &self.event_bus {
            if let Err(e) = event_bus
                .publish(EventEnvelope::engine(
                    EventType::SystemHealth,
                    EventPhase::Completed,
                    serde_json::json!({ "event": "system_shutdown" }),
                ))
                .await
            {
                error!("Failed to publish system shutdown event: {e}");
            }

            event_bus.stop();
        }

        info!("Schedule shutdown completed");
    }

    /// Returns a lightweight runtime status snapshot for diagnostics.
    pub async fn get_system_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "processors": {
                "task": "active",
                "download": "active",
                "parser": "active",
                "error": "active"
            },
            "event_bus": if self.event_bus.is_some() { "active" } else { "disabled" },
            "uptime": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        })
    }

    /// Starts the HTTP API server in a detached task.
    pub async fn start_api(&self, port: u16) {
        let queue_manager = self.queue_manager.clone();
        let prometheus_handle = self.prometheus_handle.clone();
        let state = self.state.clone();
        let node_registry = self.node_registry.clone();
        tokio::spawn(async move {
            let api_state = crate::api::state::ApiState {
                queue_manager,
                prometheus_handle,
                state,
                node_registry,
            };
            let app = crate::api::router::router(api_state);
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("Successfully bound to address {:?}", listener);
                    listener
                }
                Err(e) => {
                    error!("Failed to bind to address {:?}", e);
                    std::process::exit(1);
                }
            };
            match axum::serve(listener, app.into_make_service()).await {
                Ok(_) => {
                    info!("API server stopped gracefully");
                }
                Err(e) => {
                    error!("API server error: {:?}", e);
                    std::process::exit(1);
                }
            }
        });
    }

    /// Starts cron scheduling loop.
    pub fn start_cron_scheduler(&self) {
        info!("Starting cron scheduler");
        self.cron_scheduler.clone().start();
    }
}
