#![allow(unused)]
use crate::events::{
    EventBus, RedisEventHandler,
    event_bus::{LogEventHandler, MetricsEventHandler},
};
use downloader::DownloaderManager;
use queue::{Identifiable, RedisCompensator, RedisQueue};

use crate::chain::{
    create_download_chain, create_error_task_chain, create_parser_chain, create_parser_task_chain,
    create_task_model_chain,
};
use crate::events::EventSystem::{ComponentHealthCheck, SystemShutdown, SystemStarted};

use crate::chain::stream_chain::create_wss_download_chain;
use futures::StreamExt;
use common::state::State;
use log::{error, info};
use queue::QueueManager;
use queue::kafka::KafkaQueue;
use common::processors::processor::ProcessorContext;
use proxy::ProxyManager;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::broadcast;
use common::interface::{DataMiddleware, DataStoreMiddleware, DownloadMiddleware, MiddlewareManager, ModuleTrait};
use utils::connector::create_redis_pool;
use crate::task::TaskManager;

/// channel->TaskModel->Task->Vec<Model>->Vec<Request>->channel
/// channel->Request->Downloader->Response->channel
/// channel->Response->Parser->ParserTask->channel
/// 判断是否有错误是根据Parser是否返回ParserError来决定，TaskFactory根据配置和当前缓存的错误来决定是否重新执行chain
/// 需要进一步测试这种设计的合理性
/// 不合理，这种方法只有在返回一个ParserErrorModel的时候才会增加错误计数，重新设计，在chain里增加错误计数并在chain里做错误次数验证
/// 由于是链式调用，一个Request无论在下载阶段还是解析阶段，都应该有错误计数，
/// 1、Task,Module生成阶段做错误验证，超出错误的则不生成Request
/// 2、下载阶段里对Task,Module增加错误计数
/// 3、解析阶段里对Task,Module增加错误计数
/// 问题1：如果一个Module生成了多个Request，这些Request有的成功有的失败，如何统计错误次数？
/// 目前的设计是每个Request独立统计错误次数，失败的Request会增加错误次数，成功的不会
/// 这样会导致一个Module即使大部分Request成功，只要有少部分失败，整个Module的错误次数就会增加
/// 下载阶段Request->Response这个环节会出现这个问题
/// 解决方案：不同的module设置不同的最大错误次数，使用这个来控制
/// state.config.crawler里的设置作为默认设置
///
/// 目前只在下载和解析阶段做错误统计
///
/// 目前问题，一个module并发生成的话，会导致错误次数超过限制
/// 例如：一个module设置错误次数为3，生成了10个Request，这10个Request全部失败，那么这个module的错误次数会增加10次 //这是分布式的并发问题
/// 此时module会进入重试阶段，重试阶段理论上应该不会再产生Request了，但是此时还会有3个request生成
/// 问题分析：download是串行执行，多一个Request会顺序下载，而不是并发下载
/// 理论上当前3个Request会顺序下载，前3个失败，module错误次数达到3，后面的Request就不会再下载了
/// 需要在download里做错误验证，对超出错误次数的Request直接丢弃
///
/// [INCORRECT] 当前的设计里，downloader默认情况下在同一个task_id里是串行下载
/// 问题中前10个已经提交到消息队里里了，即使Request解析失败，仍会对队列里的Request进行下载
/// 但是由于是串行下载，前面的下载后进入解析队列，解析队列增加错误计数，此时错误计数到达10
/// 在所有任务未下载完成的情况下，解析器增加了错误计数，此时缓存器里的错误计数未达到最大值，是可以提交ParserTaskModel的
/// 这样就导致了至少有13个请求被发起了，理论上应该只能发送3个请求
/// 问题点1：分布式下，同时提交多个相同的Module会导致错误次数无法控制，程序需要知道当前的Module的执行阶段，同样的Module不能同时执行
/// 解决方案1：使用分布式锁，锁住当前的Module，直到当前的Module执行完成，但是Module会生成多个Request，Response也会生成多个Data，这会导致锁的时间过长，也会导致锁多次释放
/// 解决方案2：最后活跃时间，每次chain执行的时候，更新module_id最后活跃时间，定时器检查最后活跃时间，如果超过一定时间没有更新，则认为当前的Module已经完成，可以执行同样的Module
/// [INCORRECT] 问题点1已解决，使用方案2
pub struct Engine {
    pub queue_manager: Arc<QueueManager>,
    pub downloader_manager: Arc<DownloaderManager>,
    pub task_manager: Arc<TaskManager>,
    pub proxy_manager: Option<Arc<ProxyManager>>,
    pub middleware_manager: Arc<MiddlewareManager>,
    pub event_bus: Arc<EventBus>,
    pub state: Arc<State>,
    // 广播型关闭信号，所有处理器都能订阅到
    shutdown_tx: broadcast::Sender<()>,
    // module_loader: Arc<ModuleLoader>,
}

impl Engine {
    pub async fn new(state: Arc<State>) -> Self {
        // 创建事件总线
        let event_bus = Arc::new(EventBus::new());
        // 创建关闭信号通道
        let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);
        let task_manager = Arc::new(TaskManager::new(Arc::clone(&state)));
        // task_manager.add_works(crawler::register_modules()).await;

        let cfg = state.config.read().await.clone();
        let channel_config = cfg.channel_config.clone();
        let namespace = cfg.name.clone();

        let mut queue_manager = if let Some(redis_config) = &channel_config.redis {
            let redis_queue = RedisQueue::new(redis_config, channel_config.minid_time, &namespace)
                .expect("Failed to create RedisQueue");
            info!("RedisQueue initialized successfully");
            QueueManager::new(Some(Arc::new(redis_queue)), channel_config.capacity)
        } else if let Some(kafka_config) = &channel_config.kafka {
            let kafka_queue = KafkaQueue::new(kafka_config, channel_config.minid_time, &namespace)
                .expect("Failed to create KafkaQueue");
            info!("KafkaQueue initialized successfully");
            QueueManager::new(Some(Arc::new(kafka_queue)), channel_config.capacity)
        } else {
            info!("NoneQueue initialized successfully");
            QueueManager::new(None, 1024)
        };

        // Initialize compensator
        if let Some(redis_config) = &channel_config.compensator {
            let compensator = Arc::new(
                RedisCompensator::new(redis_config, &namespace)
                    .expect("Failed to create RedisCompensator"),
            );
            queue_manager.with_compensator(compensator);
        }

        queue_manager.subscribe();
        // 状态信息，记录数据，用于记录task,request,response,parser_task,parser_error等状态
        // downloader_manager会默认注册RequestDownloader
        // 目前暂时无法解决Box<dyn Downloader> trait对象无法被克隆的问题，必须使用RequestDownloader
        // Arc无法获取dyn Downloader的所有权，为了确保每个task_id拥有单独的Downloader实例，只能使用Box<RequestDownloader>
        // 使用dyn_clone解决Box<dyn Downloader>无法克隆的问题，已完成
        let downloader_manager = DownloaderManager::new(Arc::clone(&state));
        let proxy_path = state.config.read().await.crawler.proxy_path.clone();
        let proxy_config = fs::read_to_string(proxy_path).await.unwrap();
        let proxy_manager = Arc::new(
            ProxyManager::from_config(&proxy_config)
                .await
                .expect("Failed to create ProxyManager"),
        );
        let middleware_manager = MiddlewareManager::new(state.clone());

        Self {
            queue_manager: Arc::new(queue_manager),
            downloader_manager: Arc::new(downloader_manager),
            task_manager,
            proxy_manager: Some(proxy_manager),
            middleware_manager: Arc::new(middleware_manager),
            event_bus,
            state,
            shutdown_tx,
        }
    }
    pub async fn register_download_middleware(&self, middleware: Arc<dyn DownloadMiddleware>) {
        self.middleware_manager
            .register_download_middleware(middleware)
            .await;
    }
    pub async fn register_data_middleware(&self, middleware: Arc<dyn DataMiddleware>) {
        self.middleware_manager
            .register_data_middleware(middleware)
            .await;
    }
    pub async fn register_store_middleware(&self, middleware: Arc<dyn DataStoreMiddleware>) {
        self.middleware_manager
            .register_store_middleware(middleware)
            .await;
    }

    pub async fn register_module(&self, module: Arc<dyn ModuleTrait>) {
        self.task_manager.add_module(module).await;
    }

    /// 初始化事件处理器
    async fn setup_event_handlers(&self) {
        // 注册默认事件处理器
        self.event_bus
            .register_handler("*".to_string(), LogEventHandler)
            .await;

        self.event_bus
            .register_handler("*".to_string(), MetricsEventHandler::new())
            .await;

        // 如果配置了Redis，注册Redis事件处理器
        let config = self.state.config.read().await;
        if let Some(redis_config) = &config.cookie
            && let Some(pool) = create_redis_pool(&redis_config.redis_host,
                redis_config.redis_port,
                redis_config.redis_db,
                &redis_config.redis_username,
                &redis_config.redis_password)
        {
            let redis_handler = RedisEventHandler::new(
                Arc::new(pool),
                config.name.clone(),
                3600, // 1小时TTL，可以从配置中读取
            );

            // self.event_bus
            //     .register_handler("*".to_string(), redis_handler)
            //     .await;

            info!("Redis event handler registered successfully");
        } else {
            info!("Redis not configured, skipping Redis event handler");
        }

        info!("Event handlers registered successfully");
    }

    /// 启动调度器
    pub async fn start(&self) {
        info!("Starting Schedule with event-driven architecture");
        let api_config = self.state.config.read().await.api.clone();
        if let Some(api) = api_config {
            self.start_api(api.port).await;
            info!("API server started on host:  http://127.0.0.1:{}", api.port);
        }

        // 设置事件处理器
        self.setup_event_handlers().await;

        // 启动事件总线
        self.event_bus.start().await;

        // 发布系统启动事件
        if let Err(e) = self
            .event_bus
            .publish(crate::events::SystemEvent::System(SystemStarted))
            .await
        {
            error!("Failed to publish system started event: {e}");
        }

        // Start Cron Scheduler
        {
            use crate::scheduler::CronScheduler;
            let cron_scheduler = Arc::new(CronScheduler::new(
                self.task_manager.clone(),
                self.state.clone(),
                self.queue_manager.clone(),
            ));
            cron_scheduler.start();
            info!("CronScheduler started in background");
        }

        info!("Starting all processors concurrently...");

        // 并发启动所有处理器
        tokio::join!(
            self.start_task_processor(),
            self.start_download_processor(),
            self.start_parser_model_processor(),
            self.start_error_processor(),
            self.start_health_monitor(),
            self.start_response_parser_processor(),
        );
    }

    /// 启动任务处理器
    async fn start_task_processor(&self) {
        info!("Starting task processor");
        let mut shutdown = self.shutdown_tx.subscribe();
        let task_model_chain = Arc::new(
            create_task_model_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
            )
            .await,
        );
        let capacity = self.state.config.read().await.channel_config.capacity;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));
        let task_receiver = self.queue_manager.get_task_pop_channel();
        let mut rx = task_receiver.lock().await;
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Task processor received shutdown signal, exiting loop");
                    break;
                }
                account_task = rx.recv() => {
                    match account_task {
                        Some(account_task) => {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let chain = task_model_chain.clone();
                            let queue_manager = self.queue_manager.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let id = account_task.get_id();
                                match chain.execute(account_task, ProcessorContext::default()).await {
                                    common::processors::processor::ProcessorResult::Success(mut stream) => {
                                        while stream.next().await.is_some() {}
                                        if let Some(comp) = &queue_manager.compensator {
                                            let _ = comp.remove_task("task", &id).await;
                                        }
                                    }
                                    _ => {}
                                }
                            });
                        }
                        None => {
                            info!("Task channel closed, exiting task processor loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// 启动下载处理器
    async fn start_download_processor(&self) {
        info!("Starting download processor");
        let mut shutdown = self.shutdown_tx.subscribe();
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
        let capacity = self.state.config.read().await.channel_config.capacity;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));
        let request_receiver = self.queue_manager.get_request_pop_channel();
        let mut rx = request_receiver.lock().await;
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Download processor received shutdown signal, exiting loop");
                    break;
                }
                request = rx.recv() => {
                    match request {
                        Some(request) => {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let queue_manager = self.queue_manager.clone();
                            if request.downloader.eq_ignore_ascii_case("wss_downloader") {
                                let chain = wss_download_chain.clone();
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    let id = request.get_id();
                                    if chain.execute(request, ProcessorContext::default()).await.is_success() && let Some(comp) = &queue_manager.compensator {
                                            let _ = comp.remove_task("request", &id).await;
                                        }

                                });
                            } else {
                                let chain = download_chain.clone();
                                tokio::spawn(async move {
                                    let _permit = permit;
                                    let id = request.get_id();
                                    if chain.execute(request, ProcessorContext::default()).await.is_success() && let Some(comp) = &queue_manager.compensator {
                                            let _ = comp.remove_task("request", &id).await;

                                    }
                                });
                            }
                        }
                        None => {
                            info!("Request channel closed, exiting download processor loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// 启动解析处理器
    async fn start_parser_model_processor(&self) {
        info!("Starting parser processor");
        let mut shutdown = self.shutdown_tx.subscribe();
        let parser_model_chain = Arc::new(
            create_parser_task_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
            )
            .await,
        );
        let capacity = self.state.config.read().await.channel_config.capacity;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));
        let parser_receiver = self.queue_manager.get_parser_task_pop_channel();
        let mut rx = parser_receiver.lock().await;
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Parser processor received shutdown signal, exiting loop");
                    break;
                }
                task = rx.recv() => {
                    match task {
                        Some(task) => {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let chain = parser_model_chain.clone();
                            let queue_manager = self.queue_manager.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let id = task.get_id();
                                match chain.execute(task, ProcessorContext::default()).await {
                                    common::processors::processor::ProcessorResult::Success(mut stream) => {
                                        while stream.next().await.is_some() {}
                                        if let Some(comp) = &queue_manager.compensator {
                                            let _ = comp.remove_task("parser_task", &id).await;
                                        }
                                    }
                                    _ => {}
                                }
                            });
                        }
                        None => {
                            info!("Parser task channel closed, exiting parser processor loop");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// 启动错误处理器
    async fn start_error_processor(&self) {
        info!("Starting error processor");
        let mut shutdown = self.shutdown_tx.subscribe();
        let error_chain = Arc::new(
            create_error_task_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
            )
            .await,
        );
        let capacity = self.state.config.read().await.channel_config.capacity;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));
        let error_receiver = self.queue_manager.get_error_pop_channel();
        let mut rx = error_receiver.lock().await;
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Error processor received shutdown signal, exiting loop");
                    break;
                }
                task = rx.recv() => {
                    match task {
                        Some(task) => {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let chain = error_chain.clone();
                            let queue_manager = self.queue_manager.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let id = task.get_id();
                                match chain.execute(task, ProcessorContext::default()).await {
                                    common::processors::processor::ProcessorResult::Success(mut stream) => {
                                        while stream.next().await.is_some() {}
                                        if let Some(comp) = &queue_manager.compensator {
                                            let _ = comp.remove_task("error_task", &id).await;
                                        }
                                    }
                                    _ => {}
                                }
                            });
                        }
                        None => {
                            info!("Error channel closed, exiting error processor loop");
                            break;
                        }
                    }
                }
            }
        }
    }
    async fn start_response_parser_processor(&self) {
        info!("Starting response processor");
        let mut shutdown = self.shutdown_tx.subscribe();
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
        let capacity = self.state.config.read().await.channel_config.capacity;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(capacity));
        let response_receiver = self.queue_manager.get_response_pop_channel();
        let mut rx = response_receiver.lock().await;
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Response processor received shutdown signal, exiting loop");
                    break;
                }
                response = rx.recv() => {
                    match response {
                        Some(response) => {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let chain = parser_chain.clone();
                            let queue_manager = self.queue_manager.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let id = response.get_id();
                                if chain.execute(response, ProcessorContext::default()).await.is_success() && let Some(comp) = &queue_manager.compensator {
                                        let _ = comp.remove_task("response", &id).await;

                                }
                            });
                        }
                        None => {
                            info!("Response channel closed, exiting response processor loop");
                            break;
                        }
                    }
                }
            }
        }
    }
    /// 启动健康监控
    async fn start_health_monitor(&self) {
        info!("Starting health monitor");

        let event_bus = Arc::clone(&self.event_bus);
        let mut shutdown = self.shutdown_tx.subscribe();

        // 每30秒进行一次健康检查
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Health monitor received shutdown signal, exiting loop");
                    break;
                }
                _ = interval.tick() => {
                    // 发布健康检查事件
                    let health_event = crate::events::SystemEvent::System(ComponentHealthCheck(
                        crate::events::HealthCheckEvent {
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
                    ));

                    if let Err(e) = event_bus.publish(health_event).await {
                        error!("Failed to publish health check event: {e}");
                    }
                }
            }
        }
    }

    /// 优雅关闭
    pub async fn shutdown(&self) {
        info!("Shutting down Schedule");

        // 通知所有后台处理器优雅退出
        let _ = self.shutdown_tx.send(());

        // 发布系统关闭事件
        if let Err(e) = self
            .event_bus
            .publish(crate::events::SystemEvent::System(SystemShutdown))
            .await
        {
            error!("Failed to publish system shutdown event: {e}");
        }

        // 停止事件总线
        self.event_bus.stop();

        info!("Schedule shutdown completed");
    }

    /// 获取系统统计信息
    pub async fn get_system_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "processors": {
                "task": "active",
                "download": "active",
                "parser": "active",
                "error": "active"
            },
            "event_bus": "active",
            "uptime": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        })
    }

    pub async fn start_api(&self, port: u16) {
        let queue_manager = self.queue_manager.clone();
        tokio::spawn(async move {
            let api_state = crate::api::state::ApiState { queue_manager };
            let app = crate::api::router::router().with_state(api_state);
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
}
