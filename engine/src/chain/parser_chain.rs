use common::status_tracker::ErrorDecision;
use crate::events::EventParser::ParserFailed;
use crate::events::EventResponseModuleLoad::{ModuleGenerateFailed, ModuleGenerateRetry};
use crate::events::{
    DataMiddlewareEvent, DataStoreEvent, DynFailureEvent, DynRetryEvent, EventBus,
    EventDataMiddleware, EventDataStore, EventParser, EventResponseModuleLoad, ParserEvent,
    ResponseModuleLoad, SystemEvent,
};
use crate::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use async_trait::async_trait;
use errors::{Error, Result};
use crate::task::TaskManager;
use common::interface::middleware_manager::MiddlewareManager;
use common::model::data::Data;
use common::model::message::{ErrorTaskModel, TaskModel};
use common::model::{ModuleConfig, Response};
use common::state::State;

use log::{debug, error, info, warn};
use queue::{QueueManager, QueuedItem};
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use common::processors::processor_chain::ErrorStrategy;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use dashmap::DashMap;
use cacheable::{CacheAble, CacheService};
use crate::task::module::Module;
use futures::StreamExt;
use common::model::login_info::LoginInfo;

pub struct ResponseModuleProcessor {
    task_manager: Arc<TaskManager>,
    cache_service: Arc<CacheService>,
    state: Arc<State>,
    config_cache: Arc<DashMap<String, (Arc<ModuleConfig>, Instant)>>,
}
#[async_trait]
impl ProcessorTrait<Response, (Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)> for ResponseModuleProcessor {
    fn name(&self) -> &'static str {
        "ResponseModuleProcessor"
    }

    async fn process(
        &self,
        input: Response,
        context: ProcessorContext,
    ) -> ProcessorResult<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)> {
        // [LOG_OPTIMIZATION] Removed start log to reduce I/O blocking latency
        // info!(
        //     "[ResponseModuleProcessor] start: request_id={} module_id={}",
        //     input.id,
        //     input.module_id()
        // );

        // 在解析之前检查 Task 和 Module 是否已达到错误阈值
        // 1. 检查 Task 级别
        match self
            .state
            .status_tracker
            .should_task_continue(&input.task_id())
            .await
        {
            Ok(ErrorDecision::Continue) => {
                // [LOG_OPTIMIZATION] debug!("[ResponseModuleProcessor] task check passed: task_id={}", input.task_id());
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[ResponseModuleProcessor] task terminated before parsing: task_id={} reason={}",
                    input.task_id(),
                    reason
                );
                // 释放锁
                self.state.status_tracker
                    .release_module_locker(&input.module_id())
                    .await;
                return ProcessorResult::FatalFailure(
                    errors::ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] task error check failed, continue anyway: task_id={} error={}",
                    input.task_id(),
                    e
                );
            }
            _ => {}
        }

        // 2. 检查 Module 级别
        match self
            .state
            .status_tracker
            .should_module_continue(&input.module_id())
            .await
        {
            Ok(ErrorDecision::Continue) => {
                // [LOG_OPTIMIZATION] debug!("[ResponseModuleProcessor] module check passed: module_id={}", input.module_id());
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[ResponseModuleProcessor] module terminated before parsing: module_id={} reason={}",
                    input.module_id(),
                    reason
                );
                // 释放锁
                self.state.status_tracker
                    .release_module_locker(&input.module_id())
                    .await;
                return ProcessorResult::FatalFailure(
                    errors::ModuleError::ModuleMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] module error check failed, continue anyway: module_id={} error={}",
                    input.module_id(),
                    e
                );
            }
            _ => {}
        }

        let task: Result<(Arc<Module>, Option<LoginInfo>)> = self.task_manager.load_module_with_response(&input).await;
        match task {
            Ok((module, login_info)) => {
                // 优先从本地缓存获取配置
                let module_id = module.id();
                let cached_config = if let Some(entry) = self.config_cache.get(&module_id) {
                    let (cfg, expires_at) = entry.value();
                    if Instant::now() < *expires_at {
                        Some(cfg.clone())
                    } else {
                        None
                    }
                } else {
                    None
                };

                let config = if let Some(c) = cached_config {
                    c
                } else {
                    match ModuleConfig::sync(
                        &module_id,
                        &self.cache_service,
                    )
                        .await
                    {
                        Ok(Some(config)) => {
                            let config = Arc::new(config);
                            self.config_cache.insert(module_id, (config.clone(), Instant::now() + Duration::from_secs(10)));
                            config
                        },
                        _ => module.config.clone(),
                    }
                };
                
                // info!(
                //     "[ResponseModuleProcessor] module loaded: module_name={} module_id={}",
                //     module.module.name(),
                //     module.id()
                // );
                ProcessorResult::Success((input, module, config, login_info))
            }
            Err(e) => {
                warn!("[ResponseModuleProcessor] load_with_response failed, will retry: err={e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, input: &Response, _context: &ProcessorContext) -> Result<()> {
        if self.state.config.read().await.download_config.enable_locker {
            debug!(
                "[ResponseModuleProcessor] lock module before parsing: module_id={}",
                input.module_id()
            );
            self.state.status_tracker.lock_module(&input.module_id()).await;
        }
        Ok(())
    }
}
#[async_trait]
impl EventProcessorTrait<Response, (Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)> for ResponseModuleProcessor {
    fn pre_status(&self, input: &Response) -> Option<SystemEvent> {
        Some(SystemEvent::ResponseModuleLoad(EventResponseModuleLoad::ModuleGenerate(input.into())))
    }

    fn finish_status(&self, input: &Response, _output: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)) -> Option<SystemEvent> {
        Some(SystemEvent::ResponseModuleLoad(EventResponseModuleLoad::ModuleGenerateCompleted(
            input.into(),
        )))
    }

    fn working_status(&self, input: &Response) -> Option<SystemEvent> {
        Some(SystemEvent::ResponseModuleLoad(EventResponseModuleLoad::ModuleGenerateStarted(
            input.into(),
        )))
    }

    fn error_status(&self, input: &Response, err: &Error) -> Option<SystemEvent> {
        let event: ResponseModuleLoad = input.into();
        let failure = DynFailureEvent {
            data: event,
            error: err.to_string(),
        };
        Some(SystemEvent::ResponseModuleLoad(ModuleGenerateFailed(failure)))
    }

    fn retry_status(&self, input: &Response, retry_policy: &RetryPolicy) -> Option<SystemEvent> {
        let event: ResponseModuleLoad = input.into();
        let retry = DynRetryEvent {
            data: event,
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        Some(SystemEvent::ResponseModuleLoad(ModuleGenerateRetry(retry)))
    }
}

pub struct ResponseParserProcessor {
    queue_manager: Arc<QueueManager>,
    state: Arc<State>,
    cache_service: Arc<CacheService>,
}
#[async_trait]
impl ProcessorTrait<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), Vec<Data>> for ResponseParserProcessor {
    fn name(&self) -> &'static str {
        "ResponseParserProcessor"
    }

    async fn process(
        &self,
        input: (Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        context: ProcessorContext,
    ) -> ProcessorResult<Vec<Data>> {
        info!(
            "[ResponseParserProcessor] start parse: request_id={} module_id={}",
            input.0.id,
            input.0.module_id()
        );
        let module = input.1.clone();
        let config = input.2.clone();
        let login_info = input.3.clone();

        // StateHandle has been removed; SyncService is used internally by ModuleProcessor.
        let task_id = input.0.task_id();
        let module_id = input.0.module_id();
        let request_id = input.0.id.to_string();

        let data = module.parser(input.0.clone(), Some(config)).await;
        let mut data = match data {
            Ok(d) => {
                let has_next_task = d.parser_task.is_some();
                let has_error = d.error_task.is_some();
                info!(
                    "[ResponseParserProcessor] parser returned: request_id={} data_len={} has_next_task={} has_error={}",
                    request_id,
                    d.data.len(),
                    has_next_task,
                    has_error
                );

                // 记录解析成功
                self.state
                    .status_tracker
                    .record_parse_success(&request_id)
                    .await
                    .ok();

                d
            }
            Err(e) => {
                warn!("[ResponseParserProcessor] parser error: err={e}");

                // 记录解析错误并获取决策
                match self
                    .state
                    .status_tracker
                    .record_parse_error(&task_id, &module_id, &request_id, &e)
                    .await
                {
                    Ok(ErrorDecision::Continue)
                    | Ok(ErrorDecision::RetryAfter(_)) => {
                        debug!(
                            "[ResponseParserProcessor] will retry parsing: request_id={}",
                            request_id
                        );
                        return ProcessorResult::RetryableFailure(
                            context
                                .retry_policy
                                .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                        );
                    }
                    Ok(ErrorDecision::Skip) => {
                        warn!(
                            "[ResponseParserProcessor] skip parse after max retries: request_id={}",
                            request_id
                        );
                        // 跳过该解析，返回空数据
                        return ProcessorResult::Success(vec![]);
                    }
                    Ok(ErrorDecision::Terminate(reason)) => {
                        error!("[ResponseParserProcessor] terminate: {}", reason);
                        return ProcessorResult::FatalFailure(e);
                    }
                    Err(err) => {
                        error!("[ResponseParserProcessor] error tracker failed: {}", err);
                        return ProcessorResult::FatalFailure(e);
                    }
                }
            }
        };

        if let Some(task) = data.parser_task.take() {
             // OPTIMIZATION: Check if we can bypass the queue and generate requests locally
             let target_module_name = task.account_task.module.as_ref().and_then(|v| v.first());
             let is_same_module = target_module_name.map_or(true, |name| name == module.module.name().as_str());
             
             // Check if account/platform match (usually yes for parser tasks)
             let is_same_context = task.account_task.account == module.account.name && task.account_task.platform == module.platform.name;

             if is_same_module && is_same_context {
                 debug!("[ResponseParserProcessor] Optimizing: Generating requests locally for same module");
                 
                 // Create a shallow clone of the module to inject context
                 let mut module_clone = (*module).clone(); 
                 module_clone.pending_ctx = Some(task.context.clone());
                 module_clone.prefix_request = task.prefix_request;
                 module_clone.run_id = task.run_id;

                 let task_meta = task.metadata.as_object().cloned().unwrap_or_default();
                 
                 match module_clone.generate(task_meta, login_info).await {
                     Ok(mut stream) => {
                         let request_queue = self.queue_manager.get_request_push_channel();
                         let mut generated_count = 0;
                         while let Some(req) = stream.next().await {
                               if let Err(e) = request_queue.send(QueuedItem::new(req)).await {
                                   error!("[ResponseParserProcessor] Failed to send generated request: {}", e);
                               } else {
                                   generated_count += 1;
                               }
                         }
                         info!("[ResponseParserProcessor] Locally generated {} requests", generated_count);
                         // Successfully handled, do NOT enqueue ParserTaskModel
                     },
                     Err(e) => {
                         error!("[ResponseParserProcessor] Failed to generate requests locally: {}, falling back to queue", e);
                         // Fallback: put the task back into parser_task queue
                         let queue = self.queue_manager.get_parser_task_push_channel();
                         if let Err(e) = queue.send(QueuedItem::new(task)).await {
                             error!("[ResponseParserProcessor] failed to send parser task fallback: {e}");
                         }
                     }
                 }
             } else {
                // Different module/context, enqueue as usual
                let queue = self.queue_manager.get_parser_task_push_channel();
                if let Err(e) = queue.send(QueuedItem::new(task)).await {
                     error!("[ResponseParserProcessor] failed to send parser task: {e}");
                }
             }
        }
        
        if let Some(mut msg) = data.error_task {
            warn!(
                "[ResponseParserProcessor] recorded response error for request_id={}, message={}",
                input.0.id, msg.error_msg
            );
            msg.prefix_request = input.0.prefix_request;
            msg.run_id = input.0.run_id;
            let queue = self.queue_manager.get_error_push_channel();
            if let Err(e) = queue.send(QueuedItem::new(msg)).await {
                error!("[ResponseParserProcessor] failed to send parser error: {e}");
            }

            // 不再使用旧的 record_response_error
            // 错误已经通过 error_tracker 记录
        }

        data.data.iter_mut().for_each(|x| {
            x.account = input.0.account.clone();
            x.platform = input.0.platform.clone();
            // 保留原有的data_middleware，避免覆盖掉之前的中间件
            // 一般情况下data_middleware不会在parser中设置
            // 只有在特殊情况下才会在parser中设置data_middleware，比如需要对某些数据进行特殊处理或者在测试情况下
            // 这种情况下需要在module的config中设置data_middleware
            // 如果parser中没有设置data_middleware，则使用module的config中的data_middleware
            // 如果parser中设置了data_middleware，则使用parser中的data_middleware
            if x.data_middleware.is_empty() {
                x.data_middleware = input.0.data_middleware.clone();
            }
            x.request_id = input.0.id;
            x.meta = input.0.metadata.clone();
        });
        ProcessorResult::Success(data.data)
    }
    async fn post_process(
        &self,
        input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        _output: &Vec<Data>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        // 若parser返回了ParserTaskModel 需要释放锁避免死锁
        // 所有的锁释放都在这里进行，避免重复释放锁

        let config = self.state.config.read().await;

        // 缓存响应结果，出现重复下载可跳过下载阶段
        if config.download_config.enable_cache {
            if let Some(request_hash) = &input.0.request_hash
            {
                input.0.send(request_hash, &self.cache_service).await.ok();
            }
        }

        if config.download_config.enable_locker {
            self.state.status_tracker
                .release_module_locker(&input.0.module_id())
                .await;
            debug!(
                "[ResponseParserProcessor] released module lock after parsing: module_id={}",
                input.0.module_id()
            );
        }
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Vec<Data>> {
        error!(
            "[ResponseParserProcessor] fatal parser error: request_id={} module_id={} error={}",
            input.0.id,
            input.0.module_id(),
            error
        );

        // 错误已经在 process() 方法中通过 error_tracker 记录
        // 这里只需要创建 ErrorTaskModel 并入队

        let error_task = ErrorTaskModel {
            id: input.0.id,
            account_task: TaskModel {
                account: input.0.account.clone(),
                platform: input.0.platform.clone(),
                module: Some(vec![input.0.module.clone()]),
                run_id: input.0.run_id,
                priority: input.0.priority,
            },
            error_msg: error.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metadata: input.0.metadata.clone().into(),
            context: input.0.context.clone(),
            run_id: input.0.run_id,
            prefix_request: input.0.prefix_request,
        };
        let queue = self.queue_manager.get_error_push_channel();
        if let Err(e) = queue.send(QueuedItem::new(error_task)).await {
            error!("[ResponseParserProcessor] failed to enqueue ErrorTaskModel: {e}");
        }

        // 释放模块锁
        if self.state.config.read().await.download_config.enable_locker {
           self.state
               .status_tracker
               .release_module_locker(&input.0.module_id())
               .await;
        }

        ProcessorResult::FatalFailure(error)
    }
}
impl EventProcessorTrait<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), Vec<Data>> for ResponseParserProcessor {
    fn pre_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)) -> Option<SystemEvent> {
        Some(SystemEvent::Parser(EventParser::ParserReceived((&input.0).into())))
    }

    fn finish_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), _output: &Vec<Data>) -> Option<SystemEvent> {
        Some(SystemEvent::Parser(EventParser::ParserCompleted((&input.0).into())))
    }

    fn working_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)) -> Option<SystemEvent> {
        Some(SystemEvent::Parser(EventParser::ParserStarted((&input.0).into())))
    }

    fn error_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), err: &Error) -> Option<SystemEvent> {
        let event: ParserEvent = (&input.0).into();
        let failure = DynFailureEvent {
            data: event,
            error: err.to_string(),
        };
        Some(SystemEvent::Parser(ParserFailed(failure)))
    }

    fn retry_status(
        &self,
        input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        retry_policy: &RetryPolicy,
    ) -> Option<SystemEvent> {
        let event: ParserEvent = (&input.0).into();
        let retry = DynRetryEvent {
            data: event,
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        Some(SystemEvent::Parser(EventParser::ParserRetry(retry)))
    }
}

pub struct DataMiddlewareProcessor {
    middleware_manager: Arc<MiddlewareManager>,
}

#[async_trait]
impl ProcessorTrait<Data, Data> for DataMiddlewareProcessor {
    fn name(&self) -> &'static str {
        "DataMiddlewareProcessor"
    }

    async fn process(&self, input: Data, context: ProcessorContext) -> ProcessorResult<Data> {
        // info!(
        //    "[DataMiddlewareProcessor] start: account={} platform={} size={}",
        //    input.account,
        //    input.platform,
        //    input.size()
        // );
        let start = std::time::Instant::now();
        let config = context
            .metadata
            .read()
            .await
            .get("config")
            .map(|x| serde_json::from_value::<ModuleConfig>(x.clone()).unwrap_or_default());
        let modified_data = self.middleware_manager.handle_data(input, &config).await;
        if start.elapsed().as_millis() > 10 {
            info!("[DataMiddlewareProcessor] SLOW middleware execution: {} ms", start.elapsed().as_millis());
        }
        ProcessorResult::Success(modified_data)
    }
}
impl EventProcessorTrait<Data, Data> for DataMiddlewareProcessor {
    fn pre_status(&self, input: &Data) -> Option<SystemEvent> {
        Some(SystemEvent::DataMiddleware(EventDataMiddleware::MiddlewareBefore(input.into())))
    }

    fn finish_status(&self, input: &Data, output: &Data) -> Option<SystemEvent> {
        let mut event: DataMiddlewareEvent = input.into();
        event.after_size = output.size().into();
        Some(SystemEvent::DataMiddleware(EventDataMiddleware::MiddlewareCompleted(event)))
    }

    fn working_status(&self, input: &Data) -> Option<SystemEvent> {
        Some(SystemEvent::DataMiddleware(EventDataMiddleware::MiddlewareStarted(input.into())))
    }

    fn error_status(&self, input: &Data, err: &Error) -> Option<SystemEvent> {
        let event: DataMiddlewareEvent = input.into();
        let failure = DynFailureEvent {
            data: event,
            error: err.to_string(),
        };
        Some(SystemEvent::DataMiddleware(EventDataMiddleware::MiddlewareFailed(failure)))
    }

    fn retry_status(&self, input: &Data, retry_policy: &RetryPolicy) -> Option<SystemEvent> {
        let event: DataMiddlewareEvent = input.into();
        let retry = DynRetryEvent {
            data: event,
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        Some(SystemEvent::DataMiddleware(EventDataMiddleware::MiddlewareRetry(retry)))
    }
}

pub struct DataStoreProcessor {
    middleware_manager: Arc<MiddlewareManager>,
}
#[async_trait]
impl ProcessorTrait<Data, ()> for DataStoreProcessor {
    fn name(&self) -> &'static str {
        "DataStoreProcessor"
    }
    /// 此阶段使用RetryableFailure来触发重试机制,并使用retry_policy的meta字段来传递一些重试的参数，供以后使用重试功能作参考
    async fn process(&self, input: Data, context: ProcessorContext) -> ProcessorResult<()> {
        info!(
            "[DataStoreProcessor] start store: request_id={} account={} platform={} module={} size={}",
            input.request_id,
            input.account,
            input.platform,
            input.module,
            input.size()
        );
        let mut middleware = vec![];
        if let Some(retry_policy) = &context.retry_policy
            && let Some(m_val) = retry_policy.meta.get("middleware")
                && let Some(m) = m_val.as_array() {
                    middleware = m.iter()
                        .filter_map(|x| x.as_str())
                        .map(|x| x.to_string())
                        .collect::<Vec<String>>();
                }
        let config = context
            .metadata
            .read()
            .await
            .get("config")
            .map(|x| serde_json::from_value::<ModuleConfig>(x.clone()).unwrap_or_default());
        let request_id = input.request_id;
        let res = if middleware.is_empty() {
            self.middleware_manager
                .handle_store_data(input, &config)
                .await
        } else {
            self.middleware_manager
                .handle_store_data_with_middleware(input, middleware, &config)
                .await
        };
        if res.is_empty() {
            info!(
                "[DataStoreProcessor] store success, request_id={}",
                request_id
            );
            ProcessorResult::Success(())
        } else {
            let error_msg = res
                .iter()
                .map(|(m, e)| format!("Middleware: {m}, Error: {e:?}"))
                .collect::<Vec<String>>()
                .join("; ");
            let mut retry_policy = context
                .retry_policy
                .unwrap_or_default()
                .with_reason(error_msg);
            let error_middleware = res.keys().map(|x| x.to_string()).collect::<Vec<String>>();
            retry_policy.meta = serde_json::json!({ "middleware": error_middleware });
            warn!(
                "[DataStoreProcessor] request={}, store error, will retry: {}",
                request_id,
                retry_policy.reason.clone().unwrap_or_default()
            );
            ProcessorResult::RetryableFailure(retry_policy)
        }
    }
}
impl EventProcessorTrait<Data, ()> for DataStoreProcessor {
    fn pre_status(&self, input: &Data) -> Option<SystemEvent> {
        Some(SystemEvent::DataStore(EventDataStore::StoreBefore(input.into())))
    }

    fn finish_status(&self, input: &Data, _output: &()) -> Option<SystemEvent> {
        Some(SystemEvent::DataStore(EventDataStore::StoreCompleted(input.into())))
    }

    fn working_status(&self, input: &Data) -> Option<SystemEvent> {
        Some(SystemEvent::DataStore(EventDataStore::StoreStarted(input.into())))
    }

    fn error_status(&self, input: &Data, err: &Error) -> Option<SystemEvent> {
        let event: DataStoreEvent = input.into();
        let failure = DynFailureEvent {
            data: event,
            error: err.to_string(),
        };
        Some(SystemEvent::DataStore(EventDataStore::StoreFailed(failure)))
    }

    fn retry_status(&self, input: &Data, retry_policy: &RetryPolicy) -> Option<SystemEvent> {
        let event: DataStoreEvent = input.into();
        let retry = DynRetryEvent {
            data: event,
            retry_count: retry_policy.current_retry,
            reason: retry_policy.reason.clone().unwrap_or_default(),
        };
        Some(SystemEvent::DataStore(EventDataStore::StoreRetry(retry)))
    }
}

/// 解析与存储暂时只作为一个整体来处理
/// 同一个chain属于同一个response，即所有的Data使用的ModuleConfig是同样的
/// 使用ProcessorContext.meta来传递module的config，这样Data可以拆分出来，不用将Vec<Data>作为一个整体
/// response -> (module, config) -> parser -> data -> (data, config) -> data_middleware -> data -> data_store -> ()
pub async fn create_parser_chain(
    state: Arc<State>,
    task_manager: Arc<TaskManager>,
    middleware_manager: Arc<MiddlewareManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    cache_service: Arc<CacheService>,
) -> EventAwareTypedChain<Response, Vec<()>> {
    let response_module_processor = ResponseModuleProcessor {
        task_manager,
        cache_service: cache_service.clone(),
        state: state.clone(),
        config_cache: Arc::new(DashMap::new()),
    };
    let response_parser_processor = ResponseParserProcessor {
        queue_manager,
        state,
        cache_service: cache_service.clone(),
    };
    let data_middleware_processor = DataMiddlewareProcessor {
        middleware_manager: middleware_manager.clone(),
    };
    let data_store_processor = DataStoreProcessor { middleware_manager };

    EventAwareTypedChain::<Response, Response>::new(event_bus)
        .then::<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), _>(response_module_processor)
        .then::<Vec<Data>, _>(response_parser_processor)
        .then_map_vec_parallel_with_strategy_silent::<Data, _>(
            data_middleware_processor,
            64,
            ErrorStrategy::Skip,
        )
        .then_map_vec_parallel_with_strategy_silent::<(), _>(data_store_processor, 64, ErrorStrategy::Skip)
}
