#![allow(unused)]
use common::status_tracker::ErrorDecision;
use log::info;
use crate::events::{
    EventBus, EventEnvelope, EventPhase, EventType, ModuleGenerateEvent, ParserTaskModelEvent,
    RequestEvent, TaskModelEvent,
};
use crate::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};


use async_trait::async_trait;
use errors::{Error, ModuleError, Result};
use common::model::login_info::LoginInfo;
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::model::{ModuleConfig, Request};
use common::state::State;

use log::{debug, error, warn};
use queue::{QueueManager, QueuedItem};
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use common::processors::processor_chain::ErrorStrategy;
use std::sync::Arc;
use uuid::Uuid;
use futures::stream::{StreamExt};
use std::marker::PhantomData;
use serde_json::json;

use crate::deduplication::Deduplicator;

/// 任务模型处理器
///
/// 负责处理 TaskModel，协调配置加载、任务转换、请求发布等流程。
/// 作为事件驱动的处理链的一部分，它会发布处理过程中的状态变更事件。
pub struct TaskModelProcessor {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
}
#[async_trait]
impl ProcessorTrait<TaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(&self, input: TaskModel, context: ProcessorContext) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor] Processing Task: account={} platform={} modules={:?} retry={}",
            input.account,
            input.platform,
            input.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        let task_id = format!("{}-{}", input.account, input.platform);
        match self.state.status_tracker.should_task_continue(&task_id).await {
            Ok(ErrorDecision::Continue) => {}
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskModelProcessor<TaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                if let Err(e) = self.queue_manager.send_to_dlq("task", &input, &reason).await {
                    error!("[TaskModelProcessor<TaskModel>] failed to send to DLQ: {}", e);
                }
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[TaskModelProcessor<TaskModel>] error tracker check failed: task_id={} error={}, continue anyway",
                    task_id, e
                );
            }
            _ => {}
        }

        let task = self.task_manager.load_with_model(&input).await;
        debug!(
            "[TaskModelProcessor] load_with_model result: {:?}",
            task.as_ref().map(|t| t.id())
        );

        match task {
            Ok(task) => {
                if task.is_empty() {
                    return ProcessorResult::FatalFailure(
                        ModuleError::ModuleNotFound(
                            format!(
                                "No modules found for the given TaskModel, task_model: {input:?}"
                            )
                            .into(),
                        )
                        .into(),
                    );
                }
                let default_locker_ttl = self.state.config.read().await.crawler.module_locker_ttl;
                let mut all_locked = true;
                for m in task.modules.iter() {
                    if !self
                        .state
                        .status_tracker
                        .is_module_locker(m.id().as_ref(), default_locker_ttl)
                        .await
                    {
                        all_locked = false;
                        break;
                    }
                }
                if all_locked {
                    warn!(
                        "[TaskModelProcessor<TaskModel>] all target modules locked, requeue TaskModel: account={} platform={}",
                        input.account, input.platform
                    );
                    let sender = self.queue_manager.get_task_push_channel();
                    if let Err(e) = sender.send(QueuedItem::new(input.clone())).await {
                        warn!(
                            "[TaskModelProcessor<TaskModel>] requeue TaskModel failed, will retry: {}",
                            e
                        );
                    }
                    return ProcessorResult::RetryableFailure(
                        context.retry_policy.unwrap_or_default(),
                    );
                }

                info!(
                    "[TaskModelProcessor] Successfully converted TaskModel to Task. Task ID: {}, Modules count: {}",
                    task.id(),
                    task.modules.len()
                );

                ProcessorResult::Success(task)
            }
            Err(e) => {
                debug!("[TaskModelProcessor] load_with_model failed: {e}");
                warn!("[TaskModelProcessor<TaskModel>] load_with_model failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &TaskModel, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &TaskModel,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorTaskModel {
            id: Default::default(),
            account_task: input.clone(),
            error_msg: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: Uuid::nil(),
        };
        if let Err(e) = sender.send(QueuedItem::new(error_msg)).await {
            error!("[TaskModelProcessor<TaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<TaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &TaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Started,
            TaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &TaskModel, output: &Task) -> Option<EventEnvelope> {
        let mut task_model_event: TaskModelEvent = input.into();
        task_model_event.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Completed,
            task_model_event,
        ))
    }

    fn working_status(&self, input: &TaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Started,
            TaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &TaskModel, error: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::TaskModel,
            EventPhase::Failed,
            TaskModelEvent::from(input),
            error,
        ))
    }

    fn retry_status(&self, input: &TaskModel, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Retry,
            json!({
                "data": TaskModelEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
#[async_trait]
impl ProcessorTrait<ParserTaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: ParserTaskModel,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<ParserTaskModel>] start: account={} platform={} crawler={:?} retry_count={}",
            input.account_task.account,
            input.account_task.platform,
            input.account_task.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        // 首先检查 Task 是否已被标记为终止
        let task_id = format!("{}:{}:{}", input.account_task.platform, input.account_task.account, input.run_id);
        match self.state.status_tracker.should_task_continue(&task_id).await {
            Ok(ErrorDecision::Continue) => {
                debug!(
                    "[TaskModelProcessor<ParserTaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskModelProcessor<ParserTaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[TaskModelProcessor<ParserTaskModel>] error tracker check failed: task_id={} error={}, continue anyway",
                    task_id, e
                );
            }
            _ => {}
        }

        let task = self.task_manager.load_parser(&input).await;
        match task {
            Ok(task) => {
                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<ParserTaskModel>] load_parser failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(
        &self,
        _input: &ParserTaskModel,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &ParserTaskModel,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorTaskModel {
            id: Default::default(),
            account_task: input.account_task.clone(),
            error_msg: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: input.prefix_request,
        };
        if let Err(e) = sender.send(QueuedItem::new(error_msg)).await {
            error!("[TaskModelProcessor<ParserTaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<ParserTaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &ParserTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &ParserTaskModel, output: &Task) -> Option<EventEnvelope> {
        let mut evt: ParserTaskModelEvent = input.into();
        evt.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Completed,
            evt,
        ))
    }

    fn working_status(&self, input: &ParserTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &ParserTaskModel, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ParserTaskModel,
            EventPhase::Failed,
            ParserTaskModelEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &ParserTaskModel, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Retry,
            json!({
                "data": ParserTaskModelEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
#[async_trait]
impl ProcessorTrait<ErrorTaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: ErrorTaskModel,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<ErrorTaskModel>] start: account={} platform={} crawler={:?} retry_count={}",
            input.account_task.account,
            input.account_task.platform,
            input.account_task.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        // 首先检查 Task 是否已被标记为终止
        let task_id = format!("{}:{}:{}", input.account_task.platform, input.account_task.account, input.run_id);
        match self.state.status_tracker.should_task_continue(&task_id).await {
            Ok(ErrorDecision::Continue) => {
                debug!(
                    "[TaskModelProcessor<ErrorTaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskModelProcessor<ErrorTaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                if let Err(e) = self.queue_manager.send_to_dlq("error_task", &input, &reason).await {
                    error!("[TaskModelProcessor<ErrorTaskModel>] failed to send to DLQ: {}", e);
                }
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[TaskModelProcessor<ErrorTaskModel>] error tracker check failed: task_id={} error={}, continue anyway",
                    task_id, e
                );
            }
            _ => {}
        }

        let task = self.task_manager.load_error(&input).await;
        match task {
            Ok(task) => {
                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<ErrorTaskModel>] load_error failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, input: &ErrorTaskModel, _context: &ProcessorContext) -> Result<()> {
        // 从 ErrorTaskModel 生成 task_id 和 module_id
        // task_id 格式：{platform}:{account}:{run_id}
        let task_id = format!(
            "{}:{}:{}",
            input.account_task.platform, input.account_task.account, input.run_id
        );

        // 记录到 error tracker
        // 对于 ErrorTaskModel，我们将其视为一个通用错误并记录到 Task 级别
        // 这样可以追踪重试次数和错误模式
        debug!(
            "[TaskModelProcessor<ErrorTaskModel>] recording error to tracker: task_id={} error={}",
            task_id, input.error_msg
        );

        // 如果有具体的 module 信息，记录到 module 级别
        if let Some(modules) = &input.account_task.module {
            for module_name in modules {
                // Module ID 格式必须与 Module.id() 保持一致：{account}-{platform}-{module}
                let module_id = format!(
                    "{}-{}-{}",
                    input.account_task.account,
                    input.account_task.platform,
                    module_name
                );

                // 创建一个简单的 Error 用于分类
                let error = ModuleError::ModuleNotFound(input.error_msg.clone().into()).into();

                // 记录解析错误（因为 ErrorTaskModel 通常是解析或处理阶段的错误）
                if input.prefix_request != Uuid::nil() {
                    let request_id = input.prefix_request.to_string();
                    debug!(
                        "[TaskModelProcessor<ErrorTaskModel>] recording parse error: task_id={} module_id={} request_id={}",
                        task_id, module_id, request_id
                    );
                    let _ = self
                        .state
                        .status_tracker
                        .record_parse_error(&task_id, &module_id, &request_id, &error)
                        .await;
                } else {
                    warn!(
                        "[TaskModelProcessor<ErrorTaskModel>] skipping error record: no prefix_request, task_id={} module_id={}",
                        task_id, module_id
                    );
                }
            }
        }

        Ok(())
    }
    // async fn handle_error(
    //     &self,
    //     input: &ParserErrorMessage,
    //     _error: Error,
    //     _context: &ProcessorContext,
    // ) -> ProcessorResult<Task> {
    //     // 由于使用的是动态加载dylib,task_manager.load_error可能未能加载到正确的库
    //     let sender = self.queue_manager.get_error_push_channel();
    //     sender.send(input.to_owned()).await.unwrap();
    //     ProcessorResult::FatalFailure(
    //         ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
    //     )
    // }
}
#[async_trait]
impl EventProcessorTrait<ErrorTaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &ErrorTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &ErrorTaskModel, output: &Task) -> Option<EventEnvelope> {
        let mut evt: ParserTaskModelEvent = input.into();
        evt.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Completed,
            evt,
        ))
    }

    fn working_status(&self, input: &ErrorTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &ErrorTaskModel, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ParserTaskModel,
            EventPhase::Failed,
            ParserTaskModelEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &ErrorTaskModel, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Retry,
            json!({
                "data": ParserTaskModelEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}

pub struct TaskModuleProcessor {
    state: Arc<State>,
}
#[async_trait]
impl ProcessorTrait<Task, Vec<Module>> for TaskModuleProcessor {
    fn name(&self) -> &'static str {
        "TaskModuleProcessor"
    }

    async fn process(
        &self,
        input: Task,
        context: ProcessorContext,
    ) -> ProcessorResult<Vec<Module>> {
        debug!(
            "[TaskModuleProcessor] start: task_id={} module_count={}",
            input.id(),
            input.modules.len()
        );
        let task_id = input.id();
        let metadata = input.metadata.clone();
        let login_info = input.login_info.clone();
        let mut modules: Vec<Module> = Vec::new();
        let default_locker_ttl = self.state.config.read().await.crawler.module_locker_ttl;
        for mut module in input.modules {
            // 使用 ErrorTracker 检查 Module 是否应该继续
            match self
                .state
                .status_tracker
                .should_module_continue(&module.id())
                .await
            {
                Ok(ErrorDecision::Continue) => {
                    debug!(
                        "[TaskModuleProcessor] module can continue: module_id={}",
                        module.id()
                    );
                }
                Ok(ErrorDecision::Terminate(reason)) => {
                    // Module 已达到错误阈值，跳过该 Module，继续处理其他 Module
                    error!(
                        "[TaskModuleProcessor] skip terminated module: module_id={} reason={}",
                        module.id(),
                        reason
                    );
                    // 不返回错误，只是跳过这个 Module
                    continue;
                }
                Err(e) => {
                    warn!(
                        "[TaskModuleProcessor] error tracker check failed for module: module_id={} error={}, continue anyway",
                        module.id(),
                        e
                    );
                }
                _ => {}
            }

            module.locker_ttl = module
                .config
                .get_config_value("module_locker_ttl")
                .and_then(|v| v.as_u64())
                .unwrap_or(default_locker_ttl);
            modules.push(module);
        }

        let mut filtered_modules: Vec<Module> = Vec::new();
        for x in modules.into_iter() {
            filtered_modules.push(x);
        }
        let modules = filtered_modules;
        if !modules.is_empty() {
            let (task_meta_val, login_info_val) = match tokio::task::spawn_blocking(move || {
                let task_meta_val = serde_json::to_value(metadata)
                    .map_err(|e| format!("task_meta serialize failed: {e}"))?;
                let login_info_val = serde_json::to_value(login_info)
                    .map_err(|e| format!("login_info serialize failed: {e}"))?;
                Ok::<_, String>((task_meta_val, login_info_val))
            })
                .await
            {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    error!(
                        "[TaskModuleProcessor] metadata serialization failed: task_id={} error={}",
                        task_id, e
                    );
                    return ProcessorResult::FatalFailure(
                        ModuleError::ModuleNotFound(format!("metadata serialize failed: {e}").into())
                            .into(),
                    );
                }
                Err(e) => {
                    error!(
                        "[TaskModuleProcessor] spawn_blocking failed: task_id={} error={}",
                        task_id, e
                    );
                    return ProcessorResult::FatalFailure(
                        ModuleError::ModuleNotFound(format!("spawn_blocking failed: {e}").into())
                            .into(),
                    )
                }
            };

            let mut meta_guard = context.metadata.write().await;
            meta_guard.insert("task_meta".to_string(), task_meta_val);
            meta_guard.insert("login_info".to_string(), login_info_val);
        }
        ProcessorResult::Success(modules)
    }
}

#[async_trait]
impl EventProcessorTrait<Task, Vec<Module>> for TaskModuleProcessor {
    fn pre_status(&self, _input: &Task) -> Option<EventEnvelope> {
        None
    }

    fn finish_status(&self, _input: &Task, _output: &Vec<Module>) -> Option<EventEnvelope> {
        None
    }

    fn working_status(&self, _input: &Task) -> Option<EventEnvelope> {
        None
    }

    fn error_status(&self, _input: &Task, _err: &Error) -> Option<EventEnvelope> {
        None
    }

    fn retry_status(&self, _input: &Task, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        None
    }
}

pub struct TaskProcessor {
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
}
#[async_trait]
impl ProcessorTrait<Module, SyncBoxStream<'static, Request>> for TaskProcessor {
    fn name(&self) -> &'static str {
        "TaskProcessor"
    }

    async fn process(
        &self,
        input: Module,
        context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, Request>> {
        debug!(
            "[TaskProcessor] start generate: module_id={} module_name={}",
            input.id(),
            input.module.name()
        );

        // 在生成 Request 之前检查 Module 是否已被终止
        match self.state.status_tracker.should_module_continue(&input.id()).await {
            Ok(ErrorDecision::Continue) => {
                debug!(
                    "[TaskProcessor] module can continue generating requests: module_id={}",
                    input.id()
                );
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[TaskProcessor] module terminated, skip request generation: module_id={} reason={}",
                    input.id(),
                    reason
                );
                // 返回空的 Request 列表，后处理会释放锁
                return ProcessorResult::Success(Box::pin(futures::stream::empty()));
            }
            Err(e) => {
                warn!(
                    "[TaskProcessor] error tracker check failed: module_id={} error={}, continue anyway",
                    input.id(), e
                );
            }
            _ => {}
        }

        // Task.metadata
        let meta = match context.metadata.read().await.get("task_meta") {
            Some(m) => {
                debug!("[TaskProcessor] Found task_meta");
                m.as_object().cloned().unwrap_or_default()
            }
            None => {
                warn!("[TaskProcessor] task_meta missing in context, will retry");
                return ProcessorResult::RetryableFailure(context.retry_policy.unwrap_or_default());
            }
        };
        let login_info = if input.module.should_login() {
            match context.metadata.read().await.get("login_info") {
                Some(m) => {
                    let m_clone = m.clone();
                    let info_res = tokio::task::spawn_blocking(move || {
                        serde_json::from_value::<LoginInfo>(m_clone)
                    })
                        .await
                        .map_err(|e| {
                            warn!("[TaskProcessor] spawn_blocking failed: {e}");
                            e
                        });

                    match info_res {
                        Ok(Ok(info)) => Some(info),
                        Ok(Err(e)) => {
                            warn!("[TaskProcessor] invalid login_info, will retry: {e}");
                            return ProcessorResult::RetryableFailure(
                                context
                                    .retry_policy
                                    .unwrap_or_default()
                                    .with_reason("not found login info".to_string()),
                            );
                        }
                        Err(_) => {
                            return ProcessorResult::RetryableFailure(
                                context
                                    .retry_policy
                                    .unwrap_or_default()
                                    .with_reason("spawn_blocking failed".to_string()),
                            );
                        }
                    }
                }
                None => {
                    warn!("[TaskProcessor] missing login_info, will retry");
                    return ProcessorResult::RetryableFailure(
                        context
                            .retry_policy
                            .unwrap_or_default()
                            .with_reason("not found login info".to_string()),
                    );
                }
            }
        } else {
            None
        };
        // TaskModel
        // ParserTaskModel.meta=>Task.metadata=>Context.meta=>Module.generate=>ModuleTrait.generate
        // [LOG_OPTIMIZATION] debug!("[TaskProcessor] start generate: module_id={}", input.id());
        // panic!("DEBUG PANIC: Reached input.generate"); 
        let requests: SyncBoxStream<'static, Request> = match input.generate(meta, login_info).await {
            Ok(stream) => {
                // Direct publish optimization
                let queue_manager = self.queue_manager.clone();
                let cache_service = self.state.cache_service.clone();
                let concurrency = self.state.config.read().await.crawler.publish_concurrency.unwrap_or(100);

                let dedup_ttl = self
                    .state
                    .config
                    .read()
                    .await
                    .crawler
                    .dedup_ttl_secs
                    .unwrap_or(3600) as usize;
                let namespace = self.state.cache_service.namespace().to_string();
                let deduplicator = self
                    .state
                    .locker
                    .get_pool()
                    .map(|p| Arc::new(Deduplicator::new(p.clone(), dedup_ttl, namespace)));

                let stream = stream.chunks(100)
                    .map(move |batch: Vec<Request>| {
                        let queue_manager = queue_manager.clone();
                        let cache_service = cache_service.clone();
                        let deduplicator = deduplicator.clone();

                        async move {
                            if batch.is_empty() {
                                return Vec::new();
                            }

                            let batch_len = batch.len();
                            let hashes = if deduplicator.is_some() {
                                Some(std::sync::Arc::new(
                                    batch.iter().map(|req| req.hash()).collect::<Vec<_>>(),
                                ))
                            } else {
                                None
                            };

                            let results = if let Some(dedup) = &deduplicator {
                                let hashes = hashes.clone().unwrap_or_else(|| std::sync::Arc::new(Vec::new()));
                                let dedup_clone = dedup.clone();
                                tokio::spawn(async move {
                                    match dedup_clone.check_and_set_batch(hashes.as_ref()).await {
                                        Ok(res) => res,
                                        Err(e) => {
                                            error!("[TaskProcessor] batch deduplication check failed: {}, allowing requests", e);
                                            vec![true; batch_len]
                                        }
                                    }
                                })
                                    .await
                                    .unwrap_or_else(|e| {
                                        error!("[TaskProcessor] batch deduplication spawn failed: {}, allowing requests", e);
                                        vec![true; batch_len]
                                    })
                            } else {
                                vec![true; batch_len]
                            };

                            for (index, (req, is_new)) in batch.into_iter().zip(results).enumerate() {
                                if !is_new {
                                    let hash = hashes
                                        .as_ref()
                                        .and_then(|values| values.get(index).map(String::as_str))
                                        .unwrap_or("unknown");
                                    info!("[TaskProcessor] duplicate request skipped: request_id={} module_id={} hash={}", req.id, req.module_id(), hash);
                                    continue;
                                }

                                let id = req.id.to_string();
                                let module_id = req.module_id();
                                let req_clone = req.clone();
                                let cache_service = cache_service.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = req_clone.send(&id, &cache_service).await {
                                        debug!("[RequestPublish] persist failed (background): {e}");
                                    }
                                });

                                let request_id = req.id;
                                let item = QueuedItem::new(req);
                                let tx = queue_manager.get_request_push_channel();
                                match tx.try_send(item) {
                                    Ok(_) => {
                                        info!("[RequestPublish] publish request: request_id={} module_id={}", request_id, module_id);
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(item)) => {
                                        if let Err(e) = tx.send(item).await {
                                            error!("Failed to send request to queue: {e}");
                                        } else {
                                            info!("[RequestPublish] publish request: request_id={} module_id={}", request_id, module_id);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to send request to queue: {e}");
                                    }
                                }
                            }

                            Vec::<Request>::new()
                        }
                    })
                    .buffer_unordered(concurrency / 10 + 1)
                    .map(futures::stream::iter)
                    .flatten();

                Box::pin(stream)
            }
            Err(e) => {
                warn!("[TaskProcessor] generate error, will retry: {e}");
                return ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                );
            }
        };
        ProcessorResult::Success(requests)
    }
    async fn post_process(
        &self,
        _input: &Module,
        _output: &SyncBoxStream<'static, Request>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        // Stream based post_process can't easily check for empty output without consuming.
        // We might need to move lock release logic elsewhere or wrap the stream.
        // For now, we skip the empty check.
        // if output.is_empty() {
        //    self.sync_service.release_module_locker(&input.id()).await;
        // }
        Ok(())
    }
}
#[async_trait]
impl EventProcessorTrait<Module, SyncBoxStream<'static, Request>> for TaskProcessor {
    fn pre_status(&self, input: &Module) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Module, _output: &SyncBoxStream<'static, Request>) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Completed,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn working_status(&self, input: &Module) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Module, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ModuleGenerate,
            EventPhase::Failed,
            ModuleGenerateEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Module, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Retry,
            json!({
                "data": ModuleGenerateEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
pub struct RequestPublish {
    queue_manager: Arc<QueueManager>,
    state: Arc<State>,
}

#[async_trait]
impl ProcessorTrait<Request, ()> for RequestPublish {
    fn name(&self) -> &'static str {
        "RequestPublish"
    }

    async fn process(&self, input: Request, context: ProcessorContext) -> ProcessorResult<()> {
        info!(
            "[RequestPublish] publish request: request_id={} module_id={}",
            input.id,
            input.module_id()
        );

        // 1. Persist request to Redis (for chain fallback)
        // Moved from ModuleProcessorWithChain to support streaming
        let id = input.id.to_string();

        // Performance Optimization: Fire-and-forget cache write using spawn
        // Offload serialization and IO to background task to unblock stream processing
        let cache_service = self.state.cache_service.clone();
        let request_clone = input.clone();

        tokio::spawn(async move {
            if let Err(e) = request_clone.send(&id, &cache_service).await {
                // Log at debug level to avoid spamming warns if cache is just busy
                debug!("[RequestPublish] persist failed (background): {e}");
            }
        });

        // [LOG_OPTIMIZATION] debug!("[RequestPublish] start queue send: request_id={}", input.id);
        if let Err(e) = self
            .queue_manager
            .get_request_push_channel()
            .send(QueuedItem::new(input))
            .await
        {
            error!("Failed to send request to queue: {e}");
            warn!("[RequestPublish] will retry due to queue send error");
            return ProcessorResult::RetryableFailure(
                context
                    .retry_policy
                    .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
            );
        }
        // [LOG_OPTIMIZATION] debug!("[RequestPublish] end queue send: request_id={}", id); // id is string here
        ProcessorResult::Success(())
    }
    async fn handle_error(
        &self,
        input: &Request,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<()> {
        // If we can't publish the request after retries, release the module lock
        // to avoid locking out future runs of this module.
        self.state.status_tracker
            .release_module_locker(&input.module_id())
            .await;
        ProcessorResult::FatalFailure(error)
    }
}
#[async_trait]
impl EventProcessorTrait<Request, ()> for RequestPublish {
    fn pre_status(&self, input: &Request) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Started,
            RequestEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Request, _output: &()) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Completed,
            RequestEvent::from(input),
        ))
    }

    fn working_status(&self, input: &Request) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Started,
            RequestEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Request, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::RequestPublish,
            EventPhase::Failed,
            RequestEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Request, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Retry,
            json!({
                "data": RequestEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
pub struct ConfigProcessor {
    pub state: Arc<State>,
}
#[async_trait]
impl ProcessorTrait<Request, (Request, Option<ModuleConfig>)> for ConfigProcessor {
    fn name(&self) -> &'static str {
        "ConfigProcessor"
    }

    async fn process(
        &self,
        input: Request,
        context: ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        // ModuleConfig在factory::load_with_model里进行了上传，使用module::id()作为唯一标识
        // 这里进行下载
        match ModuleConfig::sync(&input.module_id(), &self.state.cache_service).await {
            Ok(Some(config)) => ProcessorResult::Success((input, Some(config))),
            Ok(None) => ProcessorResult::Success((input, None)),
            Err(e) => {
                error!(
                    "Failed to fetch config for module {}: {}",
                    input.task_id(),
                    e
                );
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &Request, _context: &ProcessorContext) -> Result<()> {
        self.state.status_tracker.lock_module(&_input.module_id()).await;
        Ok(())
    }
    async fn handle_error(
        &self,
        _input: &Request,
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        ProcessorResult::Success((_input.clone(), None))
    }
}
#[async_trait]
impl EventProcessorTrait<Request, (Request, Option<ModuleConfig>)> for ConfigProcessor {
    fn pre_status(&self, _input: &Request) -> Option<EventEnvelope> {
        None
    }

    fn finish_status(
        &self,
        _input: &Request,
        _out: &(Request, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        None
    }

    fn working_status(&self, _input: &Request) -> Option<EventEnvelope> {
        None
    }

    fn error_status(&self, _input: &Request, _err: &Error) -> Option<EventEnvelope> {
        None
    }

    fn retry_status(&self, _input: &Request, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        None
    }
}

use futures::stream::Stream;
use std::pin::Pin;
use cacheable::{CacheAble};
use crate::task::{Task, TaskManager};
use crate::task::module::Module;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item=T> + Send + Sync + 'a>>;

/// task_model -> task -> request -> () (publish request to queue)
pub struct VecToStreamProcessor<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for VecToStreamProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> VecToStreamProcessor<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<Vec<T>, SyncBoxStream<'static, T>> for VecToStreamProcessor<T> {
    fn name(&self) -> &'static str {
        "VecToStreamProcessor"
    }

    async fn process(
        &self,
        input: Vec<T>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let stream = futures::stream::iter(input);
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<Vec<T>, SyncBoxStream<'static, T>> for VecToStreamProcessor<T> {
    fn pre_status(&self, _input: &Vec<T>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &Vec<T>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &Vec<T>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &Vec<T>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &Vec<T>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub struct FlattenStreamVecProcessor<T> {
    _marker: PhantomData<T>,
    #[allow(clippy::type_complexity)]
    logger: Option<Arc<dyn Fn(&T) + Send + Sync>>,
}

impl<T> Default for FlattenStreamVecProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> FlattenStreamVecProcessor<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
            logger: None,
        }
    }
    pub fn with_logger<F>(mut self, logger: F) -> Self
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.logger = Some(Arc::new(logger));
        self
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<SyncBoxStream<'static, Vec<T>>, SyncBoxStream<'static, T>>
for FlattenStreamVecProcessor<T>
{
    fn name(&self) -> &'static str {
        "FlattenStreamVecProcessor"
    }

    async fn process(
        &self,
        input: SyncBoxStream<'static, Vec<T>>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let logger = self.logger.clone();
        let stream = input.flat_map(move |v| {
            let logger = logger.clone();
            let iter = v.into_iter().inspect(move |item| {
                if let Some(l) = &logger {
                    l(item);
                }
            });
            futures::stream::iter(iter)
        });
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<SyncBoxStream<'static, Vec<T>>, SyncBoxStream<'static, T>> for FlattenStreamVecProcessor<T> {
    fn pre_status(&self, _input: &SyncBoxStream<'static, Vec<T>>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &SyncBoxStream<'static, Vec<T>>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &SyncBoxStream<'static, Vec<T>>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &SyncBoxStream<'static, Vec<T>>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &SyncBoxStream<'static, Vec<T>>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub struct StreamLoggerProcessor<T> {
    _marker: PhantomData<T>,
    name: String,
    #[allow(clippy::type_complexity)]
    logger: Option<Arc<dyn Fn(&T) + Send + Sync>>,
}

impl<T> StreamLoggerProcessor<T> {
    pub fn new(name: &str) -> Self {
        Self {
            _marker: PhantomData,
            name: name.to_string(),
            logger: None,
        }
    }
    pub fn with_logger<F>(mut self, logger: F) -> Self
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.logger = Some(Arc::new(logger));
        self
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<SyncBoxStream<'static, T>, SyncBoxStream<'static, T>>
for StreamLoggerProcessor<T>
{
    fn name(&self) -> &'static str {
        "StreamLoggerProcessor"
    }

    async fn process(
        &self,
        input: SyncBoxStream<'static, T>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let logger = self.logger.clone();
        let stream = input.map(move |item| {
            if let Some(l) = &logger {
                l(&item);
            }
            item
        });
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<SyncBoxStream<'static, T>, SyncBoxStream<'static, T>> for StreamLoggerProcessor<T> {
    fn pre_status(&self, _input: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &SyncBoxStream<'static, T>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &SyncBoxStream<'static, T>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &SyncBoxStream<'static, T>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub struct FlattenStreamProcessor<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for FlattenStreamProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> FlattenStreamProcessor<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<SyncBoxStream<'static, SyncBoxStream<'static, T>>, SyncBoxStream<'static, T>>
for FlattenStreamProcessor<T>
{
    fn name(&self) -> &'static str {
        "FlattenStreamProcessor"
    }

    async fn process(
        &self,
        input: SyncBoxStream<'static, SyncBoxStream<'static, T>>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let stream = input.flatten();
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<SyncBoxStream<'static, SyncBoxStream<'static, T>>, SyncBoxStream<'static, T>> for FlattenStreamProcessor<T> {
    fn pre_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub async fn create_task_model_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    state: Arc<State>,
) -> EventAwareTypedChain<TaskModel, SyncBoxStream<'static, ()>> {
    let task_model_processor = TaskModelProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let request_publish = RequestPublish {
        queue_manager: queue_manager.clone(),
        state: state.clone(),
    };
    let task_module_processor = TaskModuleProcessor {
        state: state.clone(),
    };
    let task_processor = TaskProcessor {
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    EventAwareTypedChain::<TaskModel, TaskModel>::new(event_bus)
        .then::<Task, _>(task_model_processor)
        .then::<Vec<Module>, _>(task_module_processor)
        .then(VecToStreamProcessor::new())
        .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
            task_processor,
            state.config.read().await.crawler.task_concurrency.unwrap_or(1024),
            ErrorStrategy::Skip,
        )
        .then_one_shot(FlattenStreamProcessor::new())
        .then_one_shot(StreamLoggerProcessor::new("AfterFlatten").with_logger(|req: &Request| {
            info!(
                "[FlattenStreamProcessor] yielding request: request_id={}",
                req.id
            );
        }))
        .then_map_stream_in_with_strategy::<(), _>(
            request_publish,
            state.config.read().await.crawler.publish_concurrency.unwrap_or(1024),
            ErrorStrategy::Skip,
        )
}
pub async fn create_parser_task_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    state: Arc<State>,
) -> EventAwareTypedChain<ParserTaskModel, SyncBoxStream<'static, ()>> {
    let task_model_processor = TaskModelProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    let request_publish = RequestPublish {
        queue_manager: queue_manager.clone(),
        state: state.clone(),
    };
    let task_module_processor = TaskModuleProcessor {
        state: state.clone(),
    };
    let task_processor = TaskProcessor {
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    EventAwareTypedChain::<ParserTaskModel, ParserTaskModel>::new(event_bus)
        .then::<Task, _>(task_model_processor)
        .then::<Vec<Module>, _>(task_module_processor)
        .then(VecToStreamProcessor::new())
        .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
            task_processor,
            state.config.read().await.crawler.task_concurrency.unwrap_or(256), // Lower default for parser chain if needed, using user logic for now
            ErrorStrategy::Skip,
        )
        .then_one_shot(FlattenStreamProcessor::new())
        .then_map_stream_in_with_strategy::<(), _>(
            request_publish,
            state.config.read().await.crawler.publish_concurrency.unwrap_or(256),
            ErrorStrategy::Skip,
        )
}
pub async fn create_error_task_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    state: Arc<State>,
) -> EventAwareTypedChain<ErrorTaskModel, SyncBoxStream<'static, ()>> {
    let task_model_processor = TaskModelProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };

    let request_publish = RequestPublish {
        queue_manager: queue_manager.clone(),
        state: state.clone(),
    };
    let task_module_processor = TaskModuleProcessor {
        state: state.clone(),
    };
    let task_processor = TaskProcessor {
        state: state.clone(),
        queue_manager: queue_manager.clone(),
    };
    EventAwareTypedChain::<ErrorTaskModel, ErrorTaskModel>::new(event_bus)
        .then::<Task, _>(task_model_processor)
        .then::<Vec<Module>, _>(task_module_processor)
        .then(VecToStreamProcessor::new())
        .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
            task_processor,
            4,
            ErrorStrategy::Skip,
        )
        .then_one_shot(FlattenStreamProcessor::new())
        .then_map_stream_in_with_strategy::<(), _>(request_publish, 32, ErrorStrategy::Skip)
}
