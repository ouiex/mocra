#![allow(unused)]
use crate::core::events::{EventBus, SystemEvent};
use async_trait::async_trait;
use error::{Error, ProcessorChainError, Result};
use log::{debug, error, info};
use processor_chain::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use processor_chain::processors::processor_chain::{ErrorStrategy, TypedChain, SyncBoxStream};
use std::sync::Arc;
use crate::core::chain::DataMiddlewareProcessor;

/// 带事件发布的处理器
pub struct EventAwareProcessor<P> {
    inner_processor: P,
    event_bus: Arc<EventBus>,
}

impl<P> EventAwareProcessor<P> {
    pub fn new<Input, Output>(processor: P, event_bus: Arc<EventBus>) -> Self
    where
        P: ProcessorTrait<Input, Output>,
    {
        Self {
            inner_processor: processor,
            event_bus,
        }
    }

    /// 发布事件
    async fn publish_event(&self, event: Option<SystemEvent>) {
        if let Some(event) = event {
            // 优化：直接在 info! 中使用 {:?}，利用 log 宏的惰性求值特性
            // 只有当日志级别允许时才会进行格式化，避免无谓的 format! 开销
            info!("Publishing event: {:?}", event);
            
            if let Err(e) = self.event_bus.publish(event).await {
                error!("Failed to publish event: {e}");
            }
        }
    }

    fn now_ts() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // fn build_error_event(
    //     &self,
    //     error_type: &str,
    //     message: String,
    //     ctx: &ProcessorContext,
    // ) -> SystemEvent {
    //     // 尽量避免对 RetryPolicy 做直接序列化，手动采样关键信息
    //     let retry_json = ctx.retry_policy.as_ref().map(|rp| {
    //         json!({
    //             "max_retries": rp.max_retries,
    //             "retry_delay": rp.retry_delay,
    //             "current_retry": rp.current_retry,
    //         })
    //     });
    //     let context = json!({
    //         "processor": std::any::type_name::<P>(),
    //         "metadata": ctx.metadata,
    //         "retry_policy": retry_json,
    //         "step_timeout_ms": ctx.step_timeout_ms,
    //         "cancelled": ctx.cancelled,
    //     });
    //     SystemEvent::ErrorOccurred(ErrorEvent {
    //         error_id: uuid::Uuid::new_v4().to_string(),
    //         error_type: error_type.to_string(),
    //         message,
    //         context,
    //         timestamp: Self::now_ts(),
    //     })
    // }
    //
    // fn build_error_handled_event(&self, error_type: &str, message: String) -> SystemEvent {
    //     let context = json!({
    //         "processor": std::any::type_name::<P>(),
    //     });
    //     SystemEvent::ErrorHandled(ErrorEvent {
    //         error_id: uuid::Uuid::new_v4().to_string(),
    //         error_type: error_type.to_string(),
    //         message,
    //         context,
    //         timestamp: Self::now_ts(),
    //     })
    // }
}

#[async_trait]
impl<P, Input, Output> ProcessorTrait<Input, Output> for EventAwareProcessor<P>
where
    P: EventProcessorTrait<Input, Output> + Send + Sync,
    Input: Send + 'static + Sync,
    Output: Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        self.inner_processor.name()
    }

    async fn process(&self, input: Input, context: ProcessorContext) -> ProcessorResult<Output> {
        debug!("Starting processing with processor: {}", self.name());
        // 执行主要处理逻辑（重试逻辑由 TypedProcessorExecutor 处理）
        self.publish_event(self.inner_processor.working_status(&input))
            .await;
        self.inner_processor
            .process(input, context.clone())
            .await
    }

    async fn pre_process(&self, input: &Input, context: &ProcessorContext) -> Result<()> {
        self.publish_event(self.inner_processor.pre_status(input))
            .await;
        self.inner_processor.pre_process(input, context).await
    }

    async fn post_process(
        &self,
        input: &Input,
        output: &Output,
        context: &ProcessorContext,
    ) -> Result<()> {
        let r = self
            .inner_processor
            .post_process(input, output, context)
            .await;
        if r.is_ok() {
            // 成功后的 finish 事件放在真正 post_process 完成之后
            self.publish_event(self.inner_processor.finish_status(input, output))
                .await;
        }
        r
    }

    async fn handle_error(
        &self,
        input: &Input,
        error: Error,
        context: &ProcessorContext,
    ) -> ProcessorResult<Output> {
        // 发布事件：进入 handle_error
        self.publish_event(self.inner_processor.error_status(input, &error))
            .await;
        self.inner_processor
            .handle_error(input, error, context)
            .await
    }

    async fn should_process(&self, input: &Input, context: &ProcessorContext) -> bool {
        self.inner_processor.should_process(input, context).await
    }
}

/// 事件感知的 TypedChain 包装器：保持 TypedChain 的完整功能，对每一步处理器进行事件增强
pub struct EventAwareTypedChain<In, Out> {
    inner: TypedChain<In, Out>,
    event_bus: Arc<EventBus>,
}


impl<In> EventAwareTypedChain<In, In>
where
    In: Send + Clone + 'static,
{
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self {
            inner: TypedChain::<In, In>::new(),
            event_bus,
        }
    }
}

impl<In, Out> EventAwareTypedChain<In, Out>
where
    In: Send + 'static,
    Out: Send + Sync + 'static,
{
    pub fn then<Next, P>(self, processor: P) -> EventAwareTypedChain<In, Next>
    where
        Out: Send + Sync + Clone + 'static,
        Next: Send + Sync + 'static,
        P: EventProcessorTrait<Out, Next> + Send + Sync + 'static,
    {
        let wrapped = EventAwareProcessor::new::<Out, Next>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self.inner.then::<Next, _>(wrapped),
            event_bus: self.event_bus,
        }
    }

    pub fn then_one_shot<Next, P>(self, processor: P) -> EventAwareTypedChain<In, Next>
    where
        Out: Send + Sync + 'static,
        Next: Send + Sync + 'static,
        P: EventProcessorTrait<Out, Next> + Send + Sync + 'static,
    {
        let wrapped = EventAwareProcessor::new::<Out, Next>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self.inner.then_one_shot::<Next, _>(wrapped),
            event_bus: self.event_bus,
        }
    }

    pub async fn execute(&self, input: In, context: ProcessorContext) -> ProcessorResult<Out> {
        let res = self.inner.execute(input, context.clone()).await;
        // 链级别的致命错误事件
        if let ProcessorResult::FatalFailure(ref _e) = res {
            // 尝试发布链错误事件（不阻塞返回）
            // let ctx_json = json!({
            //     "step_timeout_ms": context.step_timeout_ms,
            //     "cancelled": context.cancelled,
            //     "metadata": context.metadata,
            // });
            // let event = SystemEvent::ErrorOccurred(ErrorEvent {
            //     error_id: uuid::Uuid::new_v4().to_string(),
            //     error_type: "processor_chain_fatal".to_string(),
            //     message: format!("{}", e),
            //     context: ctx_json,
            //     timestamp: EventAwareProcessor::<Dummy>::now_ts(),
            // });
            // // Fire and forget
            // let bus = self.event_bus.clone();
            // tokio::spawn(async move {
            //     let _ = bus.publish(event).await;
            // });
        }
        res
    }
}

// Vec 输出特化：追加逐元素 map
impl<In, ElemIn> EventAwareTypedChain<In, Vec<ElemIn>>
where
    In: Send + Clone + 'static,
    ElemIn: Send + Sync + Clone + 'static,
{
    pub fn then_map_vec<ElemOut, P>(self, processor: P) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let wrapped =
            EventAwareProcessor::new::<ElemIn, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self.inner.then_map_vec::<ElemOut, _>(wrapped),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_vec_parallel_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let wrapped =
            EventAwareProcessor::new::<ElemIn, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_vec_parallel_with::<ElemOut, _>(wrapped, concurrency),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_vec_parallel_with_strategy<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let wrapped =
            EventAwareProcessor::new::<ElemIn, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_vec_parallel_with_strategy::<ElemOut, _>(wrapped, concurrency, strategy),
            event_bus: self.event_bus,
        }
    }
}

// Vec<Vec<T>> 输出特化：提供扁平化为 Vec<T>
impl<In, T> EventAwareTypedChain<In, Vec<Vec<T>>>
where
    In: Send + Clone + 'static,
    T: Send + Sync + 'static,
{
    pub fn flatten_vec(self) -> EventAwareTypedChain<In, Vec<T>> {
        EventAwareTypedChain {
            inner: self.inner.flatten_vec(),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_nested_vec_flatten<ElemOut, P>(
        self,
        processor: P,
    ) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<T, ElemOut> + Send + Sync + 'static,
        T: Send + Sync + Clone + 'static,
    {
        let wrapped = EventAwareProcessor::new::<T, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_nested_vec_flatten::<ElemOut, _>(wrapped),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_nested_vec_flatten_parallel_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<T, ElemOut> + Send + Sync + 'static,
        T: Send + Sync + Clone + 'static,
    {
        let wrapped = EventAwareProcessor::new::<T, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_nested_vec_flatten_parallel_with::<ElemOut, _>(wrapped, concurrency),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_nested_vec_flatten_parallel_with_strategy<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<T, ElemOut> + Send + Sync + 'static,
        T: Send + Sync + Clone + 'static,
    {
        let wrapped = EventAwareProcessor::new::<T, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_nested_vec_flatten_parallel_with_strategy::<ElemOut, _>(
                    wrapped,
                    concurrency,
                    strategy,
                ),
            event_bus: self.event_bus,
        }
    }
}

// Stream 输出特化：追加流式 map
impl<In, ElemIn> EventAwareTypedChain<In, SyncBoxStream<'static, ElemIn>>
where
    In: Send + Clone + 'static,
    ElemIn: Send + Sync + Clone + 'static,
{
    pub fn then_map_stream_in_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> EventAwareTypedChain<In, SyncBoxStream<'static, ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let wrapped =
            EventAwareProcessor::new::<ElemIn, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_stream_in_with::<ElemOut, _>(wrapped, concurrency),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_stream_in_with_strategy<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> EventAwareTypedChain<In, SyncBoxStream<'static, ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let wrapped =
            EventAwareProcessor::new::<ElemIn, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self.inner.then_map_stream_in_with_strategy::<ElemOut, _>(
                wrapped,
                concurrency,
                strategy,
            ),
            event_bus: self.event_bus,
        }
    }

    pub fn then_map_stream_in_result_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> EventAwareTypedChain<In, SyncBoxStream<'static, Result<ElemOut>>>
    where
        ElemOut: Send + Sync + 'static,
        P: EventProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let wrapped =
            EventAwareProcessor::new::<ElemIn, ElemOut>(processor, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_stream_in_result_with::<ElemOut, _>(wrapped, concurrency),
            event_bus: self.event_bus,
        }
    }
}

// 一个空类型占位，仅用于访问静态 now_ts 方法
struct Dummy;

pub trait EventProcessorTrait<Input, Output>: ProcessorTrait<Input, Output> {
    fn pre_status(&self, input: &Input) -> Option<SystemEvent>;
    fn finish_status(&self, input: &Input, output: &Output) -> Option<SystemEvent>;
    fn working_status(&self, input: &Input) -> Option<SystemEvent>;
    fn error_status(&self, input: &Input, err: &Error) -> Option<SystemEvent>;
    fn retry_status(&self, input: &Input, retry_policy: &RetryPolicy) -> Option<SystemEvent>;
}

#[async_trait]
impl<P, Input, Output> EventProcessorTrait<Input, Output> for EventAwareProcessor<P>
where
    P: EventProcessorTrait<Input, Output> + Send + Sync,
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    fn pre_status(&self, input: &Input) -> Option<SystemEvent> {
        self.inner_processor.pre_status(input)
    }

    fn finish_status(&self, input: &Input, out: &Output) -> Option<SystemEvent> {
        self.inner_processor.finish_status(input, out)
    }

    fn working_status(&self, input: &Input) -> Option<SystemEvent> {
        self.inner_processor.working_status(input)
    }

    fn error_status(&self, input: &Input, err: &Error) -> Option<SystemEvent> {
        self.inner_processor.error_status(input, err)
    }
    fn retry_status(&self, input: &Input, retry_policy: &RetryPolicy) -> Option<SystemEvent> {
        self.inner_processor.retry_status(input, retry_policy)
    }
}
