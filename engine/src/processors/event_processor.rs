#![allow(unused)]
use crate::events::{EventBus, EventEnvelope};
use async_trait::async_trait;
use errors::{Error, ProcessorChainError, Result};
use log::{debug, error, info};
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use common::processors::processor_chain::{ErrorStrategy, TypedChain, SyncBoxStream};
use std::sync::Arc;
use crate::chain::DataMiddlewareProcessor;

/// Processor wrapper that publishes lifecycle events around processor execution.
pub struct EventAwareProcessor<P> {
    inner_processor: P,
    event_bus: Option<Arc<EventBus>>,
}

impl<P> EventAwareProcessor<P> {
    pub fn new<Input, Output>(processor: P, event_bus: Option<Arc<EventBus>>) -> Self
    where
        P: ProcessorTrait<Input, Output>,
    {
        Self {
            inner_processor: processor,
            event_bus,
        }
    }

    /// Publishes an event envelope when both event and bus are available.
    async fn publish_event(&self, event: Option<EventEnvelope>) {
        if let (Some(event), Some(event_bus)) = (event, &self.event_bus) {
            if let Err(_e) = event_bus.publish(event).await {
                // log::error!("Failed to publish event: {_e}");
            }
        }
    }

    fn now_ts() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

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
        // Execute primary processing logic.
        // Retry orchestration is handled by the typed chain executor.
        self.publish_event(self.inner_processor.working_status(&input))
            .await;
        self.inner_processor
            .process(input, context)
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
            // Emit finish event only after `post_process` succeeds.
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
        // Emit error event when entering error handling.
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

/// Event-aware typed-chain wrapper.
///
/// Preserves the original typed-chain behavior while adding event emission to
/// each wrapped processor stage.
pub struct EventAwareTypedChain<In, Out> {
    inner: TypedChain<In, Out>,
    event_bus: Option<Arc<EventBus>>,
}


impl<In> EventAwareTypedChain<In, In>
where
    In: Send + Clone + 'static,
{
    pub fn new(event_bus: Option<Arc<EventBus>>) -> Self {
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

    pub fn then_silent<Next, P>(self, processor: P) -> EventAwareTypedChain<In, Next>
    where
        Out: Send + Sync + Clone + 'static,
        Next: Send + Sync + 'static,
        P: ProcessorTrait<Out, Next> + Send + Sync + 'static,
    {
        struct SilentWrapper<P>(P);
        #[async_trait]
        impl<P, Input, Output> ProcessorTrait<Input, Output> for SilentWrapper<P>
        where
            P: ProcessorTrait<Input, Output> + Send + Sync,
            Input: Send + Sync + 'static,
            Output: Send + Sync + 'static,
        {
            fn name(&self) -> &'static str { self.0.name() }
            async fn process(&self, i: Input, c: ProcessorContext) -> ProcessorResult<Output> { self.0.process(i, c).await }
            async fn pre_process(&self, i: &Input, c: &ProcessorContext) -> Result<()> { self.0.pre_process(i, c).await }
            async fn post_process(&self, i: &Input, o: &Output, c: &ProcessorContext) -> Result<()> { self.0.post_process(i, o, c).await }
            async fn handle_error(&self, i: &Input, e: Error, c: &ProcessorContext) -> ProcessorResult<Output> { self.0.handle_error(i, e, c).await }
            async fn should_process(&self, i: &Input, c: &ProcessorContext) -> bool { self.0.should_process(i, c).await }
        }
        #[async_trait]
        impl<P, Input, Output> EventProcessorTrait<Input, Output> for SilentWrapper<P>
        where
            P: ProcessorTrait<Input, Output> + Send + Sync,
            Input: Send + Sync + 'static,
            Output: Send + Sync + 'static,
        {
            fn pre_status(&self, _: &Input) -> Option<EventEnvelope> { None }
            fn finish_status(&self, _: &Input, _: &Output) -> Option<EventEnvelope> { None }
            fn working_status(&self, _: &Input) -> Option<EventEnvelope> { None }
            fn error_status(&self, _: &Input, _: &Error) -> Option<EventEnvelope> { None }
            fn retry_status(&self, _: &Input, _: &RetryPolicy) -> Option<EventEnvelope> { None }
        }

        let wrapped = SilentWrapper(processor);
        let wrapped_event_aware = EventAwareProcessor::new::<Out, Next>(wrapped, self.event_bus.clone());
        EventAwareTypedChain {
            inner: self.inner.then::<Next, _>(wrapped_event_aware),
            event_bus: self.event_bus,
        }
    }

    pub async fn execute(&self, input: In, context: ProcessorContext) -> ProcessorResult<Out> {
        self.inner.execute(input, context.clone()).await
    }
}

// Vec-output specialization with element-wise map helpers.
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

    pub fn then_map_vec_parallel_with_strategy_silent<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> EventAwareTypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        struct SilentWrapper<P>(P);
        #[async_trait]
        impl<P, Input, Output> ProcessorTrait<Input, Output> for SilentWrapper<P>
        where
            P: ProcessorTrait<Input, Output> + Send + Sync,
            Input: Send + Sync + 'static,
            Output: Send + Sync + 'static,
        {
            fn name(&self) -> &'static str { self.0.name() }
            async fn process(&self, i: Input, c: ProcessorContext) -> ProcessorResult<Output> { self.0.process(i, c).await }
            async fn pre_process(&self, i: &Input, c: &ProcessorContext) -> Result<()> { self.0.pre_process(i, c).await }
            async fn post_process(&self, i: &Input, o: &Output, c: &ProcessorContext) -> Result<()> { self.0.post_process(i, o, c).await }
            async fn handle_error(&self, i: &Input, e: Error, c: &ProcessorContext) -> ProcessorResult<Output> { self.0.handle_error(i, e, c).await }
            async fn should_process(&self, i: &Input, c: &ProcessorContext) -> bool { self.0.should_process(i, c).await }
        }
        #[async_trait]
        impl<P, Input, Output> EventProcessorTrait<Input, Output> for SilentWrapper<P>
        where
            P: ProcessorTrait<Input, Output> + Send + Sync,
            Input: Send + Sync + 'static,
            Output: Send + Sync + 'static,
        {
            fn pre_status(&self, _: &Input) -> Option<EventEnvelope> { None }
            fn finish_status(&self, _: &Input, _: &Output) -> Option<EventEnvelope> { None }
            fn working_status(&self, _: &Input) -> Option<EventEnvelope> { None }
            fn error_status(&self, _: &Input, _: &Error) -> Option<EventEnvelope> { None }
            fn retry_status(&self, _: &Input, _: &RetryPolicy) -> Option<EventEnvelope> { None }
        }

        let wrapped = SilentWrapper(processor);
        let wrapped_event_aware = EventAwareProcessor::new::<ElemIn, ElemOut>(wrapped, self.event_bus.clone());
        
        EventAwareTypedChain {
            inner: self
                .inner
                .then_map_vec_parallel_with_strategy::<ElemOut, _>(wrapped_event_aware, concurrency, strategy),
            event_bus: self.event_bus,
        }
    }

}

// Vec<Vec<T>> specialization with flattening helpers.
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

// Stream-output specialization with streaming map helpers.
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

// Empty marker type kept for static method access compatibility.
struct Dummy;

pub trait EventProcessorTrait<Input, Output>: ProcessorTrait<Input, Output> {
    fn pre_status(&self, input: &Input) -> Option<EventEnvelope>;
    fn finish_status(&self, input: &Input, output: &Output) -> Option<EventEnvelope>;
    fn working_status(&self, input: &Input) -> Option<EventEnvelope>;
    fn error_status(&self, input: &Input, err: &Error) -> Option<EventEnvelope>;
    fn retry_status(&self, input: &Input, retry_policy: &RetryPolicy) -> Option<EventEnvelope>;
}

#[async_trait]
impl<P, Input, Output> EventProcessorTrait<Input, Output> for EventAwareProcessor<P>
where
    P: EventProcessorTrait<Input, Output> + Send + Sync,
    Input: Send + Sync + 'static,
    Output: Send + Sync + 'static,
{
    fn pre_status(&self, input: &Input) -> Option<EventEnvelope> {
        self.inner_processor.pre_status(input)
    }

    fn finish_status(&self, input: &Input, out: &Output) -> Option<EventEnvelope> {
        self.inner_processor.finish_status(input, out)
    }

    fn working_status(&self, input: &Input) -> Option<EventEnvelope> {
        self.inner_processor.working_status(input)
    }

    fn error_status(&self, input: &Input, err: &Error) -> Option<EventEnvelope> {
        self.inner_processor.error_status(input, err)
    }
    fn retry_status(&self, input: &Input, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        self.inner_processor.retry_status(input, retry_policy)
    }
}
