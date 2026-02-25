#![allow(unused)]
use crate::common::processors::processor::{ProcessorContext, ProcessorResult, ProcessorTrait};
use async_trait::async_trait;
use crate::errors::Error;
use crate::errors::ProcessorChainError;
use futures::{Stream, StreamExt, stream};
use log::{debug, error, info, warn};
use std::any::{Any, TypeId};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;

/// Type-erased processor chain executor.
///
/// Highlights:
/// - Step-by-step orchestration where each output becomes the next input.
/// - Runtime type continuity checks via `TypeId`.
/// - Explicit failure semantics (`Success`, `RetryableFailure`, `FatalFailure`).
/// - Context controls for cancellation, timeout, and retry state.
/// - Adapters for vector and stream mapping scenarios.
struct ProcessorChain {
    steps: Vec<ProcessorStep>,
}

/// Internal wrapper around a chain step using type erasure.
struct ProcessorStep {
    name: String,
    executor: Box<dyn ProcessorExecutor + Send + Sync>,
    // Type metadata for validation and better diagnostics
    input_type_id: TypeId,
    output_type_id: TypeId,
    input_type_name: &'static str,
    output_type_name: &'static str,
}

/// Type-erased executor abstraction for chain steps.
///
/// Wraps concrete `ProcessorTrait<Input, Output>` implementations into a uniform
/// runtime interface.
#[async_trait]
trait ProcessorExecutor: Send + Sync {
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>>;
    fn name(&self) -> &str;
}

/// Typed executor implementation for a concrete processor.
struct TypedProcessorExecutor<Input, Output, P> {
    processor: P,
    // Function-pointer PhantomData prevents auto-trait leakage from Input/Output.
    _phantom: std::marker::PhantomData<fn(Input) -> Output>,
}

impl<Input, Output, P> TypedProcessorExecutor<Input, Output, P>
where
    Input: Send + Clone + 'static,
    Output: Send + 'static,
    P: ProcessorTrait<Input, Output> + Send + Sync,
{
    fn new(processor: P) -> Self {
        Self {
            processor,
            _phantom: std::marker::PhantomData,
        }
    }

    async fn execute_typed(
        &self,
        input: Input,
        context: ProcessorContext,
    ) -> ProcessorResult<Output> {
        // Prepare mutable execution context.
        let mut current_context = context;

        // Evaluate optional skip. Skip is only safe when Input == Output.
        let mut skip_processing = false;
        let should_do = self
            .processor
            .should_process(&input, &current_context)
            .await;
        if !should_do {
            if TypeId::of::<Input>() == TypeId::of::<Output>() {
                skip_processing = true;
            } else {
                debug!(
                    "Processor {} skip requested but Input!=Output; will still process to maintain type chain",
                    self.processor.name()
                );
            }
        }

        if skip_processing {
            debug!(
                "Processor {} decided to skip; passthrough input",
                self.processor.name()
            );
            // Input == Output, safe cast through Any.
            let any_in = Box::new(input) as Box<dyn Any + Send>;
            match any_in.downcast::<Output>() {
                Ok(out) => return ProcessorResult::Success(*out),
                Err(_) => {
                    return ProcessorResult::FatalFailure(
                        ProcessorChainError::Unexpected(
                            "Failed to cast Input to Output despite TypeId match".into(),
                        )
                        .into(),
                    );
                }
            }
        }

        // Run pre-process hook before the first attempt.
        if let Err(e) = self.processor.pre_process(&input, &current_context).await {
            error!("Pre-processing failed for {}: {}", self.processor.name(), e);
            return ProcessorResult::FatalFailure(e);
        }

        loop {
            // Cancellation check.
            if current_context.cancelled {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::Cancelled("cancelled".into()).into(),
                );
            }

            // Execute processor with optional timeout.
            // Clone input because `process` takes ownership and retries may be required.
            let fut = self
                .processor
                .process(input.clone(), current_context.clone());
            let result = if let Some(ms) = current_context.step_timeout_ms {
                match tokio::time::timeout(Duration::from_millis(ms), fut).await {
                    Ok(r) => r,
                    Err(_to) => {
                        return ProcessorResult::FatalFailure(
                            ProcessorChainError::Timeout(
                                format!(
                                    "processor {} timeout after {}ms",
                                    self.processor.name(),
                                    ms
                                )
                                .into(),
                            )
                            .into(),
                        );
                    }
                }
            } else {
                fut.await
            };

            match result {
                ProcessorResult::Success(output) => {
                    // Post-process failures are delegated to `handle_error` for compensation.
                    if let Err(post_err) = self
                        .processor
                        .post_process(&input, &output, &current_context)
                        .await
                    {
                        error!("Post-processing failed for {}: {}", self.name(), post_err);
                        // Treat post-process error as fatal and delegate to error handler.
                        let handled = self
                            .processor
                            .handle_error(&input, post_err, &current_context)
                            .await;
                        return match handled {
                            ProcessorResult::Success(out2) => ProcessorResult::Success(out2),
                            ProcessorResult::RetryableFailure(policy) => {
                                ProcessorResult::RetryableFailure(policy)
                            }
                            ProcessorResult::FatalFailure(e) => ProcessorResult::FatalFailure(e),
                        };
                    }
                    return ProcessorResult::Success(output);
                }
                ProcessorResult::RetryableFailure(mut retry_policy) => {
                    debug!(
                        "TypedProcessorExecutor {}: Got RetryableFailure, checking retry policy: max_retries={}, current_retry={}",
                        self.processor.name(),
                        retry_policy.max_retries,
                        retry_policy.current_retry
                    );

                    // Evaluate retry policy.
                    if let Some(delay_ms) = retry_policy.delay_next_retry().await {
                        warn!(
                            "TypedProcessorExecutor {}: Will retry after {}ms delay (attempt {}), message: {:?}",
                            self.processor.name(),
                            delay_ms,
                            retry_policy.current_retry,
                            retry_policy.reason
                        );

                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        current_context = current_context.with_retry_policy(retry_policy);
                        continue;
                    } else {
                        warn!(
                            "TypedProcessorExecutor {}: Max retries exceeded, giving up",
                            self.processor.name()
                        );

                        let err = ProcessorChainError::MaxRetriesExceeded(
                            format!(
                                "Max retries exceeded for processor {}",
                                self.processor.name()
                            )
                            .into(),
                        )
                        .into();

                        let handled = self
                            .processor
                            .handle_error(&input, err, &current_context)
                            .await;

                        return match handled {
                            ProcessorResult::Success(output) => ProcessorResult::Success(output),
                            ProcessorResult::RetryableFailure(policy) => {
                                ProcessorResult::RetryableFailure(policy)
                            }
                            ProcessorResult::FatalFailure(e) => ProcessorResult::FatalFailure(e),
                        };
                    }
                }
                ProcessorResult::FatalFailure(err) => {
                    // `process` failed; delegate to `handle_error`.
                    let handled = self
                        .processor
                        .handle_error(&input, err, &current_context)
                        .await;
                    return match handled {
                        ProcessorResult::Success(output) => ProcessorResult::Success(output),
                        ProcessorResult::RetryableFailure(policy) => {
                            ProcessorResult::RetryableFailure(policy)
                        }
                        ProcessorResult::FatalFailure(e) => ProcessorResult::FatalFailure(e),
                    };
                }
            }
        }
    }
}

#[async_trait]
impl<Input, Output, P> ProcessorExecutor for TypedProcessorExecutor<Input, Output, P>
where
    Input: Send + Clone + 'static,
    Output: Send + 'static,
    P: ProcessorTrait<Input, Output> + Send + Sync,
{
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>> {
        // 1) Downcast to concrete input type; fail fast on mismatch.
        let typed_input = match input.downcast::<Input>() {
            Ok(typed) => *typed,
            Err(_e) => {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::DowncastFailure(
                        format!(
                            "Type mismatch when entering processor {}: expected input {}",
                            self.processor.name(),
                            std::any::type_name::<Input>()
                        )
                        .into(),
                    )
                    .into(),
                );
            }
        };

        match self.execute_typed(typed_input, context).await {
            ProcessorResult::Success(output) => ProcessorResult::Success(Box::new(output)),
            ProcessorResult::RetryableFailure(p) => ProcessorResult::RetryableFailure(p),
            ProcessorResult::FatalFailure(e) => ProcessorResult::FatalFailure(e),
        }
    }

    fn name(&self) -> &str {
        self.processor.name()
    }
}

/// One-shot processor executor.
///
/// This executor does not support retries and does not require `Input: Clone`.
struct OneShotProcessorExecutor<Input, Output, P> {
    processor: P,
    _phantom: std::marker::PhantomData<fn(Input) -> Output>,
}

impl<Input, Output, P> OneShotProcessorExecutor<Input, Output, P>
where
    Input: Send + 'static,
    Output: Send + 'static,
    P: ProcessorTrait<Input, Output> + Send + Sync,
{
    fn new(processor: P) -> Self {
        Self {
            processor,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<Input, Output, P> ProcessorExecutor for OneShotProcessorExecutor<Input, Output, P>
where
    Input: Send + 'static,
    Output: Send + 'static,
    P: ProcessorTrait<Input, Output> + Send + Sync,
{
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>> {
        let typed_input = match input.downcast::<Input>() {
            Ok(typed) => *typed,
            Err(_e) => {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::DowncastFailure(
                        format!(
                            "Type mismatch when entering one-shot processor {}: expected input {}",
                            self.processor.name(),
                            std::any::type_name::<Input>()
                        )
                        .into(),
                    )
                    .into(),
                );
            }
        };

        if let Err(e) = self.processor.pre_process(&typed_input, &context).await {
            error!("Pre-processing failed for {}: {}", self.processor.name(), e);
            return ProcessorResult::FatalFailure(e);
        }

        let result = self.processor.process(typed_input, context).await;

        match result {
            ProcessorResult::Success(output) => {
                ProcessorResult::Success(Box::new(output) as Box<dyn Any + Send>)
            }
            ProcessorResult::FatalFailure(e) => ProcessorResult::FatalFailure(e),
            ProcessorResult::RetryableFailure(policy) => {
                warn!(
                    "OneShotProcessorExecutor {}: Retry requested but not supported (Input not Clone). Failing.",
                    self.processor.name()
                );
                let msg = policy
                    .reason
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Retry not supported".into());
                ProcessorResult::FatalFailure(ProcessorChainError::Unexpected(msg.into()).into())
            }
        }
    }

    fn name(&self) -> &str {
        self.processor.name()
    }
}

struct FlattenVecExecutor<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> FlattenVecExecutor<T> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> ProcessorExecutor for FlattenVecExecutor<T>
where
    T: Send + Sync + 'static,
{
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        _context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>> {
        let nested = match input.downcast::<Vec<Vec<T>>>() {
            Ok(b) => *b,
            Err(_) => {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::DowncastFailure(
                        format!(
                            "Type mismatch for flatten step {}: expected input Vec<Vec<{}>>",
                            self.name(),
                            std::any::type_name::<T>()
                        )
                        .into(),
                    )
                    .into(),
                );
            }
        };

        let total_cap: usize = nested.iter().map(|v| v.len()).sum();
        let mut flat: Vec<T> = Vec::with_capacity(total_cap);
        for mut v in nested.into_iter() {
            flat.append(&mut v);
        }
        ProcessorResult::Success(Box::new(flat) as Box<dyn Any + Send>)
    }

    fn name(&self) -> &str {
        "Flatten<Vec<_>>"
    }
}

/// Vec-map executor that applies a processor to each `Vec<ElemIn>` item.
/// - `Sequential`: process one by one.
/// - `Parallel(n)`: process concurrently and reassemble results by original index.
enum VecMapMode {
    Sequential,
    Parallel(usize),
}

struct MapVecExecutor<ElemIn, ElemOut, P> {
    inner: TypedProcessorExecutor<ElemIn, ElemOut, P>,
    mode: VecMapMode,
    // Enables per-element error strategy (parallel mode only):
    // - Drop: ignore failed elements and continue.
    // - Stop: stop at first failure and return successful prefix.
    // `ReturnResult` is intentionally unsupported in this executor.
    strategy: Option<ErrorStrategy>,
}

impl<ElemIn, ElemOut, P> MapVecExecutor<ElemIn, ElemOut, P>
where
    ElemIn: Send + Clone + 'static,
    ElemOut: Send + 'static,
    P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
{
    fn new(processor: P, mode: VecMapMode) -> Self {
        Self {
            inner: TypedProcessorExecutor::new(processor),
            mode,
            strategy: None,
        }
    }
}

#[async_trait]
impl<ElemIn, ElemOut, P> ProcessorExecutor for MapVecExecutor<ElemIn, ElemOut, P>
where
    ElemIn: Send + Clone + 'static,
    ElemOut: Send + 'static,
    P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
{
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>> {
        // Downcast to `Vec<ElemIn>`.
        let vec_input = match input.downcast::<Vec<ElemIn>>() {
            Ok(b) => *b,
            Err(_) => {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::DowncastFailure(
                        format!(
                            "Type mismatch for vec map step {}: expected input Vec<{}>",
                            self.name(),
                            std::any::type_name::<ElemIn>()
                        )
                        .into(),
                    )
                    .into(),
                );
            }
        };

        match self.mode {
            VecMapMode::Sequential => {
                let mut outputs = Vec::with_capacity(vec_input.len());
                for (idx, elem) in vec_input.into_iter().enumerate() {
                    match self.inner.execute_typed(elem, context.clone()).await {
                        ProcessorResult::Success(v) => outputs.push(v),
                        ProcessorResult::RetryableFailure(_) => {
                            return ProcessorResult::FatalFailure(
                                ProcessorChainError::Unexpected(
                                    format!(
                                        "Vec map step {} returned unexpected RetryableFailure",
                                        self.name()
                                    )
                                    .into(),
                                )
                                .into(),
                            );
                        }
                        ProcessorResult::FatalFailure(err) => {
                            return ProcessorResult::FatalFailure(err);
                        }
                    }
                }
                ProcessorResult::Success(Box::new(outputs) as Box<dyn Any + Send>)
            }
            VecMapMode::Parallel(concurrency) => {
                let conc = concurrency.max(1);
                let results: Vec<(usize, ProcessorResult<ElemOut>)> =
                    stream::iter(vec_input.into_iter().enumerate())
                        .map(|(idx, elem)| {
                            let ctx = context.clone();
                            async move {
                                let res = self.inner.execute_typed(elem, ctx).await;
                                (idx, res)
                            }
                        })
                        .buffer_unordered(conc)
                        .collect()
                        .await;
                // let mut results = results;
                // results.sort_by_key(|(idx, _)| *idx);
                // Select error handling behavior based on configured strategy.
                if let Some(strategy) = self.strategy {
                    if matches!(strategy, ErrorStrategy::ReturnResult) {
                        return ProcessorResult::FatalFailure(
                            ProcessorChainError::Unexpected("vec parallel map configured for ReturnResult but not supported here (use a result variant)".into()).into()
                        );
                    }
                    let mut outputs: Vec<ElemOut> = Vec::with_capacity(results.len());
                    let mut stop = false;
                    for (idx, res) in results.into_iter() {
                        if stop {
                            continue;
                        }
                        match res {
                            ProcessorResult::Success(v) => outputs.push(v),
                            ProcessorResult::FatalFailure(e) => {
                                error!(
                                    "parallel vec map: [{}] fatal failure at step {}: {}",
                                    idx,
                                    self.name(),
                                    e
                                );
                                match strategy {
                                    ErrorStrategy::Skip => { /* skip */ }
                                    ErrorStrategy::Stop => {
                                        stop = true;
                                    }
                                    ErrorStrategy::ReturnResult => unreachable!(),
                                }
                            }
                            ProcessorResult::RetryableFailure(_) => {
                                error!("parallel vec map: unexpected retry at parallel map output");
                                match strategy {
                                    ErrorStrategy::Skip => { /* skip */ }
                                    ErrorStrategy::Stop => {
                                        stop = true;
                                    }
                                    ErrorStrategy::ReturnResult => unreachable!(),
                                }
                            }
                        }
                    }
                    ProcessorResult::Success(Box::new(outputs) as Box<dyn Any + Send>)
                } else {
                    let mut outputs: Vec<ElemOut> = Vec::with_capacity(results.len());
                    for (_idx, res) in results.into_iter() {
                        match res {
                            ProcessorResult::Success(v) => outputs.push(v),
                            ProcessorResult::FatalFailure(err) => {
                                return ProcessorResult::FatalFailure(err);
                            }
                            ProcessorResult::RetryableFailure(_) => {
                                return ProcessorResult::FatalFailure(
                                    ProcessorChainError::Unexpected(
                                        "unexpected retry at parallel map output".into(),
                                    )
                                    .into(),
                                );
                            }
                        }
                    }
                    ProcessorResult::Success(Box::new(outputs) as Box<dyn Any + Send>)
                }
            }
        }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

struct SyncWrapper<T>(T);
unsafe impl<T: Send> Sync for SyncWrapper<T> {}
impl<F: std::future::Future + Send + Unpin> std::future::Future for SyncWrapper<F> {
    type Output = F::Output;
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Stream-map executor.
///
/// Input: `SyncBoxStream<ElemIn>`
/// Output: `SyncBoxStream<ElemOut>`
/// Supports bounded internal concurrency.
///
/// Supported error strategies:
/// - `Skip`: drop failed item and continue.
/// - `Stop`: stop yielding subsequent items after first failure.
/// - `ReturnResult`: unsupported here (use `MapStreamInResultExecutor`).
struct MapStreamInExecutor<ElemIn, ElemOut, P> {
    inner: Arc<TypedProcessorExecutor<ElemIn, ElemOut, P>>,
    concurrency: usize,
    // Element-level error strategy for stream mapping.
    strategy: ErrorStrategy,
}

impl<ElemIn, ElemOut, P> MapStreamInExecutor<ElemIn, ElemOut, P>
where
    ElemIn: Send + Clone + Sync + 'static,
    ElemOut: Send + Sync + 'static,
    P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
{
    fn new(processor: P, concurrency: usize) -> Self {
        Self {
            inner: Arc::new(TypedProcessorExecutor::new(processor)),
            concurrency,
            strategy: ErrorStrategy::Skip,
        }
    }
}

#[derive(Clone, Copy)]
pub enum ErrorStrategy {
    Skip,
    Stop,
    ReturnResult,
}

#[async_trait]
impl<ElemIn, ElemOut, P> ProcessorExecutor for MapStreamInExecutor<ElemIn, ElemOut, P>
where
    ElemIn: Send + Clone + Sync + 'static,
    ElemOut: Send + Sync + 'static,
    P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
{
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>> {
        let stream_in = match input.downcast::<SyncBoxStream<'static, ElemIn>>() {
            Ok(b) => *b,
            Err(_) => {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::DowncastFailure(
                        format!(
                            "Type mismatch for stream map step {}: expected input SyncBoxStream<{}>",
                            self.name(),
                            std::any::type_name::<ElemIn>()
                        )
                        .into(),
                    )
                    .into(),
                );
            }
        };

        let concurrency = self.concurrency.max(1);
        let ctx = context.clone();
        let inner_arc = self.inner.clone();
        let strategy = self.strategy;
        // Use an atomic stop flag for lock-free `Stop` semantics.
        use std::sync::atomic::{AtomicBool, Ordering};
        let stop_flag = Arc::new(AtomicBool::new(false));
        if matches!(strategy, ErrorStrategy::ReturnResult) {
            return ProcessorResult::FatalFailure(
                ProcessorChainError::Unexpected("stream map step configured for ReturnResult but wrong builder used (expected _result variant)".into()).into()
            );
        }
        // Execute concurrently, then filter/stop according to strategy.
        let mapped = stream_in
            .map(move |elem| {
                let ctx2 = ctx.clone();
                let inner = inner_arc.clone();
                let fut = Box::pin(async move { inner.execute_typed(elem, ctx2).await });
                SyncWrapper(fut)
            })
            .buffer_unordered(concurrency)
            .map({
                let flag = stop_flag.clone();
                move |res| match res {
                    ProcessorResult::Success(v) => Some(v),
                    ProcessorResult::FatalFailure(e) => {
                        error!("stream map: fatal failure: {e}");
                        if matches!(strategy, ErrorStrategy::Stop) {
                            flag.store(true, Ordering::Relaxed);
                        }
                        None
                    }
                    ProcessorResult::RetryableFailure(_) => {
                        error!("stream map: unexpected retry at stream stage");
                        None
                    }
                }
            })
            .take_while({
                let flag = stop_flag.clone();
                move |_| {
                    let flag = flag.clone();
                    async move { !flag.load(Ordering::Relaxed) }
                }
            })
            .filter_map(futures::future::ready);
        // `mapped` is now `SyncBoxStream<'static, ElemOut>`.
        let mapped: SyncBoxStream<'static, ElemOut> = Box::pin(mapped);
        ProcessorResult::Success(Box::new(mapped) as Box<dyn Any + Send>)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

/// Stream-map executor that yields `Result<ElemOut, Error>` items.
struct MapStreamInResultExecutor<ElemIn, ElemOut, P> {
    inner: Arc<TypedProcessorExecutor<ElemIn, ElemOut, P>>,
    concurrency: usize,
}

impl<ElemIn, ElemOut, P> MapStreamInResultExecutor<ElemIn, ElemOut, P>
where
    ElemIn: Send + Clone + Sync + 'static,
    ElemOut: Send + Sync + 'static,
    P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
{
    fn new(processor: P, concurrency: usize) -> Self {
        Self {
            inner: Arc::new(TypedProcessorExecutor::new(processor)),
            concurrency,
        }
    }
}

#[async_trait]
impl<ElemIn, ElemOut, P> ProcessorExecutor for MapStreamInResultExecutor<ElemIn, ElemOut, P>
where
    ElemIn: Send + Clone + Sync + 'static,
    ElemOut: Send + Sync + 'static,
    P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
{
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>> {
        use crate::errors::Error as E;
        let stream_in = match input.downcast::<SyncBoxStream<'static, ElemIn>>() {
            Ok(b) => *b,
            Err(_) => {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::DowncastFailure(
                        format!(
                            "Type mismatch for stream map (result) step {}: expected input SyncBoxStream<{}>",
                            self.name(),
                            std::any::type_name::<ElemIn>()
                        ).into(),
                    ).into(),
                );
            }
        };
        let ctx = context.clone();
        let inner = self.inner.clone();
        let conc = self.concurrency.max(1);
        let mapped = stream_in
            .map(move |elem| {
                let ctx2 = ctx.clone();
                let inner2 = inner.clone();
                let fut = Box::pin(async move { inner2.execute_typed(elem, ctx2).await });
                SyncWrapper(fut)
            })
            .buffer_unordered(conc)
            .map(|res| match res {
                ProcessorResult::Success(v) => Ok(v),
                ProcessorResult::FatalFailure(err) => Err(err),
                ProcessorResult::RetryableFailure(_) => Err(E::from(
                    ProcessorChainError::Unexpected("unexpected retry at stream result map".into()),
                )),
            });
        let mapped: SyncBoxStream<'static, Result<ElemOut, Error>> = Box::pin(mapped);
        ProcessorResult::Success(Box::new(mapped) as Box<dyn Any + Send>)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}
impl ProcessorChain {
    /// Creates an empty processor chain.
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Adds a processor to the chain tail.
    ///
    /// This method does not immediately validate step type connectivity.
    /// Call `validate_link_types` after assembly, or use `try_add_processor`
    /// for add-and-validate in one step.
    pub fn add_processor<Input, Output, P>(mut self, processor: P) -> Self
    where
        Input: Send + Clone + 'static,
        Output: Send + 'static,
        P: ProcessorTrait<Input, Output> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let executor = TypedProcessorExecutor::new(processor);

        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<Input>(),
            output_type_id: TypeId::of::<Output>(),
            input_type_name: std::any::type_name::<Input>(),
            output_type_name: std::any::type_name::<Output>(),
        });

        self
    }

    /// Adds a one-shot processor (no retry support, no `Input: Clone` requirement).
    pub fn add_one_shot_processor<Input, Output, P>(mut self, processor: P) -> Self
    where
        Input: Send + 'static,
        Output: Send + 'static,
        P: ProcessorTrait<Input, Output> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let executor = OneShotProcessorExecutor::new(processor);

        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<Input>(),
            output_type_id: TypeId::of::<Output>(),
            input_type_name: std::any::type_name::<Input>(),
            output_type_name: std::any::type_name::<Output>(),
        });

        self
    }

    /// Adds an element-wise map processor for `Vec<ElemIn>`.
    pub fn add_processor_map<ElemIn, ElemOut, P>(mut self, processor: P) -> Self
    where
        ElemIn: Send + Clone + 'static,
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let executor = MapVecExecutor::new(processor, VecMapMode::Sequential);

        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<Vec<ElemIn>>(),
            output_type_id: TypeId::of::<Vec<ElemOut>>(),
            input_type_name: std::any::type_name::<Vec<ElemIn>>(),
            output_type_name: std::any::type_name::<Vec<ElemOut>>(),
        });

        self
    }

    /// Attempts to add a processor with build-time type connectivity validation.
    ///
    /// Returns an error on mismatch without mutating the original chain state.
    pub fn try_add_processor<Input, Output, P>(
        self,
        processor: P,
    ) -> Result<Self, ProcessorChainError>
    where
        Input: Send + Clone + 'static,
        Output: Send + 'static,
        P: ProcessorTrait<Input, Output> + Send + Sync + 'static,
    {
        let new_self = self.add_processor::<Input, Output, P>(processor);
        new_self.validate_link_types()?;
        Ok(new_self)
    }

    /// Validates that adjacent processor step types match.
    ///
    /// Specifically, each previous step `output_type_id` must equal the next
    /// step `input_type_id`. On failure, returns detailed step/type diagnostics.
    pub fn validate_link_types(&self) -> Result<(), ProcessorChainError> {
        for i in 1..self.steps.len() {
            let prev = &self.steps[i - 1];
            let next = &self.steps[i];
            if prev.output_type_id != next.input_type_id {
                return Err(ProcessorChainError::DowncastFailure(
                    format!(
                        "Type mismatch between steps [{}] {} (output: {}) -> [{}] {} (input: {})",
                        i - 1,
                        prev.name,
                        prev.output_type_name,
                        i,
                        next.name,
                        next.input_type_name
                    )
                    .into(),
                ));
            }
        }
        Ok(())
    }

    /// Adds a parallel map processor (`Vec<ElemIn>` -> `Vec<ElemOut>`) with custom concurrency.
    pub fn add_processor_map_parallel_with<ElemIn, ElemOut, P>(
        mut self,
        processor: P,
        concurrency: usize,
    ) -> Self
    where
        ElemIn: Send + Clone + 'static,
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let executor = MapVecExecutor::new(processor, VecMapMode::Parallel(concurrency));
        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<Vec<ElemIn>>(),
            output_type_id: TypeId::of::<Vec<ElemOut>>(),
            input_type_name: std::any::type_name::<Vec<ElemIn>>(),
            output_type_name: std::any::type_name::<Vec<ElemOut>>(),
        });
        self
    }

    /// Adds a parallel map processor with default concurrency `16`.
    pub fn add_processor_map_parallel<ElemIn, ElemOut, P>(self, processor: P) -> Self
    where
        ElemIn: Send + Clone + 'static,
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        self.add_processor_map_parallel_with::<ElemIn, ElemOut, P>(processor, 16)
    }

    /// Adds a parallel map processor from `Vec<ElemIn>` to `Vec<ElemOut>` with custom
    /// concurrency and error strategy.
    pub fn add_processor_map_parallel_with_strategy<ElemIn, ElemOut, P>(
        mut self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> Self
    where
        ElemIn: Send + Clone + 'static,
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let mut executor = MapVecExecutor::new(processor, VecMapMode::Parallel(concurrency));
        executor.strategy = Some(strategy);
        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<Vec<ElemIn>>(),
            output_type_id: TypeId::of::<Vec<ElemOut>>(),
            input_type_name: std::any::type_name::<Vec<ElemIn>>(),
            output_type_name: std::any::type_name::<Vec<ElemOut>>(),
        });
        self
    }

    /// Adds a stream map processor from `SyncBoxStream<ElemIn>` to
    /// `SyncBoxStream<ElemOut>` with custom concurrency.
    pub fn add_processor_map_stream_in_with<ElemIn, ElemOut, P>(
        mut self,
        processor: P,
        concurrency: usize,
    ) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let executor = MapStreamInExecutor::new(processor, concurrency);
        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<SyncBoxStream<'static, ElemIn>>(),
            output_type_id: TypeId::of::<SyncBoxStream<'static, ElemOut>>(),
            input_type_name: std::any::type_name::<SyncBoxStream<'static, ElemIn>>(),
            output_type_name: std::any::type_name::<SyncBoxStream<'static, ElemOut>>(),
        });
        self
    }

    /// Adds a stream map processor with default concurrency `16`.
    pub fn add_processor_map_stream_in<ElemIn, ElemOut, P>(self, processor: P) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        self.add_processor_map_stream_in_with::<ElemIn, ElemOut, P>(processor, 16)
    }

    /// Adds a stream map processor with custom concurrency and error strategy.
    pub fn add_processor_map_stream_in_with_strategy<ElemIn, ElemOut, P>(
        mut self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let mut exec = MapStreamInExecutor::new(processor, concurrency);
        exec.strategy = strategy;
        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(exec),
            input_type_id: TypeId::of::<SyncBoxStream<'static, ElemIn>>(),
            output_type_id: TypeId::of::<SyncBoxStream<'static, ElemOut>>(),
            input_type_name: std::any::type_name::<SyncBoxStream<'static, ElemIn>>(),
            output_type_name: std::any::type_name::<SyncBoxStream<'static, ElemOut>>(),
        });
        self
    }

    /// Adds a stream map processor whose output items are `Result<ElemOut, Error>`,
    /// with custom concurrency.
    pub fn add_processor_map_stream_in_result_with<ElemIn, ElemOut, P>(
        mut self,
        processor: P,
        concurrency: usize,
    ) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        let name = processor.name().to_string();
        let executor = MapStreamInResultExecutor::new(processor, concurrency);
        self.steps.push(ProcessorStep {
            name,
            executor: Box::new(executor),
            input_type_id: TypeId::of::<SyncBoxStream<'static, ElemIn>>(),
            output_type_id: TypeId::of::<SyncBoxStream<'static, Result<ElemOut, Error>>>(),
            input_type_name: std::any::type_name::<SyncBoxStream<'static, ElemIn>>(),
            output_type_name: std::any::type_name::<SyncBoxStream<'static, Result<ElemOut, Error>>>(
            ),
        });
        self
    }

    /// Adds a stream map processor that returns `Result` items with default
    /// concurrency `16`.
    pub fn add_processor_map_stream_in_result<ElemIn, ElemOut, P>(self, processor: P) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        self.add_processor_map_stream_in_result_with::<ElemIn, ElemOut, P>(processor, 16)
    }

    /// Adds a flatten step from `Vec<Vec<T>>` to `Vec<T>`.
    pub fn add_flatten_vec<T>(mut self) -> Self
    where
        T: Send + Sync + 'static,
    {
        let exec: FlattenVecExecutor<T> = FlattenVecExecutor::new();
        self.steps.push(ProcessorStep {
            name: "Flatten<Vec<_>>".to_string(),
            executor: Box::new(exec),
            input_type_id: TypeId::of::<Vec<Vec<T>>>(),
            output_type_id: TypeId::of::<Vec<T>>(),
            input_type_name: std::any::type_name::<Vec<Vec<T>>>(),
            output_type_name: std::any::type_name::<Vec<T>>(),
        });
        self
    }

    /// Executes the entire processor chain.
    ///
    /// Two fast validations are performed before execution:
    /// 1) Link connectivity (adjacent input/output types must match).
    /// 2) Head/tail types must match the caller-declared `Input`/`Output`.
    pub async fn execute<Input, Output>(
        &self,
        input: Input,
        context: ProcessorContext,
    ) -> ProcessorResult<Output>
    where
        Input: Send + 'static,
        Output: Send + 'static,
    {
        if self.steps.is_empty() {
            return ProcessorResult::FatalFailure(
                ProcessorChainError::EmptyChain("Empty processor chain".into()).into(),
            );
        }

        // Fast pre-run type checks: head input, tail output, and link connectivity.
        if let Err(e) = self.validate_link_types() {
            return ProcessorResult::FatalFailure(e.into());
        }

        let head = &self.steps[0];
        if head.input_type_id != TypeId::of::<Input>() {
            return ProcessorResult::FatalFailure(
                ProcessorChainError::DowncastFailure(
                    format!(
                        "Chain input type mismatch: chain expects {}, but execute() was called with {}",
                        head.input_type_name,
                        std::any::type_name::<Input>()
                    )
                        .into(),
                )
                    .into(),
            );
        }
        let tail = &self.steps[self.steps.len() - 1];
        if tail.output_type_id != TypeId::of::<Output>() {
            return ProcessorResult::FatalFailure(
                ProcessorChainError::DowncastFailure(
                    format!(
                        "Chain output type mismatch: chain produces {}, but execute() expects {}",
                        tail.output_type_name,
                        std::any::type_name::<Output>()
                    )
                    .into(),
                )
                .into(),
            );
        }

        let mut current_data: Box<dyn Any + Send> = Box::new(input);

        // Execute each processor step by step: each `Any` output becomes next input.
        for (idx, step) in self.steps.iter().enumerate() {
            debug!(
                "Executing processor [{} / {}]: {}",
                idx + 1,
                self.steps.len(),
                step.name
            );

            // Execute processor (retry behavior is handled inside `TypedProcessorExecutor`).
            match step.executor.execute(current_data, context.clone()).await {
                ProcessorResult::Success(output) => {
                    current_data = output;
                }
                ProcessorResult::RetryableFailure(_retry_policy) => {
                    // This path should not happen because retries are handled at executor level.
                    error!(
                        "Unexpected RetryableFailure from processor {} - retries should be handled at processor level",
                        step.name
                    );
                    return ProcessorResult::FatalFailure(
                        ProcessorChainError::Unexpected(
                            format!("Unexpected RetryableFailure from processor {}", step.name)
                                .into(),
                        )
                        .into(),
                    );
                }
                ProcessorResult::FatalFailure(err) => {
                    error!("Fatal failure in processor {}: {}", step.name, err);
                    return ProcessorResult::FatalFailure(err);
                }
            }
        }
        // Try downcasting the final output to the expected type.
        match current_data.downcast::<Output>() {
            Ok(typed_output) => ProcessorResult::Success(*typed_output),
            Err(_e) => ProcessorResult::FatalFailure(
                ProcessorChainError::DowncastFailure(
                    format!(
                        "Final type mismatch: expected {}, got unknown type",
                        std::any::type_name::<Output>()
                    )
                    .into(),
                )
                .into(),
            ),
        }
    }

    /// Returns the number of processors in the chain.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Returns `true` when the chain has no processors.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Returns all processor names in execution order.
    pub fn processor_names(&self) -> Vec<&str> {
        self.steps.iter().map(|step| step.executor.name()).collect()
    }
}

impl Default for ProcessorChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Statically typed chain wrapper.
///
/// Uses type parameters to enforce adjacent step `Input`/`Output` compatibility at compile
/// time, while runtime link validation is still performed before execution.
pub struct TypedChain<In, Out> {
    inner: ProcessorChain,
    _marker: std::marker::PhantomData<fn(In) -> Out>,
}

impl<In> Default for TypedChain<In, In> {
    fn default() -> Self {
        Self::new()
    }
}

impl<In> TypedChain<In, In> {
    pub fn new() -> Self {
        Self {
            inner: ProcessorChain::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<In, Out> TypedChain<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    pub fn then<Next, P>(self, processor: P) -> TypedChain<In, Next>
    where
        Out: Send + Clone + 'static,
        Next: Send + 'static,
        P: ProcessorTrait<Out, Next> + Send + Sync + 'static,
    {
        // `Out -> Next` is enforced at compile time; runtime link checks still apply.
        TypedChain {
            inner: self.inner.add_processor::<Out, Next, _>(processor),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn execute(&self, input: In, context: ProcessorContext) -> ProcessorResult<Out> {
        self.inner.execute::<In, Out>(input, context).await
    }

    pub fn then_one_shot<Next, P>(self, processor: P) -> TypedChain<In, Next>
    where
        Out: Send + 'static,
        Next: Send + 'static,
        P: ProcessorTrait<Out, Next> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self.inner.add_one_shot_processor::<Out, Next, _>(processor),
            _marker: std::marker::PhantomData,
        }
    }
}

// Specialization for `Vec` outputs: append per-element map steps.
impl<In, ElemIn> TypedChain<In, Vec<ElemIn>>
where
    In: Send + Clone + 'static,
    ElemIn: Send + Clone + 'static,
{
    pub fn then_map_vec<ElemOut, P>(self, processor: P) -> TypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_processor_map::<ElemIn, ElemOut, _>(processor),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn then_map_vec_parallel_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> TypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_processor_map_parallel_with::<ElemIn, ElemOut, _>(processor, concurrency),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn then_map_vec_parallel_with_strategy<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> TypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_processor_map_parallel_with_strategy::<ElemIn, ElemOut, _>(
                    processor,
                    concurrency,
                    strategy,
                ),
            _marker: std::marker::PhantomData,
        }
    }
}

// Specialization for `Vec<Vec<T>>` outputs with flattening helpers.
impl<In, T> TypedChain<In, Vec<Vec<T>>>
where
    In: Send + Clone + 'static,
    T: Send + Sync + 'static,
{
    pub fn flatten_vec(self) -> TypedChain<In, Vec<T>> {
        TypedChain {
            inner: self.inner.add_flatten_vec::<T>(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Flattens into `Vec<T>`, then maps each `T` into `Vec<U>`.
    pub fn then_map_nested_vec_flatten<ElemOut, P>(
        self,
        processor: P,
    ) -> TypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + 'static,
        P: ProcessorTrait<T, ElemOut> + Send + Sync + 'static,
        T: Send + Clone + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_flatten_vec::<T>()
                .add_processor_map::<T, ElemOut, _>(processor),
            _marker: std::marker::PhantomData,
        }
    }

    /// Flattens into `Vec<T>`, then performs parallel map into `Vec<U>`.
    pub fn then_map_nested_vec_flatten_parallel_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> TypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + 'static,
        P: ProcessorTrait<T, ElemOut> + Send + Sync + 'static,
        T: Send + Clone + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_flatten_vec::<T>()
                .add_processor_map_parallel_with::<T, ElemOut, _>(processor, concurrency),
            _marker: std::marker::PhantomData,
        }
    }

    /// Flattens into `Vec<T>`, then performs parallel map with error strategy.
    pub fn then_map_nested_vec_flatten_parallel_with_strategy<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> TypedChain<In, Vec<ElemOut>>
    where
        ElemOut: Send + 'static,
        P: ProcessorTrait<T, ElemOut> + Send + Sync + 'static,
        T: Send + Clone + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_flatten_vec::<T>()
                .add_processor_map_parallel_with_strategy::<T, ElemOut, _>(
                    processor,
                    concurrency,
                    strategy,
                ),
            _marker: std::marker::PhantomData,
        }
    }
}

// Specialization for stream outputs: append stream element-wise map steps.
impl<In, ElemIn> TypedChain<In, SyncBoxStream<'static, ElemIn>>
where
    In: Send + Clone + 'static,
    ElemIn: Send + Clone + Sync + 'static,
{
    pub fn then_map_stream_in_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> TypedChain<In, SyncBoxStream<'static, ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_processor_map_stream_in_with::<ElemIn, ElemOut, _>(processor, concurrency),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn then_map_stream_in_with_strategy<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
        strategy: ErrorStrategy,
    ) -> TypedChain<In, SyncBoxStream<'static, ElemOut>>
    where
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_processor_map_stream_in_with_strategy::<ElemIn, ElemOut, _>(
                    processor,
                    concurrency,
                    strategy,
                ),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn then_map_stream_in_result_with<ElemOut, P>(
        self,
        processor: P,
        concurrency: usize,
    ) -> TypedChain<In, SyncBoxStream<'static, Result<ElemOut, Error>>>
    where
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        TypedChain {
            inner: self
                .inner
                .add_processor_map_stream_in_result_with::<ElemIn, ElemOut, _>(
                    processor,
                    concurrency,
                ),
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::processors::processor::RetryPolicy;
    use futures::stream::{BoxStream, StreamExt};
    use std::sync::Once;
    use crate::utils::logger;

    static INIT: Once = Once::new();
    fn init_logger() {
        INIT.call_once(|| {
            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::spawn(async {
                    let _ = logger::init_app_logger("common-tests").await;
                });
            } else if let Ok(rt) = tokio::runtime::Runtime::new() {
                let _ = rt.block_on(logger::init_app_logger("common-tests"));
            }
        });
    }

    // A simple processor from i32 -> String
    struct IntToString;
    #[async_trait]
    impl ProcessorTrait<i32, String> for IntToString {
        fn name(&self) -> &'static str {
            "IntToString"
        }
        async fn process(&self, input: i32, _ctx: ProcessorContext) -> ProcessorResult<String> {
            ProcessorResult::Success(format!("v={}", input))
        }
    }

    // Next: String -> usize (length)
    struct StringLen;
    #[async_trait]
    impl ProcessorTrait<String, usize> for StringLen {
        fn name(&self) -> &'static str {
            "StringLen"
        }
        async fn process(&self, input: String, _ctx: ProcessorContext) -> ProcessorResult<usize> {
            ProcessorResult::Success(input.len())
        }
    }

    // A retrying processor: i32 -> i32, fails a few times then succeeds
    struct FlakyAddOne {
        fail_times: std::sync::Mutex<u32>,
        target_failures: u32,
    }
    impl FlakyAddOne {
        fn new(target_failures: u32) -> Self {
            Self {
                fail_times: std::sync::Mutex::new(0),
                target_failures,
            }
        }
    }
    #[async_trait]
    impl ProcessorTrait<i32, i32> for FlakyAddOne {
        fn name(&self) -> &'static str {
            "FlakyAddOne"
        }
        async fn process(&self, input: i32, ctx: ProcessorContext) -> ProcessorResult<i32> {
            let mut count = self.fail_times.lock().unwrap();
            if *count < self.target_failures {
                *count += 1;
                // Simulate retry: return the policy, chain/executor will handle delay and retry bookkeeping
                let policy = ctx.retry_policy.clone().unwrap_or_else(|| RetryPolicy {
                    max_retries: 3,
                    retry_delay: 5,
                    current_retry: 0,
                    reason: None,
                    meta: Default::default(),
                });
                ProcessorResult::RetryableFailure(policy)
            } else {
                ProcessorResult::Success(input + 1)
            }
        }
    }

    // StringEcho helper removed; TypedChain no longer uses runtime mismatch tests

    #[tokio::test]
    async fn chain_happy_path_int_to_string_to_len() {
        init_logger();
        // Build typed chain: i32 -> String -> usize
        let chain = TypedChain::<i32, i32>::new()
            .then::<String, _>(IntToString)
            .then::<usize, _>(StringLen);

        let ctx = ProcessorContext::default();
        let res = chain.execute(7, ctx).await;
        match res {
            ProcessorResult::Success(n) => assert_eq!(n, "v=7".len()),
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn chain_retry_succeeds() {
        init_logger();
        // i32 -> i32 (flaky) -> String via TypedChain
        let chain = TypedChain::<i32, i32>::new()
            .then::<i32, _>(FlakyAddOne::new(2))
            .then::<String, _>(IntToString);

        // context with a retry policy
        let ctx = ProcessorContext::default().with_retry_policy(RetryPolicy {
            max_retries: 5,
            retry_delay: 1,
            current_retry: 0,
            reason: None,
            meta: Default::default(),
        });
        let res = chain.execute(10, ctx).await;
        match res {
            ProcessorResult::Success(s) => assert_eq!(s, "v=11"),
            other => panic!("unexpected result: {:?}", other),
        }
    }

    // Removed: runtime mismatch test not applicable with TypedChain

    // ==== New tests for map step (A -> Vec<B>, then map B -> C) ====

    // A -> Vec<B>
    struct RangeToVec;
    #[async_trait]
    impl ProcessorTrait<(i32, i32), Vec<i32>> for RangeToVec {
        fn name(&self) -> &'static str {
            "RangeToVec"
        }
        async fn process(
            &self,
            input: (i32, i32),
            _ctx: ProcessorContext,
        ) -> ProcessorResult<Vec<i32>> {
            let (start, end) = input;
            let v: Vec<i32> = (start..end).collect();
            ProcessorResult::Success(v)
        }
    }

    // B -> C where B=i32 and C=String
    struct ElemIntToString;
    #[async_trait]
    impl ProcessorTrait<i32, String> for ElemIntToString {
        fn name(&self) -> &'static str {
            "ElemIntToString"
        }
        async fn process(&self, input: i32, _ctx: ProcessorContext) -> ProcessorResult<String> {
            ProcessorResult::Success(format!("n={}", input))
        }
    }

    #[tokio::test]
    async fn chain_vec_then_map_elements() {
        init_logger();
        // (2, 5) -> Vec<i32> = [2,3,4] -> map each i32 to String via TypedChain
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<Vec<i32>, _>(RangeToVec)
            .then_map_vec::<String, _>(ElemIntToString);

        let ctx = ProcessorContext::default();
        let res = chain.execute((2, 5), ctx).await;
        match res {
            ProcessorResult::Success(v) => {
                assert_eq!(
                    v,
                    vec!["n=2".to_string(), "n=3".to_string(), "n=4".to_string()]
                );
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    // Removed: wrong input type test not applicable with TypedChain

    // Parallel map: process `Vec<i32> -> Vec<String>` with configured concurrency.
    #[tokio::test]
    async fn chain_vec_parallel_map() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<Vec<i32>, _>(RangeToVec)
            .then_map_vec_parallel_with::<String, _>(ElemIntToString, 8);

        let out = chain.execute((0, 20), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(v) => {
                assert_eq!(v.len(), 20);
                // Parallel mode preserves original order.
                for (i, s) in v.iter().enumerate() {
                    assert_eq!(s, &format!("n={}", i as i32));
                }
            }
            _other => panic!("unexpected non-success result"),
        }
    }

    // (i32,i32) -> SyncBoxStream<i32>
    struct RangeToStream;
    #[async_trait]
    impl ProcessorTrait<(i32, i32), SyncBoxStream<'static, i32>> for RangeToStream {
        fn name(&self) -> &'static str {
            "RangeToStream"
        }
        async fn process(
            &self,
            input: (i32, i32),
            _ctx: ProcessorContext,
        ) -> ProcessorResult<SyncBoxStream<'static, i32>> {
            let (start, end) = input;
            let st = Box::pin(futures::stream::iter(start..end));
            ProcessorResult::Success(st)
        }
    }

    // Collection is done directly in tests via `SyncBoxStream::collect`, not as a chain step.

    #[tokio::test]
    async fn chain_stream_in_map_and_collect() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<SyncBoxStream<'static, i32>, _>(RangeToStream)
            .then_map_stream_in_with::<String, _>(ElemIntToString, 4);

        let out = chain.execute((1, 5), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(stream_out) => {
                let v: Vec<String> = stream_out.collect().await;
                assert_eq!(
                    v,
                    vec![
                        "n=1".to_string(),
                        "n=2".to_string(),
                        "n=3".to_string(),
                        "n=4".to_string()
                    ]
                );
            }
            _other => panic!("unexpected non-success result"),
        }
    }

    // ==== New tests: timeout / cancellation / stream error strategies ====

    // Simulates a slow processor: `i32 -> i32` with a configured delay in milliseconds.
    struct SlowAddOne {
        delay_ms: u64,
    }
    #[async_trait]
    impl ProcessorTrait<i32, i32> for SlowAddOne {
        fn name(&self) -> &'static str {
            "SlowAddOne"
        }
        async fn process(&self, input: i32, _ctx: ProcessorContext) -> ProcessorResult<i32> {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            ProcessorResult::Success(input + 1)
        }
    }

    #[tokio::test]
    async fn step_timeout_triggers_error() {
        init_logger();
        let chain = TypedChain::<i32, i32>::new().then::<i32, _>(SlowAddOne { delay_ms: 50 });

        let ctx = ProcessorContext::default().with_step_timeout_ms(10);
        let res = chain.execute(1, ctx).await;
        match res {
            ProcessorResult::FatalFailure(e) => {
                let s = format!("{}", e);
                assert!(
                    s.to_lowercase().contains("timeout"),
                    "unexpected error: {}",
                    s
                );
            }
            other => panic!("expected timeout failure, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn step_cancelled_triggers_error() {
        init_logger();
        let chain = TypedChain::<i32, i32>::new().then::<i32, _>(SlowAddOne { delay_ms: 0 });

        let ctx = ProcessorContext::default().cancel();
        let res = chain.execute(1, ctx).await;
        match res {
            ProcessorResult::FatalFailure(e) => {
                let s = format!("{}", e);
                assert!(
                    s.to_lowercase().contains("cancel"),
                    "unexpected error: {}",
                    s
                );
            }
            other => panic!("expected cancelled failure, got: {:?}", other),
        }
    }

    // Element-level processor that fails on a specific input value.
    struct ElemIntToStringMaybeFail {
        fail_on: i32,
    }
    #[async_trait]
    impl ProcessorTrait<i32, String> for ElemIntToStringMaybeFail {
        fn name(&self) -> &'static str {
            "ElemIntToStringMaybeFail"
        }
        async fn process(&self, input: i32, _ctx: ProcessorContext) -> ProcessorResult<String> {
            if input == self.fail_on {
                ProcessorResult::FatalFailure(
                    ProcessorChainError::Unexpected("elem fail".into()).into(),
                )
            } else {
                ProcessorResult::Success(format!("n={}", input))
            }
        }
    }

    #[tokio::test]
    async fn stream_map_drop_strategy_drops_bad_element() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<SyncBoxStream<'static, i32>, _>(RangeToStream)
            .then_map_stream_in_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 2 },
                2,
                ErrorStrategy::Skip,
            );

        let out = chain.execute((0, 5), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(st) => {
                let mut v: Vec<String> = st.collect().await;
                v.sort();
                // Should include four elements: 0, 1, 3, and 4.
                assert_eq!(
                    v,
                    vec!["n=0", "n=1", "n=3", "n=4"]
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                );
            }
            _other => panic!("unexpected non-success result"),
        }
    }

    #[tokio::test]
    async fn stream_map_stop_strategy_stops_on_first_error() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<SyncBoxStream<'static, i32>, _>(RangeToStream)
            // Concurrency `1` for deterministic ordering and stop point.
            .then_map_stream_in_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 2 },
                1,
                ErrorStrategy::Stop,
            );

        let out = chain.execute((0, 5), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(st) => {
                let v: Vec<String> = st.collect().await;
                // 0->ok, 1->ok, 2->error then stop; no further output is produced.
                assert_eq!(v, vec!["n=0".to_string(), "n=1".to_string()]);
            }
            _other => panic!("unexpected non-success result"),
        }
    }

    // ==== TypedChain tests ====

    #[tokio::test]
    async fn typed_chain_happy_path() {
        init_logger();
        // i32 -> String -> usize
        let chain = TypedChain::<i32, i32>::new()
            .then::<String, _>(IntToString)
            .then::<usize, _>(StringLen);

        let out = chain.execute(5, ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(n) => assert_eq!(n, "v=5".len()),
            _ => panic!("unexpected non-success result"),
        }
    }

    #[tokio::test]
    async fn typed_chain_vec_then_map() {
        init_logger();
        // (0, 4) -> Vec<i32> -> Vec<String>
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<Vec<i32>, _>(RangeToVec)
            .then_map_vec::<String, _>(ElemIntToString);

        let out = chain.execute((0, 4), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(v) => assert_eq!(
                v,
                vec!["n=0", "n=1", "n=2", "n=3"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            ),
            _ => panic!("unexpected non-success result"),
        }
    }

    #[tokio::test]
    async fn typed_chain_vec_parallel_map() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<Vec<i32>, _>(RangeToVec)
            .then_map_vec_parallel_with::<String, _>(ElemIntToString, 8);

        let out = chain.execute((0, 10), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(v) => {
                assert_eq!(v.len(), 10);
                for (i, s) in v.iter().enumerate() {
                    assert_eq!(s, &format!("n={}", i));
                }
            }
            _ => panic!("unexpected non-success result"),
        }
    }

    #[tokio::test]
    async fn typed_chain_stream_drop_and_stop() {
        init_logger();
        // Drop strategy
        let chain_drop = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<SyncBoxStream<'static, i32>, _>(RangeToStream)
            .then_map_stream_in_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 3 },
                2,
                ErrorStrategy::Skip,
            );
        let out = chain_drop
            .execute((0, 5), ProcessorContext::default())
            .await;
        match out {
            ProcessorResult::Success(st) => {
                let mut v: Vec<String> = st.collect().await;
                v.sort();
                assert_eq!(
                    v,
                    vec!["n=0", "n=1", "n=2", "n=4"]
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                );
            }
            _ => panic!("unexpected"),
        }

        // Stop strategy with concurrency=1
        let chain_stop = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<SyncBoxStream<'static, i32>, _>(RangeToStream)
            .then_map_stream_in_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 2 },
                1,
                ErrorStrategy::Stop,
            );
        let out2 = chain_stop
            .execute((0, 5), ProcessorContext::default())
            .await;
        match out2 {
            ProcessorResult::Success(st) => {
                let v: Vec<String> = st.collect().await;
                assert_eq!(v, vec!["n=0".to_string(), "n=1".to_string()]);
            }
            _ => panic!("unexpected"),
        }
    }

    #[tokio::test]
    async fn stream_map_return_result_variant() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<SyncBoxStream<'static, i32>, _>(RangeToStream)
            .then_map_stream_in_result_with::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 2 },
                4,
            );

        let out = chain.execute((0, 5), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(st) => {
                let collected: Vec<Result<String, Error>> = st.collect().await;
                let oks = collected.iter().filter(|r| r.is_ok()).count();
                let errs = collected.iter().filter(|r| r.is_err()).count();
                assert!(oks > 0 && errs > 0, "expected mix of ok and err results");
            }
            _ => panic!("unexpected non-success result"),
        }
    }

    // ==== New test: read test.txt -> bytes -> String -> Vec<String> -> Vec<i32> -> i64 sum ====

    // Read file path (`String`) -> bytes (`Vec<u8>`)
    struct PathToBytes;
    #[async_trait]
    impl ProcessorTrait<String, Vec<u8>> for PathToBytes {
        fn name(&self) -> &'static str {
            "PathToBytes"
        }
        async fn process(&self, path: String, _ctx: ProcessorContext) -> ProcessorResult<Vec<u8>> {
            match tokio::task::spawn_blocking(move || std::fs::read(&path)).await {
                Ok(Ok(bytes)) => ProcessorResult::Success(bytes),
                Ok(Err(e)) => ProcessorResult::FatalFailure(
                    ProcessorChainError::Unexpected(format!("read file error: {}", e).into())
                        .into(),
                ),
                Err(join_err) => ProcessorResult::FatalFailure(
                    ProcessorChainError::Unexpected(format!("join error: {}", join_err).into())
                        .into(),
                ),
            }
        }
    }

    // Bytes (`Vec<u8>`) -> UTF-8 string (`String`)
    struct BytesToString;
    #[async_trait]
    impl ProcessorTrait<Vec<u8>, String> for BytesToString {
        fn name(&self) -> &'static str {
            "BytesToString"
        }
        async fn process(&self, input: Vec<u8>, _ctx: ProcessorContext) -> ProcessorResult<String> {
            match String::from_utf8(input) {
                Ok(s) => ProcessorResult::Success(s),
                Err(e) => ProcessorResult::FatalFailure(
                    ProcessorChainError::Unexpected(format!("utf8 decode error: {}", e).into())
                        .into(),
                ),
            }
        }
    }

    // String (`String`) -> line-based `Vec<String>`
    struct StringToLines;
    #[async_trait]
    impl ProcessorTrait<String, Vec<String>> for StringToLines {
        fn name(&self) -> &'static str {
            "StringToLines"
        }
        async fn process(
            &self,
            input: String,
            _ctx: ProcessorContext,
        ) -> ProcessorResult<Vec<String>> {
            let lines: Vec<String> = input
                .lines()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            ProcessorResult::Success(lines)
        }
    }

    // Element-level: `String -> i32` (parse failures return errors; non-numeric lines are not
    // treated as `0`).
    struct StrToI32;
    #[async_trait]
    impl ProcessorTrait<String, i32> for StrToI32 {
        fn name(&self) -> &'static str {
            "StrToI32"
        }
        async fn process(&self, input: String, _ctx: ProcessorContext) -> ProcessorResult<i32> {
            match input.trim().parse::<i32>() {
                Ok(v) => ProcessorResult::Success(v),
                Err(e) => ProcessorResult::FatalFailure(
                    ProcessorChainError::Unexpected(format!("parse i32 error: {}", e).into())
                        .into(),
                ),
            }
        }
    }

    // `Vec<i32> -> i64` (sum)
    struct SumVecI32ToI64;
    #[async_trait]
    impl ProcessorTrait<Vec<i32>, i64> for SumVecI32ToI64 {
        fn name(&self) -> &'static str {
            "SumVecI32ToI64"
        }
        async fn process(&self, input: Vec<i32>, _ctx: ProcessorContext) -> ProcessorResult<i64> {
            let sum: i64 = input.into_iter().map(|n| n as i64).sum();
            ProcessorResult::Success(sum)
        }
    }

    #[tokio::test]
    async fn pipeline_file_sum_numbers() {
        init_logger();

        // Create a temp file
        let path = "test_pipeline_sum.txt";
        let content = "10\n20\n30\nnot_a_number\n40";
        tokio::fs::write(path, content).await.unwrap();

        // Build typed chain: path (`String`) -> bytes (`Vec<u8>`) -> `String` -> `Vec<String>`
        // -> `Vec<i32>` -> `i64`.
        // Since `StrToI32` returns errors on parse failures, non-numeric lines would fail the
        // pipeline without an error strategy like `Skip`.
        let chain = TypedChain::<String, String>::new()
            .then::<Vec<u8>, _>(PathToBytes)
            .then::<String, _>(BytesToString)
            .then::<Vec<String>, _>(StringToLines)
            .then_map_vec_parallel_with_strategy::<i32, _>(StrToI32, 8, ErrorStrategy::Skip)
            .then::<i64, _>(SumVecI32ToI64);

        // Execute
        let ctx = ProcessorContext::default();
        let res = chain.execute(path.to_string(), ctx).await;

        // Cleanup
        let _ = tokio::fs::remove_file(path).await;

        match res {
            ProcessorResult::Success(sum) => {
                // 10 + 20 + 30 + 40 = 100
                assert_eq!(sum, 100);
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    // ==== New tests: error strategies for parallel `Vec` map (`Skip` / `Stop`) ====

    #[tokio::test]
    async fn vec_parallel_map_drop_strategy_drops_bad_element() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<Vec<i32>, _>(RangeToVec)
            .then_map_vec_parallel_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 3 },
                4,
                ErrorStrategy::Skip,
            );
        let out = chain.execute((0, 6), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(v) => {
                let mut got = v;
                got.sort();
                assert_eq!(
                    got,
                    vec!["n=0", "n=1", "n=2", "n=4", "n=5"]
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                );
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn vec_parallel_map_stop_strategy_prefix_on_error() {
        init_logger();
        let chain = TypedChain::<(i32, i32), (i32, i32)>::new()
            .then::<Vec<i32>, _>(RangeToVec)
            .then_map_vec_parallel_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 2 },
                8,
                ErrorStrategy::Stop,
            );
        let out = chain.execute((0, 6), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(v) => {
                // Expected prefix before first error: [0, 1]
                assert_eq!(v, vec!["n=0".to_string(), "n=1".to_string()]);
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_flatten_vec() {
        init_logger();

        struct GenNested;
        #[async_trait]
        impl ProcessorTrait<(), Vec<Vec<i32>>> for GenNested {
            fn name(&self) -> &'static str {
                "GenNested"
            }
            async fn process(&self, _: (), _: ProcessorContext) -> ProcessorResult<Vec<Vec<i32>>> {
                ProcessorResult::Success(vec![vec![1, 2], vec![3], vec![], vec![4, 5]])
            }
        }

        let chain = TypedChain::<(), ()>::new()
            .then::<Vec<Vec<i32>>, _>(GenNested)
            .flatten_vec();

        let res = chain.execute((), ProcessorContext::default()).await;
        match res {
            ProcessorResult::Success(v) => assert_eq!(v, vec![1, 2, 3, 4, 5]),
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_one_shot() {
        init_logger();

        struct NonCloneInput(i32);
        // Input is not Clone, so we must use add_one_shot_processor (or then_one_shot)

        struct OneShotProc;
        #[async_trait]
        impl ProcessorTrait<NonCloneInput, i32> for OneShotProc {
            fn name(&self) -> &'static str {
                "OneShotProc"
            }
            async fn process(
                &self,
                input: NonCloneInput,
                _: ProcessorContext,
            ) -> ProcessorResult<i32> {
                ProcessorResult::Success(input.0 * 2)
            }
        }

        struct MakeNonClone;
        #[async_trait]
        impl ProcessorTrait<i32, NonCloneInput> for MakeNonClone {
            fn name(&self) -> &'static str {
                "MakeNonClone"
            }
            async fn process(
                &self,
                input: i32,
                _: ProcessorContext,
            ) -> ProcessorResult<NonCloneInput> {
                ProcessorResult::Success(NonCloneInput(input))
            }
        }

        let chain = TypedChain::<i32, i32>::new()
            .then::<NonCloneInput, _>(MakeNonClone)
            .then_one_shot::<i32, _>(OneShotProc);

        let res = chain.execute(10, ProcessorContext::default()).await;
        match res {
            ProcessorResult::Success(v) => assert_eq!(v, 20),
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_context_metadata() {
        init_logger();

        struct WriteMeta;
        #[async_trait]
        impl ProcessorTrait<(), ()> for WriteMeta {
            fn name(&self) -> &'static str {
                "WriteMeta"
            }
            async fn process(&self, _: (), ctx: ProcessorContext) -> ProcessorResult<()> {
                let mut meta = ctx.metadata.write().await;
                meta.insert("key".to_string(), serde_json::json!("value"));
                ProcessorResult::Success(())
            }
        }

        struct ReadMeta;
        #[async_trait]
        impl ProcessorTrait<(), String> for ReadMeta {
            fn name(&self) -> &'static str {
                "ReadMeta"
            }
            async fn process(&self, _: (), ctx: ProcessorContext) -> ProcessorResult<String> {
                let meta = ctx.metadata.read().await;
                let val = meta
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                ProcessorResult::Success(val)
            }
        }

        let chain = TypedChain::<(), ()>::new()
            .then::<(), _>(WriteMeta)
            .then::<String, _>(ReadMeta);

        let res = chain.execute((), ProcessorContext::default()).await;
        match res {
            ProcessorResult::Success(v) => assert_eq!(v, "value"),
            other => panic!("unexpected result: {:?}", other),
        }
    }
}
