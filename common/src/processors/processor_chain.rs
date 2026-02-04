#![allow(unused)]
use crate::processors::processor::{ProcessorContext, ProcessorResult, ProcessorTrait};
use async_trait::async_trait;
use errors::Error;
use errors::ProcessorChainError;
use futures::{Stream, StreamExt, stream};
use log::{debug, error, info, warn};
use std::any::{Any, TypeId};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;

/// 处理器链
///
/// 特性：
/// - 链式编排：每一步接受上一步的输出作为输入；
/// - 强类型校验：构建/执行前用 `TypeId` 做运行期类型连通性校验；
/// - 失败语义：支持 Success / RetryableFailure / FatalFailure；
/// - 上下文控制：支持取消、每步超时、重试策略等；
/// - 扩展适配器：支持 Vec 映射、流式映射（并发、错误策略）。
struct ProcessorChain {
    steps: Vec<ProcessorStep>,
}

/// 处理器步骤的包装（对外隐藏具体泛型类型，通过类型擦除来统一调度）
struct ProcessorStep {
    name: String,
    executor: Box<dyn ProcessorExecutor + Send + Sync>,
    // Type metadata for validation and better diagnostics
    input_type_id: TypeId,
    output_type_id: TypeId,
    input_type_name: &'static str,
    output_type_name: &'static str,
}

/// 处理器执行器 trait，用于类型擦除。
///
/// 将具体的 `ProcessorTrait<Input, Output>` 封装为统一的 `ProcessorExecutor`，
/// 以便链路在运行时按步骤顺序调用。
#[async_trait]
trait ProcessorExecutor: Send + Sync {
    async fn execute(
        &self,
        input: Box<dyn Any + Send>,
        context: ProcessorContext,
    ) -> ProcessorResult<Box<dyn Any + Send>>;
    fn name(&self) -> &str;
}

/// 具体的处理器执行器实现
struct TypedProcessorExecutor<Input, Output, P> {
    processor: P,
    // 使用函数指针 PhantomData 避免从 Input/Output 传播自动 trait（如 Sync）
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
        // 2) 准备上下文
        let mut current_context = context;

        // 2.1 should_process 判定
        // 跳过场景：仅当 Input 与 Output 类型相同（透传）。否则只能继续处理以避免类型断裂。
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
            // Input == Output, safe to cast via Any
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

        // 2.2 pre_process 在第一次尝试之前执行
        if let Err(e) = self.processor.pre_process(&input, &current_context).await {
            error!("Pre-processing failed for {}: {}", self.processor.name(), e);
            return ProcessorResult::FatalFailure(e);
        }

        loop {
            // 3) 取消检查
            if current_context.cancelled {
                return ProcessorResult::FatalFailure(
                    ProcessorChainError::Cancelled("cancelled".into()).into(),
                );
            }

            // 4) 执行处理器（可选超时）
            // 必须 clone input 给 process，因为 process 消耗所有权，而我们需要保留 typed_input 用于重试或错误处理
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
                    // 后置处理失败可进入 handle_error 以允许补偿。
                    if let Err(post_err) = self
                        .processor
                        .post_process(&input, &output, &current_context)
                        .await
                    {
                        error!("Post-processing failed for {}: {}", self.name(), post_err);
                        // 将 post_err 当作 Fatal 交给 handle_error
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

                    // 检查是否应该重试
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
                        continue; // 重试，下一次循环将再次使用 typed_input.clone()
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
                    // process 失败 -> 交给 handle_error
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
        // 1) 向下转型为具体输入类型；若失败，立即返回致命错误（类型不匹配）
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

/// 扁平化执行器：将 Vec<Vec<T>> 扁平为 Vec<T>
/// 一次性处理器执行器（不支持重试，不要求 Input: Clone）
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

/// Vec map 执行器：对 `Vec<ElemIn>` 中的每个元素运行处理器（保持输出顺序）
/// - Sequential：逐个处理；
/// - Parallel(n)：并发处理，但通过 (idx, result) 收集再排序，确保顺序稳定。
enum VecMapMode {
    Sequential,
    Parallel(usize),
}

struct MapVecExecutor<ElemIn, ElemOut, P> {
    inner: TypedProcessorExecutor<ElemIn, ElemOut, P>,
    mode: VecMapMode,
    // 当设置为 Some(strategy) 时，启用与流式 map 类似的元素级错误处理策略（仅在并行模式下生效）：
    // - Drop：丢弃出错元素，继续；
    // - Stop：在遇到第一个错误时停止产出后续元素（返回已成功的前缀）。
    // 注意：ReturnResult 不在此执行器中支持（如需返回 Result，请新增专门的 result 版本）。
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
        // 下转为 Vec<ElemIn>
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
                // 根据是否启用策略决定错误处理方式
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

/// 流式 map 执行器（输入为 SyncBoxStream<ElemIn>，输出为 SyncBoxStream<ElemOut>），内部支持并发
///
/// 支持三种错误策略：
/// - Drop：丢弃错误项，继续流；
/// - Stop：遇到错误立即停止后续产出；
/// - ReturnResult：不在该执行器中支持（请使用 `MapStreamInResultExecutor`）。
struct MapStreamInExecutor<ElemIn, ElemOut, P> {
    inner: Arc<TypedProcessorExecutor<ElemIn, ElemOut, P>>,
    concurrency: usize,
    // 错误处理策略：drop（默认，丢弃错误）、stop（遇错停止整个流）、result（输出 Result<ElemOut, Error>）
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
        // 使用原子布尔标识实现 Stop 策略（无锁、轻量）
        use std::sync::atomic::{AtomicBool, Ordering};
        let stop_flag = Arc::new(AtomicBool::new(false));
        if matches!(strategy, ErrorStrategy::ReturnResult) {
            return ProcessorResult::FatalFailure(
                ProcessorChainError::Unexpected("stream map step configured for ReturnResult but wrong builder used (expected _result variant)".into()).into()
            );
        }
        // 先并发执行，再根据策略进行过滤或停产
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
        // mapped 现为 SyncBoxStream<'static, ElemOut>
        let mapped: SyncBoxStream<'static, ElemOut> = Box::pin(mapped);
        ProcessorResult::Success(Box::new(mapped) as Box<dyn Any + Send>)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

/// 流式 map 执行器（返回 `Result<ElemOut, Error>` 的流）
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
        use errors::Error as E;
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
    /// 创建一个空的处理器链
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// 添加处理器到链中
    ///
    /// 将一个 `ProcessorTrait<Input, Output>` 加入链尾。注意：该方法不会立即做类型连通性校验，
    /// 可在构建完成后调用 `validate_link_types` 或使用 `try_add_processor` 进行一步到位校验。
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

    /// 添加一次性处理器（不支持重试，不要求 Input: Clone）
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

    /// 添加一个对 `Vec<ElemIn>` 进行逐元素处理的处理器（map）
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

    /// 尝试添加处理器：在构建阶段进行类型连通性校验（上一步输出 == 本步输入）
    ///
    /// 若校验失败返回错误，不会污染原有链（因 `self` 按值移动，新链在出错时被丢弃）。
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

    /// 校验链路中相邻处理器的类型是否匹配
    ///
    /// 包括：上一步输出类型 `output_type_id` 必须等于下一步输入类型 `input_type_id`。
    /// 校验失败将返回包含具体步骤与期望/实际类型名的错误，便于诊断。
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

    /// 添加并行 map 处理器（输入 `Vec<ElemIn>`，输出 `Vec<ElemOut>`），自定义并发度
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

    /// 添加并行 map 处理器（默认并发度 16）
    pub fn add_processor_map_parallel<ElemIn, ElemOut, P>(self, processor: P) -> Self
    where
        ElemIn: Send + Clone + 'static,
        ElemOut: Send + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        self.add_processor_map_parallel_with::<ElemIn, ElemOut, P>(processor, 16)
    }

    /// 添加并行 map 处理器（输入 `Vec<ElemIn>`，输出 `Vec<ElemOut>`），自定义并发度 + 错误策略
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

    /// 添加流式 map（输入 `SyncBoxStream<ElemIn>`，输出 `SyncBoxStream<ElemOut>`），自定义并发度
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

    /// 添加流式 map（默认并发度 16）
    pub fn add_processor_map_stream_in<ElemIn, ElemOut, P>(self, processor: P) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        self.add_processor_map_stream_in_with::<ElemIn, ElemOut, P>(processor, 16)
    }

    /// 添加流式 map（自定义并发度 + 错误策略）
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

    /// 添加流式 map（返回 `Result<ElemOut, Error>` 的项），自定义并发度
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

    /// 添加流式 map（返回 Result 的项），默认并发度 16
    pub fn add_processor_map_stream_in_result<ElemIn, ElemOut, P>(self, processor: P) -> Self
    where
        ElemIn: Send + Clone + Sync + 'static,
        ElemOut: Send + Sync + 'static,
        P: ProcessorTrait<ElemIn, ElemOut> + Send + Sync + 'static,
    {
        self.add_processor_map_stream_in_result_with::<ElemIn, ElemOut, P>(processor, 16)
    }

    /// 添加一个扁平化步骤：将 Vec<Vec<T>> -> Vec<T>
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

    /// 执行整个处理器链
    ///
    /// 执行前做两类快速校验：
    /// 1) 链路连通性（相邻类型匹配）；
    /// 2) 首/尾类型与调用方声明的 `Input/Output` 是否一致（避免运行中再出错）。
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

        // 运行前的快速类型校验：首入/末出 以及 链路连通性
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

        // 逐步执行每个处理器：每一步返回的 Any 会成为下一步的输入
        for (idx, step) in self.steps.iter().enumerate() {
            debug!(
                "Executing processor [{} / {}]: {}",
                idx + 1,
                self.steps.len(),
                step.name
            );

            // 执行处理器（重试逻辑由 TypedProcessorExecutor 处理）
            match step.executor.execute(current_data, context.clone()).await {
                ProcessorResult::Success(output) => {
                    current_data = output;
                }
                ProcessorResult::RetryableFailure(_retry_policy) => {
                    // 这种情况不应该发生，因为 TypedProcessorExecutor 应该处理所有重试
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
        // 尝试将最终结果转换为期望的输出类型
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

    /// 获取处理器链的长度
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// 检查处理器链是否为空
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// 获取所有处理器的名称（按执行顺序）
    pub fn processor_names(&self) -> Vec<&str> {
        self.steps.iter().map(|step| step.executor.name()).collect()
    }
}

impl Default for ProcessorChain {
    fn default() -> Self {
        Self::new()
    }
}

/// 静态类型版 Chain：在编译期通过类型参数约束确保相邻步骤的 Input/Output 连通
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
        // 在类型层面对 Out -> Next 做编译期约束；运行期依然会做链路校验
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

// 针对 Vec 输出的特化：在链上追加“逐元素 map”的步骤
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

// 针对 Vec<Vec<T>> 输出的特化：提供扁平化为 Vec<T>
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

    /// 先扁平化为 Vec<T>，再对每个 T 应用处理器，得到 Vec<U>
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

    /// 先扁平化为 Vec<T>，再并行 map 为 Vec<U>
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

    /// 先扁平化为 Vec<T>，再并行 map 并应用错误策略
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

// 针对 Stream 输出的特化：在链上追加“流式逐元素 map”的步骤
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

    use crate::processors::processor::RetryPolicy;
    use futures::stream::{BoxStream, StreamExt};
    use std::sync::Once;
    use utils::logger;

    static INIT: Once = Once::new();
    fn init_logger() {
        INIT.call_once(|| {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let _ = handle.block_on(logger::init_app_logger("common-tests"));
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

    // B -> C 这里 B=i32, C=String
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

    // 并行 map: 使用并发度处理 Vec<i32> -> Vec<String>
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
                // 并行保持原顺序
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

    // 收集逻辑在测试里直接对 SyncBoxStream 进行 collect，不再作为处理器加入链

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

    // ==== 新增：超时 / 取消 / 流式错误策略 测试 ====

    // 模拟一个慢处理器：i32 -> i32，延迟指定毫秒
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

    // 元素级处理器：失败在某个元素上
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
                // 应包含 0,1,3,4 四个元素
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
            // 并发度 1 以获得确定的顺序与停点
            .then_map_stream_in_with_strategy::<String, _>(
                ElemIntToStringMaybeFail { fail_on: 2 },
                1,
                ErrorStrategy::Stop,
            );

        let out = chain.execute((0, 5), ProcessorContext::default()).await;
        match out {
            ProcessorResult::Success(st) => {
                let v: Vec<String> = st.collect().await;
                // 0->ok, 1->ok, 2->error then stop, 后面不再产出
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

    // ==== 新增测试：读取 test.txt -> 二进制 -> String -> Vec<String> -> Vec<i32> -> i64 求和 ====

    // 读取文件路径(String) -> 二进制(Vec<u8>)
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

    // 二进制(Vec<u8>) -> UTF-8 字符串(String)
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

    // 字符串(String) -> 按行 Vec<String>
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

    // 元素级：String -> i32（解析失败返回错误，不再将非数字行视为 0）
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

    // Vec<i32> -> i64（求和）
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

        // 构建 typed 链：路径(String) -> 二进制(Vec<u8>) -> String -> Vec<String> -> Vec<i32> -> i64
        // 由于 StrToI32 在解析失败时返回错误，若文件包含非数字行，则该链会失败
        let chain = TypedChain::<String, String>::new()
            .then::<Vec<u8>, _>(PathToBytes)
            .then::<String, _>(BytesToString)
            .then::<Vec<String>, _>(StringToLines)
            .then_map_vec_parallel_with_strategy::<i32, _>(StrToI32, 8, ErrorStrategy::Skip)
            .then::<i64, _>(SumVecI32ToI64);

        // 运行
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

    // ==== 新增测试：Vec 并行 map 的错误策略（Drop / Stop）====

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
                // 期望返回第一个错误前的前缀 [0,1]
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
