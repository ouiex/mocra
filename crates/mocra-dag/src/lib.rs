//! mocra-dag:通用分布式 DAG 执行引擎(从 mocra 主 crate 抽出,**零爬虫耦合**)。
//!
//! - [`Dag`] / [`DagChainBuilder`]:构图(节点 + 依赖边,拓扑校验、环检测)。
//! - [`DagScheduler`]:分层并发执行、重试策略、fencing 保护的分布式运行守卫。
//! - 节点派发经 [`DagNodeDispatcher`] 抽象注入(内置 [`LocalNodeDispatcher`],宿主可自定义)。
//! - 运行时依赖经 trait 注入([`DagStore`] / [`DagEventSink`] / [`DagFencingStore`]),
//!   宿主用嵌入式集群 KV / 分布式 pub-sub 实现,故本 crate 不反依赖宿主。

// 迁移自主 crate 的既有风格:emit_sync_state 遥测函数参数多(逐个状态字段),
// graph 的空列表 guard —— 保留原逻辑(已被 54 个测试覆盖),豁免这两项 pedantic lint。
#![allow(clippy::too_many_arguments, clippy::redundant_guards)]

mod graph;
mod metrics;
mod scheduler;
pub mod store;
pub mod types;

pub use graph::{Dag, DagChainBuilder, DagNodePtr};
pub use scheduler::{DagExecutionReport, DagScheduler, DagSchedulerOptions, NodeExecutionResult};
pub use store::{DagEventSink, DagStore};
pub use types::{
    DagError, DagErrorClass, DagErrorCode, DagFencingStore, DagNodeDispatcher,
    DagNodeExecutionPolicy, DagNodeRecord, DagNodeRetryMode, DagNodeStatus, DagNodeSyncState,
    DagNodeTrait, DagRunGuard, DagRunGuardAcquireOutcome, DagRunResumeState, DagRunStateStore,
    LocalNodeDispatcher, NodeExecutionContext, NodePlacement, TaskPayload,
};
