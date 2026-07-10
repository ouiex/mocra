use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cacheable::CacheService;
use crate::common::model::config::Config;
use crate::common::status_tracker::StatusTracker;
use crate::utils::redis_lock::DistributedLockManager;

/// 采集管线(download / parser / task-model / stream chains)所需的**聚焦运行时上下文**。
///
/// 它是 [`State`](crate::common::state::State) 的一个窄化视图 —— 只暴露管线真正用到的
/// 四样共享服务,而非整个 10 字段上帝对象。chains 依赖它(而非 `State`),使核心管线与
/// 数据库 / API 限流器 / cookie / 原始 Redis 池 / 协调后端等**可选或集群子系统解耦**,
/// 是后续把管线抽成 `mocra-core` 的前置条件。
///
/// 字段名与类型与 `State` 的对应字段一致,[`State::pipeline_ctx`](crate::common::state::State::pipeline_ctx)
/// 直接克隆这几个 `Arc` 构造 —— 因此与 `State` **共享**同一份服务(配置热更新等可见)。
///
/// > 说明:`locker` 看似是协调设施,但管线本就经 `status_tracker`(内部持有同一个
/// > `DistributedLockManager`)间接依赖它;这里直接暴露仅供请求去重器取 Redis 池,
/// > 未扩大真实依赖面。
pub struct PipelineContext {
    /// 动态配置(与 `State` 共享同一 `RwLock`,配置热更新对管线可见)。
    pub config: Arc<RwLock<Config>>,
    /// 通用缓存服务(去重 / 会话 / 中间结果等)。
    pub cache_service: Arc<CacheService>,
    /// 任务 / 模块状态与错误追踪(重试、熔断、锁等)。
    pub status_tracker: Arc<StatusTracker>,
    /// 分布式锁管理器(管线仅用其 Redis 池构造请求去重器;单机无池时去重降级为关闭)。
    pub locker: Arc<DistributedLockManager>,
}
