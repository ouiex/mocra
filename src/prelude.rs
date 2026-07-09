//! mocra 的公开 API 契约 —— `use mocra::prelude::*` 一站式引入。
//!
//! 分层:
//! - **核心门面**:`Spider` / `Mocra` / `DataSink` / `on_item` / `Ctx` / `Seeds`
//!   + `Request` / `Response` / `Result` —— 80% 场景只需这些。
//! - **集群**(`cluster-embedded`):`ClusterConfig` / `RaftTuning`。
//! - **后台管理 / 监控**(`dashboard`):`prelude::dashboard`(`EngineStats` / `ClusterStatusView`),
//!   配合门面 `Mocra::builder().dashboard(port)` 暴露 admin + 可观测 HTTP API。
//! - **扩展点**:自定义后端时实现 `queue::MqBackend` / `sync::CoordinationBackend` /
//!   `common::ModuleTrait` / `common::DataMiddleware` 等。
//!
//! 进阶 / 扩展类型在子模块 `prelude::{engine, queue, sync, schedule, common, utils, proxy,
//! cacheable, errors}`;内部实现细节已收敛为 `pub(crate)`,不进公开 API。

// High-level Spider facade (重构 Phase 1)
pub use crate::facade::{
    on_item, ChannelSink, Ctx, DataSink, Mocra, MocraBuilder, Seeds, Spider,
};
#[cfg(feature = "cluster-embedded")]
pub use crate::facade::ClusterConfig;
#[cfg(feature = "cluster-embedded")]
pub use mocra_cluster::RaftTuning;

// Common Traits and Structs
pub use crate::common::interface::{
    DataMiddleware, DataStoreMiddleware, DownloadMiddleware, MiddlewareManager, ModuleNodeTrait,
    ModuleTrait, StoreTrait, SyncBoxStream,
};
pub use crate::common::model::data::DataEvent;
pub use crate::common::model::login_info::LoginInfo;
pub use crate::common::model::message::{TaskOutputEvent, TaskParserEvent};
pub use crate::common::model::request::RequestMethod;
pub use crate::common::model::{
    Cookies, ExecutionMark, Headers, ModuleConfig, Request, Response,
};

// Errors
pub use crate::errors::{
    BoxError, Error, ErrorKind, ParserError, RequestError, Result,
};

// Utils
pub use crate::utils::date_utils::DateUtils;

pub mod common {
    pub use crate::common::interface::DataMiddleware;
    pub use crate::common::interface::DataStoreMiddleware;
    pub use crate::common::interface::DownloadMiddleware;
    pub use crate::common::interface::MiddlewareManager;
    pub use crate::common::interface::ModuleNodeTrait;
    pub use crate::common::interface::ModuleTrait;
    pub use crate::common::interface::StoreTrait;
    pub use crate::common::interface::SyncBoxStream;
    pub use crate::common::interface::module::ToSyncBoxStream;

    pub use crate::common::model::Cookies;
    pub use crate::common::model::ExecutionMark;
    pub use crate::common::model::Headers;
    pub use crate::common::model::ModuleConfig;
    pub use crate::common::model::Request;
    pub use crate::common::model::Response;
}
pub mod downloader {
    pub use crate::downloader::Downloader;
    pub use crate::downloader::DownloaderManager;
    pub use crate::downloader::WebSocketDownloader;
}
pub mod engine {
    pub use crate::engine::engine::Engine;
}
/// 后台管理 / 监控可观测数据类型(需 `dashboard` 特性)。供后台管理页面 / 客户端消费。
#[cfg(feature = "dashboard")]
pub mod dashboard {
    pub use crate::engine::api::observability::{EngineStats, PendingBreakdown};
    pub use crate::engine::monitor::SystemSnapshot;
    pub use crate::sync::ClusterStatusView;
    pub use crate::utils::logger::LogRecord;
}
pub mod queue {
    pub use crate::queue::Channel;
    pub use crate::queue::Compensator;
    pub use crate::queue::Identifiable;
    pub use crate::queue::Message;
    pub use crate::queue::MqBackend;
    pub use crate::queue::QueueManager;
    pub use crate::queue::RedisCompensator;
    pub use crate::queue::RedisQueue;
}
pub mod schedule {
    pub use crate::schedule::Dag;
    pub use crate::schedule::DagError;
    pub use crate::schedule::DagScheduler;
    pub use crate::schedule::TaskPayload;
}
pub mod sync {
    pub use crate::sync::DistributedSync;
    #[cfg(feature = "queue-kafka")]
    pub use crate::sync::KafkaBackend;
    pub use crate::sync::ClusterStatusView;
    pub use crate::sync::CoordinationBackend;
    pub use crate::sync::RedisBackend;
    pub use crate::sync::SyncAble;
    pub use crate::sync::SyncService;
}
pub mod utils {
    pub use crate::utils::date_utils::DateUtils;
    pub use crate::utils::distributed_rate_limit::{
        DistributedSlidingWindowRateLimiter, RateLimitConfig,
    };
    pub use crate::utils::redis_lock::DistributedLockManager;
}
pub mod proxy {
    pub use crate::proxy::*;
}
pub mod errors {
    pub use crate::errors::BoxError;
    pub use crate::errors::Error;
    pub use crate::errors::ErrorKind;
    pub use crate::errors::ParserError;
    pub use crate::errors::RequestError;
    pub use crate::errors::Result;
}
pub mod cacheable {
    pub use crate::cacheable::CacheAble;
    pub use crate::cacheable::CacheService;
}

#[cfg(feature = "polars")]
pub mod polars {
    pub mod polars {
        pub use ::polars::*;
    }
    pub mod polars_lazy {
        pub use ::polars_lazy::*;
    }
    pub mod polars_ops {
        pub use ::polars_ops::*;
    }
}

