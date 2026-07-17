//! mocra's public API contract — `use mocra::prelude::*` pulls it in one shot.
//!
//! Layers:
//! - **Core facade**: `Spider` / `Mocra` / `DataSink` / `on_item` / `Ctx` / `Seeds`
//!   + `Request` / `Response` / `Result` — these cover 80% of use cases.
//! - **Cluster** (`cluster-embedded`): `ClusterConfig` / `RaftTuning`.
//! - **Admin / monitoring** (`dashboard`): `prelude::dashboard` (`EngineStats` /
//!   `ClusterStatusView`), paired with the facade's `Mocra::builder().dashboard(port)` to expose
//!   the admin + observability HTTP API.
//! - **Extension points**: implement `queue::MqBackend` / `sync::CoordinationBackend` /
//!   `common::ModuleTrait` / `common::DataMiddleware` and friends to plug in custom backends.
//!
//! Advanced / extension types live in the `prelude::{engine, queue, sync, schedule, common, utils,
//! proxy, cacheable, errors}` submodules; internal implementation details have been narrowed to
//! `pub(crate)` and are not part of the public API.

// High-level Spider facade (refactor Phase 1)
#[cfg(feature = "cluster-embedded")]
pub use crate::facade::ClusterConfig;
pub use crate::facade::{ChannelSink, Ctx, DataSink, Mocra, MocraBuilder, Seeds, Spider, on_item};
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
pub use crate::common::model::{Cookies, ExecutionMark, Headers, ModuleConfig, Request, Response};

// Errors
pub use crate::errors::{BoxError, Error, ErrorKind, ParserError, RequestError, Result};

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
    /// Needed to implement [`Downloader::set_config`] in a custom downloader.
    pub use crate::common::model::download_config::DownloadConfig;
    pub use crate::downloader::Downloader;
    pub use crate::downloader::DownloaderManager;
    pub use crate::downloader::WebSocketDownloader;
}
pub mod engine {
    pub use crate::engine::engine::Engine;
}
/// Admin / monitoring observability data types (requires the `dashboard` feature). Consumed by the
/// admin page / clients.
#[cfg(feature = "dashboard")]
pub mod dashboard {
    pub use crate::engine::api::observability::{EngineStats, QueueBreakdown};
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
}
pub mod schedule {
    pub use crate::schedule::Dag;
    pub use crate::schedule::DagError;
    pub use crate::schedule::DagScheduler;
    pub use crate::schedule::TaskPayload;
}
pub mod sync {
    pub use crate::sync::ClusterStatusView;
    pub use crate::sync::CoordinationBackend;
    pub use crate::sync::DistributedSync;
    pub use crate::sync::SyncAble;
    pub use crate::sync::SyncService;
}
pub mod utils {
    pub use crate::utils::date_utils::DateUtils;
    pub use crate::utils::distributed_rate_limit::{
        DistributedSlidingWindowRateLimiter, RateLimitConfig,
    };
    pub use crate::utils::lock::DistributedLockManager;
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
