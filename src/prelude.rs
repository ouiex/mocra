// Common Traits and Structs
pub use crate::common::interface::{
    DataMiddleware, DataStoreMiddleware, DownloadMiddleware, MiddlewareManager, ModuleNodeTrait,
    ModuleTrait, StoreTrait, SyncBoxStream,
};
pub use crate::common::model::data::Data;
pub use crate::common::model::login_info::LoginInfo;
pub use crate::common::model::message::{ParserData, ParserTaskModel};
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
pub mod sync {
    pub use crate::sync::DistributedSync;
    pub use crate::sync::KafkaBackend;
    pub use crate::sync::CoordinationBackend;
    pub use crate::sync::RedisBackend;
    pub use crate::sync::SyncAble;
    pub use crate::sync::SyncService;
}
pub mod utils {
    pub use crate::utils::*;
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

