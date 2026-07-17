use std::sync::Arc;
use tokio::sync::RwLock;

use crate::cacheable::CacheService;
use crate::common::model::config::Config;
use crate::common::status_tracker::StatusTracker;
use crate::utils::lock::DistributedLockManager;

/// The **focused runtime context** required by the collection pipeline (download / parser /
/// task-model / stream chains).
///
/// It is a narrowed view of [`State`](crate::common::state::State) — it exposes only the four
/// shared services the pipeline actually uses, rather than the whole 10-field god object. Chains
/// depend on it (instead of on `State`), which **decouples the core pipeline from optional or
/// cluster-only subsystems** such as the database / API rate limiter / cookies / coordination
/// backend, and is a prerequisite for later extracting the pipeline into `mocra-core`.
///
/// The field names and types mirror the corresponding fields on `State`;
/// [`State::pipeline_ctx`](crate::common::state::State::pipeline_ctx) builds it by cloning those
/// `Arc`s directly — so it **shares** the very same services with `State` (config hot reloads stay
/// visible to the pipeline, and so on).
///
/// > Note: `locker` looks like coordination infrastructure, but the pipeline already depends on it
/// > indirectly through `status_tracker` (which internally holds the same `DistributedLockManager`);
/// > exposing it directly here serves only coordination purposes such as serialization / rate
/// > limiting, and does not widen the real dependency surface.
pub struct PipelineContext {
    /// Dynamic configuration (shares the same `RwLock` as `State`, so config hot reloads are
    /// visible to the pipeline).
    pub config: Arc<RwLock<Config>>,
    /// General purpose cache service (deduplication / sessions / intermediate results, etc.).
    pub cache_service: Arc<CacheService>,
    /// Task / module status and error tracking (retries, circuit breaker, locks, etc.).
    pub status_tracker: Arc<StatusTracker>,
    /// Distributed lock manager (coordination backend / in-process local locks; serves coordination
    /// purposes such as serialization / rate limiting).
    pub locker: Arc<DistributedLockManager>,
}
