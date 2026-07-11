//! Adapts the multi-level [`CacheService`] into `mocra-dag`'s distributed KV / atomic
//! [`DagStore`] backend.
//!
//! This impl lives here (not in the host) because both `CacheService` (this crate) and
//! `DagStore` (mocra-dag) are now defined outside the host — the orphan rule requires the
//! impl to sit in the crate that owns one of them.

use std::time::Duration;

use async_trait::async_trait;
use mocra_dag::DagStore;

use super::CacheService;

#[async_trait]
impl DagStore for CacheService {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        CacheService::get(self, key)
            .await
            .map_err(|e| e.to_string())
    }
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), String> {
        CacheService::set(self, key, value, ttl)
            .await
            .map_err(|e| e.to_string())
    }
    async fn del(&self, key: &str) -> Result<(), String> {
        CacheService::del(self, key)
            .await
            .map_err(|e| e.to_string())
    }
    async fn incr(&self, key: &str, delta: i64) -> Result<i64, String> {
        CacheService::incr(self, key, delta)
            .await
            .map_err(|e| e.to_string())
    }
}
