use super::service::CacheService;
use crate::errors::CacheError;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[async_trait::async_trait]
pub trait CacheAble: Send + Sync + Sized
where
    Self: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn field() -> impl AsRef<str>;

    fn serialized_size_hint(&self) -> Option<usize> {
        None
    }

    fn clone_for_serialize(&self) -> Option<Self> {
        None
    }

    async fn send(&self, id: &str, sync: &CacheService) -> Result<(), CacheError> {
        let key = Self::cache_id(id, sync);

        let content = serialize(self, sync).await?;

        sync.backend.set(&key, &content, sync.default_ttl).await?;
        Ok(())
    }

    /// Store this value in the cache with no TTL (persistent until explicitly deleted).
    async fn send_persistent(&self, id: &str, sync: &CacheService) -> Result<(), CacheError> {
        let key = Self::cache_id(id, sync);
        let content = serialize(self, sync).await?;
        sync.backend.set(&key, &content, None).await?;
        Ok(())
    }

    async fn send_with_ttl(
        &self,
        id: &str,
        sync: &CacheService,
        ttl: Duration,
    ) -> Result<(), CacheError> {
        let key = Self::cache_id(id, sync);
        let content = serialize(self, sync).await?;
        sync.backend.set(&key, &content, Some(ttl)).await?;
        Ok(())
    }

    async fn send_nx(
        &self,
        id: &str,
        sync: &CacheService,
        ttl: Option<Duration>,
    ) -> Result<bool, CacheError> {
        let key = Self::cache_id(id, sync);
        let content = serialize(self, sync).await?;
        sync.backend.set_nx(&key, &content, ttl).await
    }

    async fn sync(id: &str, sync: &CacheService) -> Result<Option<Self>, CacheError> {
        let key = Self::cache_id(id, sync);
        if let Some(bytes) = sync.backend.get(&key).await? {
            let val = serde_json::from_slice(&bytes).map_err(CacheError::Serde)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    async fn delete(id: &str, sync: &CacheService) -> Result<(), CacheError> {
        let key = Self::cache_id(id, sync);
        sync.backend.del(&key).await?;
        Ok(())
    }

    async fn scan(pattern_suffix: &str, sync: &CacheService) -> Result<Vec<String>, CacheError> {
        let pattern = format!(
            "{}:{}:{}",
            sync.namespace,
            Self::field().as_ref(),
            pattern_suffix
        );
        sync.backend.keys(&pattern).await
    }

    fn cache_id(id: &str, cache: &CacheService) -> String {
        format!("{}:{}:{id}", cache.namespace, Self::field().as_ref())
    }
}

async fn serialize<T: CacheAble>(value: &T, cache: &CacheService) -> Result<Vec<u8>, CacheError> {
    if value
        .serialized_size_hint()
        .is_some_and(|size| size >= cache.serialize_blocking_threshold)
        && let Some(data) = value.clone_for_serialize()
    {
        return tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?
            .map_err(CacheError::Serde);
    }
    serde_json::to_vec(value).map_err(CacheError::Serde)
}
