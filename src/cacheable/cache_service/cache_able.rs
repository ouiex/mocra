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

        let content = if self
            .serialized_size_hint()
            .map_or(false, |size| size >= sync.serialize_blocking_threshold)
        {
            if let Some(data) = self.clone_for_serialize() {
                tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??
            } else {
                serde_json::to_vec(self)?
            }
        } else {
            serde_json::to_vec(self)?
        };

        sync.backend.set(&key, &content, sync.default_ttl).await?;
        Ok(())
    }

    async fn send_with_ttl(&self, id: &str, sync: &CacheService, ttl: Duration) -> Result<(), CacheError> {
        let key = Self::cache_id(id, sync);
        let content = if self
            .serialized_size_hint()
            .map_or(false, |size| size >= sync.serialize_blocking_threshold)
        {
            if let Some(data) = self.clone_for_serialize() {
                tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??
            } else {
                serde_json::to_vec(self)?
            }
        } else {
            serde_json::to_vec(self)?
        };
        sync.backend.set(&key, &content, Some(ttl)).await?;
        Ok(())
    }

    async fn send_nx(&self, id: &str, sync: &CacheService, ttl: Option<Duration>) -> Result<bool, CacheError> {
        let key = Self::cache_id(id, sync);
        let content = if self
            .serialized_size_hint()
            .map_or(false, |size| size >= sync.serialize_blocking_threshold)
        {
            if let Some(data) = self.clone_for_serialize() {
                tokio::task::spawn_blocking(move || serde_json::to_vec(&data))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??
            } else {
                serde_json::to_vec(self)?
            }
        } else {
            serde_json::to_vec(self)?
        };
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
        let pattern = format!("{}:{}:{}", sync.namespace, Self::field().as_ref(), pattern_suffix);
        sync.backend.keys(&pattern).await
    }

    fn cache_id(id: &str, cache: &CacheService) -> String {
        format!("{}:{}:{id}", cache.namespace, Self::field().as_ref())
    }
}
