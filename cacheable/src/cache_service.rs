use bytecheck::CheckBytes;
use dashmap::DashMap;
use deadpool_redis::Pool;
use errors::CacheError;
use std::sync::Arc;
use std::time::{Duration, Instant};
// once_cell removed: global singleton not used
use deadpool_redis::redis::AsyncCommands;
use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::rancor::Error as RancorError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

// Define simplified aliases for the rkyv 0.8 High API
pub type DefaultSerializer<'a> = HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>;
pub type DefaultDeserializer = HighDeserializer<RancorError>;
pub type DefaultValidator<'a> = HighValidator<'a, RancorError>;

#[async_trait::async_trait]
pub trait CacheBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;
}

struct LocalBackend {
    store: DashMap<String, (Vec<u8>, Option<Instant>)>,
}

impl LocalBackend {
    fn new() -> Self {
        Self {
            store: DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl CacheBackend for LocalBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        if let Some(entry) = self.store.get(key) {
            let (val, expires_at) = entry.value();
            if let Some(exp) = expires_at {
                if Instant::now() > *exp {
                    let captured_exp = *exp;
                    drop(entry);
                    // Critical Fix: Use remove_if to prevent race condition where a new value
                    // is set by another thread between drop(entry) and remove(key).
                    // We only remove if the expiration timestamp matches what we saw.
                    self.store.remove_if(key, |_, (_, current_exp)| {
                        *current_exp == Some(captured_exp)
                    });
                    return Ok(None);
                }
            }
            return Ok(Some(val.clone()));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.store
            .insert(key.to_string(), (value.to_vec(), expires_at));
        Ok(())
    }
}

pub struct RedisBackend {
    pool: Pool,
}

impl RedisBackend {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl CacheBackend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        let result: Option<Vec<u8>> = conn.get(key).await.map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        if let Some(duration) = ttl {
            let _: () = conn
                .set_ex(key, value, duration.as_secs())
                .await
                .map_err(CacheError::Redis)?;
        } else {
            let _: () = conn.set(key, value).await.map_err(CacheError::Redis)?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
pub trait CacheAble: Send + Sync + Sized + 'static + Archive
where
    Self: for<'a> Serialize<DefaultSerializer<'a>>,
    Self::Archived:
        for<'a> CheckBytes<DefaultValidator<'a>> + Deserialize<Self, DefaultDeserializer>,
{
    fn field() -> impl AsRef<str>;

    async fn send(&self,id:&str, sync: &CacheService) -> Result<(), CacheError> {
        // Construct key: namespace:cache:field
        let key = format!("{}:cache:{}:{id}", sync.namespace, Self::field().as_ref());

        let bytes = rkyv::to_bytes::<RancorError>(self).map_err(CacheError::Rkyv)?;

        // Use default_ttl from CacheService
        sync.backend.set(&key, &bytes, sync.default_ttl).await?;
        println!("Sent {}: {} bytes", key, bytes.len());
        Ok(())
    }

    async fn sync(id:&str,sync: &CacheService) -> Result<Option<Self>, CacheError> {
        let key = format!("{}:cache:{}:{id}", sync.namespace, Self::field().as_ref());

        if let Some(bytes) = sync.backend.get(&key).await? {
            // Use rkyv from_bytes with explicit error type
            let val = rkyv::from_bytes::<Self, RancorError>(&bytes).map_err(CacheError::Rkyv)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
}

pub struct CacheService {
    backend: Arc<dyn CacheBackend>,
    namespace: String,
    default_ttl: Option<Duration>,
}

impl CacheService {
    pub fn new(
        pool: Option<Pool>,
        namespace: String,
        default_ttl: Option<Duration>,
    ) -> Result<Arc<Self>, CacheError> {
        let backend: Arc<dyn CacheBackend> = match pool {
            Some(p) => Arc::new(RedisBackend::new(p)),
            None => Arc::new(LocalBackend::new()),
        };

        let service = Arc::new(CacheService {
            backend,
            namespace,
            default_ttl,
        });
        Ok(service)
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, CheckBytes)]
// #[check_bytes(crate = rkyv::bytecheck)]
struct MyConfig {
    name: String,
    value: i32,
}

impl CacheAble for MyConfig {
    fn field() -> String {
        "global_config".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn test() {
        // Example Usage:

        // 1. Initialize for Local Mode (DashMap) with namespace "myapp" and 60s TTL
        let sync_service =
            match CacheService::new(None, "myapp".to_string(), Some(Duration::from_secs(60))) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Failed to initialize CacheService: {}", e);
                    return;
                }
            };

        let config = MyConfig {
            name: "test".to_string(),
            value: 123,
        };

        // 2. Developer calls send()
        if let Err(e) = config.send(&"test",sync_service.as_ref()).await {
            eprintln!("Failed to send: {}", e);
        }

        // 3. Developer calls sync()
        match MyConfig::sync(&"test",sync_service.as_ref()).await {
            Ok(Some(fetched)) => println!("Synced: {:?}", fetched),
            Ok(None) => println!("No data found"),
            Err(e) => eprintln!("Error syncing: {}", e),
        }

        println!("StateSync implemented.");
    }
}
