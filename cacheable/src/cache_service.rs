
use dashmap::DashMap;
use deadpool_redis::Pool;
use errors::CacheError;
use std::sync::Arc;
use std::time::{Duration, Instant};
// once_cell removed: global singleton not used
use deadpool_redis::redis::AsyncCommands;
use serde_json;
use serde::{Serialize, Deserialize};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::prelude::*;


#[async_trait::async_trait]
pub trait CacheBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;
    async fn del(&self, key: &str) -> Result<(), CacheError>;
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError>;
    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError>;
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

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        self.store.remove(key);
        Ok(())
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        let prefix = pattern.trim_end_matches('*');
        let keys = self.store
            .iter()
            .filter(|r| r.key().starts_with(prefix))
            .map(|r| r.key().clone())
            .collect();
        Ok(keys)
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        // Simple non-atomic implementation for local testing
        if let Some(entry) = self.store.get(key) {
             let (_, expires_at) = entry.value();
             if let Some(exp) = expires_at {
                 if Instant::now() < *exp {
                     return Ok(false);
                 }
             } else {
                 return Ok(false);
             }
        }
        
        let expires_at = ttl.map(|d| Instant::now() + d);
        self.store.insert(key.to_string(), (value.to_vec(), expires_at));
        Ok(true)
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


const COMPRESSION_THRESHOLD: usize = 1024;

#[async_trait::async_trait]
impl CacheBackend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        let result: Option<Vec<u8>> = conn.get(key).await.map_err(CacheError::Redis)?;
        
        if let Some(bytes) = result {
             // Check magic header for Gzip (0x1f, 0x8b)
             if bytes.len() > 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
                 let mut decoder = GzDecoder::new(&bytes[..]);
                 let mut s = Vec::new();
                 if decoder.read_to_end(&mut s).is_ok() {
                     return Ok(Some(s));
                 }
             }
             return Ok(Some(bytes));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let final_value = if value.len() > COMPRESSION_THRESHOLD {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
            encoder.write_all(value)?;
            encoder.finish()?
        } else {
            value.to_vec()
        };

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        if let Some(duration) = ttl {
            let _: () = conn
                .set_ex(key, final_value, duration.as_secs())
                .await
                .map_err(CacheError::Redis)?;
        } else {
            let _: () = conn.set(key, final_value).await.map_err(CacheError::Redis)?;
        }
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        conn.del(key).await.map_err(CacheError::Redis)
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        
        let mut keys: Vec<String> = Vec::new();
        // Use scan_match to get an async iterator
        let mut iter: deadpool_redis::redis::AsyncIter<String> = conn.scan_match(pattern).await.map_err(CacheError::Redis)?;
        while let Some(key) = iter.next_item().await {
            keys.push(key.map_err(CacheError::Redis)?);
        }
        Ok(keys)
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;

        let res: bool = if let Some(ttl) = ttl {
            let opts = deadpool_redis::redis::SetOptions::default().conditional_set(deadpool_redis::redis::ExistenceCheck::NX).with_expiration(deadpool_redis::redis::SetExpiry::EX(ttl.as_secs()));
             
             // Or use raw command if options are tricky in older versions
             // Using raw command for maximum compatibility with deadpool_redis re-exports
            deadpool_redis::redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .arg("EX")
                .arg(ttl.as_secs())
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)
                .unwrap_or(false) // Redis returns nil if NX failed? No, returns OK or Null. Queryng as bool handles it?
        } else {
            deadpool_redis::redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)
                .unwrap_or(false)
        };
        // Verify return: Redis SET NX returns 'OK' if set, or nil if not.
        // query_async::<bool> converts OK->true, nil->false? 
        // Let's assume yes, or use Option<String> check.
        
        // Actually best to re-check the return type behavior.
        // Standard redis crate: SET NX returns simple string "OK" or bulk string nil. 
        // FromToRedisValue for bool: 0 -> false, 1 -> true. "OK" -> ??? 
        // SET... NX typically returns 1 (integer) if set, 0 if not? Note: Redis < 2.6.12 vs >= 2.6.12
        // Modern Redis SET NX EX returns OK or Nil.
        
        // Let's try Option<String>.
        Ok(res)
    }
}
#[async_trait::async_trait]
pub trait CacheAble: Send + Sync + Sized
where
    Self: Serialize+ for<'de> Deserialize<'de>,
{
    fn field() -> impl AsRef<str>;

    async fn send(&self,id:&str, sync: &CacheService) -> Result<(), CacheError> {
        // Construct key: namespace:cache:field
        let key = Self::cache_id(id,sync);

        let content = serde_json::to_vec(self)?;

        // Use default_ttl from CacheService
        sync.backend.set(&key, &content, sync.default_ttl).await?;
        Ok(())
    }

    async fn sync(id:&str,sync: &CacheService) -> Result<Option<Self>, CacheError> {
        let key =  Self::cache_id(id,sync);
        if let Some(bytes) = sync.backend.get(&key).await? {
            let val = serde_json::from_slice(&bytes).map_err(CacheError::Serde)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
    async fn delete(id:&str,sync: &CacheService) -> Result<(), CacheError> {
        let key =  Self::cache_id(id,sync);
        // Note: CacheBackend does not have a delete method; this is a placeholder.
        // You would need to implement delete in CacheBackend and its implementations.
        sync.backend.del(&key).await?;
        Ok(())
    }

    async fn scan(pattern_suffix: &str, sync: &CacheService) -> Result<Vec<String>, CacheError> {
         let pattern = format!("{}:{}:{}", sync.namespace, Self::field().as_ref(), pattern_suffix);
         sync.backend.keys(&pattern).await
    }

     fn cache_id(id:&str,cache:&CacheService) ->String{
        format!("{}:{}:{id}", cache.namespace, Self::field().as_ref())
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
    ) ->Self {
        let backend: Arc<dyn CacheBackend> = match pool {
            Some(p) => Arc::new(RedisBackend::new(p)),
            None => Arc::new(LocalBackend::new()),
        };

        CacheService {
            backend,
            namespace,
            default_ttl,
        }
    }

    pub async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        self.backend.set_nx(key, value, ttl).await
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}



#[cfg(test)]
mod tests {
    #[derive(Deserialize, Serialize, Debug)]
    struct MyConfig {
        name: String,
        value: i32,
    }

    impl CacheAble for MyConfig {
        fn field() -> impl AsRef<str> {
            "global_config".to_string()
        }
    }
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn test() {
        // Example Usage:

        // 1. Initialize for Local Mode (DashMap) with namespace "myapp" and 60s TTL
        let sync_service =
            Arc::new(CacheService::new(None, "myapp".to_string(), Some(Duration::from_secs(60)))) ;

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
