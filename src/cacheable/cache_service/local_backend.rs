use super::backend::CacheBackend;
use dashmap::DashMap;
use deadpool_redis::redis::Value as RedisValue;
use crate::errors::CacheError;
use std::time::{Duration, Instant};

pub struct LocalBackend {
    pub(crate) store: DashMap<String, (Vec<u8>, Option<Instant>)>,
    zstore: DashMap<String, Vec<(f64, Vec<u8>)>>,
}

impl LocalBackend {
    pub fn new() -> Self {
        Self {
            store: DashMap::new(),
            zstore: DashMap::new(),
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
                    drop(entry);
                    self.store.remove(key);
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

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        let mut count = 0u64;
        for key in keys {
            if self.store.remove(*key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        self.keys_with_limit(pattern, usize::MAX).await
    }

    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        let prefix = pattern.trim_end_matches('*');
        let now = Instant::now();
        let mut keys = Vec::new();

        for entry in self.store.iter() {
            if keys.len() >= limit {
                break;
            }
            let key = entry.key();
            let (_, expires_at) = entry.value();
            if let Some(exp) = expires_at {
                if now > *exp {
                    let key_to_remove = key.clone();
                    drop(entry);
                    self.store.remove(&key_to_remove);
                    continue;
                }
            }
            if key.starts_with(prefix) {
                keys.push(key.clone());
            }
        }
        Ok(keys)
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        let now = Instant::now();
        let expires_at = ttl.map(|d| now + d);

        let entry = self.store.entry(key.to_string());
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let (_, old_expires_at) = occupied.get();
                if let Some(exp) = old_expires_at {
                    if now < *exp {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }

                occupied.insert((value.to_vec(), expires_at));
                Ok(true)
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert((value.to_vec(), expires_at));
                Ok(true)
            }
        }
    }

    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.set_nx(key, value, ttl).await?);
        }
        Ok(results)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        let entry = self.store.entry(key.to_string());
        match entry {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
                let (val, _) = occupied.get();
                let s = String::from_utf8(val.clone()).unwrap_or_default();
                let current = s.parse::<i64>().unwrap_or(0);
                let new_val = current + delta;
                occupied.insert((new_val.to_string().into_bytes(), None));
                Ok(new_val)
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert((delta.to_string().into_bytes(), None));
                Ok(delta)
            }
        }
    }

    async fn ping(&self) -> Result<(), CacheError> {
        Ok(())
    }

    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        let mut entry = self.zstore.entry(key.to_string()).or_insert(Vec::new());
        let vec = entry.value_mut();

        let mut removed = false;
        if let Some(pos) = vec.iter().position(|(_, m)| m == member) {
            vec.remove(pos);
            removed = true;
        }

        vec.push((score, member.to_vec()));
        vec.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(if removed { 0 } else { 1 })
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        if let Some(entry) = self.zstore.get(key) {
            let vec = entry.value();
            let res = vec
                .iter()
                .filter(|(s, _)| *s >= min && *s <= max)
                .map(|(_, m)| m.clone())
                .collect();
            return Ok(res);
        }
        Ok(Vec::new())
    }

    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        if let Some(mut entry) = self.zstore.get_mut(key) {
            let vec = entry.value_mut();
            let len_before = vec.len();
            vec.retain(|(s, _)| *s < min || *s > max);
            return Ok((len_before - vec.len()) as i64);
        }
        Ok(0)
    }

    async fn script_load(&self, script: &str) -> Result<String, CacheError> {
        let _ = script;
        Err(CacheError::Pool(
            "Lua scripts are only supported in distributed Redis mode".to_string(),
        ))
    }

    async fn evalsha(&self, sha: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        let _ = (sha, keys, args);
        Err(CacheError::Pool(
            "Lua scripts are only supported in distributed Redis mode".to_string(),
        ))
    }

    async fn eval_lua(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        let _ = (script, keys, args);
        Err(CacheError::Pool(
            "Lua scripts are only supported in distributed Redis mode".to_string(),
        ))
    }
}
