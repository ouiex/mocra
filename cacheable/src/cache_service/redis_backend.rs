use super::backend::CacheBackend;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::redis::Value as RedisValue;
use deadpool_redis::Pool;
use errors::CacheError;
use std::time::Duration;

pub struct RedisBackend {
    pool: Pool,
    compression_threshold: usize,
}

fn is_gzip(bytes: &[u8]) -> bool {
    bytes.len() > 2 && bytes[0] == 0x1f && bytes[1] == 0x8b
}

fn is_zstd(bytes: &[u8]) -> bool {
    bytes.len() > 4 && bytes[0] == 0x28 && bytes[1] == 0xb5 && bytes[2] == 0x2f && bytes[3] == 0xfd
}

fn decode_compressed(bytes: Vec<u8>) -> Result<Vec<u8>, CacheError> {
    if is_gzip(&bytes) {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded)?;
        return Ok(decoded);
    }
    if is_zstd(&bytes) {
        let decoded = zstd::stream::decode_all(std::io::Cursor::new(bytes.as_slice()))?;
        return Ok(decoded);
    }
    Ok(bytes)
}

fn decode_compressed_best_effort(bytes: Vec<u8>) -> Vec<u8> {
    if is_gzip(&bytes) {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(bytes.as_slice());
        let mut decoded = Vec::new();
        if decoder.read_to_end(&mut decoded).is_ok() {
            return decoded;
        }
        return bytes;
    }
    if is_zstd(&bytes) {
        if let Ok(decoded) = zstd::stream::decode_all(std::io::Cursor::new(bytes.as_slice())) {
            return decoded;
        }
        return bytes;
    }
    bytes
}

impl RedisBackend {
    pub fn new(pool: Pool, compression_threshold: usize) -> Self {
        Self {
            pool,
            compression_threshold,
        }
    }
}

#[async_trait::async_trait]
impl CacheBackend for RedisBackend {
    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let result: i64 = deadpool_redis::redis::cmd("ZADD")
            .arg(key)
            .arg(score)
            .arg(member)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let min_str = if min == f64::NEG_INFINITY { "-inf".to_string() } else { min.to_string() };
        let max_str = if max == f64::INFINITY { "+inf".to_string() } else { max.to_string() };

        let result: Vec<Vec<u8>> = deadpool_redis::redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min_str)
            .arg(max_str)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let min_str = if min == f64::NEG_INFINITY { "-inf".to_string() } else { min.to_string() };
        let max_str = if max == f64::INFINITY { "+inf".to_string() } else { max.to_string() };

        let result: i64 = deadpool_redis::redis::cmd("ZREMRANGEBYSCORE")
            .arg(key)
            .arg(min_str)
            .arg(max_str)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(result)
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let result: Option<Vec<u8>> = conn.get(key).await.map_err(CacheError::Redis)?;

        if let Some(bytes) = result {
            if is_gzip(&bytes) || is_zstd(&bytes) {
                let decoded = tokio::task::spawn_blocking(move || decode_compressed(bytes))
                    .await
                    .map_err(|e| CacheError::Pool(e.to_string()))??;
                return Ok(Some(decoded));
            }
            return Ok(Some(bytes));
        }
        Ok(None)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
        let threshold = self.compression_threshold;

        let final_value = if value.len() > threshold {
            let val = value.to_vec();
            tokio::task::spawn_blocking(move || {
                zstd::stream::encode_all(std::io::Cursor::new(val), 3)
                    .map_err(|e| CacheError::Pool(e.to_string()))
            })
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))??
        } else {
            value.to_vec()
        };

        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
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
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        conn.del(key).await.map_err(CacheError::Redis)
    }

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;

        let mut pipe = deadpool_redis::redis::pipe();
        for key in keys {
            pipe.del(*key);
        }

        let results: Vec<i64> = pipe.query_async(&mut conn).await.map_err(CacheError::Redis)?;
        Ok(results.iter().sum::<i64>() as u64)
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        self.keys_with_limit(pattern, usize::MAX).await
    }

    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;

        let mut keys: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let scan_count = std::cmp::min(limit, 1000);

        loop {
            let (new_cursor, batch): (u64, Vec<String>) = deadpool_redis::redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(scan_count)
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)?;

            for key in batch {
                if keys.len() >= limit {
                    return Ok(keys);
                }
                keys.push(key);
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }
        Ok(keys)
    }

    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;

        let result: Option<String> = if let Some(ttl) = ttl {
            deadpool_redis::redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .arg("EX")
                .arg(ttl.as_secs())
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)?
        } else {
            deadpool_redis::redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("NX")
                .query_async(&mut conn)
                .await
                .map_err(CacheError::Redis)?
        };
        Ok(result.is_some())
    }

    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;

        let mut pipe = deadpool_redis::redis::pipe();

        for key in keys {
            let mut cmd = deadpool_redis::redis::cmd("SET");
            cmd.arg(key).arg(value).arg("NX");

            if let Some(d) = ttl {
                cmd.arg("EX").arg(d.as_secs());
            }
            pipe.add_command(cmd);
        }

        let results: Vec<Option<String>> = pipe.query_async(&mut conn).await.map_err(CacheError::Redis)?;

        Ok(results.into_iter().map(|r| r.is_some()).collect())
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;

        let values: Vec<Option<Vec<u8>>> = deadpool_redis::redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;

        let has_compressed = values
            .iter()
            .any(|val| val.as_ref().is_some_and(|bytes| is_gzip(bytes) || is_zstd(bytes)));
        if !has_compressed {
            return Ok(values);
        }

        let decoded_values = tokio::task::spawn_blocking(move || {
            values
                .into_iter()
                .map(|val| val.map(decode_compressed_best_effort))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| CacheError::Pool(e.to_string()))?;

        Ok(decoded_values)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;

        deadpool_redis::redis::cmd("INCRBY")
            .arg(key)
            .arg(delta)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)
    }

    async fn ping(&self) -> Result<(), CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let _: String = deadpool_redis::redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(())
    }

    async fn script_load(&self, script: &str) -> Result<String, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let sha: String = deadpool_redis::redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(script)
            .query_async(&mut conn)
            .await
            .map_err(CacheError::Redis)?;
        Ok(sha)
    }

    async fn evalsha(&self, sha: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let mut cmd = deadpool_redis::redis::cmd("EVALSHA");
        cmd.arg(sha).arg(keys.len());
        for key in keys {
            cmd.arg(*key);
        }
        for arg in args {
            cmd.arg(*arg);
        }
        let value: RedisValue = cmd.query_async(&mut conn).await.map_err(CacheError::Redis)?;
        Ok(value)
    }

    async fn eval_lua(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        let mut conn = self.pool.get().await.map_err(|e| CacheError::Pool(e.to_string()))?;
        let mut cmd = deadpool_redis::redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for key in keys {
            cmd.arg(*key);
        }
        for arg in args {
            cmd.arg(*arg);
        }
        let value: RedisValue = cmd.query_async(&mut conn).await.map_err(CacheError::Redis)?;
        Ok(value)
    }
}
