#![allow(unused)]
use redis::{AsyncCommands};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Redis事件监控API，用于其他程序查询事件数据
pub struct RedisEventMonitor {
    redis_pool: Arc<deadpool_redis::Pool>,
    key_prefix: String,
}

impl RedisEventMonitor {
    pub fn new(redis_pool: Arc<deadpool_redis::Pool>, key_prefix: String) -> Self {
        Self {
            redis_pool,
            key_prefix,
        }
    }

    /// 获取实时系统统计信息
    pub async fn get_system_stats(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;

        // 获取各种计数统计
        let request_success: i64 = conn.get(format!("{}:stats:request_success_count", self.key_prefix)).await.unwrap_or(0);
        let request_fail: i64 = conn.get(format!("{}:stats:request_fail_count", self.key_prefix)).await.unwrap_or(0);
        let task_fail: i64 = conn.get(format!("{}:stats:task_fail_count", self.key_prefix)).await.unwrap_or(0);

        // 获取事件类型计数
        let mut event_counts = json!({});
        let event_types = [
            "task_started", "task_completed", "task_failed",
            "request_generated", "request_sent", "request_completed", "request_failed",
            "download_started", "download_completed", "download_failed",
            "parser_task_received", "parser_task_completed", "parser_task_failed"
        ];

        for event_type in &event_types {
            let count: i64 = conn.get(format!("{}:stats:count:{}", self.key_prefix, event_type)).await.unwrap_or(0);
            event_counts[event_type] = json!(count);
        }

        Ok(json!({
            "summary": {
                "request_success_count": request_success,
                "request_fail_count": request_fail,
                "task_fail_count": task_fail,
                "request_success_rate": if request_success + request_fail > 0 {
                    (request_success as f64) / ((request_success + request_fail) as f64) * 100.0
                } else { 0.0 }
            },
            "event_counts": event_counts,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        }))
    }

    /// 获取任务进度信息
    pub async fn get_task_progress(&self, task_id: &str) -> Result<Option<Value>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        let progress_key = format!("{}:progress:task:{}", self.key_prefix, task_id);

        let progress_data: Option<String> = conn.get(&progress_key).await?;
        match progress_data {
            Some(data) => Ok(Some(serde_json::from_str(&data)?)),
            None => Ok(None)
        }
    }

    /// 获取所有活跃任务的进度
    pub async fn get_all_task_progress(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        let pattern = format!("{}:progress:task:*", self.key_prefix);

        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut *conn)
            .await?;

        let mut tasks = json!({});

        for key in keys {
            if let Ok(Some(data)) = conn.get::<&str, Option<String>>(&key).await {
                if let Ok(task_data) = serde_json::from_str::<Value>(&data) {
                    let task_id = key.split(':').next_back().unwrap_or("unknown");
                    tasks[task_id] = task_data;
                }
            }
        }

        Ok(json!({
            "tasks": tasks,
            "total_count": tasks.as_object().map(|o| o.len()).unwrap_or(0),
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        }))
    }

    /// 获取下载性能数据
    pub async fn get_download_performance(&self, limit: usize) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        let perf_key = format!("{}:stats:download_performance", self.key_prefix);

        let performance_data: Vec<String> = redis::cmd("LRANGE")
            .arg(&perf_key)
            .arg(0)
            .arg((limit as isize) - 1)
            .query_async(&mut *conn)
            .await?;
        
        let mut performances = Vec::new();
        let mut total_duration = 0u64;
        let mut total_size = 0u64;
        
        for data_str in performance_data {
            if let Ok(data) = serde_json::from_str::<Value>(&data_str) {
                if let (Some(duration), Some(size)) = (data["duration_ms"].as_u64(), data["response_size"].as_u64()) {
                    total_duration += duration;
                    total_size += size;
                }
                performances.push(data);
            }
        }

        let count = performances.len() as u64;
        Ok(json!({
            "recent_downloads": performances,
            "statistics": {
                "count": count,
                "avg_duration_ms": if count > 0 { total_duration / count } else { 0 },
                "avg_response_size": if count > 0 { total_size / count } else { 0 },
                "total_duration_ms": total_duration,
                "total_response_size": total_size
            },
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        }))
    }

    /// 获取错误详情
    pub async fn get_error_details(&self, limit: usize) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        
        // 获取最近的错误事件
        let error_pattern = format!("{}:events:*_failed:*", self.key_prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&error_pattern)
            .query_async(&mut *conn)
            .await?;
        
        let mut errors = Vec::new();
        for key in keys.into_iter().take(limit) {
            if let Ok(Some(data)) = conn.get::<&str, Option<String>>(&key).await {
                if let Ok(error_data) = serde_json::from_str::<Value>(&data) {
                    errors.push(error_data);
                }
            }
        }

        // 按时间戳排序
        errors.sort_by(|a, b| {
            let ts_a = a["timestamp"].as_u64().unwrap_or(0);
            let ts_b = b["timestamp"].as_u64().unwrap_or(0);
            ts_b.cmp(&ts_a) // 降序排列
        });

        Ok(json!({
            "recent_errors": errors,
            "count": errors.len(),
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        }))
    }

    /// 获取时间序列数据
    pub async fn get_timeseries_data(&self, event_type: &str, hours: u64) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        let ts_key = format!("{}:timeseries:{}", self.key_prefix, event_type);

        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let start_time = current_time - (hours * 3600);

        let data: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&ts_key)
            .arg(start_time as f64)
            .arg(current_time as f64)
            .query_async(&mut *conn)
            .await?;
        
        let mut timeseries = Vec::new();
        for item in data {
            if let Ok(ts_data) = serde_json::from_str::<Value>(&item) {
                timeseries.push(ts_data);
            }
        }

        Ok(json!({
            "event_type": event_type,
            "time_range_hours": hours,
            "data": timeseries,
            "count": timeseries.len(),
            "timestamp": current_time
        }))
    }

    /// 获取健康状态报告
    pub async fn get_health_report(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        
        // 获取最近的健康检查事件
        let health_pattern = format!("{}:events:component_health_check:*", self.key_prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&health_pattern)
            .query_async(&mut *conn)
            .await?;
        
        let mut health_data = json!({});
        for key in keys {
            if let Ok(Some(data)) = conn.get::<&str, Option<String>>(&key).await {
                if let Ok(health_info) = serde_json::from_str::<Value>(&data) {
                    if let Some(component) = health_info["data"]["component"].as_str() {
                        health_data[component] = health_info["data"].clone();
                    }
                }
            }
        }

        // 计算整体健康状态
        let mut healthy_components = 0;
        let mut total_components = 0;
        
        if let Some(components) = health_data.as_object() {
            total_components = components.len();
            for (_, component_data) in components {
                if component_data["status"].as_str() == Some("healthy") {
                    healthy_components += 1;
                }
            }
        }

        let overall_health = if total_components > 0 {
            if healthy_components == total_components { "healthy" }
            else if healthy_components > 0 { "degraded" }
            else { "unhealthy" }
        } else { "unknown" };

        Ok(json!({
            "overall_status": overall_health,
            "healthy_components": healthy_components,
            "total_components": total_components,
            "components": health_data,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
        }))
    }

    /// 清理过期数据
    pub async fn cleanup_expired_data(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_pool.get().await
            .map_err(|e| format!("Redis connection failed: {e}"))?;
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let cutoff_time = current_time - (24 * 3600); // 24小时前

        let mut cleaned_count = 0u64;

        // 清理时间序列数据
        let ts_pattern = format!("{}:timeseries:*", self.key_prefix);
        let ts_keys: Vec<String> = redis::cmd("KEYS")
            .arg(&ts_pattern)
            .query_async(&mut *conn)
            .await?;
        
        for key in ts_keys {
            let removed: u64 = redis::cmd("ZREMRANGEBYSCORE")
                .arg(&key)
                .arg(0.0)
                .arg(cutoff_time as f64)
                .query_async(&mut *conn)
                .await?;
            cleaned_count += removed;
        }

        Ok(cleaned_count)
    }
}
