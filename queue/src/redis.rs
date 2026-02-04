use crate::{AckAction, Message, MqBackend};
use async_trait::async_trait;
use common::model::config::RedisConfig;
use errors::Result;
use errors::error::QueueError;
use log::{error, info, warn, debug};
use deadpool_redis::redis::{AsyncCommands, FromRedisValue};
use deadpool_redis::redis;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use metrics::{counter, gauge};
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::sync::Arc;

struct StreamRouter {
    routes: HashMap<String, mpsc::Sender<Message>>,
}

pub struct RedisQueue {
    pool: deadpool_redis::Pool,
    group_id: String,
    consumer_name: String,
    minid_time: u64,
    namespace: String,
    shards: usize,
    batch_size: usize,
    claim_min_idle: u64,
    claim_count: usize,
    claim_interval: u64,
    listener_count: usize,
    router: Arc<RwLock<StreamRouter>>,
    ack_tx: mpsc::Sender<(String, AckAction)>,
}

impl RedisQueue {
    pub fn new(redis_config: &RedisConfig, minid_time: u64, namespace: &str, batch_size: usize) -> Result<Self> {
        let pool = utils::connector::create_redis_pool(
            &redis_config.redis_host,
            redis_config.redis_port,
            redis_config.redis_db,
            &redis_config.redis_username,
            &redis_config.redis_password,
            redis_config.pool_size,
            redis_config.tls.unwrap_or(false),
        )
        .ok_or(QueueError::ConnectionFailed)?;

        let consumer_name = uuid::Uuid::new_v4().to_string();
        // Use a default group ID or maybe pass it in?
        // The previous implementation didn't use group ID for lists, but streams need it.
        // I'll use a default one or maybe "crawler_group".
        let group_id = format!("{}:crawler_group", namespace);
        let shards = redis_config.shards.unwrap_or(8);
        let claim_min_idle = redis_config.claim_min_idle.unwrap_or(60000); // Default 60s
        let claim_count = redis_config.claim_count.unwrap_or(100); // Default 100
        let claim_interval = redis_config.claim_interval.unwrap_or(60000); // Default 60s
        let listener_count = redis_config.listener_count.unwrap_or(8);
        
        // Create shared ACK channel (Buffer size 10000 to handle high throughput)
        let (ack_tx, ack_rx) = mpsc::channel::<(String, AckAction)>(10000);
        let router = Arc::new(RwLock::new(StreamRouter { routes: HashMap::new() }));

        let queue = Self {
            pool: pool.clone(),
            group_id: group_id.clone(),
            consumer_name: consumer_name.clone(),
            minid_time,
            namespace: namespace.to_string(),
            shards,
            batch_size,
            claim_min_idle,
            claim_count,
            claim_interval,
            listener_count,
            ack_tx,
            router,
        };
        
        // Spawn shared components
        queue.spawn_ack_processor(pool.clone(), group_id.clone(), ack_rx);
        queue.spawn_shared_claimer();
        queue.spawn_shared_lag_monitor();
        queue.spawn_sharded_listeners();
        
        Ok(queue)
    }

    fn extract_shard_id(key: &str) -> Option<usize> {
        let key = if key.starts_with('{') && key.ends_with('}') {
            &key[1..key.len()-1]
        } else {
            key
        };
        if let Some(last_colon) = key.rfind(':') {
            key[last_colon + 1..].parse::<usize>().ok()
        } else {
            None
        }
    }

    fn spawn_sharded_listeners(&self) {
        let listener_count = self.listener_count;

        for i in 0..listener_count {
            let pool = self.pool.clone();
            let group_id = self.group_id.clone();
            // Use distinct consumer name for observability, though they handle disjoint key sets
            let consumer_name = format!("{}-{}", self.consumer_name, i); 
            let router = self.router.clone();
            let batch_size = self.batch_size;
            let ack_tx = self.ack_tx.clone();
            let listener_index = i;

            tokio::spawn(async move {
                info!("Starting Sharded Redis Listener {}/{} (Group: {}, Consumer: {})", listener_index + 1, listener_count, group_id, consumer_name);
                let mut conn: Option<deadpool_redis::Connection> = None;

                loop {
                    // 1. Snapshot and Filter Routes
                    let routes = {
                        let r = router.read().await;
                        r.routes.clone()
                    };

                    let keys: Vec<String> = routes.keys()
                        .filter(|k| {
                            // Try to extract shard ID from the key (format: ...:shard_id)
                            if let Some(shard_id) = Self::extract_shard_id(k) {
                                return shard_id % listener_count == listener_index;
                            }

                            // Fallback to hashing for non-sharded keys or parsing failures
                            let mut hasher = crc32fast::Hasher::new();
                            hasher.update(k.as_bytes());
                            (hasher.finalize() as usize) % listener_count == listener_index
                        })
                        .cloned()
                        .collect();

                    if keys.is_empty() {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    let ids: Vec<&str> = vec![">"; keys.len()];
                    
                    // 2. Get Connection
                    if conn.is_none() {
                        match pool.get().await {
                            Ok(c) => conn = Some(c),
                            Err(e) => {
                                error!("Listener {} failed to get connection: {}. Retrying...", listener_index, e);
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                    }
                    let active_conn = match conn.as_mut() {
                        Some(c) => c,
                        None => {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    // 3. XREADGROUP
                    let opts = redis::streams::StreamReadOptions::default()
                        .group(&group_id, &consumer_name)
                        .block(2000)
                        .count(batch_size);
                    
                    let result: redis::RedisResult<redis::streams::StreamReadReply> = 
                        active_conn.xread_options(&keys, &ids, &opts).await;

                    match result {
                        Ok(reply) => {
                            for stream_key in reply.keys {
                                let s_key = stream_key.key;
                                if let Some(sender) = routes.get(&s_key) {
                                    for stream_id in stream_key.ids {
                                        let id = stream_id.id.clone();
                                        if let Some(val) = stream_id.map.get("payload")
                                            && let Ok(data) = Vec::<u8>::from_redis_value(val) {
                                                
                                                let mut headers = HashMap::new();
                                                for (k, v) in &stream_id.map {
                                                    if k.starts_with("h:")
                                                        && let Ok(s) = String::from_redis_value(v) {
                                                            headers.insert(k[2..].to_string(), s);
                                                        }
                                                }

                                                let msg = Message {
                                                    payload: std::sync::Arc::new(data),
                                                    id: format!("{}@{}", s_key, id),
                                                    headers: std::sync::Arc::new(headers),
                                                    ack_tx: ack_tx.clone(),
                                                };
                                                
                                                counter!("queue_consume_total", "topic" => s_key.clone(), "listener" => listener_index.to_string()).increment(1);

                                                if sender.send(msg).await.is_err() {
                                                    warn!("Subscriber for {} dropped", s_key);
                                                }
                                            }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                             if let Some(code) = e.code() && code == "NOGROUP" {
                                 // NOGROUP usually handled by subscribe ensuring group creation.
                             }
                             error!("Error in listener {}: {}. Retrying...", listener_index, e);
                             conn = None;
                             tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            });
        }
    }

    fn spawn_ack_processor(
        &self,
        pool: deadpool_redis::Pool,
        group_id: String,
        mut ack_rx: mpsc::Receiver<(String, AckAction)>,
    ) {
        tokio::spawn(async move {
            let mut batches: HashMap<String, Vec<String>> = HashMap::new();
            let mut pending_count: usize = 0;
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    Some((id, action)) = ack_rx.recv() => {
                        let (stream_key, real_id) = match id.split_once('@') {
                            Some((k, i)) => (k.to_string(), i.to_string()),
                            None => {
                                error!("Invalid ID format in ACK: {}", id);
                                continue;
                            }
                        };

                        match action {
                            AckAction::Ack => {
                                batches.entry(stream_key).or_default().push(real_id);
                                pending_count += 1;
                            }
                            AckAction::Nack(reason, payload) => {
                                if let Ok(mut ack_conn) = pool.get().await {
                                     let dlq_key = format!("{}:dlq", stream_key);
                                     let script = redis::Script::new(r"
                                         redis.call('XADD', KEYS[3], '*', 'payload', ARGV[1], 'reason', ARGV[2], 'original_id', ARGV[3])
                                         return redis.call('XACK', KEYS[1], KEYS[2], ARGV[3])
                                     ");
                                     
                                     let _: () = script
                                         .key(&stream_key)
                                         .key(&group_id)
                                         .key(&dlq_key)
                                         .arg(payload.as_slice())
                                         .arg(&reason)
                                         .arg(&real_id)
                                         .invoke_async(&mut ack_conn)
                                         .await
                                         .map_err(|e| error!("Failed to atomic NACK: {}", e))
                                         .unwrap_or(());
                                         
                                      counter!("queue_dlq_total", "topic" => stream_key.clone()).increment(1);
                                 }
                            }
                        }

                        if pending_count >= 50 {
                             if Self::flush_acks(&pool, &group_id, &mut batches).await {
                                 pending_count = 0;
                             }
                        }
                    }
                    _ = interval.tick() => {
                        if !batches.is_empty() {
                             if Self::flush_acks(&pool, &group_id, &mut batches).await {
                                 pending_count = 0;
                             }
                        }
                    }
                    else => break,
                }
            }
        });
    }

    async fn flush_acks(pool: &deadpool_redis::Pool, group_id: &str, batches: &mut HashMap<String, Vec<String>>) -> bool {
        if batches.is_empty() { return true; }
        
        match pool.get().await {
            Ok(mut ack_conn) => {
                let mut pipeline = redis::pipe();
                let mut count_map = HashMap::new();
                
                for (s_key, ids) in batches.iter() {
                    if ids.is_empty() { continue; }
                    pipeline.xack(s_key, group_id, ids).ignore();
                    count_map.insert(s_key.clone(), ids.len());
                }

                let result: redis::RedisResult<()> = pipeline.query_async(&mut ack_conn).await;
                
                match result {
                    Ok(_) => {
                        for (k, v) in count_map {
                            counter!("queue_ack_total", "topic" => k).increment(v as u64);
                        }
                        batches.clear();
                        true
                    }
                    Err(e) => {
                        error!("Failed to flush ACKs: {}", e);
                        // Do not clear batches, retry next time
                        false
                    }
                }
            }
            Err(e) => {
                error!("Failed to get Redis connection for ACK flush: {}. Retrying next tick.", e);
                // Do NOT clear batches, retry next time
                false
            }
        }
    }

    fn spawn_shared_lag_monitor(&self) {
        let pool = self.pool.clone();
        let router = self.router.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;

                let topic_keys: Vec<String> = {
                    let r = router.read().await;
                    r.routes.keys().cloned().collect()
                };

                if topic_keys.is_empty() {
                    continue;
                }

                if let Ok(mut conn) = pool.get().await {
                    let mut totals: HashMap<String, u64> = HashMap::new();
                    for t_key in &topic_keys {
                        let len: u64 = redis::cmd("XLEN")
                            .arg(t_key)
                            .query_async(&mut conn)
                            .await
                            .unwrap_or(0);
                        let label = Self::topic_label_from_key(t_key);
                        *totals.entry(label).or_insert(0) += len;
                    }

                    for (topic_label, total_len) in totals {
                        gauge!("queue_len", "topic" => topic_label.clone()).set(total_len as f64);
                        if total_len > 1000 {
                            warn!("High queue depth for {}: {}", topic_label, total_len);
                        } else if total_len > 0 {
                            debug!("Queue depth for {}: {}", topic_label, total_len);
                        }
                    }
                }
            }
        });
    }

    fn spawn_shared_claimer(&self) {
        let pool = self.pool.clone();
        let router = self.router.clone();
        let group_id = self.group_id.clone();
        let consumer_name = format!("{}-claimer", self.consumer_name);
        let ack_tx = self.ack_tx.clone();
        let claim_min_idle = self.claim_min_idle;
        let claim_count = self.claim_count;
        let claim_interval = self.claim_interval;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(claim_interval));

            loop {
                interval.tick().await;

                let routes: Vec<(String, mpsc::Sender<Message>)> = {
                    let r = router.read().await;
                    r.routes
                        .iter()
                        .map(|(k, s)| (k.clone(), s.clone()))
                        .collect()
                };

                if routes.is_empty() {
                    continue;
                }

                let mut conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("Claimer connection failed: {}", e);
                        continue;
                    }
                };

                for (topic_claim, sender) in routes {
                    // Loop to drain all stuck messages if there are more than claim_count
                    loop {
                        let result: redis::RedisResult<(String, Vec<(String, Vec<Vec<u8>>)>, Vec<String>)> = redis::cmd("XAUTOCLAIM")
                            .arg(topic_claim.as_str())
                            .arg(&group_id)
                            .arg(&consumer_name)
                            .arg(claim_min_idle)
                            .arg("0-0")
                            .arg("COUNT")
                            .arg(claim_count)
                            .query_async(&mut conn)
                            .await;

                        match result {
                            Ok((cursor, messages, _deleted)) => {
                                let is_empty = messages.is_empty();
                                if !is_empty {
                                    info!("Claimed {} stuck messages from topic {}", messages.len(), topic_claim);
                                    counter!("queue_claim_total", "topic" => topic_claim.clone()).increment(messages.len() as u64);
                                    for (id, fields) in messages {
                                        if let Some(msg) = Self::parse_message(&topic_claim, id, fields, ack_tx.clone()) {
                                            if let Err(e) = sender.send(msg).await {
                                                warn!("Failed to send claimed message: {}", e);
                                            }
                                        }
                                    }
                                }

                                if cursor == "0-0" || is_empty {
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("XAUTOCLAIM check failed for {}: {}", topic_claim, e);
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn topic_label_from_key(key: &str) -> String {
        let trimmed = if key.starts_with('{') && key.ends_with('}') {
            &key[1..key.len() - 1]
        } else {
            key
        };
        let parts: Vec<&str> = trimmed.split(':').collect();
        if parts.len() >= 2 {
            parts[1].to_string()
        } else {
            trimmed.to_string()
        }
    }



    fn get_topic_key(&self, topic: &str, key: Option<&str>) -> String {
        if self.shards > 1 {
            let shard = if let Some(k) = key {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(k.as_bytes());
                (hasher.finalize() as usize) % self.shards
            } else {
                (rand::random::<u64>() as usize) % self.shards
            };
            format!("{{{}:{}:{}}}", self.namespace, topic, shard)
        } else {
            format!("{{{}:{}}}", self.namespace, topic)
        }
    }

    fn get_minid_threshold(&self) -> Option<String> {
        if self.minid_time > 0 {
            let retention_ms = (self.minid_time as u128) * 60 * 60 * 1000;
            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
            let threshold = now_ms.saturating_sub(retention_ms);
            Some(threshold.to_string())
        } else {
            None
        }
    }

    async fn get_connection(&self) -> Result<deadpool_redis::Connection> {
        self.pool.get().await.map_err(|_| QueueError::ConnectionFailed.into())
    }

    fn parse_message(topic_claim: &str, id: String, fields: Vec<Vec<u8>>, ack_tx: mpsc::Sender<(String, AckAction)>) -> Option<Message> {
        let mut found_payload = None;
        let mut headers = HashMap::new();
        
        let mut iter = fields.into_iter();
        while let Some(key) = iter.next() {
            if let Some(val) = iter.next() {
                if key == b"payload" {
                    found_payload = Some(val);
                } else if key.starts_with(b"h:") {
                    // Optimized header parsing to avoid intermediate allocations
                    // "h:key" -> key
                    if key.len() > 2 {
                        let mut k_vec = key;
                        k_vec.drain(0..2);
                        if let Ok(k_str) = String::from_utf8(k_vec) {
                            if let Ok(v_str) = String::from_utf8(val) {
                                headers.insert(k_str, v_str);
                            }
                        }
                    }
                }
            }
        }
        
        if let Some(payload) = found_payload {
            Some(Message {
                payload: std::sync::Arc::new(payload),
                id: format!("{}@{}", topic_claim, id),
                headers: std::sync::Arc::new(headers),
                ack_tx,
            })
        } else {
            None
        }
    }
}

#[async_trait]
impl MqBackend for RedisQueue {
    async fn publish_with_headers(&self, topic: &str, key: Option<&str>, payload: &[u8], headers: &HashMap<String, String>) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let topic_key = self.get_topic_key(topic, key);

        let mut cmd = redis::cmd("XADD");
        cmd.arg(topic_key);

        if let Some(threshold) = self.get_minid_threshold() {
            cmd.arg("MINID").arg("~").arg(threshold);
        }

        cmd.arg("*").arg("payload").arg(payload);
        
        for (k, v) in headers {
             cmd.arg(format!("h:{}", k)).arg(v);
        }

        let _: String = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| QueueError::PushFailed(Box::new(e)))?;
        
        counter!("queue_publish_total", "topic" => topic.to_string(), "status" => "success").increment(1);
        Ok(())
    }

    async fn publish_batch(&self, topic: &str, items: &[(Option<String>, Vec<u8>)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get_connection().await?;
        
        let mut batches: HashMap<String, Vec<&Vec<u8>>> = HashMap::new();
        for (key, payload) in items {
            let topic_key = self.get_topic_key(topic, key.as_deref());
            batches.entry(topic_key).or_default().push(payload);
        }

        let minid_threshold = self.get_minid_threshold().unwrap_or_default();

        let script = redis::Script::new(r"
            local key = KEYS[1]
            local minid = KEYS[2]
            for i, payload in ipairs(ARGV) do
                if minid ~= '' then
                    redis.call('XADD', key, 'MINID', '~', minid, '*', 'payload', payload)
                else
                    redis.call('XADD', key, '*', 'payload', payload)
                end
            end
        ");

        for attempt in 0..2 {
            let mut pipe = redis::pipe();
            for (topic_key, payloads) in &batches {
                 let mut invocation = script.key(topic_key);
                 invocation.key(&minid_threshold);
                 for p in payloads {
                     invocation.arg(p.as_slice());
                 }
                 pipe.invoke_script(&invocation);
            }
            
            let result: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
            
            match result {
                Ok(_) => {
                    counter!("queue_publish_batch_total", "topic" => topic.to_string(), "status" => "success").increment(items.len() as u64);
                    return Ok(());
                },
                Err(e) => {
                    let is_noscript = e.kind() == redis::ErrorKind::NoScriptError || e.to_string().contains("NOSCRIPT");
                    if is_noscript && attempt == 0 {
                        let _ = script.key("").key("").arg("").invoke_async::<()>(&mut conn).await;
                        continue;
                    }
                    return Err(QueueError::PushFailed(Box::new(e)).into());
                }
            }
        }
        Ok(())
    }
    
    async fn publish_batch_with_headers(&self, topic: &str, items: &[(Option<String>, Vec<u8>, HashMap<String, String>)]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.get_connection().await?;
        
        let mut batches: HashMap<String, Vec<(&Vec<u8>, &HashMap<String, String>)>> = HashMap::new();
        for (key, payload, headers) in items {
            let topic_key = self.get_topic_key(topic, key.as_deref());
            batches.entry(topic_key).or_default().push((payload, headers));
        }

        let minid_threshold = self.get_minid_threshold().unwrap_or_default();

        let script = redis::Script::new(r"
            local key = KEYS[1]
            local minid = KEYS[2]
            for i = 1, #ARGV, 2 do
                local payload = ARGV[i]
                local headers_str = ARGV[i+1]
                local args = {'XADD', key}
                
                if minid ~= '' then
                    table.insert(args, 'MINID')
                    table.insert(args, '~')
                    table.insert(args, minid)
                end
                
                table.insert(args, '*')
                table.insert(args, 'payload')
                table.insert(args, payload)
                
                if headers_str ~= '' then
                    local headers = cjson.decode(headers_str)
                    for k, v in pairs(headers) do
                        table.insert(args, 'h:' .. k)
                        table.insert(args, v)
                    end
                end
                
                redis.call(unpack(args))
            end
        ");

        for attempt in 0..2 {
            let mut pipe = redis::pipe();
            for (topic_key, item_list) in &batches {
                 let mut invocation = script.key(topic_key);
                 invocation.key(&minid_threshold);
                 for (p, h) in item_list {
                     invocation.arg(p.as_slice());
                     if h.is_empty() {
                         invocation.arg("");
                     } else {
                         let h_json = serde_json::to_string(h).unwrap_or_default();
                         invocation.arg(h_json);
                     }
                 }
                 pipe.invoke_script(&invocation);
            }
            
            let result: redis::RedisResult<()> = pipe.query_async(&mut conn).await;
            
            match result {
                Ok(_) => {
                    counter!("queue_publish_batch_total", "topic" => topic.to_string(), "status" => "success").increment(items.len() as u64);
                    return Ok(());
                },
                Err(e) => {
                    let is_noscript = e.kind() == redis::ErrorKind::NoScriptError || e.to_string().contains("NOSCRIPT");
                    if is_noscript && attempt == 0 {
                         let _ = script.key("").key("").arg("").arg("").invoke_async::<()>(&mut conn).await;
                         continue;
                    }
                    return Err(QueueError::PushFailed(Box::new(e)).into());
                }
            }
        }
        Ok(())
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()> {
        let topic = topic.to_string();
        
        let topic_keys: Vec<String> = if self.shards > 1 {
            (0..self.shards).map(|i| format!("{{{}:{}:{}}}", self.namespace, topic, i)).collect()
        } else {
            vec![format!("{{{}:{}}}", self.namespace, topic)]
        };

        let group_id = self.group_id.clone();
        // let consumer_name = self.consumer_name.clone(); // Not used here anymore

        // Ensure Groups Exist & Register Routes
        {
             let mut conn = self.pool.get().await.map_err(|_e| QueueError::ConnectionFailed)?;
             let mut router = self.router.write().await;
             
             for t_key in &topic_keys {
                 // Register route
                 router.routes.insert(t_key.clone(), sender.clone());
                 
                 // Ensure group exists
                 match conn
                    .xgroup_create_mkstream::<&str, &str, &str, ()>(t_key, &group_id, "$")
                    .await
                {
                    Ok(_) => info!("Created consumer group {} for topic {}", group_id, t_key),
                    Err(e) => {
                        if e.code() == Some("BUSYGROUP") {
                            // Group already exists, which is fine.
                        } else {
                            error!("Failed to create consumer group for {}: {}", t_key, e);
                        }
                    }
                }
             }
        }

        Ok(())
    }



    async fn clean_storage(&self) -> Result<()> {
        if self.minid_time == 0 {
            return Ok(());
        }

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| QueueError::ConnectionFailed)?;
        let retention_period = self.minid_time as u128 * 60 * 60 * 1000;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();
        let min_id = now.saturating_sub(retention_period);

        info!(
            "Starting Redis storage cleanup for namespace {}, min_id: {}",
            self.namespace, min_id
        );

        let pattern = format!("{}:*", self.namespace);
        let mut keys: Vec<String> = Vec::new();

        {
            let mut cursor = 0;
            loop {
                let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                    .cursor_arg(cursor)
                    .arg("MATCH")
                    .arg(&pattern)
                    .arg("COUNT")
                    .arg(100)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| QueueError::OperationFailed(Box::new(e)))?;

                keys.extend(batch);
                cursor = next_cursor;

                if cursor == 0 {
                    break;
                }
            }
        }

        // Batch check types
        if keys.is_empty() {
            return Ok(());
        }

        let mut pipe = redis::pipe();
        for key in &keys {
            pipe.cmd("TYPE").arg(key);
        }
        let types: Vec<String> = pipe.query_async(&mut conn).await.map_err(|e| QueueError::OperationFailed(Box::new(e)))?;

        let mut trim_pipe = redis::pipe();
        let mut has_trim = false;

        for (key, key_type) in keys.iter().zip(types.iter()) {
            if key_type == "stream" {
                trim_pipe
                    .cmd("XTRIM")
                    .arg(key)
                    .arg("MINID")
                    .arg("~")
                    .arg(min_id.to_string())
                    .ignore();
                has_trim = true;
            }
        }

        if has_trim {
            let _: () = trim_pipe.query_async(&mut conn).await.map_err(|e| QueueError::OperationFailed(Box::new(e)))?;
        }

        Ok(())
    }

    async fn send_to_dlq(&self, topic: &str, id: &str, payload: &[u8], reason: &str) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| QueueError::ConnectionFailed)?;
        
        let topic_key = format!("{}:{}", self.namespace, topic);
        let dlq_key = format!("{}:dlq", topic_key);
        
        let _: () = redis::cmd("XADD")
             .arg(&dlq_key)
             .arg("*")
             .arg("payload")
             .arg(payload)
             .arg("reason")
             .arg(reason)
             .arg("original_id")
             .arg(id)
             .query_async(&mut conn)
             .await
             .map_err(|e| QueueError::PushFailed(Box::new(e)))?;
             
         counter!("queue_dlq_total", "topic" => topic.to_string()).increment(1);
         Ok(())
    }

    async fn read_dlq(&self, topic: &str, count: usize) -> Result<Vec<(String, Vec<u8>, String, String)>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| QueueError::ConnectionFailed)?;

        let topic_key = format!("{}:{}", self.namespace, topic);
        let dlq_key = format!("{}:dlq", topic_key);

        // XREVRANGE key + - COUNT count
        let result: redis::RedisResult<Vec<(String, HashMap<String, Vec<u8>>)>> = redis::cmd("XREVRANGE")
            .arg(&dlq_key)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(messages) => {
                let mut output = Vec::new();
                for (id, map) in messages {
                    let payload = map.get("payload").cloned().unwrap_or_default();
                    let reason = String::from_utf8(map.get("reason").cloned().unwrap_or_default())
                        .unwrap_or_else(|_| "Invalid UTF-8".to_string());
                    let original_id = String::from_utf8(map.get("original_id").cloned().unwrap_or_default())
                        .unwrap_or_default();
                    
                    output.push((id, payload, reason, original_id));
                }
                Ok(output)
            }
            Err(e) => {
                // If key doesn't exist, it might be fine, return empty
                if let Some(_code) = e.code() {
                    // Redis returns empty list if key missing for XREVRANGE usually, but check just in case
                     warn!("Error reading DLQ {}: {}", dlq_key, e);
                }
                Ok(Vec::new())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_shard_id() {
        assert_eq!(RedisQueue::extract_shard_id("mocra:task:0"), Some(0));
        assert_eq!(RedisQueue::extract_shard_id("mocra:task:7"), Some(7));
        assert_eq!(RedisQueue::extract_shard_id("mocra:task:123"), Some(123));
        assert_eq!(RedisQueue::extract_shard_id("mocra:task"), None);
        assert_eq!(RedisQueue::extract_shard_id("mocra:task:a"), None);
        assert_eq!(RedisQueue::extract_shard_id("simple"), None);
    }
}

