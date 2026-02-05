use super::backend::CoordinationBackend;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::{Serialize, de::DeserializeOwned};
use rmp_serde as rmps;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, watch};

fn msgpack_encode<T: Serialize>(value: &T) -> Result<Vec<u8>, rmps::encode::Error> {
    rmps::to_vec(value)
}

fn msgpack_decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, rmps::decode::Error> {
    rmps::from_slice(bytes)
}

pub trait SyncAble: Send + Sync + Sized + 'static + Serialize + DeserializeOwned {
    // 消息队列topic
    fn topic() -> String;
}

pub struct DistributedSync<T>
where
    T: SyncAble,
{
    state: watch::Receiver<Option<T>>,
}

impl<T> DistributedSync<T>
where
    T: SyncAble,
{
    pub fn get(&self) -> Option<T>
    where
        T: Clone,
    {
        self.state.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.state.changed().await
    }
}

struct LocalTopicState {
    value: RwLock<Option<Vec<u8>>>,
    tx: broadcast::Sender<Vec<u8>>,
}

impl LocalTopicState {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(100);
        Self {
            value: RwLock::new(None),
            tx,
        }
    }
}

static LOCAL_STORE: Lazy<Arc<DashMap<String, Arc<LocalTopicState>>>> =
    Lazy::new(|| Arc::new(DashMap::new()));

#[derive(Clone)]
pub struct SyncService {
    backend: Option<Arc<dyn CoordinationBackend>>,
    local_store: Arc<DashMap<String, Arc<LocalTopicState>>>,
    namespace: String,
}

impl SyncService {
    pub fn new(backend: Option<Arc<dyn CoordinationBackend>>, namespace: String) -> Self {
        Self {
            backend,
            local_store: Arc::clone(&LOCAL_STORE),
            namespace,
        }
    }

    fn stream_topic_for(&self, topic: &str) -> String {
        if self.namespace.is_empty() {
            format!("sync_stream_{}", topic)
        } else {
            // Use underscore to ensure Kafka topic compatibility
            format!("sync_stream_{}_{}", self.namespace, topic)
        }
    }

    fn kv_key_for(&self, topic: &str) -> String {
        if self.namespace.is_empty() {
            format!("sync_kv:{}", topic)
        } else {
            // Redis keys can safely use ':' separators
            format!("sync_kv:{}:{}", self.namespace, topic)
        }
    }

    fn get_local_state(&self, topic: &str) -> Arc<LocalTopicState> {
        let key = if self.namespace.is_empty() {
            format!("sync:{}", topic)
        } else {
            format!("sync:{}:{}", self.namespace, topic)
        };
        self.local_store
            .entry(key)
            .or_insert_with(|| Arc::new(LocalTopicState::new()))
            .value()
            .clone()
    }

    pub async fn sync<T>(&self) -> Result<DistributedSync<T>, String>
    where
        T: SyncAble,
    {
        let topic = T::topic();

        if let Some(backend) = &self.backend {
            // Distributed Mode
            let stream_topic = self.stream_topic_for(&topic);
            let kv_key = self.kv_key_for(&topic);

            let mut rx = backend.subscribe(&stream_topic).await?;
            let backend = Arc::clone(backend);

            let initial_data = backend.get(&kv_key).await?;
            let initial_value = if let Some(bytes) = initial_data {
                match msgpack_decode::<T>(&bytes) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!(
                            "Failed to deserialize initial value for topic {}: {}",
                            topic, e
                        );
                        None
                    }
                }
            } else {
                None
            };

            let (tx, state) = watch::channel(initial_value);

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        recv = rx.recv() => {
                            match recv {
                                Some(bytes) => {
                                    match msgpack_decode::<T>(&bytes) {
                                        Ok(value) => {
                                            if tx.send(Some(value)).is_err() {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Failed to deserialize update for topic {}: {}",
                                                T::topic(),
                                                e
                                            );
                                        }
                                    }
                                }
                                None => break,
                            }
                        }
                        _ = interval.tick() => {
                            match backend.get(&kv_key).await {
                                Ok(Some(bytes)) => {
                                    match msgpack_decode::<T>(&bytes) {
                                        Ok(value) => {
                                            if tx.send(Some(value)).is_err() {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Failed to deserialize refresh value for topic {}: {}",
                                                T::topic(),
                                                e
                                            );
                                        }
                                    }
                                }
                                Ok(None) => {
                                    if tx.send(None).is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Failed to refresh state for topic {}: {}",
                                        T::topic(),
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            });

            Ok(DistributedSync { state })
        } else {
            // Local Mode
            let local_state = self.get_local_state(&topic);

            // Initial value
            let initial_value = {
                let lock = local_state.value.read().unwrap();
                if let Some(bytes) = &*lock {
                    match msgpack_decode::<T>(bytes) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            eprintln!(
                                "Failed to deserialize local value for topic {}: {}",
                                topic, e
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            };

            let (tx, state) = watch::channel(initial_value);
            let mut rx = local_state.tx.subscribe();

            tokio::spawn(async move {
                while let Ok(bytes) = rx.recv().await {
                    match msgpack_decode::<T>(&bytes) {
                        Ok(value) => {
                            if tx.send(Some(value)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "Failed to deserialize local update for topic {}: {}",
                                T::topic(),
                                e
                            );
                        }
                    }
                }
            });

            Ok(DistributedSync { state })
        }
    }

    pub async fn send<T>(&self, data: &T) -> Result<(), String>
    where
        T: SyncAble,
    {
        let topic = T::topic();
        let bytes = msgpack_encode(data).map_err(|e| e.to_string())?;

        if let Some(backend) = &self.backend {
            let stream_topic = self.stream_topic_for(&topic);
            let kv_key = self.kv_key_for(&topic);
            backend.set(&kv_key, &bytes).await?;
            Self::publish_with_retry(backend, &stream_topic, &bytes).await
        } else {
            let local_state = self.get_local_state(&topic);
            {
                let mut lock = local_state.value.write().unwrap();
                *lock = Some(bytes.clone());
            }
            let _ = local_state.tx.send(bytes);
            Ok(())
        }
    }

    /// Optimistically update the state.
    pub async fn optimistic_update<T, F>(&self, f: F) -> Result<T, String>
    where
        T: SyncAble + Clone,
        F: Fn(&mut T),
    {
        let topic = T::topic();

        if let Some(backend) = &self.backend {
            let kv_key = self.kv_key_for(&topic);
            let stream_topic = self.stream_topic_for(&topic);
            let mut attempts: u32 = 0;

            loop {
                let old_bytes_opt = backend.get(&kv_key).await?;

                let mut state = if let Some(ref bytes) = old_bytes_opt {
                    msgpack_decode::<T>(bytes).map_err(|e| e.to_string())?
                } else {
                    return Err("Cannot update non-existent state".to_string());
                };

                f(&mut state);

                let new_bytes = msgpack_encode(&state).map_err(|e| e.to_string())?;

                let old_bytes_slice = old_bytes_opt.as_deref();
                if backend.cas(&kv_key, old_bytes_slice, &new_bytes).await? {
                    Self::publish_with_retry(backend, &stream_topic, &new_bytes).await?;
                    return Ok(state);
                }

                attempts = attempts.saturating_add(1);
                let backoff = 10u64.saturating_mul(1u64 << attempts.min(4));
                let jitter = Self::jitter_ms(25);
                tokio::time::sleep(Duration::from_millis(backoff + jitter)).await;
            }
        } else {
            // Local Mode: Just lock and update
            let local_state = self.get_local_state(&topic);

            // We need to hold the write lock throughout the read-modify-write process to ensure atomicity
            let mut lock = local_state.value.write().unwrap();

            let mut state = if let Some(ref bytes) = *lock {
                msgpack_decode::<T>(bytes).map_err(|e| e.to_string())?
            } else {
                return Err("Cannot update non-existent state".to_string());
            };

            f(&mut state);

            let new_bytes = msgpack_encode(&state).map_err(|e| e.to_string())?;
            *lock = Some(new_bytes.clone());

            // Release lock before sending to avoid potential deadlocks (though unlikely here)
            drop(lock);

            let _ = local_state.tx.send(new_bytes);
            Ok(state)
        }
    }

    pub async fn fetch_latest<T>(&self) -> Result<Option<T>, String>
    where
        T: SyncAble,
    {
        let topic = T::topic();

        if let Some(backend) = &self.backend {
            let kv_key = self.kv_key_for(&topic);
            let data = backend.get(&kv_key).await?;
            if let Some(bytes) = data {
                msgpack_decode::<T>(&bytes)
                    .map(Some)
                    .map_err(|e| e.to_string())
            } else {
                Ok(None)
            }
        } else {
            let local_state = self.get_local_state(&topic);
            let lock = local_state.value.read().unwrap();
            if let Some(bytes) = &*lock {
                msgpack_decode::<T>(bytes)
                    .map(Some)
                    .map_err(|e| e.to_string())
            } else {
                Ok(None)
            }
        }
    }

    fn jitter_ms(max: u64) -> u64 {
        if max == 0 {
            return 0;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        nanos % max
    }

    async fn publish_with_retry(
        backend: &Arc<dyn CoordinationBackend>,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), String> {
        const MAX_RETRIES: u32 = 3;
        let mut attempt = 0;
        loop {
            match backend.publish(topic, payload).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        return Err(err);
                    }
                    eprintln!(
                        "Sync publish failed for topic {} (attempt {}/{}): {}",
                        topic, attempt, MAX_RETRIES, err
                    );
                    let backoff = 50u64.saturating_mul(attempt as u64) + Self::jitter_ms(25);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
            }
        }
    }
}
