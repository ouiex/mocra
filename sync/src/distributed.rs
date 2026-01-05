use super::backend::MqBackend;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::{Arc, RwLock};
use tokio::sync::{watch, broadcast};
use std::time::Duration;
use dashmap::DashMap;

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

#[derive(Clone)]
pub struct SyncService {
    backend: Option<Arc<dyn MqBackend>>,
    local_store: Arc<DashMap<String, Arc<LocalTopicState>>>,
    namespace: String,
}

impl SyncService {
    pub fn new(backend: Option<Arc<dyn MqBackend>>, namespace: String) -> Self {
        Self {
            backend,
            local_store: Arc::new(DashMap::new()),
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

            let initial_data = backend.get(&kv_key).await?;
            let initial_value = if let Some(bytes) = initial_data {
                match serde_json::from_slice::<T>(&bytes) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!("Failed to deserialize initial value for topic {}: {}", topic, e);
                        None
                    }
                }
            } else {
                None
            };

            let (tx, state) = watch::channel(initial_value);

            tokio::spawn(async move {
                while let Some(bytes) = rx.recv().await {
                    match serde_json::from_slice::<T>(&bytes) {
                        Ok(value) => {
                            if tx.send(Some(value)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize update for topic {}: {}", T::topic(), e);
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
                    match serde_json::from_slice::<T>(bytes) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            eprintln!("Failed to deserialize local value for topic {}: {}", topic, e);
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
                    match serde_json::from_slice::<T>(&bytes) {
                        Ok(value) => {
                            if tx.send(Some(value)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to deserialize local update for topic {}: {}", T::topic(), e);
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
        let bytes = serde_json::to_vec(data).map_err(|e| e.to_string())?;

        if let Some(backend) = &self.backend {
            let stream_topic = self.stream_topic_for(&topic);
            let kv_key = self.kv_key_for(&topic);
            backend.set(&kv_key, &bytes).await?;
            backend.publish(&stream_topic, &bytes).await
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

            loop {
                let old_bytes_opt = backend.get(&kv_key).await?;
                
                let mut state = if let Some(ref bytes) = old_bytes_opt {
                    serde_json::from_slice::<T>(bytes).map_err(|e| e.to_string())?
                } else {
                    return Err("Cannot update non-existent state".to_string());
                };

                f(&mut state);

                let new_bytes = serde_json::to_vec(&state).map_err(|e| e.to_string())?;

                let old_bytes_slice = old_bytes_opt.as_deref();
                if backend.cas(&kv_key, old_bytes_slice, &new_bytes).await? {
                    backend.publish(&stream_topic, &new_bytes).await?;
                    return Ok(state);
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } else {
            // Local Mode: Just lock and update
            let local_state = self.get_local_state(&topic);
            
            // We need to hold the write lock throughout the read-modify-write process to ensure atomicity
            let mut lock = local_state.value.write().unwrap();
            
            let mut state = if let Some(ref bytes) = *lock {
                serde_json::from_slice::<T>(bytes).map_err(|e| e.to_string())?
            } else {
                return Err("Cannot update non-existent state".to_string());
            };

            f(&mut state);

            let new_bytes = serde_json::to_vec(&state).map_err(|e| e.to_string())?;
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
                serde_json::from_slice::<T>(&bytes).map(Some).map_err(|e| e.to_string())
            } else {
                Ok(None)
            }
        } else {
            let local_state = self.get_local_state(&topic);
            let lock = local_state.value.read().unwrap();
            if let Some(bytes) = &*lock {
                serde_json::from_slice::<T>(bytes).map(Some).map_err(|e| e.to_string())
            } else {
                Ok(None)
            }
        }
    }
}

