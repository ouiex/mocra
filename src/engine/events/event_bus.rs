use super::EventEnvelope;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use log::{error, info};
use dashmap::DashMap;

/// In-process event bus for fan-out delivery to typed subscribers.
pub struct EventBus {
    /// Subscribers map: EventType -> List of Senders
    subscribers: Arc<DashMap<String, Vec<mpsc::Sender<EventEnvelope>>>>,
    sender: mpsc::Sender<EventEnvelope>,
    _receiver: Arc<RwLock<Option<mpsc::Receiver<EventEnvelope>>>>,
}

impl EventBus {
    pub fn new(capacity: usize, _concurrency: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        
        Self {
            subscribers: Arc::new(DashMap::new()),
            sender,
            _receiver: Arc::new(RwLock::new(Some(receiver))),
        }
    }

    /// Subscribes to a specific event key (or `*` for all events).
    ///
    /// Returns a bounded receiver that yields cloned envelopes published to
    /// the matching topic.
    pub async fn subscribe(&self, event_type: String) -> mpsc::Receiver<EventEnvelope> {
        let (tx, rx) = mpsc::channel(1000);
        self.subscribers
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(tx);
        rx
    }

    /// Publishes an event into the internal queue.
    ///
    /// When the queue is full, the event is intentionally dropped to avoid
    /// propagating backpressure into critical producer paths.
    pub async fn publish(&self, event: EventEnvelope) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self.sender.try_send(event) {
            Ok(_) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                Ok(())
            },
            Err(e) => {
                error!("EventBus channel closed: {}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Starts the dedicated dispatch loop that forwards events to subscribers.
    pub async fn start(&self) {
        let receiver = {
            let mut receiver_guard = self._receiver.write().await;
            receiver_guard.take()
        };

        if let Some(mut receiver) = receiver {
            let subscribers = Arc::clone(&self.subscribers);
            
            std::thread::Builder::new()
                .name("event-bus-worker".to_string())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .expect("Failed to create event bus runtime");

                    rt.block_on(async move {
                        info!("EventBus dedicated runtime started");
                        while let Some(event) = receiver.recv().await {
                            let event_type = event.event_key();
                            
                            let broadcast_to = |senders: Vec<mpsc::Sender<EventEnvelope>>| {
                                for tx in senders {
                                    let event_clone = event.clone();
                                    tokio::spawn(async move {
                                        if let Err(_) = tx.send(event_clone).await {
                                            // Receiver dropped, ignore
                                        }
                                    });
                                }
                            };

                            if let Some(s) = subscribers.get(&event_type).map(|v| v.clone()) {
                                broadcast_to(s);
                            }
                            
                            if let Some(universal) = subscribers.get("*").map(|v| v.clone()) {
                                broadcast_to(universal);
                            }
                        }
                        info!("EventBus runtime stopping...");
                    });
                })
                .expect("Failed to spawn event bus thread");
        } else {
             error!("EventBus already started or receiver missing");
        }
    }

    pub fn stop(&self) {
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(10000, 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::events::{EventPhase, EventType};
    use serde_json::json;

    #[tokio::test]
    async fn semantic_event_subscription_receives_parser_task_produced() {
        let bus = EventBus::new(128, 4);
        let mut rx = bus
            .subscribe("engine.parser_task_produced.completed".to_string())
            .await;
        bus.start().await;

        let event = EventEnvelope::engine(
            EventType::ParserTaskProduced,
            EventPhase::Completed,
            json!({"account":"acc","platform":"pf"}),
        );
        bus.publish(event).await.expect("publish should succeed");

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("should receive event within timeout")
            .expect("receiver should yield one event");

        assert_eq!(received.event_key(), "engine.parser_task_produced.completed");
        assert_eq!(received.event_type, EventType::ParserTaskProduced);
        assert_eq!(received.phase, EventPhase::Completed);
    }
}

