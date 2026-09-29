use super::EventEnvelope;
use dashmap::DashMap;
use log::{error, info, warn};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc, watch};

/// In-process event bus for fan-out delivery to typed subscribers.
pub struct EventBus {
    /// Subscribers map: EventType -> List of Senders
    subscribers: Arc<DashMap<String, Vec<mpsc::Sender<EventEnvelope>>>>,
    sender: mpsc::Sender<EventEnvelope>,
    receiver: RwLock<Option<mpsc::Receiver<EventEnvelope>>>,
    stop_tx: watch::Sender<bool>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl EventBus {
    /// Compatibility constructor; handler concurrency is no longer needed.
    pub fn new(capacity: usize, _concurrency: usize) -> Self {
        Self::with_capacity(capacity)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (stop_tx, _) = watch::channel(false);

        Self {
            subscribers: Arc::new(DashMap::new()),
            sender,
            receiver: RwLock::new(Some(receiver)),
            stop_tx,
            worker: Mutex::new(None),
        }
    }

    /// Subscribes to a specific event key (or `*` for all events).
    ///
    /// Returns a bounded receiver that yields cloned envelopes published to
    /// the matching topic.
    pub async fn subscribe(&self, event_type: String) -> mpsc::Receiver<EventEnvelope> {
        let (tx, rx) = mpsc::channel(1000);
        self.subscribers.entry(event_type).or_default().push(tx);
        rx
    }

    /// Publishes an event into the internal queue.
    ///
    /// When the queue is full, the event is intentionally dropped to avoid
    /// propagating backpressure into critical producer paths.
    pub async fn publish(
        &self,
        event: EventEnvelope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *self.stop_tx.borrow() {
            return Err(std::io::Error::other("event bus stopped").into());
        }
        match self.sender.try_send(event) {
            Ok(_) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(e) => {
                error!("EventBus channel closed: {}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Starts one dispatch task on the caller's Tokio runtime.
    ///
    /// Returns `true` if the event bus was started successfully, `false` if it was already running.
    pub async fn start(&self) -> bool {
        if *self.stop_tx.borrow() {
            return false;
        }
        let receiver = {
            let mut receiver_guard = self.receiver.write().await;
            receiver_guard.take()
        };

        if let Some(mut receiver) = receiver {
            let subscribers = Arc::clone(&self.subscribers);

            let mut stop_rx = self.stop_tx.subscribe();
            let worker = tokio::spawn(async move {
                info!("EventBus started");
                loop {
                    if *stop_rx.borrow() {
                        break;
                    }
                    tokio::select! {
                        biased;
                        changed = stop_rx.changed() => {
                            if changed.is_err() || *stop_rx.borrow() {
                                break;
                            }
                        }
                        event = receiver.recv() => {
                            match event {
                                Some(event) => dispatch(&subscribers, event),
                                None => break,
                            }
                        }
                    }
                }
                while let Ok(event) = receiver.try_recv() {
                    dispatch(&subscribers, event);
                }
                info!("EventBus stopped");
            });
            *self.worker.lock().await = Some(worker);
            true
        } else {
            warn!("EventBus.start() called but already running — ignoring duplicate start");
            false
        }
    }

    pub fn stop(&self) {
        self.stop_tx.send_replace(true);
    }

    /// Drains queued events and waits for dispatch to finish.
    pub async fn stop_and_wait(&self) {
        self.stop();
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
    }
}

fn dispatch(subscribers: &DashMap<String, Vec<mpsc::Sender<EventEnvelope>>>, event: EventEnvelope) {
    let event_type = event.event_key();
    for key in [event_type.as_str(), "*"] {
        if let Some(mut senders) = subscribers.get_mut(key) {
            senders.retain(|tx| match tx.try_send(event.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("EventBus subscriber queue full for {key}; event dropped");
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            });
        }
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::with_capacity(10000)
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

        assert_eq!(
            received.event_key(),
            "engine.parser_task_produced.completed"
        );
        assert_eq!(received.event_type, EventType::ParserTaskProduced);
        assert_eq!(received.phase, EventPhase::Completed);
        bus.stop_and_wait().await;
    }

    #[tokio::test]
    async fn stop_drains_events_in_order() {
        let bus = EventBus::new(8, 1);
        let mut rx = bus.subscribe("*".to_string()).await;
        assert!(bus.start().await);
        for index in 0..4 {
            bus.publish(EventEnvelope::engine(
                EventType::ParserTaskProduced,
                EventPhase::Completed,
                json!({"index": index}),
            ))
            .await
            .unwrap();
        }
        bus.stop_and_wait().await;
        let mut indexes = Vec::new();
        while let Ok(event) = rx.try_recv() {
            indexes.push(event.payload["index"].as_i64().unwrap());
        }
        assert_eq!(indexes, vec![0, 1, 2, 3]);
        assert!(
            bus.publish(EventEnvelope::engine(
                EventType::ParserTaskProduced,
                EventPhase::Completed,
                json!({}),
            ))
            .await
            .is_err()
        );
    }
}
