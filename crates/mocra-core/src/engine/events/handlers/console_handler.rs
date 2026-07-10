use log::{debug, warn, error};
use tokio::sync::mpsc::Receiver;
use crate::engine::events::{EventEnvelope, EventPhase};

pub struct ConsoleLogHandler;

impl ConsoleLogHandler {
    pub async fn start(mut rx: Receiver<EventEnvelope>, _level: String) {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let event_key = event.event_key();
                let ts = event.timestamp_ms;
                match event.phase {
                    EventPhase::Failed => error!("Event: {} | Time: {}", event_key, ts),
                    EventPhase::Retry => warn!("Event: {} | Time: {}", event_key, ts),
                    _ => debug!("Event: {} | Time: {}", event_key, ts),
                }
            }
        });
    }
}
