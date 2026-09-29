use crate::engine::events::{EventEnvelope, EventPhase};
use log::{Level, error};
use tokio::sync::mpsc::Receiver;

pub struct ConsoleLogHandler;

impl ConsoleLogHandler {
    pub async fn start(mut rx: Receiver<EventEnvelope>, _level: String) {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let level = event_level(event.phase);
                if !log::log_enabled!(level) {
                    continue;
                }
                match serde_json::to_string(&event) {
                    Ok(payload) => log::log!(level, "Event: {} | {}", event.event_key(), payload),
                    Err(err) => error!("Failed to serialize event {}: {err}", event.event_key()),
                }
            }
        });
    }
}

fn event_level(phase: EventPhase) -> Level {
    match phase {
        EventPhase::Failed => Level::Error,
        EventPhase::Retry => Level::Warn,
        EventPhase::Started | EventPhase::Completed => Level::Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::events::EventType;
    use serde_json::json;

    #[test]
    fn all_event_phases_are_visible_at_info_level() {
        for (phase, level) in [
            (EventPhase::Started, Level::Info),
            (EventPhase::Completed, Level::Info),
            (EventPhase::Retry, Level::Warn),
            (EventPhase::Failed, Level::Error),
        ] {
            assert_eq!(event_level(phase), level);
            let event = EventEnvelope::engine(EventType::Download, phase, json!({"request_id": 1}));
            let payload = serde_json::to_string(&event).unwrap();
            assert!(payload.contains("\"request_id\":1"));
        }
    }
}
