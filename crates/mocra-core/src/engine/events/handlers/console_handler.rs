use crate::engine::events::{EventEnvelope, EventPhase};
use log::Level;
use std::fmt::Write;
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
                log::log!(level, "Event: {}", event_summary(&event));
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

fn event_summary(event: &EventEnvelope) -> String {
    let mut summary = event.event_key();
    for field in [
        "request_id",
        "response_id",
        "module",
        "status_code",
        "status",
    ] {
        if let Some(value) = event.payload.get(field) {
            if let Some(value) = value.as_str() {
                let _ = write!(summary, " {field}={value}");
            } else if value.is_number() {
                let _ = write!(summary, " {field}={value}");
            }
        }
    }
    if let Some(error) = &event.error {
        let _ = write!(
            summary,
            " error_kind={:?} error={}",
            error.kind, error.message
        );
    }
    summary
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
            let event = EventEnvelope::engine(
                EventType::Download,
                phase,
                json!({"request_id":"r1","status_code":200,"url":"secret"}),
            );
            let summary = event_summary(&event);
            assert!(summary.contains("request_id=r1"));
            assert!(summary.contains("status_code=200"));
            assert!(!summary.contains("secret"));
        }
    }
}
