use log::{debug, warn, error};
use tokio::sync::mpsc::Receiver;
use crate::events::SystemEvent;

pub struct ConsoleLogHandler;

impl ConsoleLogHandler {
    pub async fn start(mut rx: Receiver<SystemEvent>, _level: String) {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                // Smart logging based on event type content
                let type_name = event.event_type();
                // Avoid constructing the string if we aren't going to log it (though macros check level first)
                
                // Determine severity from the event variant name itself (heuristic)
                let is_failure = type_name.ends_with("Failed") || type_name.contains("Error");
                let is_retry = type_name.ends_with("Retry");
                
                if is_failure {
                    error!("Event: {} | Time: {}", type_name, event.timestamp());
                } else if is_retry {
                    warn!("Event: {} | Time: {}", type_name, event.timestamp());
                } else {
                    // Normal lifecycle events -> debug to reduce noise
                    debug!("Event: {} | Time: {}", type_name, event.timestamp());
                }
            }
        });
    }
}
