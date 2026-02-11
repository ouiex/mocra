use log::error;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
// use common::model::logger_config::LogOutputConfig;
use queue::{QueuedItem, QueueManager};
use utils::logger::LogModel;
use crate::events::EventEnvelope;

/// Universal queue handler for logs/events.
pub struct QueueLogHandler;

impl QueueLogHandler {
    pub async fn start(
        mut rx: Receiver<EventEnvelope>,
        queue_manager: Arc<QueueManager>,
        target_type: String
    ) {
        let sender = queue_manager.get_log_push_channel();

        // Separate task for this handler
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                 let json_content = serde_json::to_string(&event).unwrap_or_else(|_| "serialization_failed".to_string());
        
                 let log_model = LogModel {
                    task_id: "system".to_string(),
                    request_id: None,
                    status: event.event_key(),
                    level: "INFO".to_string(),
                    message: json_content,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    traceback: None,
                };
        
                let item = QueuedItem::new(log_model);
                if let Err(e) = sender.send(item).await {
                    error!("Failed to send event to {} queue: {}", target_type, e);
                }
            }
        });
    }
}
