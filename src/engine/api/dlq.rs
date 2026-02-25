use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use crate::engine::api::state::ApiState;

/// Parameters for inspecting the Dead Letter Queue
#[derive(Deserialize)]
pub struct DlqParams {
    /// The topic to inspect (default: "task")
    pub topic: Option<String>,
    /// Number of messages to retrieve (default: 10)
    pub count: Option<usize>,
}

/// Response model for DLQ messages
#[derive(Serialize)]
pub struct DlqMessageResponse {
    /// Message ID in DLQ
    pub id: String,
    /// Message content (UTF-8 string)
    pub payload: String, 
    /// Reason for failure (if available)
    pub reason: String,
    /// Original message ID before failure
    pub original_id: String,
}

/// Handler for GET /api/dlq
///
/// Retrieves messages from the Dead Letter Queue for inspection.
pub async fn get_dlq(
    State(state): State<ApiState>,
    Query(params): Query<DlqParams>,
) -> Json<Vec<DlqMessageResponse>> {
    let topic = params.topic.unwrap_or_else(|| "task".to_string());
    let count = params.count.unwrap_or(10);

    match state.queue_manager.read_dlq(&topic, count).await {
        Ok(messages) => {
            let response: Vec<DlqMessageResponse> = messages.into_iter().map(|(id, payload, reason, original_id)| {
                // Try to convert payload to string (utf8)
                let payload_str = String::from_utf8(payload).unwrap_or_else(|_| "Invalid UTF-8 payload".to_string());
                
                DlqMessageResponse {
                    id,
                    payload: payload_str,
                    reason,
                    original_id,
                }
            }).collect();
            Json(response)
        },
        Err(e) => {
            log::error!("Failed to read DLQ: {}", e);
            Json(vec![])
        }
    }
}
