use message_queue::QueueManager;
use std::sync::Arc;
#[derive(Clone)]
pub struct ApiState {
    pub(crate) queue_manager: Arc<QueueManager>,
}
