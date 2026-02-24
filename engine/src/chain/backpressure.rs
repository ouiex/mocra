use queue::QueuedItem;
use tokio::sync::mpsc::Sender;

/// Result state for queue send attempts with fast-path + awaited fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureSendState {
    /// Sent immediately via `try_send`.
    Direct,
    /// Initial `try_send` saw full queue, then awaited `send` succeeded.
    RecoveredFromFull,
}

/// Error payload preserving ownership of the queued item for caller recovery.
pub struct BackpressureSendError<T> {
    /// Original item that failed to send.
    pub item: QueuedItem<T>,
    /// Indicates whether failure happened after queue-full fallback path.
    pub after_full: bool,
}

/// Sends into an mpsc channel with a non-blocking first attempt.
///
/// Strategy:
/// - `try_send` first for low latency.
/// - On `Full`, fallback to awaited `send`.
/// - On `Closed`, return item immediately.
pub async fn send_with_backpressure<T>(
    tx: &Sender<QueuedItem<T>>,
    item: QueuedItem<T>,
) -> Result<BackpressureSendState, BackpressureSendError<T>> {
    match tx.try_send(item) {
        Ok(_) => Ok(BackpressureSendState::Direct),
        Err(tokio::sync::mpsc::error::TrySendError::Full(item)) => match tx.send(item).await {
            Ok(_) => Ok(BackpressureSendState::RecoveredFromFull),
            Err(err) => Err(BackpressureSendError {
                item: err.0,
                after_full: true,
            }),
        },
        Err(tokio::sync::mpsc::error::TrySendError::Closed(item)) => {
            Err(BackpressureSendError {
                item,
                after_full: false,
            })
        }
    }
}
