use queue::QueuedItem;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureSendState {
    Direct,
    RecoveredFromFull,
}

pub struct BackpressureSendError<T> {
    pub item: QueuedItem<T>,
    pub after_full: bool,
}

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
