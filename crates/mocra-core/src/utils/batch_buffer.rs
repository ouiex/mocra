use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use std::future::Future;
use std::pin::Pin;

pub struct BatchBuffer<T> {
    sender: mpsc::Sender<T>,
}

impl<T> BatchBuffer<T> 
where 
    T: Send + 'static,
{
    pub fn new<F, Fut>(capacity: usize, batch_size: usize, timeout: Duration, flush_callback: F) -> Self
    where
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (sender, mut receiver) = mpsc::channel(capacity);
        let flush_callback = Box::new(move |items| {
            Box::pin(flush_callback(items)) as Pin<Box<dyn Future<Output = ()> + Send>>
        });

        tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            let mut ticker = interval(timeout);

            loop {
                tokio::select! {
                    msg = receiver.recv() => {
                        match msg {
                            Some(item) => {
                                buffer.push(item);
                                if buffer.len() >= batch_size {
                                    (flush_callback)(std::mem::replace(&mut buffer, Vec::with_capacity(batch_size))).await;
                                }
                            }
                            None => {
                                // Channel closed, flush remaining items
                                if !buffer.is_empty() {
                                    (flush_callback)(buffer).await;
                                }
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        if !buffer.is_empty() {
                            (flush_callback)(std::mem::replace(&mut buffer, Vec::with_capacity(batch_size))).await;
                        }
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn add(&self, item: T) -> Result<(), mpsc::error::SendError<T>> {
        self.sender.send(item).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_batch_buffer_flush_on_size() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();

        let buffer = BatchBuffer::new(
            10,
            3, // batch size 3
            Duration::from_secs(10),
            move |items| {
                let received = received_clone.clone();
                async move {
                    received.lock().unwrap().extend(items);
                }
            }
        );

        buffer.add(1).await.unwrap();
        buffer.add(2).await.unwrap();
        buffer.add(3).await.unwrap(); // Should trigger flush

        sleep(Duration::from_millis(100)).await;

        {
            let data = received.lock().unwrap();
            assert_eq!(data.len(), 3);
            assert_eq!(*data, vec![1, 2, 3]);
        }
    }

    #[tokio::test]
    async fn test_batch_buffer_flush_on_timeout() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();

        let buffer = BatchBuffer::new(
            10,
            10,
            Duration::from_millis(200),
            move |items| {
                let received = received_clone.clone();
                async move {
                    received.lock().unwrap().extend(items);
                }
            }
        );

        buffer.add(1).await.unwrap();
        
        sleep(Duration::from_millis(300)).await;

        {
            let data = received.lock().unwrap();
            assert_eq!(data.len(), 1);
            assert_eq!(*data, vec![1]);
        }
    }
}
