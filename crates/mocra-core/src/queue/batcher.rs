use tokio::sync::mpsc::Receiver;
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::time::Duration;

pub struct Batcher;

impl Batcher {
    pub async fn run<T, F, Fut>(
        rx: &mut Receiver<T>,
        batch_size: usize,
        interval_ms: u64,
        semaphore: Arc<Semaphore>,
        processor: F,
    ) where
        T: Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut batch = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            tokio::select! {
                res = rx.recv() => {
                    match res {
                        Some(item) => {
                            // println!("DEBUG: Batcher received item");
                            batch.push(item);
                            while batch.len() < batch_size {
                                match rx.try_recv() {
                                    Ok(next) => batch.push(next),
                                    Err(_) => break,
                                }
                            }

                            if batch.len() >= batch_size {
                                let items = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                                let processor = processor.clone();
                                let semaphore = semaphore.clone();
                                
                                tokio::spawn(async move {
                                    if let Ok(_permit) = semaphore.acquire_owned().await {
                                        processor(items).await;
                                    }
                                });
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                let items = std::mem::take(&mut batch);
                                let processor = processor.clone();
                                let semaphore = semaphore.clone();
                                
                                tokio::spawn(async move {
                                    if let Ok(_permit) = semaphore.acquire_owned().await {
                                        processor(items).await;
                                    }
                                });
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                         let items = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                         let processor = processor.clone();
                         let semaphore = semaphore.clone();
                         
                         tokio::spawn(async move {
                             if let Ok(_permit) = semaphore.acquire_owned().await {
                                 processor(items).await;
                             }
                         });
                    }
                }
            }
        }
    }
}
