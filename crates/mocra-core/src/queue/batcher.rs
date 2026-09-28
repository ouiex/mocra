use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinSet;

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
        assert!(batch_size > 0, "batch_size must be positive");
        let mut batch = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
        let mut tasks = JoinSet::new();
        loop {
            // Reap finished tasks during normal operation, not only at shutdown.
            while let Some(result) = tasks.try_join_next() {
                if let Err(error) = result {
                    log::error!("batch processor task failed: {error}");
                }
            }
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
                                Self::dispatch(items, &semaphore, &processor, &mut tasks).await;
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                let items = std::mem::take(&mut batch);
                                Self::dispatch(items, &semaphore, &processor, &mut tasks).await;
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                         let items = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                         Self::dispatch(items, &semaphore, &processor, &mut tasks).await;
                    }
                }
            }
        }
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                log::error!("batch processor task failed: {error}");
            }
        }
    }

    async fn dispatch<T, F, Fut>(
        items: Vec<T>,
        semaphore: &Arc<Semaphore>,
        processor: &F,
        tasks: &mut JoinSet<()>,
    ) where
        T: Send + 'static,
        F: Fn(Vec<T>) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        // Waiting here stops rx.recv(), so the bounded input channel applies backpressure.
        match semaphore.clone().acquire_owned().await {
            Ok(permit) => {
                let processor = processor.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    processor(items).await;
                });
            }
            Err(_) => {
                log::error!("batch semaphore closed; processing accepted items inline");
                processor.clone()(items).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn bounds_dispatch_and_drains_on_close() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let runner = tokio::spawn({
            let gate = gate.clone();
            let started = started.clone();
            let finished = finished.clone();
            async move {
                Batcher::run(
                    &mut rx,
                    1,
                    100,
                    Arc::new(Semaphore::new(1)),
                    move |_items| {
                        let gate = gate.clone();
                        let started = started.clone();
                        let finished = finished.clone();
                        async move {
                            started.fetch_add(1, Ordering::SeqCst);
                            let _permit = gate.acquire().await.unwrap();
                            finished.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                )
                .await;
            }
        });
        tx.send(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
        tx.send(4).await.unwrap();
        assert!(
            tx.try_send(5).is_err(),
            "input must stay bounded while processor is blocked"
        );
        drop(tx);
        gate.add_permits(4);
        tokio::time::timeout(Duration::from_secs(1), runner)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(finished.load(Ordering::SeqCst), 4);
    }
}
