use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, Mutex, Semaphore};
use log::{info, debug};
use crate::queue::Identifiable;
use metrics::gauge;
use tracing::Instrument;

/// Generic concurrent runner for queue-driven processors.
///
/// The runner coordinates pause/resume state, graceful shutdown, and bounded
/// in-flight concurrency via a semaphore.
pub struct ProcessorRunner {
    pub name: String,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub pause_rx: watch::Receiver<bool>,
    pub concurrency: usize,
}

impl ProcessorRunner {
    /// Creates a named runner with pause/shutdown controls and max concurrency.
    pub fn new(
        name: &str,
        shutdown_rx: broadcast::Receiver<()>,
        pause_rx: watch::Receiver<bool>,
        concurrency: usize,
    ) -> Self {
        Self {
            name: name.to_string(),
            shutdown_rx,
            pause_rx,
            concurrency,
        }
    }

    /// Starts consuming items and dispatching processing tasks.
    ///
    /// Behavior:
    /// - Pulls one item, then greedily drains a small batch.
    /// - Uses a semaphore permit per spawned task to cap parallelism.
    /// - Reacts to pause/shutdown signals between receives and dispatch.
    pub async fn run<T, F, Fut>(
        mut self,
        receiver: Arc<Mutex<mpsc::Receiver<T>>>,
        execute_fn: F,
    ) where
        T: Identifiable + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send,
    {
        info!("Starting {} processor with concurrency {}", self.name, self.concurrency);
        
        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        let metric_label = self.name.to_lowercase();
        let mut rx = receiver.lock().await;
        
        let mut loop_count: u64 = 0;

        loop {
            // Honor pause signal before attempting to receive.
            if *self.pause_rx.borrow() {
                if self.pause_rx.changed().await.is_err() { break; }
                continue;
            }

            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("{} processor received shutdown signal", self.name);
                    break;
                }
                _ = self.pause_rx.changed() => {
                    continue;
                }
                item_opt = rx.recv() => {
                    match item_opt {
                        Some(first_item) => {
                            let mut items = Vec::with_capacity(100);
                            items.push(first_item);

                            // Greedy batch receive with conservative upper bound.
                            let batch_limit = 100.min(self.concurrency / 2).max(1);
                            for _ in 0..batch_limit {
                                match rx.try_recv() {
                                    Ok(item) => items.push(item),
                                    Err(_) => break,
                                }
                            }

                            loop_count += items.len() as u64;
                            if loop_count % 1000 == 0 {
                                debug!("Processor {}: processed {} items", self.name, loop_count);
                            }

                            for item in items {
                                let permit = loop {
                                    // 1) Check pause state.
                                    if *self.pause_rx.borrow() {
                                         tokio::select! {
                                             _ = self.shutdown_rx.recv() => break None,
                                             _ = self.pause_rx.changed() => continue,
                                         }
                                    }

                                    // 2) Acquire permit or react to pause/shutdown.
                                    tokio::select! {
                                        _ = self.shutdown_rx.recv() => break None,
                                        _ = self.pause_rx.changed() => continue,
                                        res = semaphore.clone().acquire_owned() => {
                                            match res {
                                                Ok(p) => break Some(p),
                                                Err(_) => break None,
                                            }
                                        }
                                    }
                                };

                                let permit = match permit {
                                    Some(p) => p,
                                    None => break,
                                };

                                let execute_fn = execute_fn.clone();
                                let task_id = item.get_id();
                                let metric_label = metric_label.clone();
                                let span_name = format!("{}_processor", metric_label);

                                tokio::spawn(async move {
                                    gauge!("engine_active_tasks", "processor" => metric_label.clone()).increment(1.0);
                                    let _permit = permit;
                                    execute_fn(item).await;
                                    gauge!("engine_active_tasks", "processor" => metric_label).decrement(1.0);
                                }.instrument(tracing::info_span!("processor_execution", processor_type = %span_name, item_id = %task_id)));
                            }
                        }
                        None => {
                            info!("{} channel closed", self.name);
                            break;
                        }
                    }
                }
            }
        }
        
        info!("{} processor loop ended", self.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::QueuedItem;
    use std::sync::Mutex as StdMutex;
    use tokio::time::{timeout, Duration};

    #[derive(Clone)]
    struct TestItem {
        id: String,
        should_fail: bool,
    }

    impl Identifiable for TestItem {
        fn get_id(&self) -> String {
            self.id.clone()
        }
    }

    #[tokio::test]
    async fn test_poison_message_triggers_nack() {
        let (tx, rx) = mpsc::channel(10);
        let receiver = Arc::new(Mutex::new(rx));
        let (result_tx, mut result_rx) = mpsc::channel::<String>(10);

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (pause_tx, pause_rx) = watch::channel(false);

        let runner = ProcessorRunner::new("Test", shutdown_rx, pause_rx, 2);

        let handle = tokio::spawn(async move {
            runner
                .run(receiver, move |item: QueuedItem<TestItem>| {
                    let result_tx = result_tx.clone();
                    async move {
                        let (inner, mut ack_fn, mut nack_fn) = item.into_parts();
                        if inner.should_fail {
                            if let Some(f) = nack_fn.take() {
                                let _ = f("decode failed".to_string()).await;
                                let _ = result_tx.send(format!("nack:{}", inner.id)).await;
                            }
                        } else if let Some(f) = ack_fn.take() {
                            let _ = f().await;
                            let _ = result_tx.send(format!("ack:{}", inner.id)).await;
                        }
                    }
                })
                .await;
        });

        let ack_marker = Arc::new(StdMutex::new(0u32));
        let nack_marker = Arc::new(StdMutex::new(0u32));

        let ack_marker_clone = ack_marker.clone();
        let nack_marker_clone = nack_marker.clone();

        let ok_item = QueuedItem::with_ack(
            TestItem { id: "ok".to_string(), should_fail: false },
            move || {
                let ack_marker_clone = ack_marker_clone.clone();
                Box::pin(async move {
                    *ack_marker_clone.lock().unwrap() += 1;
                    Ok(())
                })
            },
            move |_reason| {
                let nack_marker_clone = nack_marker_clone.clone();
                Box::pin(async move {
                    *nack_marker_clone.lock().unwrap() += 1;
                    Ok(())
                })
            },
        );

        let nack_marker_clone_err = nack_marker.clone();
        let err_item = QueuedItem::with_ack(
            TestItem { id: "bad".to_string(), should_fail: true },
            move || {
                Box::pin(async move { Ok(()) })
            },
            move |_reason| {
                let nack_marker_clone = nack_marker_clone_err.clone();
                Box::pin(async move {
                    *nack_marker_clone.lock().unwrap() += 1;
                    Ok(())
                })
            },
        );

        tx.send(ok_item).await.expect("send ok");
        tx.send(err_item).await.expect("send err");
        drop(tx);

        let first = timeout(Duration::from_secs(3), result_rx.recv())
            .await
            .ok()
            .flatten()
            .expect("result1");
        let second = timeout(Duration::from_secs(3), result_rx.recv())
            .await
            .ok()
            .flatten()
            .expect("result2");

        let combined = format!("{},{}", first, second);
        assert!(combined.contains("ack:ok"));
        assert!(combined.contains("nack:bad"));
        assert_eq!(*ack_marker.lock().unwrap(), 1);
        assert_eq!(*nack_marker.lock().unwrap(), 1);

        let _ = handle.await;
        drop(shutdown_tx);
        drop(pause_tx);
    }
}
