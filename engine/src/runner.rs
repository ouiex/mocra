use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch, Mutex, Semaphore};
use log::{info, debug};
use queue::Identifiable;
use metrics::gauge;
use tracing::Instrument;

pub struct ProcessorRunner {
    pub name: String,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub pause_rx: watch::Receiver<bool>,
    pub concurrency: usize,
}

impl ProcessorRunner {
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
            // Check pause state
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

                            // Greedy batch receive
                            // Limit batch size to 100 or concurrency/10 to avoid starving others or holding too much memory
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
                                    // 1. Check if paused
                                    if *self.pause_rx.borrow() {
                                         tokio::select! {
                                             _ = self.shutdown_rx.recv() => break None,
                                             _ = self.pause_rx.changed() => continue,
                                         }
                                    }

                                    // 2. Try acquire or wait for pause/shutdown
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
                                    None => break, // Shutdown or error, break items loop
                                };

                                let execute_fn = execute_fn.clone();
                                let task_id = item.get_id();
                                let metric_label = metric_label.clone();
                                let span_name = format!("{}_processor", metric_label);

                                tokio::spawn(async move {
                                    gauge!("engine_active_tasks", "processor" => metric_label.clone()).increment(1.0);
                                    let _permit = permit; // Hold permit
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
