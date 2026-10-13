use crate::queue::Identifiable;
use log::{debug, error, info};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc, watch};
use tokio::task::JoinSet;
use tracing::Instrument;

/// In-flight task counters, one per pipeline stage.
///
/// A task is *pending* while it waits in its queue channel and *in-flight* once a processor has
/// taken it and started executing. The two are disjoint, so `pending + inflight` is the work the
/// engine still owes — which is exactly what the idle-stop monitor treats as "busy".
///
/// Pending alone is a poor activity signal: processors drain their channel as fast as items arrive,
/// so a healthy run sits at pending 0 with all the work counted here instead. A non-zero pending is
/// backpressure, not throughput.
///
/// `remote_task` has no local processor (it is the outbound queue for remote dispatch), so it has a
/// pending depth but never any in-flight count.
#[derive(Clone, Default)]
pub struct InflightCounters {
    pub task: Arc<AtomicUsize>,
    pub download: Arc<AtomicUsize>,
    pub response: Arc<AtomicUsize>,
    pub parser: Arc<AtomicUsize>,
    pub error: Arc<AtomicUsize>,
}

impl InflightCounters {
    /// `(task, download, response, parser, error)` — mirrors the queue order used by
    /// [`QueueManager::local_pending_breakdown`](crate::queue::QueueManager::local_pending_breakdown).
    pub fn breakdown(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.task.load(Ordering::Relaxed),
            self.download.load(Ordering::Relaxed),
            self.response.load(Ordering::Relaxed),
            self.parser.load(Ordering::Relaxed),
            self.error.load(Ordering::Relaxed),
        )
    }

    /// Total tasks executing across every stage.
    pub fn total(&self) -> usize {
        let (t, d, r, p, e) = self.breakdown();
        t + d + r + p + e
    }
}

/// Cumulative outcome counters for one pipeline stage, counted since engine start.
///
/// **Terminal outcomes only.** A task that fails and gets retried is recorded once, when it finally
/// succeeds or fails for good — retry attempts are not counted. So `success + failure` is "work
/// that finished", and the success rate answers *"of the work that finished, how much worked"*
/// rather than *"how many attempts hit the network"*. A retry storm therefore shows up as the
/// counters stalling, not as a failure spike.
#[derive(Default)]
pub struct StageCounter {
    success: AtomicU64,
    failure: AtomicU64,
}

impl StageCounter {
    /// Records one item that finished successfully.
    pub fn record_success(&self) {
        self.success.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one item that failed terminally (not a retryable failure).
    pub fn record_failure(&self) {
        self.failure.fetch_add(1, Ordering::Relaxed);
    }

    /// `(success, failure)` since engine start.
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.success.load(Ordering::Relaxed),
            self.failure.load(Ordering::Relaxed),
        )
    }
}

/// Cumulative outcome counters for the four pipeline stages the dashboard reports.
///
/// `parse` covers both parser processors (the one parsing downloaded responses and the one draining
/// the parser-task queue); they are one stage as far as a user is concerned.
#[derive(Clone, Default)]
pub struct StageCounters {
    pub task: Arc<StageCounter>,
    pub request: Arc<StageCounter>,
    pub parse: Arc<StageCounter>,
    pub store: Arc<StageCounter>,
}

// Cancellation and panics must return the counters as well as the semaphore permit.
struct InflightGuard<'a> {
    counter: &'a AtomicUsize,
    stage: &'a str,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        crate::common::metrics::dec_inflight("engine", self.stage, 1.0);
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Generic concurrent runner for queue-driven processors.
///
/// The runner coordinates pause/resume state, graceful shutdown, and bounded
/// in-flight concurrency via a semaphore.
pub struct ProcessorRunner {
    pub name: String,
    pub shutdown_rx: broadcast::Receiver<()>,
    pub pause_rx: watch::Receiver<bool>,
    pub concurrency: usize,
    /// Counter for this processor's stage, incremented while a task executes.
    pub inflight_counter: Arc<AtomicUsize>,
}

impl ProcessorRunner {
    /// Creates a named runner with pause/shutdown controls and max concurrency.
    pub fn new(
        name: &str,
        shutdown_rx: broadcast::Receiver<()>,
        pause_rx: watch::Receiver<bool>,
        concurrency: usize,
        inflight_counter: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            name: name.to_string(),
            shutdown_rx,
            pause_rx,
            concurrency,
            inflight_counter,
        }
    }

    /// Starts consuming items and dispatching processing tasks.
    ///
    /// Behavior:
    /// - Pulls one item, then greedily drains a small batch.
    /// - Uses a semaphore permit per spawned task to cap parallelism.
    /// - Reacts to pause/shutdown signals between receives and dispatch.
    /// - Drains accepted work on channel closure; cancels and joins it on shutdown.
    pub async fn run<T, F, Fut>(mut self, receiver: Arc<Mutex<mpsc::Receiver<T>>>, execute_fn: F)
    where
        T: Identifiable + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send,
    {
        info!(
            "Starting {} processor with concurrency {}",
            self.name, self.concurrency
        );

        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        let metric_label = self.name.to_lowercase();
        let mut rx = receiver.lock().await;
        let mut tasks = JoinSet::new();

        let mut loop_count: u64 = 0;

        let mut cancelled = 'run: loop {
            while let Some(result) = tasks.try_join_next() {
                if let Err(error) = result {
                    error!("{} processor task failed: {error}", self.name);
                }
            }
            // Honor pause signal before attempting to receive.
            if *self.pause_rx.borrow() {
                tokio::select! {
                    _ = self.shutdown_rx.recv() => break true,
                    result = self.pause_rx.changed() => {
                        if result.is_err() {
                            break false;
                        }
                    }
                }
                continue;
            }

            tokio::select! {
                _ = self.shutdown_rx.recv() => {
                    info!("{} processor received shutdown signal", self.name);
                    break true;
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
                            if loop_count.is_multiple_of(1000) {
                                debug!("Processor {}: processed {} items", self.name, loop_count);
                            }
                            // Diagnostic: log each received item so we can verify delivery
                            for item in &items {
                                info!("[ProcessorRunner:{}] received item id={}", self.name, item.get_id());
                            }

                            for item in items {
                                let item_id = item.get_id();
                                let avail = semaphore.available_permits();
                                info!("[ProcessorRunner:{}] acquiring permit for id={} available_permits={}", self.name, item_id, avail);
                                let permit = loop {
                                    // 1) Check pause state.
                                    if *self.pause_rx.borrow() {
                                         tokio::select! {
                                             _ = self.shutdown_rx.recv() => {
                                                 info!("[ProcessorRunner:{}] shutdown while paused, dropping id={}", self.name, item_id);
                                                 break 'run true;
                                             }
                                             result = self.pause_rx.changed() => {
                                                 if result.is_err() {
                                                     break 'run false;
                                                 }
                                                 continue;
                                             }
                                         }
                                    }

                                    // 2) Acquire permit or react to pause/shutdown.
                                    tokio::select! {
                                        _ = self.shutdown_rx.recv() => {
                                            info!("[ProcessorRunner:{}] shutdown while acquiring permit, dropping id={}", self.name, item_id);
                                            break 'run true;
                                        }
                                        _ = self.pause_rx.changed() => {
                                            info!("[ProcessorRunner:{}] pause_rx changed while acquiring permit for id={}", self.name, item_id);
                                            continue;
                                        }
                                        res = semaphore.clone().acquire_owned() => {
                                            match res {
                                                Ok(p) => {
                                                    info!("[ProcessorRunner:{}] permit acquired for id={}", self.name, item_id);
                                                    break p;
                                                }
                                                Err(_) => {
                                                    info!("[ProcessorRunner:{}] semaphore closed, dropping id={}", self.name, item_id);
                                                    break 'run false;
                                                }
                                            }
                                        }
                                    }
                                };

                                let execute_fn = execute_fn.clone();
                                let task_id = item.get_id();
                                let metric_label = metric_label.clone();
                                let span_name = format!("{}_processor", metric_label);
                                let inflight = self.inflight_counter.clone();

                                tasks.spawn(async move {
                                    inflight.fetch_add(1, Ordering::Relaxed);
                                    crate::common::metrics::inc_inflight("engine", &metric_label, 1.0);
                                    let _inflight = InflightGuard {
                                        counter: &inflight,
                                        stage: &metric_label,
                                    };
                                    let _permit = permit;
                                    execute_fn(item).await;
                                }.instrument(tracing::info_span!("processor_execution", processor_type = %span_name, item_id = %task_id)));
                            }
                        }
                        None => {
                            info!("{} channel closed", self.name);
                            break false;
                        }
                    }
                }
            }
        };

        if cancelled {
            tasks.abort_all();
        }
        while !tasks.is_empty() {
            tokio::select! {
                _ = self.shutdown_rx.recv(), if !cancelled => {
                    cancelled = true;
                    tasks.abort_all();
                }
                result = tasks.join_next() => {
                    if let Some(Err(error)) = result
                        && !error.is_cancelled()
                    {
                        error!("{} processor task failed: {error}", self.name);
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
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn shutdown_cancels_active_work_while_waiting_for_capacity() {
        let (tx, rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (_pause_tx, pause_rx) = watch::channel(false);
        let inflight = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(());
        let entered = Arc::new(tokio::sync::Notify::new());
        let runner = ProcessorRunner::new("Held", shutdown_rx, pause_rx, 1, inflight.clone());
        let running = tokio::spawn({
            let resource = resource.clone();
            let entered = entered.clone();
            async move {
                runner
                    .run(Arc::new(Mutex::new(rx)), move |_: TestItem| {
                        let resource = resource.clone();
                        let entered = entered.clone();
                        async move {
                            let _resource = resource;
                            entered.notify_one();
                            std::future::pending::<()>().await;
                        }
                    })
                    .await;
            }
        });
        tx.send(TestItem {
            id: "active".into(),
            should_fail: false,
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        tx.send(TestItem {
            id: "waiting".into(),
            should_fail: false,
        })
        .await
        .unwrap();
        // Once the second item leaves the channel, dispatch is waiting for the held permit.
        timeout(Duration::from_secs(1), async {
            while tx.capacity() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            Arc::strong_count(&resource),
            1,
            "active work must release its resources"
        );
        assert_eq!(inflight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_resume() {
        let (_tx, rx) = mpsc::channel::<TestItem>(1);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (_pause_tx, pause_rx) = watch::channel(true);
        let runner = ProcessorRunner::new("Paused", shutdown_rx, pause_rx, 1, Arc::default());
        let running = tokio::spawn(async move {
            runner.run(Arc::new(Mutex::new(rx)), |_| async {}).await;
        });
        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn panicked_work_releases_capacity_and_inflight_count() {
        let (tx, rx) = mpsc::channel(2);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let (_pause_tx, pause_rx) = watch::channel(false);
        let inflight = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let runner = ProcessorRunner::new("Panic", shutdown_rx, pause_rx, 1, inflight.clone());
        for should_fail in [true, false] {
            tx.send(TestItem {
                id: "panic-then-success".into(),
                should_fail,
            })
            .await
            .unwrap();
        }
        drop(tx);
        timeout(
            Duration::from_secs(1),
            runner.run(Arc::new(Mutex::new(rx)), {
                let completed = completed.clone();
                move |item: TestItem| {
                    let completed = completed.clone();
                    async move {
                        assert!(!item.should_fail, "intentional processing panic");
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(completed.load(Ordering::Relaxed), 1);
        assert_eq!(inflight.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn closed_channel_drains_work_but_shutdown_can_cancel_it() {
        for cancel in [false, true] {
            let (tx, rx) = mpsc::channel(1);
            let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
            let (_pause_tx, pause_rx) = watch::channel(false);
            let inflight = Arc::new(AtomicUsize::new(0));
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let finished = Arc::new(AtomicUsize::new(0));
            let runner = ProcessorRunner::new("Drain", shutdown_rx, pause_rx, 1, inflight.clone());
            let running = tokio::spawn({
                let entered = entered.clone();
                let release = release.clone();
                let finished = finished.clone();
                async move {
                    runner
                        .run(Arc::new(Mutex::new(rx)), move |_: TestItem| {
                            let entered = entered.clone();
                            let release = release.clone();
                            let finished = finished.clone();
                            async move {
                                entered.notify_one();
                                release.notified().await;
                                finished.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                        .await;
                }
            });
            tx.send(TestItem {
                id: "last".into(),
                should_fail: false,
            })
            .await
            .unwrap();
            timeout(Duration::from_secs(1), entered.notified())
                .await
                .unwrap();
            drop(tx);
            tokio::task::yield_now().await;
            assert!(
                !running.is_finished(),
                "closing the channel must wait for accepted work"
            );
            if cancel {
                shutdown_tx.send(()).unwrap();
            } else {
                release.notify_one();
            }
            timeout(Duration::from_secs(1), running)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(finished.load(Ordering::Relaxed), usize::from(!cancel));
            assert_eq!(inflight.load(Ordering::Relaxed), 0);
        }
    }

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

        let runner = ProcessorRunner::new(
            "Test",
            shutdown_rx,
            pause_rx,
            2,
            Arc::new(AtomicUsize::new(0)),
        );

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
            TestItem {
                id: "ok".to_string(),
                should_fail: false,
            },
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
            TestItem {
                id: "bad".to_string(),
                should_fail: true,
            },
            move || Box::pin(async move { Ok(()) }),
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
