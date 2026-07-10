use crate::sync::CoordinationBackend;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio::time::sleep;
use uuid::Uuid;
use tracing::debug;

pub struct LeaderElector {
    backend: Option<Arc<dyn CoordinationBackend>>,
    key: String,
    id: String,
    ttl_ms: u64,
    // Provide a signal for when leadership status changes
    leader_signal: watch::Sender<bool>,
    // Keep a receiver to ensure the channel remains open so send() updates the value
    _keep_alive: watch::Receiver<bool>,
}

impl LeaderElector {
    pub fn new(backend: Option<Arc<dyn CoordinationBackend>>, key: String, ttl_ms: u64) -> (Arc<Self>, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        let elector = Arc::new(Self {
            backend,
            key,
            id: Uuid::new_v4().to_string(),
            ttl_ms,
            leader_signal: tx,
            _keep_alive: rx.clone(),
        });
        (elector, rx)
    }

    pub fn is_leader(&self) -> bool {
        let val = *self.leader_signal.borrow();
        debug!("LeaderElector[{}]::is_leader -> {}", self.id, val);
        val
    }

    pub async fn start(self: Arc<Self>) {
        debug!("LeaderElector[{}]::start for key: {}", self.id, self.key);
        if self.backend.is_none() {
            // Local mode: Always leader
            debug!("LeaderElector: No backend, enabling local mode (always leader)");
            let _ = self.leader_signal.send(true);
            return;
        }
        
        let backend = self.backend.as_ref().unwrap();
        let value = self.id.as_bytes();

        loop {
            // 1. Try to acquire lock
            // eprintln!("LeaderElector: Trying to acquire lock for key: {}", self.key);
            match backend.acquire_lock(&self.key, value, self.ttl_ms).await {
                Ok(true) => {
                    // Acquired!
                    debug!("LeaderElector[{}]: Lock ACQUIRED for key: {}", self.id, self.key);
                    if !*self.leader_signal.borrow() {
                        let _ = self.leader_signal.send(true);
                        debug!("LeaderElector[{}]: Signal set to TRUE", self.id);
                    } else {
                        debug!("LeaderElector[{}]: Signal ALREADY TRUE", self.id);
                    }
                    
                    // Maintain leadership (renew lock)
                    // We need to renew before TTL expires. Let's renew at 1/3 of TTL.
                    let renew_interval = Duration::from_millis(self.ttl_ms / 3);
                    let mut fail_count = 0;
                    loop {
                        let sleep_duration = if fail_count > 0 {
                            Duration::from_millis(std::cmp::min(self.ttl_ms / 10, 1000))
                        } else {
                            renew_interval
                        };
                        
                        let sleep_duration = sleep_duration + Duration::from_millis(Self::jitter_ms(100));
                        sleep(sleep_duration).await;

                        match backend.renew_lock(&self.key, value, self.ttl_ms).await {
                            Ok(true) => {
                                // Renewal successful, continue
                                // eprintln!("LeaderElector: Lock renewed");
                                fail_count = 0;
                            }
                            Ok(false) => {
                                // Lost lock!
                                debug!("LeaderElector[{}]: Lock LOST during renewal!", self.id);
                                let _ = self.leader_signal.send(false);
                                break;
                            }
                            Err(e) => {
                                debug!("Leader renewal error: {}", e);
                                fail_count += 1;
                                // If we fail too many times or total time exceeds TTL, we must assume lost.
                                // renew_interval * 3 = TTL. So if we fail 3 times in a row, we are close to expiration.
                                if fail_count >= 3 {
                                    debug!("LeaderElector[{}]: Too many renewal errors, stepping down", self.id);
                                    let _ = self.leader_signal.send(false);
                                    break;
                                }
                                // Retry quickly in next loop iteration
                            }
                        }
                    }
                }
                Ok(false) => {
                    // Failed to acquire. Not leader.
                    if *self.leader_signal.borrow() {
                         debug!("LeaderElector[{}]: Stepped down from leadership", self.id);
                         let _ = self.leader_signal.send(false);
                    } else {
                         // eprintln!("LeaderElector: Failed to acquire lock, retrying...");
                    }
                    // Wait before retrying. 
                    // Optimization: Check more frequently to pick up dropped leadership faster.
                    // But not too fast to spam backend.
                    // 1/10 of TTL or 1s, whichever is smaller?
                    let retry_wait = std::cmp::min(self.ttl_ms / 10, 1000) + Self::jitter_ms(50);
                    sleep(Duration::from_millis(retry_wait)).await;
                }
                Err(e) => {
                    eprintln!("Leader election error: {}", e);
                    sleep(Duration::from_millis(1000 + Self::jitter_ms(250))).await;
                }
            }
        }
    }

    fn jitter_ms(max: u64) -> u64 {
        if max == 0 {
            return 0;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        nanos % max
    }
}
