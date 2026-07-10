use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use sysinfo::System;
use log::{info, debug};
use once_cell::sync::Lazy;
use serde::Serialize;

/// 主机资源快照(供 dashboard `GET /observability/system` 消费)。
#[derive(Debug, Clone, Serialize)]
pub struct SystemSnapshot {
    pub cpu_usage_percent: f64,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_usage_percent: f64,
    pub swap_used_bytes: u64,
    pub swap_total_bytes: u64,
    /// 采集时刻(UNIX 毫秒)。
    pub updated_at_ms: u64,
}

static SYSTEM_SNAPSHOT: Lazy<RwLock<Option<SystemSnapshot>>> = Lazy::new(|| RwLock::new(None));

/// 最近一次主机资源快照(尚未采集则 `None`)。
pub fn latest_system_snapshot() -> Option<SystemSnapshot> {
    SYSTEM_SNAPSHOT.read().ok().and_then(|g| g.clone())
}

fn store_snapshot(snap: SystemSnapshot) {
    if let Ok(mut g) = SYSTEM_SNAPSHOT.write() {
        *g = Some(snap);
    }
}

/// Periodic host-level metrics collector.
///
/// Captures CPU, memory, and swap usage; emits them as Prometheus metrics **and**
/// keeps the latest snapshot queryable via [`latest_system_snapshot`] (dashboard API).
pub struct SystemMonitor {
    sys: System,
    interval: Duration,
}

impl SystemMonitor {
    /// Creates a monitor with a fixed polling interval in seconds.
    pub fn new(interval_secs: u64) -> Self {
        Self {
            sys: System::new(),
            interval: Duration::from_secs(interval_secs),
        }
    }

    fn collect(&self) -> SystemSnapshot {
        let cpu_usage = self.sys.global_cpu_usage() as f64;
        let total_memory = self.sys.total_memory();
        let used_memory = self.sys.used_memory();
        let memory_percent = if total_memory > 0 {
            (used_memory as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };
        SystemSnapshot {
            cpu_usage_percent: cpu_usage,
            memory_used_bytes: used_memory,
            memory_total_bytes: total_memory,
            memory_usage_percent: memory_percent,
            swap_used_bytes: self.sys.used_swap(),
            swap_total_bytes: self.sys.total_swap(),
            updated_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    /// Runs the monitor loop until task cancellation.
    pub async fn run(mut self) {
        info!("SystemMonitor started with interval {:?}", self.interval);

        // Initial refresh warms up system counters, then publish an immediate snapshot
        // so the dashboard has data without waiting a full interval.
        self.sys.refresh_cpu_all();
        self.sys.refresh_memory();
        store_snapshot(self.collect());

        loop {
            sleep(self.interval).await;

            self.sys.refresh_cpu_all();
            self.sys.refresh_memory();

            let snap = self.collect();

            crate::common::metrics::set_component_health("system_monitor", true);
            crate::common::metrics::observe_resource("cpu_usage_percent", snap.cpu_usage_percent);
            crate::common::metrics::observe_resource("memory_used_bytes", snap.memory_used_bytes as f64);
            crate::common::metrics::observe_resource("memory_total_bytes", snap.memory_total_bytes as f64);
            crate::common::metrics::observe_resource("memory_usage_percent", snap.memory_usage_percent);
            crate::common::metrics::observe_resource("swap_used_bytes", snap.swap_used_bytes as f64);
            crate::common::metrics::observe_resource("swap_total_bytes", snap.swap_total_bytes as f64);

            debug!(
                "System Metrics: CPU: {:.2}%, Mem: {:.2}% ({}/{})",
                snap.cpu_usage_percent, snap.memory_usage_percent, snap.memory_used_bytes, snap.memory_total_bytes
            );

            store_snapshot(snap);
        }
    }
}
