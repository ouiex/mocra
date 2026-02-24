use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use sysinfo::System;
use metrics::gauge;
use log::{info, debug};
use common::state::State;

/// Periodic host-level metrics collector.
///
/// Captures CPU, memory, and swap usage and emits them as Prometheus metrics.
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

    /// Runs the monitor loop until task cancellation.
    pub async fn run(mut self, _state: Arc<State>) {
        info!("SystemMonitor started with interval {:?}", self.interval);
        
        // Initial refresh warms up system counters.
        self.sys.refresh_cpu_all();
        self.sys.refresh_memory();

        loop {
            sleep(self.interval).await;
            
            // Refresh only the required metrics.
            self.sys.refresh_cpu_all();
            self.sys.refresh_memory();
            
            // Collect metrics.
            let cpu_usage = self.sys.global_cpu_usage();
            let total_memory = self.sys.total_memory();
            let used_memory = self.sys.used_memory();
            let memory_percent = if total_memory > 0 {
                (used_memory as f64 / total_memory as f64) * 100.0
            } else {
                0.0
            };
            
            let total_swap = self.sys.total_swap();
            let used_swap = self.sys.used_swap();
            
            // Export to Prometheus.
            gauge!("system_cpu_usage_percent").set(cpu_usage as f64);
            gauge!("system_memory_used_bytes").set(used_memory as f64);
            gauge!("system_memory_total_bytes").set(total_memory as f64);
            gauge!("system_memory_usage_percent").set(memory_percent);
            gauge!("system_swap_used_bytes").set(used_swap as f64);
            gauge!("system_swap_total_bytes").set(total_swap as f64);
            
            // Debug log for local diagnosis.
            debug!("System Metrics: CPU: {:.2}%, Mem: {:.2}% ({}/{})", 
                cpu_usage, memory_percent, used_memory, total_memory);
                
        }
    }
}
