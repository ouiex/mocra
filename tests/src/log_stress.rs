use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use utils::logger::{self, LogOutputConfig, LogSender, LoggerConfig};

#[tokio::main]
async fn main() {
    let total = arg_value("--total").unwrap_or(100_000);
    let concurrency = arg_value("--concurrency").unwrap_or(4);
    let buffer = arg_value("--buffer").unwrap_or(10_000);

    let (sender, mut receiver) = mpsc::channel(buffer);
    let log_sender = LogSender::with_capacity(sender.clone(), "info", buffer);
    let _ = logger::set_log_sender(log_sender);

    let config = LoggerConfig::new()
        .with_level("info")
        .with_output(LogOutputConfig::Mq {
            level: Some("info".to_string()),
        });
    let _ = logger::init_logger(config).await;

    let consumed = Arc::new(AtomicUsize::new(0));
    let consumed_clone = Arc::clone(&consumed);
    let max_lag = Arc::new(AtomicUsize::new(0));
    let max_lag_clone = Arc::clone(&max_lag);
    let sender_clone = sender.clone();
    let consumer_handle = tokio::spawn(async move {
        while receiver.recv().await.is_some() {
            consumed_clone.fetch_add(1, Ordering::Relaxed);
        }
    });

    let sampler_handle = tokio::spawn(async move {
        loop {
            let lag = buffer.saturating_sub(sender_clone.capacity());
            let mut current_max = max_lag_clone.load(Ordering::Relaxed);
            while lag > current_max {
                match max_lag_clone.compare_exchange(
                    current_max,
                    lag,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(value) => current_max = value,
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });

    let start = Instant::now();
    let per_worker = total / concurrency;
    let remainder = total % concurrency;
    let mut handles = Vec::new();

    for worker in 0..concurrency {
        let count = per_worker + if worker == 0 { remainder } else { 0 };
        handles.push(tokio::spawn(async move {
            for idx in 0..count {
                tracing::info!(task_id = "log_stress", status = "ok", "log item {}\n", idx);
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    sampler_handle.abort();
    drop(sender);
    logger::clear_log_sender();
    let _ = consumer_handle.await;

    let elapsed = start.elapsed();
    let produced = total as f64;
    let consumed = consumed.load(Ordering::Relaxed) as f64;
    let dropped = (produced - consumed).max(0.0);
    let drop_rate = if produced > 0.0 { dropped / produced } else { 0.0 };
    let max_lag = max_lag.load(Ordering::Relaxed);
    let seconds = elapsed.as_secs_f64().max(0.0001);
    let rate = produced / seconds;

    println!(
        "log_stress finished: total={} consumed={} dropped={} drop_rate={:.4} max_lag={} elapsed_ms={} rate={:.0}/s",
        total,
        consumed as usize,
        dropped as usize,
        drop_rate,
        max_lag,
        elapsed.as_millis(),
        rate
    );
}

fn arg_value(flag: &str) -> Option<usize> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == flag {
            if let Some(value) = args.next() {
                return value.parse::<usize>().ok();
            }
        }
    }
    None
}
