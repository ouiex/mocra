//! Lightweight telemetry (the metrics crate). Trimmed down and moved over from the host's
//! `common::metrics` — the node dimension is dropped (as a standalone library, dag is unaware of
//! cluster node ids); all other labels are unchanged.

use metrics::{counter, histogram};

pub fn inc_throughput(pipeline: &str, stage: &str, operation: &str, result: &str, delta: u64) {
    counter!(
        "mocra_throughput_total",
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "operation" => operation.to_string(),
        "result" => result.to_string()
    )
    .increment(delta);
}

pub fn observe_latency(pipeline: &str, stage: &str, operation: &str, result: &str, seconds: f64) {
    histogram!(
        "mocra_latency_seconds",
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "operation" => operation.to_string(),
        "result" => result.to_string()
    )
    .record(seconds);
}

pub fn inc_error(pipeline: &str, stage: &str, kind: &str, code: &str, delta: u64) {
    counter!(
        "mocra_errors_total",
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "kind" => kind.to_string(),
        "code" => code.to_string()
    )
    .increment(delta);
}
