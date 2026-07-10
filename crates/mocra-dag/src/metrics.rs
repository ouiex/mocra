//! 轻量遥测(metrics crate)。从宿主 `common::metrics` 精简迁入 —— 去掉 node 维度
//! (dag 作为独立库不感知集群节点 id),其余标签一致。

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
