use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::sync::OnceLock;

static METRICS_INIT: OnceLock<()> = OnceLock::new();
static NODE_ID: OnceLock<String> = OnceLock::new();

fn current_node_id() -> String {
    NODE_ID
        .get()
        .cloned()
        .unwrap_or_else(|| "standalone-node".to_string())
}

/// Registers canonical metric descriptions once per process.
pub fn init_metrics(node_id: &str) {
    let _ = NODE_ID.set(node_id.to_string());
    METRICS_INIT.get_or_init(|| {
        describe_gauge!("mocra_node_up", "Node liveness: 1 means node is up.");
        describe_gauge!("mocra_component_health", "Per-component health status (1 healthy, 0 unhealthy).");
        describe_gauge!("mocra_resource_usage", "Resource usage by resource type for this node.");
        describe_gauge!("mocra_backlog_depth", "Backlog depth by queue/topic for this node.");
        describe_gauge!("mocra_inflight", "In-flight workload by stage for this node.");

        describe_counter!("mocra_throughput_total", "Unified throughput counter by pipeline/stage/operation/result.");
        describe_histogram!("mocra_latency_seconds", "Unified latency histogram by pipeline/stage/operation/result.");
        describe_counter!("mocra_errors_total", "Unified error counter by pipeline/stage/error kind/code.");

        describe_counter!("mocra_policy_decisions_total", "Policy decision count by domain/event/action.");
    });
}

pub fn set_node_up(up: bool) {
    gauge!("mocra_node_up", "node" => current_node_id()).set(if up { 1.0 } else { 0.0 });
}

pub fn set_component_health(component: &str, healthy: bool) {
    gauge!(
        "mocra_component_health",
        "node" => current_node_id(),
        "component" => component.to_string()
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

pub fn observe_resource(resource: &str, value: f64) {
    gauge!(
        "mocra_resource_usage",
        "node" => current_node_id(),
        "resource" => resource.to_string()
    )
    .set(value);
}

pub fn set_backlog(pipeline: &str, queue: &str, depth: f64) {
    gauge!(
        "mocra_backlog_depth",
        "node" => current_node_id(),
        "pipeline" => pipeline.to_string(),
        "queue" => queue.to_string()
    )
    .set(depth);
}

pub fn inc_inflight(pipeline: &str, stage: &str, delta: f64) {
    gauge!(
        "mocra_inflight",
        "node" => current_node_id(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string()
    )
    .increment(delta);
}

pub fn dec_inflight(pipeline: &str, stage: &str, delta: f64) {
    gauge!(
        "mocra_inflight",
        "node" => current_node_id(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string()
    )
    .decrement(delta);
}

pub fn inc_throughput(pipeline: &str, stage: &str, operation: &str, result: &str, delta: u64) {
    counter!(
        "mocra_throughput_total",
        "node" => current_node_id(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "operation" => operation.to_string(),
        "result" => result.to_string()
    )
    .increment(delta);
}

pub fn observe_latency(
    pipeline: &str,
    stage: &str,
    operation: &str,
    result: &str,
    seconds: f64,
) {
    histogram!(
        "mocra_latency_seconds",
        "node" => current_node_id(),
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
        "node" => current_node_id(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "kind" => kind.to_string(),
        "code" => code.to_string()
    )
    .increment(delta);
}
