use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::sync::OnceLock;

static METRICS_INIT: OnceLock<()> = OnceLock::new();
static METRICS_SCOPE: OnceLock<MetricsScope> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsScope {
    namespace: String,
    node_id: String,
    deployment_mode: String,
}

impl MetricsScope {
    pub fn new(
        namespace: impl Into<String>,
        node_id: impl Into<String>,
        single_node_deployment: bool,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            node_id: node_id.into(),
            deployment_mode: if single_node_deployment {
                "single_node".to_string()
            } else {
                "distributed".to_string()
            },
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn deployment_mode(&self) -> &str {
        &self.deployment_mode
    }
}

fn current_scope() -> MetricsScope {
    METRICS_SCOPE
        .get()
        .cloned()
        .unwrap_or_else(|| MetricsScope::new("default", "standalone-node", true))
}

/// Registers canonical metric descriptions once per process.
pub fn init_metrics(scope: MetricsScope) {
    let _ = METRICS_SCOPE.set(scope);
    METRICS_INIT.get_or_init(|| {
        describe_gauge!(
            "mocra_node_up",
            "Node liveness: 1 means node is up for the current namespace and node_id."
        );
        describe_gauge!(
            "mocra_component_health",
            "Per-component health status (1 healthy, 0 unhealthy)."
        );
        describe_gauge!(
            "mocra_resource_usage",
            "Resource usage by resource type for this node."
        );
        describe_gauge!(
            "mocra_stage_backlog",
            "Backlog depth by pipeline and queue for this node."
        );
        describe_gauge!(
            "mocra_stage_inflight",
            "In-flight workload by stage for this node."
        );

        describe_counter!(
            "mocra_stage_events_total",
            "Unified stage event counter by pipeline/stage/action/result."
        );
        describe_histogram!(
            "mocra_stage_duration_seconds",
            "Unified latency histogram by pipeline/stage/action/result."
        );
        describe_counter!(
            "mocra_stage_errors_total",
            "Unified error counter by pipeline/stage/error class/code."
        );

        describe_counter!(
            "mocra_policy_decisions_total",
            "Policy decision count by domain/event/action."
        );
    });
}

pub fn set_node_up(up: bool) {
    let scope = current_scope();
    gauge!(
        "mocra_node_up",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string()
    )
    .set(if up { 1.0 } else { 0.0 });
}

pub fn set_component_health(component: &str, healthy: bool) {
    let scope = current_scope();
    gauge!(
        "mocra_component_health",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => component.to_string(),
        "deployment_mode" => scope.deployment_mode().to_string()
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

pub fn observe_resource(resource: &str, value: f64) {
    let scope = current_scope();
    gauge!(
        "mocra_resource_usage",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "resource" => resource.to_string()
    )
    .set(value);
}

pub fn set_backlog(pipeline: &str, queue: &str, depth: f64) {
    let scope = current_scope();
    gauge!(
        "mocra_stage_backlog",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "queue",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "pipeline" => pipeline.to_string(),
        "queue" => queue.to_string()
    )
    .set(depth);
}

pub fn inc_inflight(pipeline: &str, stage: &str, delta: f64) {
    let scope = current_scope();
    gauge!(
        "mocra_stage_inflight",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string()
    )
    .increment(delta);
}

pub fn dec_inflight(pipeline: &str, stage: &str, delta: f64) {
    let scope = current_scope();
    gauge!(
        "mocra_stage_inflight",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string()
    )
    .decrement(delta);
}

pub fn inc_throughput(pipeline: &str, stage: &str, operation: &str, result: &str, delta: u64) {
    let scope = current_scope();
    counter!(
        "mocra_stage_events_total",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "action" => operation.to_string(),
        "result" => result.to_string()
    )
    .increment(delta);
}

pub fn observe_latency(pipeline: &str, stage: &str, operation: &str, result: &str, seconds: f64) {
    let scope = current_scope();
    histogram!(
        "mocra_stage_duration_seconds",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "action" => operation.to_string(),
        "result" => result.to_string()
    )
    .record(seconds);
}

pub fn inc_error(pipeline: &str, stage: &str, kind: &str, code: &str, delta: u64) {
    let scope = current_scope();
    counter!(
        "mocra_stage_errors_total",
        "namespace" => scope.namespace().to_string(),
        "node_id" => scope.node_id().to_string(),
        "component" => "runtime",
        "deployment_mode" => scope.deployment_mode().to_string(),
        "pipeline" => pipeline.to_string(),
        "stage" => stage.to_string(),
        "error_class" => kind.to_string(),
        "error_code" => code.to_string()
    )
    .increment(delta);
}

#[cfg(test)]
mod tests {
    use super::MetricsScope;

    #[test]
    fn metrics_scope_tracks_namespace_node_and_mode() {
        let distributed = MetricsScope::new("demo", "node-a", false);
        assert_eq!(distributed.namespace(), "demo");
        assert_eq!(distributed.node_id(), "node-a");
        assert_eq!(distributed.deployment_mode(), "distributed");

        let single_node = MetricsScope::new("demo", "node-b", true);
        assert_eq!(single_node.deployment_mode(), "single_node");
    }
}
