use queue::QueueManager;
use std::sync::Arc;
use metrics_exporter_prometheus::PrometheusHandle;
use common::state::State;
use common::registry::NodeRegistry;

#[derive(Clone)]
pub struct ApiState {
    pub(crate) queue_manager: Arc<QueueManager>,
    pub(crate) prometheus_handle: Option<PrometheusHandle>,
    pub(crate) state: Arc<State>,
    pub(crate) node_registry: Arc<NodeRegistry>,
}
