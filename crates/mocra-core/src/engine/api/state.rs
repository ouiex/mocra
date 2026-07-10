use crate::common::registry::NodeRegistry;
use crate::common::state::State;
use crate::queue::QueueManager;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;

#[derive(Clone)]
pub struct ApiState {
    pub(crate) queue_manager: Arc<QueueManager>,
    pub(crate) prometheus_handle: Option<PrometheusHandle>,
    pub(crate) state: Arc<State>,
    pub(crate) node_registry: Arc<NodeRegistry>,
}
