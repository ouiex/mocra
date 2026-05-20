use crate::common::state::State;
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::engine::task::task_manager::TaskManager;
use crate::queue::QueueManager;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;

#[derive(Clone)]
pub struct ApiState {
    pub(crate) queue_manager: Arc<QueueManager>,
    pub(crate) prometheus_handle: Option<PrometheusHandle>,
    pub(crate) state: Arc<State>,
    pub(crate) task_manager: Arc<TaskManager>,
    pub(crate) profile_store: Arc<ProfileControlPlaneStore>,
}

impl ApiState {
    pub fn new(
        queue_manager: Arc<QueueManager>,
        prometheus_handle: Option<PrometheusHandle>,
        state: Arc<State>,
        task_manager: Arc<TaskManager>,
    ) -> Self {
        let profile_store = state.profile_store.clone();
        Self {
            queue_manager,
            prometheus_handle,
            state,
            task_manager,
            profile_store,
        }
    }
}
