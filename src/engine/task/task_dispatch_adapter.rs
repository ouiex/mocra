use async_trait::async_trait;
use rmp_serde as rmps;
use uuid::Uuid;

use crate::common::interface::storage::{BlobStorage, Offloadable};
use crate::common::model::message::TaskEvent;
use crate::common::model::{
    ExecutionMeta, NodeInput, PayloadCodec, Prioritizable, Priority, RoutingMeta,
    TaskDispatchEnvelope, TypedEnvelope,
};
use crate::common::processors::processor::{ProcessorContext, RetryPolicy};
use crate::errors::{Error, ErrorKind, Result};
use crate::queue::Identifiable;
use std::sync::Arc;

const TASK_EVENT_SCHEMA_ID: &str = "legacy.task_event";
const TASK_INGRESS_NODE: &str = "task_ingress";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn transport_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Queue, Some(error))
}

pub fn build_task_dispatch(task: &TaskEvent, namespace: impl Into<String>) -> Result<TaskDispatchEnvelope> {
    let payload = TypedEnvelope::new(
        TASK_EVENT_SCHEMA_ID,
        1,
        PayloadCodec::MsgPack,
        rmps::to_vec(task).map_err(transport_error)?,
    );
    let timestamp = now_ms();

    Ok(TaskDispatchEnvelope::new(
        RoutingMeta {
            namespace: namespace.into(),
            account: task.account.clone(),
            platform: task.platform.clone(),
            module: task
                .module
                .as_ref()
                .and_then(|modules| modules.first())
                .cloned()
                .unwrap_or_else(|| "*".to_string()),
            node_key: TASK_INGRESS_NODE.to_string(),
            run_id: task.run_id,
            request_id: Uuid::now_v7(),
            parent_request_id: None,
            priority: task.priority,
        },
        ExecutionMeta {
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
            ..ExecutionMeta::default()
        },
        NodeInput::new(TASK_INGRESS_NODE, payload),
    ))
}

pub fn decode_task_dispatch(dispatch: TaskDispatchEnvelope) -> Result<TaskEvent> {
    match dispatch.input.payload.codec {
        PayloadCodec::MsgPack => {
            rmps::from_slice(&dispatch.input.payload.bytes).map_err(transport_error)
        }
        PayloadCodec::Json => {
            serde_json::from_slice(&dispatch.input.payload.bytes).map_err(transport_error)
        }
    }
}

pub fn processor_context_from_dispatch(dispatch: &TaskDispatchEnvelope) -> ProcessorContext {
    let mut context = ProcessorContext::default();
    if dispatch.exec.created_at_ms > 0 {
        context.created_at = (dispatch.exec.created_at_ms / 1000) as u64;
    }

    let retry_count = dispatch.exec.task_retry_count.max(dispatch.exec.retry_count);
    if retry_count > 0 {
        context = context.with_retry_policy(RetryPolicy {
            current_retry: retry_count,
            ..RetryPolicy::default()
        });
    }
    context
}

impl Identifiable for TaskDispatchEnvelope {
    fn get_id(&self) -> String {
        self.routing.request_id.to_string()
    }
}

impl Prioritizable for TaskDispatchEnvelope {
    fn get_priority(&self) -> Priority {
        self.routing.priority
    }
}

#[async_trait]
impl Offloadable for TaskDispatchEnvelope {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }

    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> Result<()> {
        Ok(())
    }

    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_dispatch_round_trips_task_event() {
        let task = TaskEvent {
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: Some(vec!["catalog".to_string()]),
            priority: Priority::High,
            run_id: Uuid::now_v7(),
        };

        let dispatch = build_task_dispatch(&task, "demo").expect("dispatch should build");
        assert_eq!(dispatch.routing.namespace, "demo");
        assert_eq!(dispatch.routing.node_key, TASK_INGRESS_NODE);
        assert_eq!(dispatch.get_priority(), Priority::High);

        let decoded = decode_task_dispatch(dispatch.clone()).expect("dispatch should decode");
        assert_eq!(decoded.account, task.account);
        assert_eq!(decoded.platform, task.platform);
        assert_eq!(decoded.module, task.module);

        let context = processor_context_from_dispatch(&dispatch);
        assert!(context.retry_policy.is_none());
    }
}
