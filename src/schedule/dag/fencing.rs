use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

use super::types::{DagError, DagFencingStore, TaskPayload};

#[derive(Default)]
pub struct InMemoryDagFencingStore {
    latest_token_by_resource: DashMap<String, u64>,
}

#[async_trait]
impl DagFencingStore for InMemoryDagFencingStore {
    async fn commit(
        &self,
        resource: &str,
        token: u64,
        _node_id: &str,
        _payload: &TaskPayload,
    ) -> Result<(), DagError> {
        match self.latest_token_by_resource.entry(resource.to_string()) {
            Entry::Occupied(mut slot) => {
                if token <= *slot.get() {
                    return Err(DagError::FencingTokenRejected {
                        resource: resource.to_string(),
                        token,
                        reason: format!(
                            "stale token rejected by in-memory fencing store: latest_token={}",
                            slot.get()
                        ),
                    });
                }
                slot.insert(token);
                Ok(())
            }
            Entry::Vacant(slot) => {
                slot.insert(token);
                Ok(())
            }
        }
    }
}
