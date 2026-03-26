use async_trait::async_trait;
use metrics::counter;
use redis::Script;

use super::types::{DagError, DagFencingStore, TaskPayload};

#[derive(Clone)]
pub struct RedisDagFencingStore {
    client: redis::Client,
    key_prefix: String,
}

impl RedisDagFencingStore {
    pub fn new(client: redis::Client, key_prefix: impl AsRef<str>) -> Self {
        Self {
            client,
            key_prefix: key_prefix.as_ref().to_string(),
        }
    }

    fn latest_token_key(&self, resource: &str) -> String {
        format!("{}:fencing:{}:latest_token", self.key_prefix, resource)
    }
}

#[async_trait]
impl DagFencingStore for RedisDagFencingStore {
    async fn commit(
        &self,
        resource: &str,
        token: u64,
        node_id: &str,
        _payload: &TaskPayload,
    ) -> Result<(), DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::NodeExecutionFailed {
                node_id: node_id.to_string(),
                reason: format!("redis fencing connect failed: {e}"),
            })?;

        let latest_key = self.latest_token_key(resource);
        let script = Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            if not current then
                redis.call('SET', KEYS[1], ARGV[1])
                return 2
            end
            local incoming = tonumber(ARGV[1])
            local existing = tonumber(current)
            if not existing then
                return -1
            end
            if incoming > existing then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            end
            return 0
            "#,
        );

        let accepted: i64 = script
            .key(&latest_key)
            .arg(token as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::NodeExecutionFailed {
                node_id: node_id.to_string(),
                reason: format!("redis fencing commit failed: {e}"),
            })?;

        if accepted == 1 || accepted == 2 {
            counter!(
                "dag_fencing_commit_total",
                "resource" => resource.to_string(),
                "result" => "accepted".to_string()
            )
            .increment(1);
            return Ok(());
        }

        if accepted == -1 {
            let latest_raw: Option<String> = redis::cmd("GET")
                .arg(&latest_key)
                .query_async(&mut conn)
                .await
                .ok();
            counter!(
                "dag_fencing_commit_total",
                "resource" => resource.to_string(),
                "result" => "rejected_invalid_latest".to_string()
            )
            .increment(1);
            return Err(DagError::FencingTokenRejected {
                resource: resource.to_string(),
                token,
                reason: format!(
                    "invalid latest fencing token format in redis: latest_token_raw={}",
                    latest_raw.unwrap_or_else(|| "unknown".to_string())
                ),
            });
        }

        let latest: Option<u64> = redis::cmd("GET")
            .arg(&latest_key)
            .query_async(&mut conn)
            .await
            .ok();

        counter!(
            "dag_fencing_commit_total",
            "resource" => resource.to_string(),
            "result" => "rejected_stale".to_string()
        )
        .increment(1);

        Err(DagError::FencingTokenRejected {
            resource: resource.to_string(),
            token,
            reason: format!(
                "stale token rejected by redis fencing store: latest_token={}",
                latest
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            ),
        })
    }
}
