//! 单节点 Raft 控制面:装配 openraft 节点,命令经 Raft 提交后 apply 到 redb 状态机。
//!
//! 多节点(join / 成员变更 / 网络 RPC)为后续项;此处先跑通单节点共识路径:
//! `client_write(Cmd)` → Raft 复制提交 → `StateMachineStore::apply` → redb。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use openraft::Config;

use crate::cmd::{Cmd, CmdResult};
use crate::control::{ControlError, ControlPlane};
use crate::raft::{MocraRaft, Node, NodeId};
use crate::raft_network::StubNetwork;
use crate::raft_store::{LogStore, StateMachineStore};
use crate::state_machine::StateMachine;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 基于 openraft 的控制面。
#[derive(Clone)]
pub struct RaftControlPlane {
    raft: MocraRaft,
    sm: Arc<StateMachine>,
}

impl RaftControlPlane {
    /// 起一个单节点 Raft 控制面(状态机落在 `sm_path` 的 redb)。
    pub async fn start_single_node(
        node_id: NodeId,
        sm_path: impl AsRef<Path>,
    ) -> Result<Self, ControlError> {
        let sm = Arc::new(StateMachine::open(sm_path)?);

        let config = Config {
            heartbeat_interval: 250,
            election_timeout_min: 299,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| ControlError::Config(e.to_string()))?,
        );

        let log_store = LogStore::default();
        let sm_store = StateMachineStore::new(sm.clone());
        let raft = MocraRaft::new(node_id, config, StubNetwork, log_store, sm_store)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;

        // 初始化单节点集群(自己为唯一投票者)。已初始化则忽略。
        let mut members = BTreeMap::new();
        members.insert(node_id, Node::default());
        let _ = raft.initialize(members).await;

        Ok(Self { raft, sm })
    }

    /// 底层 openraft 句柄(成员变更 / 状态查询)。
    pub fn raft(&self) -> &MocraRaft {
        &self.raft
    }

    /// 把一条命令经 Raft 复制提交,返回状态机应用结果。
    async fn write(&self, cmd: Cmd) -> Result<CmdResult, ControlError> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(resp.data)
    }
}

#[async_trait]
impl ControlPlane for RaftControlPlane {
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), ControlError> {
        self.write(Cmd::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        })
        .await?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ControlError> {
        // 本地读(线性一致读可先 ensure_linearizable 再读 —— 后续)。
        Ok(self.sm.get(key)?)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), ControlError> {
        self.write(Cmd::Delete { key: key.to_vec() }).await?;
        Ok(())
    }

    async fn cas(
        &self,
        key: &[u8],
        expect: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, ControlError> {
        match self
            .write(Cmd::Cas {
                key: key.to_vec(),
                expect: expect.map(|e| e.to_vec()),
                value: value.to_vec(),
            })
            .await?
        {
            CmdResult::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    async fn acquire_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<Option<u64>, ControlError> {
        match self
            .write(Cmd::AcquireLock {
                key: key.to_string(),
                holder: holder.to_string(),
                now_ms: now_ms(),
                ttl_ms,
            })
            .await?
        {
            CmdResult::Fencing(t) => Ok(t),
            _ => Ok(None),
        }
    }

    async fn renew_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<bool, ControlError> {
        match self
            .write(Cmd::RenewLock {
                key: key.to_string(),
                holder: holder.to_string(),
                now_ms: now_ms(),
                ttl_ms,
            })
            .await?
        {
            CmdResult::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    async fn release_lock(&self, key: &str, holder: &str) -> Result<(), ControlError> {
        self.write(Cmd::ReleaseLock {
            key: key.to_string(),
            holder: holder.to_string(),
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn single_node_raft_applies_commands() {
        let dir = tempfile::tempdir().unwrap();
        let cp = RaftControlPlane::start_single_node(1, dir.path().join("sm.redb"))
            .await
            .unwrap();

        // 单节点很快成为 leader。
        cp.raft()
            .wait(Some(Duration::from_secs(10)))
            .state(openraft::ServerState::Leader, "become leader")
            .await
            .unwrap();

        // set / get 经 Raft。
        cp.set(b"k", b"v").await.unwrap();
        assert_eq!(cp.get(b"k").await.unwrap(), Some(b"v".to_vec()));

        // cas 经 Raft。
        assert!(cp.cas(b"k", Some(b"v"), b"v2").await.unwrap());
        assert_eq!(cp.get(b"k").await.unwrap(), Some(b"v2".to_vec()));

        // 分布式锁 + fencing 经 Raft。
        assert_eq!(cp.acquire_lock("lock", "a", 5000).await.unwrap(), Some(1));
        assert_eq!(cp.acquire_lock("lock", "b", 5000).await.unwrap(), None);
    }
}
