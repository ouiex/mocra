use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use log::warn;
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use crate::common::model::config::Config;
use crate::common::model::control_plane_profile::{DefaultConfigScope, LockRecord};
use crate::common::model::{
    ControlPlaneApply, ControlPlaneRaftCommand, DefaultConfigUpsert, MiddlewareBinding,
    MiddlewareType, MiddlewareUpsert, PipelineStage, ProfileValidationError, ResolvedCommonConfig,
    StatusEntry, TaskProfileIdentity, TaskProfileSnapshot, TaskProfileUpsert, TaskStatus,
    TypedEnvelope,
};
use crate::common::registry::NodeInfo;
use crate::sync::RaftRuntime;

const CF_RAFT_LOG: &str = "raft_log";
const CF_STATE_MACHINE: &str = "state_machine";
const CF_RAFT_META: &str = "raft_meta";
const META_LAST_LOG_INDEX: &[u8] = b"last_log_index";
const META_LAST_APPLIED_INDEX: &[u8] = b"last_applied_index";

#[derive(Debug, Error)]
pub enum ControlPlaneStoreError {
    #[error(transparent)]
    Validation(#[from] ProfileValidationError),
    #[error(transparent)]
    Storage(#[from] rocksdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("raft proposal failed: {0}")]
    Raft(String),
}

#[derive(Debug, Clone, Default)]
pub struct TaskProfilePatch {
    pub enabled: Option<bool>,
    pub common: Option<ResolvedCommonConfig>,
    pub node_configs: Option<BTreeMap<String, TypedEnvelope>>,
    pub download_middleware: Option<Vec<MiddlewareBinding>>,
    pub data_middleware: Option<Vec<MiddlewareBinding>>,
    pub middleware_configs: Option<BTreeMap<String, TypedEnvelope>>,
    pub debug_layers_json: Option<serde_json::Value>,
    pub updated_by: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DefaultConfigPatch {
    pub enabled: Option<bool>,
    pub config: Option<TypedEnvelope>,
}

#[derive(Debug, Clone, Default)]
pub struct MiddlewarePatch {
    pub enabled: Option<bool>,
    pub config: Option<TypedEnvelope>,
    pub weight: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTaskProfileRecord {
    pub snapshot: TaskProfileSnapshot,
    pub raft_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDefaultConfig {
    pub default: DefaultConfigUpsert,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMiddleware {
    pub middleware: MiddlewareUpsert,
    pub enabled: bool,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNodeRecord {
    pub node: NodeInfo,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StoredResponseCacheOwnerRecord {
    pub cache_key: String,
    pub owner_namespace: String,
    pub owner_node_id: Option<String>,
    pub owner_api_base_url: Option<String>,
    pub expires_at: Option<i64>,
    pub version: u64,
    pub updated_at: i64,
}

impl<'de> Deserialize<'de> for StoredResponseCacheOwnerRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct CurrentStoredResponseCacheOwnerRecord {
            cache_key: String,
            owner_namespace: String,
            owner_node_id: Option<String>,
            owner_api_base_url: Option<String>,
            expires_at: Option<i64>,
            version: u64,
            updated_at: i64,
        }

        #[derive(Deserialize)]
        struct IntermediateStoredResponseCacheOwnerRecord {
            cache_key: String,
            owner_namespace: String,
            owner_node_id: Option<String>,
            expires_at: Option<i64>,
            version: u64,
            updated_at: i64,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum CompatibleStoredResponseCacheOwnerRecord {
            Current(CurrentStoredResponseCacheOwnerRecord),
            Intermediate(IntermediateStoredResponseCacheOwnerRecord),
        }

        match CompatibleStoredResponseCacheOwnerRecord::deserialize(deserializer)? {
            CompatibleStoredResponseCacheOwnerRecord::Current(record) => Ok(Self {
                cache_key: record.cache_key,
                owner_namespace: record.owner_namespace,
                owner_node_id: record.owner_node_id,
                owner_api_base_url: record.owner_api_base_url,
                expires_at: record.expires_at,
                version: record.version,
                updated_at: record.updated_at,
            }),
            CompatibleStoredResponseCacheOwnerRecord::Intermediate(record) => Ok(Self {
                cache_key: record.cache_key,
                owner_namespace: record.owner_namespace,
                owner_node_id: record.owner_node_id,
                owner_api_base_url: None,
                expires_at: record.expires_at,
                version: record.version,
                updated_at: record.updated_at,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPauseState {
    pub paused: bool,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredModuleLock {
    pub locked_at: u64,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTerminationMarker {
    pub terminated_at: u64,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredStatusCounter {
    pub value: i64,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredStatusEntryRecord {
    pub entry: StatusEntry,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStageIndexRecord {
    pub task_id: String,
    pub updated_at: i64,
}

/// Cache KV entry stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCacheEntry {
    pub value: Vec<u8>,
    pub expires_at: Option<i64>,
    pub version: u64,
    pub updated_at: i64,
}

/// Cache counter stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCacheCounter {
    pub value: i64,
    pub expires_at: Option<i64>,
    pub version: u64,
    pub updated_at: i64,
}

/// ZSet entry stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredZSetEntry {
    pub score: f64,
    pub member: Vec<u8>,
    pub version: u64,
    pub updated_at: i64,
}

/// Command outcome stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredOutcome {
    pub success: bool,
    pub value: i64,
    pub updated_at_ms: i64,
}

/// Lock record stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLockRecord {
    pub owner_token: String,
    pub locked_at_ms: i64,
    pub expires_at_ms: i64,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug)]
pub struct ProfileControlPlaneStore {
    db: Arc<DB>,
    namespace: String,
    data_dir: PathBuf,
    write_lock: Mutex<()>,
    raft_runtime: Mutex<Option<std::sync::Weak<RaftRuntime>>>,
    pause_state_tx: watch::Sender<bool>,
    _pause_state_rx_keep_alive: watch::Receiver<bool>,
    #[allow(dead_code)]
    ephemeral_dir: Option<tempfile::TempDir>,
}

impl ProfileControlPlaneStore {
    pub fn new() -> Arc<Self> {
        Self::open("default", Path::new("./raft_data").join("default"))
            .expect("open default control-plane store")
    }

    pub fn from_config(config: &Config) -> Result<Arc<Self>, ControlPlaneStoreError> {
        let base_dir = config
            .raft
            .as_ref()
            .and_then(|raft| raft.data_dir.clone())
            .unwrap_or_else(|| "./raft_data".to_string());
        let data_dir = Path::new(&base_dir)
            .join(&config.name)
            .join("control_plane");
        Self::open(config.name.clone(), data_dir)
    }

    pub fn open(
        namespace: impl Into<String>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Arc<Self>, ControlPlaneStoreError> {
        let namespace = namespace.into();
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        let db = open_db(&data_dir)?;
        let (pause_state_tx, pause_state_rx_keep_alive) =
            watch::channel(read_initial_pause_state(&db, &namespace)?);
        Ok(Arc::new(Self {
            db: Arc::new(db),
            namespace,
            data_dir,
            write_lock: Mutex::new(()),
            raft_runtime: Mutex::new(None),
            pause_state_tx,
            _pause_state_rx_keep_alive: pause_state_rx_keep_alive,
            ephemeral_dir: None,
        }))
    }

    #[cfg(test)]
    pub fn open_temp(namespace: &str) -> Result<Self, ControlPlaneStoreError> {
        let ephemeral_dir = tempfile::tempdir()?;
        let data_dir = ephemeral_dir.path().join(namespace);
        fs::create_dir_all(&data_dir)?;
        let db = open_db(&data_dir)?;
        let (pause_state_tx, pause_state_rx_keep_alive) =
            watch::channel(read_initial_pause_state(&db, namespace)?);
        Ok(Self {
            db: Arc::new(db),
            namespace: namespace.to_string(),
            data_dir,
            write_lock: Mutex::new(()),
            raft_runtime: Mutex::new(None),
            pause_state_tx,
            _pause_state_rx_keep_alive: pause_state_rx_keep_alive,
            ephemeral_dir: Some(ephemeral_dir),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn subscribe_pause_state(&self) -> watch::Receiver<bool> {
        self.pause_state_tx.subscribe()
    }

    pub fn attach_raft_runtime(&self, runtime: Arc<RaftRuntime>) {
        *self
            .raft_runtime
            .lock()
            .expect("control-plane raft runtime lock poisoned") = Some(Arc::downgrade(&runtime));
    }

    fn raft_runtime(&self) -> Option<Arc<RaftRuntime>> {
        self.raft_runtime
            .lock()
            .expect("control-plane raft runtime lock poisoned")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
    }

    pub fn last_applied_index(&self) -> u64 {
        self.read_u64_meta(META_LAST_APPLIED_INDEX).unwrap_or(0)
    }

    pub fn list_profiles(
        &self,
        namespace: &str,
        account: Option<&str>,
        platform: Option<&str>,
        module: Option<&str>,
    ) -> Vec<TaskProfileSnapshot> {
        self.list_profile_records(namespace, account, platform, module)
            .into_iter()
            .map(|record| record.snapshot)
            .collect()
    }

    pub fn list_profile_records(
        &self,
        namespace: &str,
        account: Option<&str>,
        platform: Option<&str>,
        module: Option<&str>,
    ) -> Vec<StoredTaskProfileRecord> {
        let prefix = format!("{}:profile:", namespace);
        let mut records: Vec<_> = self
            .scan_prefix::<StoredTaskProfileRecord>(&prefix)
            .into_iter()
            .filter(|record| {
                account.is_none_or(|value| record.snapshot.account == value)
                    && platform.is_none_or(|value| record.snapshot.platform == value)
                    && module.is_none_or(|value| record.snapshot.module == value)
            })
            .collect();
        records.sort_by(|left, right| {
            left.snapshot
                .profile_key()
                .cmp(&right.snapshot.profile_key())
        });
        records
    }

    pub fn get_profile(
        &self,
        namespace: &str,
        account: &str,
        platform: &str,
        module: &str,
    ) -> Option<TaskProfileSnapshot> {
        self.get_profile_record(namespace, account, platform, module)
            .map(|record| record.snapshot)
    }

    pub fn get_profile_record(
        &self,
        namespace: &str,
        account: &str,
        platform: &str,
        module: &str,
    ) -> Option<StoredTaskProfileRecord> {
        let key = profile_key(namespace, account, platform, module);
        self.get_state(&key)
    }

    pub async fn upsert_profile(
        &self,
        upsert: TaskProfileUpsert,
    ) -> Result<TaskProfileSnapshot, ControlPlaneStoreError> {
        let snapshot = upsert.into_snapshot(
            self.last_applied_index().saturating_add(1),
            current_time_ms(),
        )?;
        self.submit_command(ControlPlaneRaftCommand::UpsertTaskProfile {
            snapshot: snapshot.clone(),
        })
        .await?;
        Ok(self
            .get_profile_record(
                &snapshot.namespace,
                &snapshot.account,
                &snapshot.platform,
                &snapshot.module,
            )
            .map(|record| record.snapshot)
            .unwrap_or(snapshot))
    }

    pub async fn set_module_lock(
        &self,
        module_id: &str,
        locked_at: u64,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::SetModuleLock {
            namespace: self.namespace.clone(),
            module_id: module_id.to_string(),
            locked_at,
        })
        .await
        .map(|_| ())
    }

    pub fn get_module_lock(&self, module_id: &str) -> Option<StoredModuleLock> {
        self.get_state::<StoredModuleLock>(&format!("{}:module_lock:{}", self.namespace, module_id))
    }

    pub async fn release_module_lock(&self, module_id: &str) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::RemoveModuleLock {
            namespace: self.namespace.clone(),
            module_id: module_id.to_string(),
        })
        .await
        .map(|_| ())
    }

    pub async fn patch_profile(
        &self,
        identity: TaskProfileIdentity,
        patch: TaskProfilePatch,
    ) -> Result<Option<TaskProfileSnapshot>, ControlPlaneStoreError> {
        let Some(existing) = self.get_profile_record(
            &identity.namespace,
            &identity.account,
            &identity.platform,
            &identity.module,
        ) else {
            return Ok(None);
        };

        let snapshot = TaskProfileUpsert {
            identity,
            enabled: patch.enabled.unwrap_or(existing.snapshot.enabled),
            common: patch.common.unwrap_or(existing.snapshot.common),
            node_configs: patch.node_configs.unwrap_or(existing.snapshot.node_configs),
            download_middleware: patch
                .download_middleware
                .unwrap_or(existing.snapshot.download_middleware),
            data_middleware: patch
                .data_middleware
                .unwrap_or(existing.snapshot.data_middleware),
            middleware_configs: patch
                .middleware_configs
                .unwrap_or(existing.snapshot.middleware_configs),
            debug_layers_json: patch
                .debug_layers_json
                .or(existing.snapshot.debug_layers_json),
            updated_by: patch.updated_by.unwrap_or(existing.snapshot.updated_by),
        }
        .into_snapshot(
            self.last_applied_index().saturating_add(1),
            current_time_ms(),
        )?;

        self.submit_command(ControlPlaneRaftCommand::UpsertTaskProfile {
            snapshot: snapshot.clone(),
        })
        .await?;
        Ok(self
            .get_profile_record(
                &snapshot.namespace,
                &snapshot.account,
                &snapshot.platform,
                &snapshot.module,
            )
            .map(|record| record.snapshot))
    }

    pub async fn disable_profile(
        &self,
        identity: TaskProfileIdentity,
    ) -> Result<bool, ControlPlaneStoreError> {
        if self
            .get_profile_record(
                &identity.namespace,
                &identity.account,
                &identity.platform,
                &identity.module,
            )
            .is_none()
        {
            return Ok(false);
        }

        self.submit_command(ControlPlaneRaftCommand::DisableTaskProfile { identity })
            .await?;
        Ok(true)
    }

    pub async fn mark_task_terminated(
        &self,
        task_id: &str,
        terminated_at: u64,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::MarkTaskTerminated {
            namespace: self.namespace.clone(),
            task_id: task_id.to_string(),
            terminated_at,
        })
        .await
        .map(|_| ())
    }

    pub fn is_task_terminated(&self, task_id: &str) -> bool {
        self.get_state::<StoredTerminationMarker>(&format!(
            "{}:termination:task:{}",
            self.namespace, task_id
        ))
        .is_some()
    }

    pub async fn mark_module_terminated(
        &self,
        module_id: &str,
        terminated_at: u64,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::MarkModuleTerminated {
            namespace: self.namespace.clone(),
            module_id: module_id.to_string(),
            terminated_at,
        })
        .await
        .map(|_| ())
    }

    pub fn is_module_terminated(&self, module_id: &str) -> bool {
        self.get_state::<StoredTerminationMarker>(&format!(
            "{}:termination:module:{}",
            self.namespace, module_id
        ))
        .is_some()
    }

    pub async fn increment_status_counter(
        &self,
        counter_key: &str,
        delta: i64,
    ) -> Result<i64, ControlPlaneStoreError> {
        let state_key = format!("{}:status_counter:{}", self.namespace, counter_key);
        let current = self
            .get_state::<StoredStatusCounter>(&state_key)
            .map(|record| record.value)
            .unwrap_or(0);
        let next_value = (current + delta).max(0);
        self.submit_command(ControlPlaneRaftCommand::UpsertStatusCounter {
            namespace: self.namespace.clone(),
            counter_key: counter_key.to_string(),
            value: next_value,
        })
        .await?;
        Ok(self.get_status_counter(counter_key))
    }

    pub async fn set_status_counter(
        &self,
        counter_key: &str,
        value: i64,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::UpsertStatusCounter {
            namespace: self.namespace.clone(),
            counter_key: counter_key.to_string(),
            value: value.max(0),
        })
        .await
        .map(|_| ())
    }

    pub fn get_status_counter(&self, counter_key: &str) -> i64 {
        self.get_state::<StoredStatusCounter>(&format!(
            "{}:status_counter:{}",
            self.namespace, counter_key
        ))
        .map(|record| record.value)
        .unwrap_or(0)
    }

    pub async fn upsert_status_entry(
        &self,
        mut entry: StatusEntry,
    ) -> Result<StoredStatusEntryRecord, ControlPlaneStoreError> {
        entry.updated_at = current_time_ms();
        let previous_stage = self
            .get_status_entry(&entry.task_id)
            .map(|record| record.entry.stage);
        let log_index = self
            .submit_command(ControlPlaneRaftCommand::UpsertStatusEntry {
                namespace: self.namespace.clone(),
                entry: entry.clone(),
                previous_stage,
            })
            .await?;
        Ok(self
            .get_status_entry(&entry.task_id)
            .unwrap_or(StoredStatusEntryRecord {
                entry,
                version: log_index,
            }))
    }

    pub fn get_status_entry(&self, task_id: &str) -> Option<StoredStatusEntryRecord> {
        self.get_state::<StoredStatusEntryRecord>(&format!("{}:status:{}", self.namespace, task_id))
    }

    pub fn list_status_entries_by_stage(
        &self,
        stage: PipelineStage,
        limit: usize,
    ) -> Vec<StoredStatusEntryRecord> {
        let prefix = format!("{}:status:index:{}:", self.namespace, stage_key(stage));
        let mut index_records = self.scan_prefix::<StoredStageIndexRecord>(&prefix);
        index_records.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        index_records
            .into_iter()
            .take(limit)
            .filter_map(|index_record| self.get_status_entry(&index_record.task_id))
            .collect()
    }

    pub fn count_status_entries_by_status(&self) -> HashMap<TaskStatus, u64> {
        let prefix = format!("{}:status:", self.namespace);
        let mut counts = HashMap::new();
        for record in self.scan_prefix::<StoredStatusEntryRecord>(&prefix) {
            *counts.entry(record.entry.status).or_insert(0) += 1;
        }
        counts
    }

    pub fn list_defaults(
        &self,
        scope: DefaultConfigScope,
        namespace: &str,
    ) -> Vec<StoredDefaultConfig> {
        let prefix = format!("{}:default:{}:", namespace, scope.as_str());
        let mut records = self.scan_prefix::<StoredDefaultConfig>(&prefix);
        records.sort_by(|left, right| left.default.name.cmp(&right.default.name));
        records
    }

    pub fn get_default(
        &self,
        scope: DefaultConfigScope,
        namespace: &str,
        name: &str,
    ) -> Option<StoredDefaultConfig> {
        let key = default_key(scope, namespace, name);
        self.get_state(&key)
    }

    pub async fn upsert_default(
        &self,
        scope: DefaultConfigScope,
        default: DefaultConfigUpsert,
    ) -> Result<StoredDefaultConfig, ControlPlaneStoreError> {
        let record = StoredDefaultConfig {
            default: default.clone(),
            version: self.last_applied_index().saturating_add(1),
            updated_at: current_time_ms(),
        };
        let command = match scope {
            DefaultConfigScope::Account => {
                ControlPlaneRaftCommand::UpsertAccountDefault { default }
            }
            DefaultConfigScope::Platform => {
                ControlPlaneRaftCommand::UpsertPlatformDefault { default }
            }
            DefaultConfigScope::Module => ControlPlaneRaftCommand::UpsertModuleDefault { default },
        };
        let log_index = self.submit_command(command).await?;
        Ok(self
            .get_default(scope, &record.default.namespace, &record.default.name)
            .unwrap_or(StoredDefaultConfig {
                version: log_index,
                ..record
            }))
    }

    pub async fn patch_default(
        &self,
        scope: DefaultConfigScope,
        namespace: &str,
        name: &str,
        patch: DefaultConfigPatch,
    ) -> Result<Option<StoredDefaultConfig>, ControlPlaneStoreError> {
        let Some(existing) = self.get_default(scope, namespace, name) else {
            return Ok(None);
        };
        let record = StoredDefaultConfig {
            default: DefaultConfigUpsert {
                namespace: existing.default.namespace,
                name: existing.default.name,
                enabled: patch.enabled.unwrap_or(existing.default.enabled),
                config: patch.config.unwrap_or(existing.default.config),
            },
            version: self.last_applied_index().saturating_add(1),
            updated_at: current_time_ms(),
        };
        let command = match scope {
            DefaultConfigScope::Account => ControlPlaneRaftCommand::UpsertAccountDefault {
                default: record.default.clone(),
            },
            DefaultConfigScope::Platform => ControlPlaneRaftCommand::UpsertPlatformDefault {
                default: record.default.clone(),
            },
            DefaultConfigScope::Module => ControlPlaneRaftCommand::UpsertModuleDefault {
                default: record.default.clone(),
            },
        };
        self.submit_command(command).await?;
        Ok(self.get_default(scope, &record.default.namespace, &record.default.name))
    }

    pub async fn disable_default(
        &self,
        scope: DefaultConfigScope,
        namespace: &str,
        name: &str,
    ) -> Result<bool, ControlPlaneStoreError> {
        Ok(self
            .patch_default(
                scope,
                namespace,
                name,
                DefaultConfigPatch {
                    enabled: Some(false),
                    ..DefaultConfigPatch::default()
                },
            )
            .await?
            .is_some())
    }

    pub fn list_middlewares(
        &self,
        namespace: &str,
        middleware_type: Option<MiddlewareType>,
    ) -> Vec<StoredMiddleware> {
        let prefix = format!("{}:middleware:", namespace);
        let mut records: Vec<_> = self
            .scan_prefix::<StoredMiddleware>(&prefix)
            .into_iter()
            .filter(|record| {
                middleware_type.is_none_or(|value| record.middleware.middleware_type == value)
            })
            .collect();
        records.sort_by(|left, right| left.middleware.name.cmp(&right.middleware.name));
        records
    }

    pub fn get_middleware(
        &self,
        namespace: &str,
        name: &str,
        middleware_type: MiddlewareType,
    ) -> Option<StoredMiddleware> {
        let key = middleware_key(namespace, middleware_type, name);
        self.get_state(&key)
    }

    pub async fn upsert_middleware(
        &self,
        middleware: MiddlewareUpsert,
        enabled: bool,
    ) -> Result<StoredMiddleware, ControlPlaneStoreError> {
        let middleware = MiddlewareUpsert {
            enabled,
            ..middleware
        };
        let record = StoredMiddleware {
            enabled,
            middleware: middleware.clone(),
            version: self.last_applied_index().saturating_add(1),
            updated_at: current_time_ms(),
        };
        let log_index = self
            .submit_command(ControlPlaneRaftCommand::UpsertMiddleware { middleware })
            .await?;
        Ok(self
            .get_middleware(
                &record.middleware.namespace,
                &record.middleware.name,
                record.middleware.middleware_type,
            )
            .unwrap_or(StoredMiddleware {
                version: log_index,
                ..record
            }))
    }

    pub async fn patch_middleware(
        &self,
        namespace: &str,
        middleware_type: MiddlewareType,
        name: &str,
        patch: MiddlewarePatch,
    ) -> Result<Option<StoredMiddleware>, ControlPlaneStoreError> {
        let Some(existing) = self.get_middleware(namespace, name, middleware_type) else {
            return Ok(None);
        };
        let enabled = patch.enabled.unwrap_or(existing.enabled);
        let middleware = MiddlewareUpsert {
            namespace: existing.middleware.namespace,
            name: existing.middleware.name,
            middleware_type: existing.middleware.middleware_type,
            enabled,
            config: patch.config.unwrap_or(existing.middleware.config),
            weight: patch.weight.unwrap_or(existing.middleware.weight),
        };
        let record = StoredMiddleware {
            enabled,
            middleware: middleware.clone(),
            version: self.last_applied_index().saturating_add(1),
            updated_at: current_time_ms(),
        };
        self.submit_command(ControlPlaneRaftCommand::UpsertMiddleware { middleware })
            .await?;
        Ok(self.get_middleware(
            &record.middleware.namespace,
            &record.middleware.name,
            record.middleware.middleware_type,
        ))
    }

    pub async fn disable_middleware(
        &self,
        namespace: &str,
        middleware_type: MiddlewareType,
        name: &str,
    ) -> Result<bool, ControlPlaneStoreError> {
        Ok(self
            .patch_middleware(
                namespace,
                middleware_type,
                name,
                MiddlewarePatch {
                    enabled: Some(false),
                    ..MiddlewarePatch::default()
                },
            )
            .await?
            .is_some())
    }

    pub fn middleware_layers(
        &self,
        namespace: &str,
        middleware_type: MiddlewareType,
        names: &[String],
    ) -> BTreeMap<String, TypedEnvelope> {
        names
            .iter()
            .filter_map(|name| {
                self.get_middleware(namespace, name, middleware_type)
                    .filter(|record| record.enabled)
                    .map(|record| (name.clone(), record.middleware.config))
            })
            .collect()
    }

    pub async fn heartbeat_node(&self, node: NodeInfo) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::UpsertNodeHeartbeat {
            namespace: self.namespace.clone(),
            node,
        })
        .await
        .map(|_| ())
    }

    pub async fn deregister_node(&self, node_id: &str) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::RemoveNode {
            namespace: self.namespace.clone(),
            node_id: node_id.to_string(),
        })
        .await
        .map(|_| ())
    }

    pub async fn upsert_response_cache_owner(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
    ) -> Result<(), ControlPlaneStoreError> {
        self.upsert_response_cache_owner_with_details(
            cache_key,
            owner_namespace,
            owner_node_id,
            None,
            None,
        )
        .await
    }

    pub async fn upsert_response_cache_owner_with_expiry(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
        expires_at: Option<i64>,
    ) -> Result<(), ControlPlaneStoreError> {
        self.upsert_response_cache_owner_with_details(
            cache_key,
            owner_namespace,
            owner_node_id,
            None,
            expires_at,
        )
        .await
    }

    pub async fn upsert_response_cache_owner_with_details(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
        owner_api_base_url: Option<&str>,
        expires_at: Option<i64>,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::UpsertResponseCacheOwner {
            namespace: self.namespace.clone(),
            cache_key: cache_key.to_string(),
            owner_namespace: owner_namespace.to_string(),
            owner_node_id: owner_node_id.map(str::to_string),
            owner_api_base_url: owner_api_base_url.map(str::to_string),
            expires_at,
        })
        .await
        .map(|_| ())
    }

    pub async fn remove_response_cache_owner_if_matches(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::RemoveResponseCacheOwnerIfMatch {
            namespace: self.namespace.clone(),
            cache_key: cache_key.to_string(),
            owner_namespace: owner_namespace.to_string(),
            owner_node_id: owner_node_id.map(str::to_string),
        })
        .await
        .map(|_| ())
    }

    pub fn get_response_cache_owner(
        &self,
        cache_key: &str,
    ) -> Option<StoredResponseCacheOwnerRecord> {
        let key = format!("{}:response_cache_owner:{}", self.namespace, cache_key);
        self.get_state::<StoredResponseCacheOwnerRecord>(&key)
    }

    pub fn list_response_cache_owners(&self) -> Vec<StoredResponseCacheOwnerRecord> {
        let prefix = format!("{}:response_cache_owner:", self.namespace);
        let mut records = self.scan_prefix::<StoredResponseCacheOwnerRecord>(&prefix);
        records.sort_by(|left, right| left.cache_key.cmp(&right.cache_key));
        records
    }

    pub fn list_active_nodes(&self, ttl: Duration) -> Vec<NodeInfo> {
        let prefix = format!("{}:node:", self.namespace);
        let cutoff = current_time_secs().saturating_sub(ttl.as_secs());
        let mut records: Vec<_> = self
            .scan_prefix::<StoredNodeRecord>(&prefix)
            .into_iter()
            .filter(|record| record.node.last_heartbeat >= cutoff)
            .collect();
        records.sort_by(|left, right| left.node.id.cmp(&right.node.id));
        records.into_iter().map(|record| record.node).collect()
    }

    pub async fn set_pause_state(&self, paused: bool) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::SetPauseState {
            namespace: self.namespace.clone(),
            paused,
        })
        .await
        .map(|_| ())
    }

    pub fn is_paused(&self) -> bool {
        *self.pause_state_tx.borrow()
    }

    pub async fn submit_command(
        &self,
        command: ControlPlaneRaftCommand,
    ) -> Result<u64, ControlPlaneStoreError> {
        if let Some(runtime) = self.raft_runtime() {
            return runtime
                .client_write(command)
                .await
                .map_err(ControlPlaneStoreError::Raft);
        }

        let _guard = self
            .write_lock
            .lock()
            .expect("control-plane write lock poisoned");
        let log_index = self.next_log_index()?;
        self.commit_command_locked(command, log_index)?;
        Ok(log_index)
    }

    pub fn apply_replicated_command(
        &self,
        command: ControlPlaneRaftCommand,
        log_index: u64,
    ) -> Result<(), ControlPlaneStoreError> {
        let _guard = self
            .write_lock
            .lock()
            .expect("control-plane write lock poisoned");
        self.commit_command_locked(command, log_index)
    }

    pub fn reset_replicated_state(&self) -> Result<(), ControlPlaneStoreError> {
        let _guard = self
            .write_lock
            .lock()
            .expect("control-plane write lock poisoned");
        let mut batch = WriteBatch::default();

        for entry in self
            .db
            .iterator_cf(self.cf(CF_RAFT_LOG), IteratorMode::Start)
        {
            let (key, _) = entry?;
            batch.delete_cf(self.cf(CF_RAFT_LOG), key);
        }

        for entry in self
            .db
            .iterator_cf(self.cf(CF_STATE_MACHINE), IteratorMode::Start)
        {
            let (key, _) = entry?;
            batch.delete_cf(self.cf(CF_STATE_MACHINE), key);
        }

        batch.delete_cf(self.cf(CF_RAFT_META), META_LAST_LOG_INDEX);
        batch.delete_cf(self.cf(CF_RAFT_META), META_LAST_APPLIED_INDEX);
        self.db.write(batch)?;
        self.publish_pause_state(false);
        Ok(())
    }

    fn commit_command_locked(
        &self,
        command: ControlPlaneRaftCommand,
        log_index: u64,
    ) -> Result<(), ControlPlaneStoreError> {
        let command_bytes = encode(&command)?;
        let apply_models = command.apply_models()?;
        let pause_state = apply_models
            .iter()
            .find_map(|apply_model| match apply_model {
                ControlPlaneApply::SetPauseState { paused, .. } => Some(*paused),
                _ => None,
            });
        let mut batch = WriteBatch::default();
        let index_bytes = log_index.to_be_bytes();

        batch.put_cf(self.cf(CF_RAFT_LOG), index_bytes, command_bytes);
        batch.put_cf(self.cf(CF_RAFT_META), META_LAST_LOG_INDEX, index_bytes);
        batch.put_cf(self.cf(CF_RAFT_META), META_LAST_APPLIED_INDEX, index_bytes);

        let updated_at = current_time_ms();
        for apply_model in apply_models {
            self.apply_model_to_batch(&mut batch, apply_model, log_index, updated_at)?;
        }
        self.db.write(batch)?;
        if let Some(paused) = pause_state {
            self.publish_pause_state(paused);
        }
        Ok(())
    }

    fn publish_pause_state(&self, paused: bool) {
        if *self.pause_state_tx.borrow() != paused {
            let _ = self.pause_state_tx.send(paused);
        }
    }

    fn apply_model_to_batch(
        &self,
        batch: &mut WriteBatch,
        apply_model: ControlPlaneApply,
        raft_index: u64,
        updated_at: i64,
    ) -> Result<(), ControlPlaneStoreError> {
        match apply_model {
            ControlPlaneApply::UpsertTaskProfile { key, snapshot } => {
                let mut snapshot = snapshot;
                snapshot.version = raft_index;
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredTaskProfileRecord {
                        snapshot,
                        raft_index,
                    })?,
                );
            }
            ControlPlaneApply::DisableTaskProfile { key } => {
                batch.delete_cf(self.cf(CF_STATE_MACHINE), key.as_bytes());
            }
            ControlPlaneApply::UpsertAccountDefault { key, default }
            | ControlPlaneApply::UpsertPlatformDefault { key, default }
            | ControlPlaneApply::UpsertModuleDefault { key, default } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredDefaultConfig {
                        default,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::UpsertMiddleware { key, middleware } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredMiddleware {
                        enabled: middleware.enabled,
                        middleware,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::UpsertNodeHeartbeat { key, node } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredNodeRecord {
                        node,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::RemoveNode { key } => {
                batch.delete_cf(self.cf(CF_STATE_MACHINE), key.as_bytes());
            }
            ControlPlaneApply::UpsertResponseCacheOwner {
                key,
                cache_key,
                owner_namespace,
                owner_node_id,
                owner_api_base_url,
                expires_at,
            } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredResponseCacheOwnerRecord {
                        cache_key,
                        owner_namespace,
                        owner_node_id,
                        owner_api_base_url,
                        expires_at,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::RemoveResponseCacheOwner { key } => {
                batch.delete_cf(self.cf(CF_STATE_MACHINE), key.as_bytes());
            }
            ControlPlaneApply::RemoveResponseCacheOwnerIfMatch {
                key,
                owner_namespace,
                owner_node_id,
            } => {
                let should_delete = self
                    .get_state::<StoredResponseCacheOwnerRecord>(&key)
                    .map(|record| {
                        record.owner_namespace == owner_namespace
                            && record.owner_node_id == owner_node_id
                    })
                    .unwrap_or(false);
                if should_delete {
                    batch.delete_cf(self.cf(CF_STATE_MACHINE), key.as_bytes());
                }
            }
            ControlPlaneApply::SetModuleLock { key, locked_at } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredModuleLock {
                        locked_at,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::RemoveModuleLock { key } => {
                batch.delete_cf(self.cf(CF_STATE_MACHINE), key.as_bytes());
            }
            ControlPlaneApply::MarkTaskTerminated { key, terminated_at }
            | ControlPlaneApply::MarkModuleTerminated { key, terminated_at } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredTerminationMarker {
                        terminated_at,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::UpsertStatusCounter { key, value } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredStatusCounter {
                        value,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::UpsertStatusEntry {
                key,
                index_key,
                previous_index_key,
                entry,
            } => {
                if let Some(previous_index_key) = previous_index_key
                    && previous_index_key != index_key
                {
                    batch.delete_cf(self.cf(CF_STATE_MACHINE), previous_index_key.as_bytes());
                }
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredStatusEntryRecord {
                        entry: entry.clone(),
                        version: raft_index,
                    })?,
                );
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    index_key.as_bytes(),
                    encode(&StoredStageIndexRecord {
                        task_id: entry.task_id,
                        updated_at: entry.updated_at,
                    })?,
                );
            }
            ControlPlaneApply::SetPauseState { key, paused } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    key.as_bytes(),
                    encode(&StoredPauseState {
                        paused,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            // ── Cache backend apply ──
            ControlPlaneApply::CacheKvUpsert {
                state_key,
                mut value,
            } => {
                value.version = raft_index;
                value.updated_at_ms = updated_at;
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    state_key.as_bytes(),
                    encode(&StoredCacheEntry {
                        value: value.value,
                        expires_at: value.expires_at_ms,
                        version: value.version,
                        updated_at: value.updated_at_ms,
                    })?,
                );
            }
            ControlPlaneApply::CacheKvUpsertNx {
                state_key,
                value,
                request_id,
            } => {
                let exists = self
                    .get_state::<StoredCacheEntry>(&state_key)
                    .is_some_and(|entry| !is_expired(entry.expires_at));
                let ns = state_key.split(':').next().unwrap_or("default");
                if exists {
                    let outcome_key = format!("{}:control:outcome:{}", ns, request_id);
                    batch.put_cf(
                        self.cf(CF_STATE_MACHINE),
                        outcome_key.as_bytes(),
                        encode(&StoredOutcome {
                            success: false,
                            value: 0,
                            updated_at_ms: updated_at,
                        })?,
                    );
                } else {
                    let mut v = value;
                    v.version = raft_index;
                    v.updated_at_ms = updated_at;
                    batch.put_cf(
                        self.cf(CF_STATE_MACHINE),
                        state_key.as_bytes(),
                        encode(&StoredCacheEntry {
                            value: v.value,
                            expires_at: v.expires_at_ms,
                            version: v.version,
                            updated_at: v.updated_at_ms,
                        })?,
                    );
                    let outcome_key = format!("{}:control:outcome:{}", ns, request_id);
                    batch.put_cf(
                        self.cf(CF_STATE_MACHINE),
                        outcome_key.as_bytes(),
                        encode(&StoredOutcome {
                            success: true,
                            value: 1,
                            updated_at_ms: updated_at,
                        })?,
                    );
                }
            }
            ControlPlaneApply::CacheKvDelete { state_key } => {
                batch.delete_cf(self.cf(CF_STATE_MACHINE), state_key.as_bytes());
            }
            ControlPlaneApply::CacheKvDeleteMany { state_keys } => {
                for key in &state_keys {
                    batch.delete_cf(self.cf(CF_STATE_MACHINE), key.as_bytes());
                }
            }
            ControlPlaneApply::CacheCounterSet {
                state_key,
                value,
                expires_at_ms,
                ..
            } => {
                let final_value = if value == 0 {
                    // Re-read current value to apply delta correctly
                    self.get_state::<StoredCacheCounter>(&state_key)
                        .map(|c| c.value)
                        .unwrap_or(0)
                } else {
                    value
                };
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    state_key.as_bytes(),
                    encode(&StoredCacheCounter {
                        value: final_value,
                        expires_at: expires_at_ms,
                        version: raft_index,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::CacheCounterIncr {
                state_key,
                delta,
                expires_at_ms,
                request_id,
            } => {
                let current = self
                    .get_state::<StoredCacheCounter>(&state_key)
                    .map(|c| c.value)
                    .unwrap_or(0);
                let new_value = current.saturating_add(delta);
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    state_key.as_bytes(),
                    encode(&StoredCacheCounter {
                        value: new_value,
                        expires_at: expires_at_ms,
                        version: raft_index,
                        updated_at,
                    })?,
                );
                let ns = state_key.split(':').next().unwrap_or("default");
                let outcome_key = format!("{}:control:outcome:{}", ns, request_id);
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    outcome_key.as_bytes(),
                    encode(&StoredOutcome {
                        success: true,
                        value: new_value,
                        updated_at_ms: updated_at,
                    })?,
                );
            }
            ControlPlaneApply::CacheZSetEntryUpsert {
                state_key,
                mut entry,
            } => {
                entry.version = raft_index;
                entry.updated_at_ms = updated_at;
                // Deduplicate: remove old score entry for same member via member index
                let member_hex = hex::encode(&entry.member);
                let parts: Vec<&str> = state_key.splitn(5, ':').collect();
                if parts.len() >= 4 {
                    let ns = parts[0];
                    let zk = parts[3];
                    let idx_key = format!("{}:cache:zset_idx:{}:{}", ns, zk, member_hex);
                    if let Ok(Some(raw)) = self
                        .db
                        .get_cf(self.cf(CF_STATE_MACHINE), idx_key.as_bytes())
                        && let Some(old_score) =
                            crate::common::model::control_plane_profile::f64_from_be_bytes(&raw)
                    {
                        let old_score_hex = hex::encode(
                            crate::common::model::control_plane_profile::f64_to_be_bytes(old_score),
                        );
                        let old_entry_key =
                            format!("{}:cache:zset:{}:{}:{}", ns, zk, old_score_hex, member_hex);
                        batch.delete_cf(self.cf(CF_STATE_MACHINE), old_entry_key.as_bytes());
                    }
                }
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    state_key.as_bytes(),
                    encode(&StoredZSetEntry {
                        score: entry.score,
                        member: entry.member,
                        version: entry.version,
                        updated_at: entry.updated_at_ms,
                    })?,
                );
            }
            ControlPlaneApply::CacheZSetEntryDeleteByMember {
                namespace,
                zset_key,
                member_hex,
            } => {
                // Scan for the zset entry with matching member and delete it
                let prefix = format!("{}:cache:zset:{}:", namespace, zset_key);
                let prefix_bytes = prefix.as_bytes();
                let iter = self.db.iterator_cf(
                    self.cf(CF_STATE_MACHINE),
                    rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
                );
                for item in iter {
                    let (key, value) = match item {
                        Ok(kv) => kv,
                        Err(_) => continue,
                    };
                    if !key.starts_with(prefix_bytes) {
                        break;
                    }
                    if let Ok(stored) = rmp_serde::from_slice::<StoredZSetEntry>(&value)
                        && hex::encode(&stored.member) == member_hex
                    {
                        batch.delete_cf(self.cf(CF_STATE_MACHINE), &key);
                    }
                }
            }
            ControlPlaneApply::CacheZSetRangeDelete {
                namespace,
                prefix,
                min,
                max,
                request_id,
            } => {
                let prefix_bytes = prefix.as_bytes();
                let iter = self.db.iterator_cf(
                    self.cf(CF_STATE_MACHINE),
                    rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
                );
                let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
                let mut member_hexes: Vec<String> = Vec::new();
                for item in iter {
                    let (key, value) = match item {
                        Ok(kv) => kv,
                        Err(_) => continue,
                    };
                    if !key.starts_with(prefix_bytes) {
                        break;
                    }
                    if let Ok(stored) = rmp_serde::from_slice::<StoredZSetEntry>(&value)
                        && stored.score >= min
                        && stored.score <= max
                    {
                        keys_to_delete.push(key.to_vec());
                        member_hexes.push(hex::encode(&stored.member));
                    }
                }
                let count = keys_to_delete.len() as i64;
                for key in &keys_to_delete {
                    batch.delete_cf(self.cf(CF_STATE_MACHINE), key);
                }
                // Clean up member index entries
                let idx_prefix = prefix.replace(":cache:zset:", ":cache:zset_idx:");
                for mh in &member_hexes {
                    let idx_key = format!("{idx_prefix}{mh}");
                    batch.delete_cf(self.cf(CF_STATE_MACHINE), idx_key.as_bytes());
                }
                // Write outcome with count
                let outcome_key = format!("{}:control:outcome:{}", namespace, request_id);
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    outcome_key.as_bytes(),
                    encode(&StoredOutcome {
                        success: true,
                        value: count,
                        updated_at_ms: updated_at,
                    })?,
                );
            }
            // ── Lock apply ──
            ControlPlaneApply::LockUpsert {
                state_key,
                mut record,
            } => {
                record.version = raft_index;
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    state_key.as_bytes(),
                    encode(&StoredLockRecord {
                        owner_token: record.owner_token,
                        locked_at_ms: record.locked_at_ms,
                        expires_at_ms: record.expires_at_ms,
                        version: record.version,
                        updated_at,
                    })?,
                );
            }
            ControlPlaneApply::LockUpsertConditional {
                state_key,
                mut record,
                request_id,
            } => {
                let now = updated_at;
                let existing = self.get_state::<StoredLockRecord>(&state_key);
                let can_acquire = match existing {
                    Some(ref rec) if rec.expires_at_ms > now => {
                        // Lock is held and not expired — only allow if same owner
                        rec.owner_token == record.owner_token
                    }
                    _ => true, // No lock or expired — can acquire
                };
                let ns = state_key.split(':').next().unwrap_or("default");
                if can_acquire {
                    record.version = raft_index;
                    batch.put_cf(
                        self.cf(CF_STATE_MACHINE),
                        state_key.as_bytes(),
                        encode(&StoredLockRecord {
                            owner_token: record.owner_token,
                            locked_at_ms: record.locked_at_ms,
                            expires_at_ms: record.expires_at_ms,
                            version: record.version,
                            updated_at,
                        })?,
                    );
                }
                let outcome_key = format!("{}:control:outcome:{}", ns, request_id);
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    outcome_key.as_bytes(),
                    encode(&StoredOutcome {
                        success: can_acquire,
                        value: if can_acquire { 1 } else { 0 },
                        updated_at_ms: updated_at,
                    })?,
                );
            }
            ControlPlaneApply::LockRenewConditional {
                state_key,
                owner_token,
                ttl_ms,
                request_id,
            } => {
                let now = updated_at;
                let existing = self.get_state::<StoredLockRecord>(&state_key);
                let can_renew = existing
                    .as_ref()
                    .is_some_and(|r| r.owner_token == owner_token && r.expires_at_ms > now);
                let ns = state_key.split(':').next().unwrap_or("default");
                if can_renew {
                    let rec = existing.unwrap();
                    let new_expires = now.saturating_add(ttl_ms as i64);
                    batch.put_cf(
                        self.cf(CF_STATE_MACHINE),
                        state_key.as_bytes(),
                        encode(&StoredLockRecord {
                            owner_token: rec.owner_token,
                            locked_at_ms: rec.locked_at_ms,
                            expires_at_ms: new_expires,
                            version: raft_index,
                            updated_at,
                        })?,
                    );
                }
                let outcome_key = format!("{}:control:outcome:{}", ns, request_id);
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    outcome_key.as_bytes(),
                    encode(&StoredOutcome {
                        success: can_renew,
                        value: if can_renew { 1 } else { 0 },
                        updated_at_ms: updated_at,
                    })?,
                );
            }
            ControlPlaneApply::LockDelete {
                state_key,
                owner_token,
                request_id,
            } => {
                let ns = state_key.split(':').next().unwrap_or("default");
                let should_delete = self
                    .get_state::<StoredLockRecord>(&state_key)
                    .is_some_and(|r| r.owner_token == owner_token);
                if should_delete {
                    batch.delete_cf(self.cf(CF_STATE_MACHINE), state_key.as_bytes());
                }
                let outcome_key = format!("{}:control:outcome:{}", ns, request_id);
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    outcome_key.as_bytes(),
                    encode(&StoredOutcome {
                        success: should_delete,
                        value: if should_delete { 1 } else { 0 },
                        updated_at_ms: updated_at,
                    })?,
                );
            }
            ControlPlaneApply::CommandOutcomeUpsert { state_key, outcome } => {
                batch.put_cf(
                    self.cf(CF_STATE_MACHINE),
                    state_key.as_bytes(),
                    encode(&StoredOutcome {
                        success: outcome.success,
                        value: outcome.value,
                        updated_at_ms: outcome.updated_at_ms,
                    })?,
                );
            }
        }
        Ok(())
    }

    fn next_log_index(&self) -> Result<u64, ControlPlaneStoreError> {
        Ok(self.read_u64_meta(META_LAST_LOG_INDEX).unwrap_or(0) + 1)
    }

    fn read_u64_meta(&self, key: &[u8]) -> Result<u64, ControlPlaneStoreError> {
        match self.db.get_cf(self.cf(CF_RAFT_META), key)? {
            Some(bytes) => Ok(decode_u64(bytes.as_ref())?),
            None => Ok(0),
        }
    }

    // ── Cache backend helpers ──

    fn cache_key(namespace: &str, key: &str) -> String {
        format!("{}:cache:kv:{}", namespace, key)
    }

    fn counter_key(namespace: &str, key: &str) -> String {
        format!("{}:cache:counter:{}", namespace, key)
    }

    /// Write a cache KV entry through Raft (or local apply).
    pub async fn submit_cache_set(
        &self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<(), ControlPlaneStoreError> {
        let now = current_time_ms();
        let expires_at_ms = ttl_ms.map(|t| now.saturating_add(t as i64));
        self.submit_command(ControlPlaneRaftCommand::CacheSet {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            expires_at_ms,
        })
        .await
        .map(|_| ())
    }

    /// Write a cache KV entry only if it does not exist (or is expired).
    /// Returns true if the write succeeded, false if the key already has an unexpired value.
    pub async fn submit_cache_set_nx(
        &self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<bool, ControlPlaneStoreError> {
        let now = current_time_ms();
        let expires_at_ms = ttl_ms.map(|t| now.saturating_add(t as i64));
        let request_id = uuid::Uuid::new_v4().to_string();
        self.submit_command(ControlPlaneRaftCommand::CacheSetNx {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value,
            expires_at_ms,
            request_id: request_id.clone(),
        })
        .await?;
        let (success, _) = self
            .read_command_outcome(namespace, &request_id)
            .unwrap_or((false, 0));
        Ok(success)
    }

    /// Delete a cache KV entry.
    pub async fn submit_cache_del(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::CacheDelete {
            namespace: namespace.to_string(),
            key: key.to_string(),
        })
        .await
        .map(|_| ())
    }

    /// Delete a batch of cache KV entries.
    pub async fn submit_cache_del_batch(
        &self,
        namespace: &str,
        keys: &[&str],
    ) -> Result<u64, ControlPlaneStoreError> {
        let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let count = key_strings.len() as u64;
        self.submit_command(ControlPlaneRaftCommand::CacheDeleteBatch {
            namespace: namespace.to_string(),
            keys: key_strings,
        })
        .await
        .map(|_| count)
    }

    /// Increment a counter by `delta`, returning the new value.
    /// Reads current value, computes new value, submits via Raft, and returns result.
    pub async fn submit_cache_incr_by(
        &self,
        namespace: &str,
        key: &str,
        delta: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64, ControlPlaneStoreError> {
        let now = current_time_ms();
        let expires_at_ms = ttl_ms.map(|t| now.saturating_add(t as i64));
        let request_id = uuid::Uuid::new_v4().to_string();
        self.submit_command(ControlPlaneRaftCommand::CacheIncrBy {
            namespace: namespace.to_string(),
            key: key.to_string(),
            delta,
            expires_at_ms,
            request_id: request_id.clone(),
        })
        .await?;
        let (_success, value) = self
            .read_command_outcome(namespace, &request_id)
            .unwrap_or((false, 0));
        Ok(value)
    }

    /// Add a member to a sorted set with the given score.
    pub async fn submit_cache_zadd(
        &self,
        namespace: &str,
        zset_key: &str,
        score: f64,
        member: Vec<u8>,
    ) -> Result<(), ControlPlaneStoreError> {
        self.submit_command(ControlPlaneRaftCommand::CacheZAdd {
            namespace: namespace.to_string(),
            zset_key: zset_key.to_string(),
            score,
            member,
        })
        .await
        .map(|_| ())
    }

    /// Read a cache value, returning None if expired or missing.
    pub fn read_cache_value(&self, namespace: &str, key: &str) -> Option<Vec<u8>> {
        let ck = Self::cache_key(namespace, key);
        self.get_state::<StoredCacheEntry>(&ck)
            .filter(|entry| !is_expired(entry.expires_at))
            .map(|entry| entry.value)
    }

    /// Read a counter value, returning None if expired or missing.
    pub fn read_cache_counter(&self, namespace: &str, key: &str) -> Option<i64> {
        let ck = Self::counter_key(namespace, key);
        self.get_state::<StoredCacheCounter>(&ck)
            .filter(|c| c.expires_at.map(|e| e > current_time_ms()).unwrap_or(true))
            .map(|c| c.value)
    }

    /// Read a sorted set range by score, returning member bytes.
    pub fn read_cache_zrange_by_score(
        &self,
        namespace: &str,
        zset_key: &str,
        min: f64,
        max: f64,
        limit: Option<usize>,
    ) -> Vec<Vec<u8>> {
        let prefix = format!("{}:cache:zset:{}:", namespace, zset_key);
        let prefix_bytes = prefix.as_bytes();
        let iter = self.db.iterator_cf(
            self.cf(CF_STATE_MACHINE),
            rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (key_bytes, value_bytes) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            if !key_bytes.starts_with(prefix_bytes) {
                break;
            }
            if let Ok(stored) = rmp_serde::from_slice::<StoredZSetEntry>(&value_bytes)
                && stored.score >= min
                && stored.score <= max
            {
                results.push(stored.member);
                if let Some(lim) = limit
                    && results.len() >= lim
                {
                    break;
                }
            }
        }
        results
    }

    // ── Lock helpers ──

    fn lock_key(namespace: &str, key: &str) -> String {
        format!("{}:lock:{}", namespace, key)
    }

    /// Try to acquire a lock. Returns true if acquired, false if already held by another owner.
    pub async fn submit_lock_acquire(
        &self,
        namespace: &str,
        key: &str,
        owner_token: &str,
        ttl_ms: u64,
    ) -> Result<bool, ControlPlaneStoreError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        self.submit_command(ControlPlaneRaftCommand::LockAcquire {
            namespace: namespace.to_string(),
            key: key.to_string(),
            owner_token: owner_token.to_string(),
            ttl_ms,
            request_id: request_id.clone(),
        })
        .await?;
        let (success, _) = self
            .read_command_outcome(namespace, &request_id)
            .unwrap_or((false, 0));
        Ok(success)
    }

    /// Renew a lock. Returns true if renewed, false if owner doesn't match.
    pub async fn submit_lock_renew(
        &self,
        namespace: &str,
        key: &str,
        owner_token: &str,
        ttl_ms: u64,
    ) -> Result<bool, ControlPlaneStoreError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        self.submit_command(ControlPlaneRaftCommand::LockRenew {
            namespace: namespace.to_string(),
            key: key.to_string(),
            owner_token: owner_token.to_string(),
            ttl_ms,
            request_id: request_id.clone(),
        })
        .await?;
        let (success, _) = self
            .read_command_outcome(namespace, &request_id)
            .unwrap_or((false, 0));
        Ok(success)
    }

    /// Release a lock. Returns true if released, false if owner doesn't match.
    pub async fn submit_lock_release(
        &self,
        namespace: &str,
        key: &str,
        owner_token: &str,
    ) -> Result<bool, ControlPlaneStoreError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        self.submit_command(ControlPlaneRaftCommand::LockRelease {
            namespace: namespace.to_string(),
            key: key.to_string(),
            owner_token: owner_token.to_string(),
            request_id: request_id.clone(),
        })
        .await?;
        let (success, _) = self
            .read_command_outcome(namespace, &request_id)
            .unwrap_or((false, 0));
        Ok(success)
    }

    fn outcome_key(namespace: &str, request_id: &str) -> String {
        format!("{}:control:outcome:{}", namespace, request_id)
    }

    /// Public helper to read a command outcome by request_id.
    pub fn read_command_outcome(&self, namespace: &str, request_id: &str) -> Option<(bool, i64)> {
        let key = Self::outcome_key(namespace, request_id);
        self.get_state::<StoredOutcome>(&key)
            .map(|o| (o.success, o.value))
    }

    /// Read a lock record. Returns None if missing or expired.
    pub fn read_lock(&self, namespace: &str, key: &str) -> Option<LockRecord> {
        let lk = Self::lock_key(namespace, key);
        self.get_state::<StoredLockRecord>(&lk)
            .filter(|r| r.expires_at_ms > current_time_ms())
            .map(|r| LockRecord {
                owner_token: r.owner_token,
                locked_at_ms: r.locked_at_ms,
                expires_at_ms: r.expires_at_ms,
                version: r.version,
            })
    }

    /// List cache keys matching a prefix pattern. Only supports `prefix*` style patterns.
    pub fn list_cache_keys(
        &self,
        namespace: &str,
        prefix_pattern: &str,
        limit: usize,
    ) -> Vec<String> {
        let prefix = if let Some(stripped) = prefix_pattern.strip_suffix('*') {
            format!("{}:cache:kv:{}", namespace, stripped)
        } else {
            format!("{}:cache:kv:{}", namespace, prefix_pattern)
        };
        let prefix_bytes = prefix.as_bytes();
        let iter = self.db.iterator_cf(
            self.cf(CF_STATE_MACHINE),
            rocksdb::IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (key, _value) = match item {
                Ok(kv) => kv,
                Err(_) => continue,
            };
            if !key.starts_with(prefix_bytes) {
                break;
            }
            if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                let name = key_str
                    .strip_prefix(&format!("{}:cache:kv:", namespace))
                    .unwrap_or(&key_str);
                results.push(name.to_string());
                if results.len() >= limit {
                    break;
                }
            }
        }
        results
    }

    fn get_state<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let bytes = match self.db.get_cf(self.cf(CF_STATE_MACHINE), key.as_bytes()) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(
                    "Control-plane state read failed: key={} type={} error={:?}",
                    key,
                    std::any::type_name::<T>(),
                    error
                );
                return None;
            }
        }?;

        match decode::<T>(bytes.as_ref()) {
            Ok(value) => Some(value),
            Err(error) => {
                warn!(
                    "Control-plane state decode failed: key={} type={} error={:?}",
                    key,
                    std::any::type_name::<T>(),
                    error
                );
                None
            }
        }
    }

    fn scan_prefix<T>(&self, prefix: &str) -> Vec<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let prefix_bytes = prefix.as_bytes();
        self.db
            .iterator_cf(
                self.cf(CF_STATE_MACHINE),
                IteratorMode::From(prefix_bytes, Direction::Forward),
            )
            .take_while(|entry| {
                entry
                    .as_ref()
                    .map(|(key, _)| key.starts_with(prefix_bytes))
                    .unwrap_or(false)
            })
            .filter_map(|entry| match entry {
                Ok((key, value)) => match decode::<T>(value.as_ref()) {
                    Ok(record) => Some(record),
                    Err(error) => {
                        warn!(
                            "Control-plane prefix decode failed: prefix={} key={} type={} error={:?}",
                            prefix,
                            String::from_utf8_lossy(key.as_ref()),
                            std::any::type_name::<T>(),
                            error
                        );
                        None
                    }
                },
                Err(error) => {
                    warn!(
                        "Control-plane prefix scan entry failed: prefix={} type={} error={:?}",
                        prefix,
                        std::any::type_name::<T>(),
                        error
                    );
                    None
                }
            })
            .collect()
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| panic!("missing RocksDB column family: {name}"))
    }
}

fn open_db(path: &Path) -> Result<DB, rocksdb::Error> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    DB::open_cf_descriptors(
        &options,
        path,
        vec![
            ColumnFamilyDescriptor::new(CF_RAFT_LOG, Options::default()),
            ColumnFamilyDescriptor::new(CF_STATE_MACHINE, Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_META, Options::default()),
        ],
    )
}

fn read_initial_pause_state(db: &DB, namespace: &str) -> Result<bool, ControlPlaneStoreError> {
    let key = format!("{}:control:pause", namespace);
    let cf = db
        .cf_handle(CF_STATE_MACHINE)
        .unwrap_or_else(|| panic!("missing RocksDB column family: {CF_STATE_MACHINE}"));
    match db.get_cf(cf, key.as_bytes())? {
        Some(bytes) => Ok(decode::<StoredPauseState>(bytes.as_ref())?.paused),
        None => Ok(false),
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ControlPlaneStoreError> {
    rmp_serde::to_vec(value)
        .map_err(|error| ControlPlaneStoreError::Serialization(error.to_string()))
}

fn decode<T>(bytes: &[u8]) -> Result<T, ControlPlaneStoreError>
where
    T: for<'de> Deserialize<'de>,
{
    rmp_serde::from_slice(bytes)
        .map_err(|error| ControlPlaneStoreError::Serialization(error.to_string()))
}

fn decode_u64(bytes: &[u8]) -> Result<u64, ControlPlaneStoreError> {
    if bytes.len() != 8 {
        return Err(ControlPlaneStoreError::Serialization(
            "invalid u64 metadata length".to_string(),
        ));
    }
    let mut buffer = [0_u8; 8];
    buffer.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buffer))
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn is_expired(expires_at: Option<i64>) -> bool {
    expires_at.is_some_and(|e| e <= current_time_ms())
}

fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn profile_key(namespace: &str, account: &str, platform: &str, module: &str) -> String {
    TaskProfileIdentity {
        namespace: namespace.to_string(),
        account: account.to_string(),
        platform: platform.to_string(),
        module: module.to_string(),
    }
    .profile_key()
}

fn default_key(scope: DefaultConfigScope, namespace: &str, name: &str) -> String {
    format!("{}:default:{}:{}", namespace, scope.as_str(), name)
}

fn middleware_key(namespace: &str, middleware_type: MiddlewareType, name: &str) -> String {
    format!(
        "{}:middleware:{}:{}",
        namespace,
        middleware_type.as_str(),
        name
    )
}

fn stage_key(stage: PipelineStage) -> &'static str {
    match stage {
        PipelineStage::Task => "task",
        PipelineStage::Request => "request",
        PipelineStage::Response => "response",
        PipelineStage::ParserTask => "parser_task",
        PipelineStage::Error => "error",
    }
}

#[cfg(test)]
impl Default for ProfileControlPlaneStore {
    fn default() -> Self {
        Self::open_temp("default").expect("open temporary control-plane store")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{DefaultConfigPatch, ProfileControlPlaneStore, TaskProfilePatch};
    use crate::common::model::control_plane_profile::DefaultConfigScope;
    use crate::common::model::{
        DefaultConfigUpsert, MiddlewareType, PayloadCodec, PipelineStage, Priority,
        ResolvedCommonConfig, StatusEntry, TaskProfileIdentity, TaskProfileUpsert, TaskStatus,
        TypedEnvelope,
    };

    fn sample_upsert() -> TaskProfileUpsert {
        TaskProfileUpsert {
            identity: TaskProfileIdentity {
                namespace: "demo".to_string(),
                account: "account-a".to_string(),
                platform: "shopee".to_string(),
                module: "search".to_string(),
            },
            enabled: true,
            common: ResolvedCommonConfig {
                priority: Priority::High,
                ..ResolvedCommonConfig::default()
            },
            node_configs: BTreeMap::from([(
                "entry".to_string(),
                TypedEnvelope::new("node.entry", 1, PayloadCodec::Json, br#"{}"#),
            )]),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            middleware_configs: BTreeMap::new(),
            debug_layers_json: Some(serde_json::json!({
                "task_profile_config": { "timeout_secs": 30 }
            })),
            updated_by: "tester".to_string(),
        }
    }

    #[tokio::test]
    async fn upsert_and_patch_profile_generates_new_versions() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        let first = store
            .upsert_profile(sample_upsert())
            .await
            .expect("first snapshot");
        assert_eq!(first.version, 1);
        assert_eq!(store.last_applied_index(), 1);

        let patched = store
            .patch_profile(
                TaskProfileIdentity {
                    namespace: "demo".to_string(),
                    account: "account-a".to_string(),
                    platform: "shopee".to_string(),
                    module: "search".to_string(),
                },
                TaskProfilePatch {
                    enabled: Some(false),
                    updated_by: Some("operator".to_string()),
                    ..TaskProfilePatch::default()
                },
            )
            .await
            .expect("patch result")
            .expect("profile exists");

        assert_eq!(patched.version, 2);
        assert!(!patched.enabled);
        assert_eq!(patched.updated_by, "operator");
        assert_eq!(store.last_applied_index(), 2);
    }

    #[tokio::test]
    async fn default_config_records_can_be_upserted_and_disabled() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");
        let record = store
            .upsert_default(
                DefaultConfigScope::Account,
                DefaultConfigUpsert {
                    namespace: "demo".to_string(),
                    name: "account-a".to_string(),
                    enabled: true,
                    config: TypedEnvelope::new(
                        "control.account",
                        1,
                        PayloadCodec::Json,
                        br#"{"timeout":30}"#,
                    ),
                },
            )
            .await
            .expect("upsert default");
        assert_eq!(record.version, 1);

        let disabled = store
            .patch_default(
                DefaultConfigScope::Account,
                "demo",
                "account-a",
                DefaultConfigPatch {
                    enabled: Some(false),
                    ..DefaultConfigPatch::default()
                },
            )
            .await
            .expect("patch default")
            .expect("record exists");
        assert!(!disabled.default.enabled);
        assert_eq!(disabled.version, 2);
    }

    #[tokio::test]
    async fn middleware_layers_return_enabled_records_only() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");
        store
            .upsert_middleware(
                crate::common::model::MiddlewareUpsert {
                    namespace: "demo".to_string(),
                    name: "proxy".to_string(),
                    middleware_type: MiddlewareType::Download,
                    enabled: true,
                    config: TypedEnvelope::new(
                        "mw.proxy",
                        1,
                        PayloadCodec::Json,
                        br#"{"pool":"sg"}"#,
                    ),
                    weight: 10,
                },
                true,
            )
            .await
            .expect("upsert middleware");

        let layers = store.middleware_layers(
            "demo",
            MiddlewareType::Download,
            &["proxy".to_string(), "missing".to_string()],
        );
        assert!(layers.contains_key("proxy"));
        assert!(!layers.contains_key("missing"));
    }

    #[tokio::test]
    async fn response_cache_owner_conditional_remove_deletes_matching_record() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        store
            .upsert_response_cache_owner("cache-key-1", "demo", Some("node-a"))
            .await
            .expect("upsert owner record");
        assert!(store.get_response_cache_owner("cache-key-1").is_some());

        store
            .remove_response_cache_owner_if_matches("cache-key-1", "demo", Some("node-a"))
            .await
            .expect("remove matching owner record");

        assert!(store.get_response_cache_owner("cache-key-1").is_none());
    }

    #[tokio::test]
    async fn response_cache_owner_conditional_remove_preserves_replaced_record() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        store
            .upsert_response_cache_owner("cache-key-1", "demo", Some("node-a"))
            .await
            .expect("upsert initial owner record");
        store
            .upsert_response_cache_owner("cache-key-1", "demo", Some("node-b"))
            .await
            .expect("replace owner record");

        store
            .remove_response_cache_owner_if_matches("cache-key-1", "demo", Some("node-a"))
            .await
            .expect("attempt stale owner removal");

        let owner = store
            .get_response_cache_owner("cache-key-1")
            .expect("newer owner record should remain");
        assert_eq!(owner.owner_namespace, "demo");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-b"));
    }

    #[tokio::test]
    async fn response_cache_owner_record_round_trips_with_expiry() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        store
            .upsert_response_cache_owner_with_expiry(
                "cache-key-1",
                "demo",
                Some("node-a"),
                Some(1_710_000_000_000),
            )
            .await
            .expect("upsert owner record with expiry");

        let owner = store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should exist");
        assert_eq!(owner.owner_api_base_url, None);
        assert_eq!(owner.expires_at, Some(1_710_000_000_000));
    }

    #[tokio::test]
    async fn response_cache_owner_record_round_trips_with_endpoint_and_expiry() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        store
            .upsert_response_cache_owner_with_details(
                "cache-key-1",
                "demo",
                Some("node-a"),
                Some("http://127.0.0.1:8080"),
                Some(1_710_000_000_000),
            )
            .await
            .expect("upsert owner record with endpoint and expiry");

        let owner = store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should exist");
        assert_eq!(
            owner.owner_api_base_url.as_deref(),
            Some("http://127.0.0.1:8080")
        );
        assert_eq!(owner.expires_at, Some(1_710_000_000_000));
    }

    #[tokio::test]
    async fn module_lock_records_can_be_set_and_released() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        store
            .set_module_lock("search-run-1", 123)
            .await
            .expect("set module lock");

        let lock = store
            .get_module_lock("search-run-1")
            .expect("stored module lock");
        assert_eq!(lock.locked_at, 123);
        assert_eq!(lock.version, 1);

        store
            .release_module_lock("search-run-1")
            .await
            .expect("release module lock");
        assert!(store.get_module_lock("search-run-1").is_none());
        assert_eq!(store.last_applied_index(), 2);
    }

    #[tokio::test]
    async fn termination_markers_can_be_persisted() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        assert!(!store.is_task_terminated("task-1"));
        assert!(!store.is_module_terminated("module-1"));

        store
            .mark_task_terminated("task-1", 111)
            .await
            .expect("mark task terminated");
        store
            .mark_module_terminated("module-1", 222)
            .await
            .expect("mark module terminated");

        assert!(store.is_task_terminated("task-1"));
        assert!(store.is_module_terminated("module-1"));
        assert_eq!(store.last_applied_index(), 2);
    }

    #[tokio::test]
    async fn status_counters_can_increment_and_reset() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        assert_eq!(store.get_status_counter("task:alpha:total_errors"), 0);
        assert_eq!(
            store
                .increment_status_counter("task:alpha:total_errors", 1)
                .await
                .expect("increment total"),
            1
        );
        assert_eq!(
            store
                .increment_status_counter("task:alpha:total_errors", 2)
                .await
                .expect("increment total again"),
            3
        );
        store
            .set_status_counter("task:alpha:consecutive_errors", 0)
            .await
            .expect("reset consecutive");

        assert_eq!(store.get_status_counter("task:alpha:total_errors"), 3);
        assert_eq!(store.get_status_counter("task:alpha:consecutive_errors"), 0);
        assert_eq!(store.last_applied_index(), 3);
    }

    #[tokio::test]
    async fn status_entries_track_stage_indexes_and_counts() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");

        store
            .upsert_status_entry(StatusEntry {
                task_id: "task-1".to_string(),
                stage: PipelineStage::Task,
                status: TaskStatus::Running,
                retry_count: 0,
                node_id: "node-a".to_string(),
                updated_at: 0,
                error_msg: None,
            })
            .await
            .expect("insert running task status");
        store
            .upsert_status_entry(StatusEntry {
                task_id: "task-2".to_string(),
                stage: PipelineStage::Request,
                status: TaskStatus::Failed,
                retry_count: 2,
                node_id: "node-b".to_string(),
                updated_at: 0,
                error_msg: Some("boom".to_string()),
            })
            .await
            .expect("insert failed request status");
        store
            .upsert_status_entry(StatusEntry {
                task_id: "task-1".to_string(),
                stage: PipelineStage::Response,
                status: TaskStatus::Done,
                retry_count: 0,
                node_id: "node-a".to_string(),
                updated_at: 0,
                error_msg: None,
            })
            .await
            .expect("move task-1 to response stage");

        let task_entries = store.list_status_entries_by_stage(PipelineStage::Task, 10);
        assert!(task_entries.is_empty());

        let response_entries = store.list_status_entries_by_stage(PipelineStage::Response, 10);
        assert_eq!(response_entries.len(), 1);
        assert_eq!(response_entries[0].entry.task_id, "task-1");
        assert_eq!(response_entries[0].entry.status, TaskStatus::Done);

        let counts = store.count_status_entries_by_status();
        assert_eq!(counts.get(&TaskStatus::Done), Some(&1));
        assert_eq!(counts.get(&TaskStatus::Failed), Some(&1));
        assert_eq!(counts.get(&TaskStatus::Running), None);
    }

    #[tokio::test]
    async fn pause_state_subscription_tracks_updates() {
        let store = ProfileControlPlaneStore::open_temp("demo").expect("open temp store");
        let mut pause_rx = store.subscribe_pause_state();

        assert!(!*pause_rx.borrow());

        store
            .set_pause_state(true)
            .await
            .expect("pause state should persist");
        pause_rx
            .changed()
            .await
            .expect("pause subscription should observe update");
        assert!(*pause_rx.borrow_and_update());

        store
            .set_pause_state(false)
            .await
            .expect("resume state should persist");
        pause_rx
            .changed()
            .await
            .expect("pause subscription should observe resume");
        assert!(!*pause_rx.borrow_and_update());
    }

    // ── Cache command model tests ──

    #[tokio::test]
    async fn cache_kv_set_and_get() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "mykey", b"hello".to_vec(), None)
            .await
            .unwrap();
        let val = store.read_cache_value("ns", "mykey");
        assert_eq!(val, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn cache_kv_ttl_not_expired() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "k1", b"data".to_vec(), Some(60_000))
            .await
            .unwrap();
        let val = store.read_cache_value("ns", "k1");
        assert_eq!(val, Some(b"data".to_vec()));
    }

    #[tokio::test]
    async fn cache_kv_ttl_expired_returns_none() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "k2", b"data".to_vec(), Some(1))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let val = store.read_cache_value("ns", "k2");
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn cache_kv_delete() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "k3", b"data".to_vec(), None)
            .await
            .unwrap();
        store.submit_cache_del("ns", "k3").await.unwrap();
        assert_eq!(store.read_cache_value("ns", "k3"), None);
    }

    #[tokio::test]
    async fn cache_kv_delete_batch() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "a", b"1".to_vec(), None)
            .await
            .unwrap();
        store
            .submit_cache_set("ns", "b", b"2".to_vec(), None)
            .await
            .unwrap();
        let count = store
            .submit_cache_del_batch("ns", &["a", "b"])
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(store.read_cache_value("ns", "a"), None);
        assert_eq!(store.read_cache_value("ns", "b"), None);
    }

    #[tokio::test]
    async fn cache_set_nx_empty_key_succeeds() {
        let store = ProfileControlPlaneStore::default();
        let ok = store
            .submit_cache_set_nx("ns", "nx1", b"val".to_vec(), None)
            .await
            .unwrap();
        assert!(ok);
        assert_eq!(store.read_cache_value("ns", "nx1"), Some(b"val".to_vec()));
    }

    #[tokio::test]
    async fn cache_set_nx_unexpired_key_fails() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "nx2", b"first".to_vec(), Some(60_000))
            .await
            .unwrap();
        let ok = store
            .submit_cache_set_nx("ns", "nx2", b"second".to_vec(), None)
            .await
            .unwrap();
        assert!(!ok);
        assert_eq!(store.read_cache_value("ns", "nx2"), Some(b"first".to_vec()));
    }

    #[tokio::test]
    async fn cache_set_nx_expired_key_succeeds() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns", "nx3", b"old".to_vec(), Some(1))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let ok = store
            .submit_cache_set_nx("ns", "nx3", b"new".to_vec(), None)
            .await
            .unwrap();
        assert!(ok);
    }

    #[tokio::test]
    async fn cache_counter_consecutive_incr() {
        let store = ProfileControlPlaneStore::default();
        let v1 = store
            .submit_cache_incr_by("ns", "ctr1", 1, None)
            .await
            .unwrap();
        assert_eq!(v1, 1);
        let v2 = store
            .submit_cache_incr_by("ns", "ctr1", 5, None)
            .await
            .unwrap();
        assert_eq!(v2, 6);
        let v3 = store
            .submit_cache_incr_by("ns", "ctr1", -1, None)
            .await
            .unwrap();
        assert_eq!(v3, 5);
    }

    #[tokio::test]
    async fn cache_zset_multi_member_and_range() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_zadd("ns", "zs1", 1.0, b"a".to_vec())
            .await
            .unwrap();
        store
            .submit_cache_zadd("ns", "zs1", 5.0, b"b".to_vec())
            .await
            .unwrap();
        store
            .submit_cache_zadd("ns", "zs1", 10.0, b"c".to_vec())
            .await
            .unwrap();

        let members = store.read_cache_zrange_by_score("ns", "zs1", 1.0, 5.0, None);
        assert_eq!(members.len(), 2);

        let limited = store.read_cache_zrange_by_score("ns", "zs1", 0.0, 100.0, Some(2));
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn cache_zset_range_delete_by_score() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_zadd("ns", "zs2", 1.0, b"low".to_vec())
            .await
            .unwrap();
        store
            .submit_cache_zadd("ns", "zs2", 5.0, b"mid".to_vec())
            .await
            .unwrap();
        store
            .submit_cache_zadd("ns", "zs2", 10.0, b"high".to_vec())
            .await
            .unwrap();

        store
            .submit_command(
                crate::common::model::control_plane_profile::ControlPlaneRaftCommand::CacheZRemRangeByScore {
                    namespace: "ns".to_string(),
                    zset_key: "zs2".to_string(),
                    min: 1.0,
                    max: 5.0,
                    request_id: "test-zrem".to_string(),
                },
            )
            .await
            .unwrap();

        let remaining = store.read_cache_zrange_by_score("ns", "zs2", 0.0, 100.0, None);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0], b"high".to_vec());
    }

    #[tokio::test]
    async fn cache_namespace_isolation() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_cache_set("ns-a", "key1", b"a".to_vec(), None)
            .await
            .unwrap();
        store
            .submit_cache_set("ns-b", "key1", b"b".to_vec(), None)
            .await
            .unwrap();

        assert_eq!(store.read_cache_value("ns-a", "key1"), Some(b"a".to_vec()));
        assert_eq!(store.read_cache_value("ns-b", "key1"), Some(b"b".to_vec()));
    }

    // ── Apply-result / concurrent safety tests ──

    #[tokio::test]
    async fn cache_set_nx_concurrent_only_one_succeeds() {
        let store = std::sync::Arc::new(ProfileControlPlaneStore::default());
        let s1 = store.clone();
        let s2 = store.clone();

        let (r1, r2) = tokio::join!(
            s1.submit_cache_set_nx("ns", "nx-key", b"first".to_vec(), Some(5000)),
            s2.submit_cache_set_nx("ns", "nx-key", b"second".to_vec(), Some(5000)),
        );

        let ok1 = r1.unwrap();
        let ok2 = r2.unwrap();
        // Exactly one must succeed
        assert!(
            ok1 != ok2,
            "expected exactly one set_nx to succeed, got ok1={ok1} ok2={ok2}"
        );
        // The value stored is whichever succeeded first
        let val = store.read_cache_value("ns", "nx-key");
        assert!(val.is_some());
    }

    #[tokio::test]
    async fn cache_incr_by_concurrent_returns_ordered_values() {
        let store = std::sync::Arc::new(ProfileControlPlaneStore::default());
        let s1 = store.clone();
        let s2 = store.clone();

        let (r1, r2) = tokio::join!(
            s1.submit_cache_incr_by("ns", "counter", 1, Some(5000)),
            s2.submit_cache_incr_by("ns", "counter", 2, Some(5000)),
        );

        let v1 = r1.unwrap();
        let v2 = r2.unwrap();
        // Each apply reads current counter and adds delta serially.
        // The larger value reflects the final state; both are positive.
        assert!(v1 > 0 && v2 > 0, "v1={v1} v2={v2}");
        assert!(
            v1 != v2,
            "expected distinct apply-ordered values, got v1={v1} v2={v2}"
        );
        // Final counter value must be 3
        let final_val = store.read_cache_counter("ns", "counter").unwrap();
        assert_eq!(final_val, 3, "v1={v1} v2={v2}");
        // The max of the two returned values should equal the final counter
        assert_eq!(v1.max(v2), final_val);
    }

    #[tokio::test]
    async fn lock_concurrent_acquire_only_one_owner() {
        let store = std::sync::Arc::new(ProfileControlPlaneStore::default());
        let s1 = store.clone();
        let s2 = store.clone();

        let (r1, r2) = tokio::join!(
            s1.submit_lock_acquire("ns", "lk", "owner-a", 5000),
            s2.submit_lock_acquire("ns", "lk", "owner-b", 5000),
        );

        let ok1 = r1.unwrap();
        let ok2 = r2.unwrap();
        assert!(
            ok1 != ok2,
            "expected exactly one lock acquire to succeed, got ok1={ok1} ok2={ok2}"
        );
        // The lock record must exist and report the winning owner
        let lock = store.read_lock("ns", "lk");
        assert!(lock.is_some());
        let owner = lock.unwrap().owner_token;
        assert!(owner == "owner-a" || owner == "owner-b");
    }

    #[tokio::test]
    async fn lock_renew_wrong_owner_returns_false_from_apply() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_lock_acquire("ns", "lk-r", "owner-1", 5000)
            .await
            .unwrap();

        // Wrong owner tries to renew — should return false
        let ok = store
            .submit_lock_renew("ns", "lk-r", "owner-2", 10_000)
            .await
            .unwrap();
        assert!(!ok);

        // Correct owner can still renew
        let ok = store
            .submit_lock_renew("ns", "lk-r", "owner-1", 10_000)
            .await
            .unwrap();
        assert!(ok);
    }

    #[tokio::test]
    async fn lock_release_wrong_owner_returns_false_from_apply() {
        let store = ProfileControlPlaneStore::default();
        store
            .submit_lock_acquire("ns", "lk-rel", "owner-1", 5000)
            .await
            .unwrap();

        // Wrong owner tries to release — should return false
        let ok = store
            .submit_lock_release("ns", "lk-rel", "owner-2")
            .await
            .unwrap();
        assert!(!ok);

        // Correct owner can release
        let ok = store
            .submit_lock_release("ns", "lk-rel", "owner-1")
            .await
            .unwrap();
        assert!(ok);

        // After release, lock is gone
        let lock = store.read_lock("ns", "lk-rel");
        assert!(lock.is_none());
    }
}
