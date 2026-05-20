use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::common::registry::NodeInfo;
use crate::common::model::{
    MiddlewareBinding, MiddlewareType, PipelineStage, ResolvedCommonConfig, StatusEntry,
    TaskProfileSnapshot, TypedEnvelope,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskProfileIdentity {
    pub namespace: String,
    pub account: String,
    pub platform: String,
    pub module: String,
}

impl TaskProfileIdentity {
    pub fn profile_key(&self) -> String {
        format!(
            "{}:profile:{}:{}:{}",
            self.namespace, self.account, self.platform, self.module
        )
    }

    pub fn validate(&self) -> Result<(), ProfileValidationError> {
        validate_non_empty("namespace", &self.namespace)?;
        validate_non_empty("account", &self.account)?;
        validate_non_empty("platform", &self.platform)?;
        validate_non_empty("module", &self.module)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskProfileUpsert {
    pub identity: TaskProfileIdentity,
    pub enabled: bool,
    pub common: ResolvedCommonConfig,
    pub node_configs: std::collections::BTreeMap<String, TypedEnvelope>,
    pub download_middleware: Vec<MiddlewareBinding>,
    pub data_middleware: Vec<MiddlewareBinding>,
    pub middleware_configs: std::collections::BTreeMap<String, TypedEnvelope>,
    pub debug_layers_json: Option<serde_json::Value>,
    pub updated_by: String,
}

impl TaskProfileUpsert {
    pub fn validate(&self) -> Result<(), ProfileValidationError> {
        self.identity.validate()?;
        validate_non_empty("updated_by", &self.updated_by)?;

        for node_key in self.node_configs.keys() {
            validate_non_empty("node_key", node_key)?;
        }
        for middleware_name in self.middleware_configs.keys() {
            validate_non_empty("middleware_config", middleware_name)?;
        }

        validate_middleware_bindings("download_middleware", &self.download_middleware)?;
        validate_middleware_bindings("data_middleware", &self.data_middleware)?;

        Ok(())
    }

    pub fn into_snapshot(
        self,
        version: u64,
        updated_at: i64,
    ) -> Result<TaskProfileSnapshot, ProfileValidationError> {
        self.validate()?;

        Ok(TaskProfileSnapshot {
            namespace: self.identity.namespace,
            account: self.identity.account,
            platform: self.identity.platform,
            module: self.identity.module,
            version,
            enabled: self.enabled,
            common: self.common,
            node_configs: self.node_configs,
            download_middleware: self.download_middleware,
            data_middleware: self.data_middleware,
            middleware_configs: self.middleware_configs,
            debug_layers_json: self.debug_layers_json,
            updated_at,
            updated_by: self.updated_by,
        })
    }

    pub fn into_command(
        self,
        version: u64,
        updated_at: i64,
    ) -> Result<ControlPlaneRaftCommand, ProfileValidationError> {
        Ok(ControlPlaneRaftCommand::UpsertTaskProfile {
            snapshot: self.into_snapshot(version, updated_at)?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DefaultConfigUpsert {
    pub namespace: String,
    pub name: String,
    pub enabled: bool,
    pub config: TypedEnvelope,
}

impl DefaultConfigUpsert {
    pub fn validate(&self) -> Result<(), ProfileValidationError> {
        validate_non_empty("namespace", &self.namespace)?;
        validate_non_empty("name", &self.name)?;
        Ok(())
    }

    pub fn storage_key(&self, scope: DefaultConfigScope) -> Result<String, ProfileValidationError> {
        self.validate()?;
        Ok(format!(
            "{}:default:{}:{}",
            self.namespace,
            scope.as_str(),
            self.name
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefaultConfigScope {
    Account,
    Platform,
    Module,
}

impl DefaultConfigScope {
    pub fn as_str(self) -> &'static str {
        match self {
            DefaultConfigScope::Account => "account",
            DefaultConfigScope::Platform => "platform",
            DefaultConfigScope::Module => "module",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MiddlewareUpsert {
    pub namespace: String,
    pub name: String,
    pub middleware_type: MiddlewareType,
    pub enabled: bool,
    pub config: TypedEnvelope,
    pub weight: i32,
}

impl MiddlewareUpsert {
    pub fn validate(&self) -> Result<(), ProfileValidationError> {
        validate_non_empty("namespace", &self.namespace)?;
        validate_non_empty("name", &self.name)?;
        Ok(())
    }

    pub fn storage_key(&self) -> Result<String, ProfileValidationError> {
        self.validate()?;
        Ok(format!(
            "{}:middleware:{}:{}",
            self.namespace,
            self.middleware_type.as_str(),
            self.name
        ))
    }
}

impl MiddlewareType {
    pub fn as_str(self) -> &'static str {
        match self {
            MiddlewareType::Data => "data",
            MiddlewareType::Download => "download",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ControlPlaneRaftCommand {
    UpsertTaskProfile { snapshot: TaskProfileSnapshot },
    DisableTaskProfile { identity: TaskProfileIdentity },
    BatchUpsertTaskProfiles { snapshots: Vec<TaskProfileSnapshot> },
    UpsertAccountDefault { default: DefaultConfigUpsert },
    UpsertPlatformDefault { default: DefaultConfigUpsert },
    UpsertModuleDefault { default: DefaultConfigUpsert },
    UpsertMiddleware { middleware: MiddlewareUpsert },
    UpsertNodeHeartbeat { namespace: String, node: NodeInfo },
    RemoveNode { namespace: String, node_id: String },
    UpsertResponseCacheOwner {
        namespace: String,
        cache_key: String,
        owner_namespace: String,
        owner_node_id: Option<String>,
        owner_api_base_url: Option<String>,
        expires_at: Option<i64>,
    },
    RemoveResponseCacheOwnerIfMatch {
        namespace: String,
        cache_key: String,
        owner_namespace: String,
        owner_node_id: Option<String>,
    },
    RemoveResponseCacheOwner { namespace: String, cache_key: String },
    SetModuleLock {
        namespace: String,
        module_id: String,
        locked_at: u64,
    },
    RemoveModuleLock { namespace: String, module_id: String },
    MarkTaskTerminated {
        namespace: String,
        task_id: String,
        terminated_at: u64,
    },
    MarkModuleTerminated {
        namespace: String,
        module_id: String,
        terminated_at: u64,
    },
    UpsertStatusCounter {
        namespace: String,
        counter_key: String,
        value: i64,
    },
    UpsertStatusEntry {
        namespace: String,
        entry: StatusEntry,
        previous_stage: Option<PipelineStage>,
    },
    SetPauseState { namespace: String, paused: bool },
    // ── Cache backend commands ──
    CacheSet { namespace: String, key: String, value: Vec<u8>, expires_at_ms: Option<i64> },
    CacheSetNx { namespace: String, key: String, value: Vec<u8>, expires_at_ms: Option<i64>, request_id: String },
    CacheDelete { namespace: String, key: String },
    CacheDeleteBatch { namespace: String, keys: Vec<String> },
    CacheIncrBy { namespace: String, key: String, delta: i64, expires_at_ms: Option<i64>, request_id: String },
    CacheZAdd { namespace: String, zset_key: String, score: f64, member: Vec<u8> },
    CacheZRem { namespace: String, zset_key: String, member: Vec<u8> },
    CacheZRemRangeByScore { namespace: String, zset_key: String, min: f64, max: f64, request_id: String },
    // ── Lock backend commands ──
    LockAcquire { namespace: String, key: String, owner_token: String, ttl_ms: u64, request_id: String },
    LockRenew { namespace: String, key: String, owner_token: String, ttl_ms: u64, request_id: String },
    LockRelease { namespace: String, key: String, owner_token: String, request_id: String },
}

impl ControlPlaneRaftCommand {
    pub fn apply_models(&self) -> Result<Vec<ControlPlaneApply>, ProfileValidationError> {
        match self {
            ControlPlaneRaftCommand::UpsertTaskProfile { snapshot } => {
                Ok(vec![ControlPlaneApply::UpsertTaskProfile {
                    key: snapshot.profile_key(),
                    snapshot: snapshot.clone(),
                }])
            }
            ControlPlaneRaftCommand::DisableTaskProfile { identity } => {
                identity.validate()?;
                Ok(vec![ControlPlaneApply::DisableTaskProfile {
                    key: identity.profile_key(),
                }])
            }
            ControlPlaneRaftCommand::BatchUpsertTaskProfiles { snapshots } => Ok(snapshots
                .iter()
                .cloned()
                .map(|snapshot| ControlPlaneApply::UpsertTaskProfile {
                    key: snapshot.profile_key(),
                    snapshot,
                })
                .collect()),
            ControlPlaneRaftCommand::UpsertAccountDefault { default } => {
                Ok(vec![ControlPlaneApply::UpsertAccountDefault {
                    key: default.storage_key(DefaultConfigScope::Account)?,
                    default: default.clone(),
                }])
            }
            ControlPlaneRaftCommand::UpsertPlatformDefault { default } => {
                Ok(vec![ControlPlaneApply::UpsertPlatformDefault {
                    key: default.storage_key(DefaultConfigScope::Platform)?,
                    default: default.clone(),
                }])
            }
            ControlPlaneRaftCommand::UpsertModuleDefault { default } => {
                Ok(vec![ControlPlaneApply::UpsertModuleDefault {
                    key: default.storage_key(DefaultConfigScope::Module)?,
                    default: default.clone(),
                }])
            }
            ControlPlaneRaftCommand::UpsertMiddleware { middleware } => {
                Ok(vec![ControlPlaneApply::UpsertMiddleware {
                    key: middleware.storage_key()?,
                    middleware: middleware.clone(),
                }])
            }
            ControlPlaneRaftCommand::UpsertNodeHeartbeat { namespace, node } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("node_id", &node.id)?;
                Ok(vec![ControlPlaneApply::UpsertNodeHeartbeat {
                    key: format!("{}:node:{}", namespace, node.id),
                    node: node.clone(),
                }])
            }
            ControlPlaneRaftCommand::RemoveNode { namespace, node_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("node_id", node_id)?;
                Ok(vec![ControlPlaneApply::RemoveNode {
                    key: format!("{}:node:{}", namespace, node_id),
                }])
            }
            ControlPlaneRaftCommand::UpsertResponseCacheOwner {
                namespace,
                cache_key,
                owner_namespace,
                owner_node_id,
                owner_api_base_url,
                expires_at,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("cache_key", cache_key)?;
                validate_non_empty("owner_namespace", owner_namespace)?;
                if let Some(node_id) = owner_node_id.as_deref() {
                    validate_non_empty("owner_node_id", node_id)?;
                }
                if let Some(owner_api_base_url) = owner_api_base_url.as_deref() {
                    validate_non_empty("owner_api_base_url", owner_api_base_url)?;
                }
                Ok(vec![ControlPlaneApply::UpsertResponseCacheOwner {
                    key: format!("{}:response_cache_owner:{}", namespace, cache_key),
                    cache_key: cache_key.clone(),
                    owner_namespace: owner_namespace.clone(),
                    owner_node_id: owner_node_id.clone(),
                    owner_api_base_url: owner_api_base_url.clone(),
                    expires_at: *expires_at,
                }])
            }
            ControlPlaneRaftCommand::RemoveResponseCacheOwner {
                namespace,
                cache_key,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("cache_key", cache_key)?;
                Ok(vec![ControlPlaneApply::RemoveResponseCacheOwner {
                    key: format!("{}:response_cache_owner:{}", namespace, cache_key),
                }])
            }
            ControlPlaneRaftCommand::RemoveResponseCacheOwnerIfMatch {
                namespace,
                cache_key,
                owner_namespace,
                owner_node_id,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("cache_key", cache_key)?;
                validate_non_empty("owner_namespace", owner_namespace)?;
                if let Some(node_id) = owner_node_id.as_deref() {
                    validate_non_empty("owner_node_id", node_id)?;
                }
                Ok(vec![ControlPlaneApply::RemoveResponseCacheOwnerIfMatch {
                    key: format!("{}:response_cache_owner:{}", namespace, cache_key),
                    owner_namespace: owner_namespace.clone(),
                    owner_node_id: owner_node_id.clone(),
                }])
            }
            ControlPlaneRaftCommand::SetModuleLock {
                namespace,
                module_id,
                locked_at,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("module_id", module_id)?;
                Ok(vec![ControlPlaneApply::SetModuleLock {
                    key: format!("{}:module_lock:{}", namespace, module_id),
                    locked_at: *locked_at,
                }])
            }
            ControlPlaneRaftCommand::RemoveModuleLock {
                namespace,
                module_id,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("module_id", module_id)?;
                Ok(vec![ControlPlaneApply::RemoveModuleLock {
                    key: format!("{}:module_lock:{}", namespace, module_id),
                }])
            }
            ControlPlaneRaftCommand::MarkTaskTerminated {
                namespace,
                task_id,
                terminated_at,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("task_id", task_id)?;
                Ok(vec![ControlPlaneApply::MarkTaskTerminated {
                    key: format!("{}:termination:task:{}", namespace, task_id),
                    terminated_at: *terminated_at,
                }])
            }
            ControlPlaneRaftCommand::MarkModuleTerminated {
                namespace,
                module_id,
                terminated_at,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("module_id", module_id)?;
                Ok(vec![ControlPlaneApply::MarkModuleTerminated {
                    key: format!("{}:termination:module:{}", namespace, module_id),
                    terminated_at: *terminated_at,
                }])
            }
            ControlPlaneRaftCommand::UpsertStatusCounter {
                namespace,
                counter_key,
                value,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("counter_key", counter_key)?;
                Ok(vec![ControlPlaneApply::UpsertStatusCounter {
                    key: format!("{}:status_counter:{}", namespace, counter_key),
                    value: *value,
                }])
            }
            ControlPlaneRaftCommand::UpsertStatusEntry {
                namespace,
                entry,
                previous_stage,
            } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("task_id", &entry.task_id)?;
                validate_non_empty("node_id", &entry.node_id)?;
                Ok(vec![ControlPlaneApply::UpsertStatusEntry {
                    key: format!("{}:status:{}", namespace, entry.task_id),
                    index_key: format!(
                        "{}:status:index:{}:{}",
                        namespace,
                        stage_key(entry.stage),
                        entry.task_id
                    ),
                    previous_index_key: previous_stage.map(|stage| {
                        format!(
                            "{}:status:index:{}:{}",
                            namespace,
                            stage_key(stage),
                            entry.task_id
                        )
                    }),
                    entry: entry.clone(),
                }])
            }
            // ── Cache backend apply models ──
            ControlPlaneRaftCommand::CacheSet { namespace, key, value, expires_at_ms } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                Ok(vec![ControlPlaneApply::CacheKvUpsert {
                    state_key: format!("{}:cache:kv:{}", namespace, key),
                    value: ControlPlaneCacheValue {
                        value: value.clone(),
                        expires_at_ms: *expires_at_ms,
                        version: 0,
                        updated_at_ms: 0,
                    },
                }])
            }
            ControlPlaneRaftCommand::CacheSetNx { namespace, key, value, expires_at_ms, request_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                Ok(vec![ControlPlaneApply::CacheKvUpsertNx {
                    state_key: format!("{}:cache:kv:{}", namespace, key),
                    value: ControlPlaneCacheValue {
                        value: value.clone(),
                        expires_at_ms: *expires_at_ms,
                        version: 0,
                        updated_at_ms: 0,
                    },
                    request_id: request_id.clone(),
                }])
            }
            ControlPlaneRaftCommand::CacheDelete { namespace, key } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                Ok(vec![ControlPlaneApply::CacheKvDelete {
                    state_key: format!("{}:cache:kv:{}", namespace, key),
                }])
            }
            ControlPlaneRaftCommand::CacheDeleteBatch { namespace, keys } => {
                validate_non_empty("namespace", namespace)?;
                let state_keys: Vec<String> = keys.iter().map(|k| format!("{}:cache:kv:{}", namespace, k)).collect();
                Ok(vec![ControlPlaneApply::CacheKvDeleteMany { state_keys }])
            }
            ControlPlaneRaftCommand::CacheIncrBy { namespace, key, delta, expires_at_ms, request_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                Ok(vec![ControlPlaneApply::CacheCounterIncr {
                    state_key: format!("{}:cache:counter:{}", namespace, key),
                    delta: *delta,
                    expires_at_ms: *expires_at_ms,
                    request_id: request_id.clone(),
                }])
            }
            ControlPlaneRaftCommand::CacheZAdd { namespace, zset_key, score, member } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("zset_key", zset_key)?;
                let score_key = f64_to_be_bytes(*score);
                let member_hex = hex::encode(member);
                Ok(vec![
                    ControlPlaneApply::CacheZSetEntryUpsert {
                        state_key: format!(
                            "{}:cache:zset:{}:{}:{}",
                            namespace, zset_key,
                            hex::encode(score_key),
                            member_hex
                        ),
                        entry: ControlPlaneZSetEntry {
                            score: *score,
                            member: member.clone(),
                            version: 0,
                            updated_at_ms: 0,
                        },
                    },
                    ControlPlaneApply::CacheKvUpsert {
                        state_key: format!(
                            "{}:cache:zset_idx:{}:{}",
                            namespace, zset_key, member_hex
                        ),
                        value: ControlPlaneCacheValue {
                            value: score_key.to_vec(),
                            expires_at_ms: None,
                            version: 0,
                            updated_at_ms: 0,
                        },
                    },
                ])
            }
            ControlPlaneRaftCommand::CacheZRem { namespace, zset_key, member } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("zset_key", zset_key)?;
                let member_hex = hex::encode(member);
                Ok(vec![
                    ControlPlaneApply::CacheZSetEntryDeleteByMember {
                        namespace: namespace.clone(),
                        zset_key: zset_key.clone(),
                        member_hex: member_hex.clone(),
                    },
                    ControlPlaneApply::CacheKvDelete {
                        state_key: format!("{}:cache:zset_idx:{}:{}", namespace, zset_key, member_hex),
                    },
                ])
            }
            ControlPlaneRaftCommand::CacheZRemRangeByScore { namespace, zset_key, min, max, request_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("zset_key", zset_key)?;
                Ok(vec![ControlPlaneApply::CacheZSetRangeDelete {
                    namespace: namespace.clone(),
                    prefix: format!("{}:cache:zset:{}:", namespace, zset_key),
                    min: *min,
                    max: *max,
                    request_id: request_id.clone(),
                }])
            }
            // ── Lock backend apply models ──
            ControlPlaneRaftCommand::LockAcquire { namespace, key, owner_token, ttl_ms, request_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                validate_non_empty("owner_token", owner_token)?;
                let now_ms = current_time_ms_i64();
                Ok(vec![ControlPlaneApply::LockUpsertConditional {
                    state_key: format!("{}:lock:{}", namespace, key),
                    record: LockRecord {
                        owner_token: owner_token.clone(),
                        locked_at_ms: now_ms,
                        expires_at_ms: now_ms.saturating_add(*ttl_ms as i64),
                        version: 0,
                    },
                    request_id: request_id.clone(),
                }])
            }
            ControlPlaneRaftCommand::LockRenew { namespace, key, owner_token, ttl_ms, request_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                validate_non_empty("owner_token", owner_token)?;
                Ok(vec![ControlPlaneApply::LockRenewConditional {
                    state_key: format!("{}:lock:{}", namespace, key),
                    owner_token: owner_token.clone(),
                    ttl_ms: *ttl_ms,
                    request_id: request_id.clone(),
                }])
            }
            ControlPlaneRaftCommand::LockRelease { namespace, key, owner_token, request_id } => {
                validate_non_empty("namespace", namespace)?;
                validate_non_empty("key", key)?;
                validate_non_empty("owner_token", owner_token)?;
                Ok(vec![ControlPlaneApply::LockDelete {
                    state_key: format!("{}:lock:{}", namespace, key),
                    owner_token: owner_token.clone(),
                    request_id: request_id.clone(),
                }])
            }
            ControlPlaneRaftCommand::SetPauseState { namespace, paused } => {
                validate_non_empty("namespace", namespace)?;
                Ok(vec![ControlPlaneApply::SetPauseState {
                    key: format!("{}:control:pause", namespace),
                    paused: *paused,
                }])
            }
        }
    }
}

/// Lock record stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LockRecord {
    pub owner_token: String,
    pub locked_at_ms: i64,
    pub expires_at_ms: i64,
    pub version: u64,
}

fn current_time_ms_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Outcome record for commands that need to return apply-side results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CommandOutcome {
    pub request_id: String,
    pub success: bool,
    pub value: i64,
    pub updated_at_ms: i64,
}

/// Cache value stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ControlPlaneCacheValue {
    pub value: Vec<u8>,
    pub expires_at_ms: Option<i64>,
    pub version: u64,
    pub updated_at_ms: i64,
}

/// ZSet entry stored in RocksDB read model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ControlPlaneZSetEntry {
    pub score: f64,
    pub member: Vec<u8>,
    pub version: u64,
    pub updated_at_ms: i64,
}

/// Encode f64 to big-endian bytes with sign-bit inversion for lexicographic ordering
/// so that negative scores sort correctly in RocksDB prefix scans.
pub fn f64_to_be_bytes(score: f64) -> [u8; 8] {
    let mut bits = score.to_bits();
    if bits & 0x8000_0000_0000_0000 != 0 {
        bits = !bits;
    } else {
        bits ^= 0x8000_0000_0000_0000;
    }
    bits.to_be_bytes()
}

/// Decode f64 from the big-endian bytes produced by `f64_to_be_bytes`.
pub fn f64_from_be_bytes(bytes: &[u8]) -> Option<f64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    let mut bits = u64::from_be_bytes(arr);
    if bits & 0x8000_0000_0000_0000 != 0 {
        bits ^= 0x8000_0000_0000_0000;
    } else {
        bits = !bits;
    }
    Some(f64::from_bits(bits))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ControlPlaneApply {
    UpsertTaskProfile {
        key: String,
        snapshot: TaskProfileSnapshot,
    },
    DisableTaskProfile {
        key: String,
    },
    UpsertAccountDefault {
        key: String,
        default: DefaultConfigUpsert,
    },
    UpsertPlatformDefault {
        key: String,
        default: DefaultConfigUpsert,
    },
    UpsertModuleDefault {
        key: String,
        default: DefaultConfigUpsert,
    },
    UpsertMiddleware {
        key: String,
        middleware: MiddlewareUpsert,
    },
    UpsertNodeHeartbeat {
        key: String,
        node: NodeInfo,
    },
    RemoveNode {
        key: String,
    },
    UpsertResponseCacheOwner {
        key: String,
        cache_key: String,
        owner_namespace: String,
        owner_node_id: Option<String>,
        owner_api_base_url: Option<String>,
        expires_at: Option<i64>,
    },
    RemoveResponseCacheOwner {
        key: String,
    },
    RemoveResponseCacheOwnerIfMatch {
        key: String,
        owner_namespace: String,
        owner_node_id: Option<String>,
    },
    SetModuleLock {
        key: String,
        locked_at: u64,
    },
    RemoveModuleLock {
        key: String,
    },
    MarkTaskTerminated {
        key: String,
        terminated_at: u64,
    },
    MarkModuleTerminated {
        key: String,
        terminated_at: u64,
    },
    UpsertStatusCounter {
        key: String,
        value: i64,
    },
    UpsertStatusEntry {
        key: String,
        index_key: String,
        previous_index_key: Option<String>,
        entry: StatusEntry,
    },
    SetPauseState {
        key: String,
        paused: bool,
    },
    // ── Cache backend apply models ──
    CacheKvUpsert {
        state_key: String,
        value: ControlPlaneCacheValue,
    },
    CacheKvUpsertNx {
        state_key: String,
        value: ControlPlaneCacheValue,
        request_id: String,
    },
    CacheKvDelete {
        state_key: String,
    },
    CacheKvDeleteMany {
        state_keys: Vec<String>,
    },
    CacheCounterSet {
        state_key: String,
        value: i64,
        expires_at_ms: Option<i64>,
        version: u64,
        updated_at_ms: i64,
    },
    CacheCounterIncr {
        state_key: String,
        delta: i64,
        expires_at_ms: Option<i64>,
        request_id: String,
    },
    CacheZSetEntryUpsert {
        state_key: String,
        entry: ControlPlaneZSetEntry,
    },
    CacheZSetEntryDeleteByMember {
        namespace: String,
        zset_key: String,
        member_hex: String,
    },
    CacheZSetRangeDelete {
        namespace: String,
        prefix: String,
        min: f64,
        max: f64,
        request_id: String,
    },
    // ── Lock apply models ──
    LockUpsert {
        state_key: String,
        record: LockRecord,
    },
    LockUpsertConditional {
        state_key: String,
        record: LockRecord,
        request_id: String,
    },
    LockRenewConditional {
        state_key: String,
        owner_token: String,
        ttl_ms: u64,
        request_id: String,
    },
    LockDelete {
        state_key: String,
        owner_token: String,
        request_id: String,
    },
    // ── Outcome ──
    CommandOutcomeUpsert {
        state_key: String,
        outcome: CommandOutcome,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProfileValidationError {
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("{field} contains duplicate binding `{name}`")]
    DuplicateBinding { field: &'static str, name: String },
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<(), ProfileValidationError> {
    if value.trim().is_empty() {
        return Err(ProfileValidationError::EmptyField { field });
    }
    Ok(())
}

fn validate_middleware_bindings(
    field: &'static str,
    bindings: &[MiddlewareBinding],
) -> Result<(), ProfileValidationError> {
    let mut names = BTreeSet::new();
    for binding in bindings {
        validate_non_empty("middleware_name", &binding.name)?;
        if !names.insert(binding.name.clone()) {
            return Err(ProfileValidationError::DuplicateBinding {
                field,
                name: binding.name.clone(),
            });
        }
    }
    Ok(())
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
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::model::{PayloadCodec, Priority};

    fn sample_envelope(schema_id: &str) -> TypedEnvelope {
        TypedEnvelope::new(schema_id, 1, PayloadCodec::MsgPack, vec![1_u8, 2_u8])
    }

    fn sample_common() -> ResolvedCommonConfig {
        ResolvedCommonConfig {
            priority: Priority::High,
            ..ResolvedCommonConfig::default()
        }
    }

    fn sample_identity() -> TaskProfileIdentity {
        TaskProfileIdentity {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: "catalog".to_string(),
        }
    }

    #[test]
    fn task_profile_upsert_builds_snapshot() {
        let upsert = TaskProfileUpsert {
            identity: sample_identity(),
            enabled: true,
            common: sample_common(),
            node_configs: BTreeMap::from([("page".to_string(), sample_envelope("page.config"))]),
            download_middleware: vec![MiddlewareBinding {
                name: "download-cache".to_string(),
                middleware_type: MiddlewareType::Download,
                weight: 10,
            }],
            data_middleware: vec![],
            middleware_configs: BTreeMap::from([(
                "download-cache".to_string(),
                sample_envelope("middleware.config"),
            )]),
            debug_layers_json: Some(serde_json::json!({"source": "api"})),
            updated_by: "tester".to_string(),
        };

        let snapshot = upsert
            .into_snapshot(7, 1000)
            .expect("snapshot should assemble");

        assert_eq!(
            snapshot.profile_key(),
            "demo:profile:account-a:platform-x:catalog"
        );
        assert_eq!(snapshot.version, 7);
        assert_eq!(snapshot.common.priority, Priority::High);
        assert_eq!(snapshot.node_configs.len(), 1);
        assert_eq!(snapshot.updated_by, "tester");
    }

    #[test]
    fn control_plane_command_expands_to_apply_models() {
        let command = ControlPlaneRaftCommand::UpsertTaskProfile {
            snapshot: TaskProfileSnapshot {
                namespace: "demo".to_string(),
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: "catalog".to_string(),
                version: 3,
                enabled: true,
                common: sample_common(),
                node_configs: BTreeMap::from([(
                    "page".to_string(),
                    sample_envelope("page.config"),
                )]),
                download_middleware: vec![],
                data_middleware: vec![],
                middleware_configs: BTreeMap::new(),
                debug_layers_json: None,
                updated_at: 99,
                updated_by: "tester".to_string(),
            },
        };

        let apply_models = command
            .apply_models()
            .expect("apply model conversion should succeed");

        assert_eq!(apply_models.len(), 1);
        match &apply_models[0] {
            ControlPlaneApply::UpsertTaskProfile { key, snapshot } => {
                assert_eq!(key, "demo:profile:account-a:platform-x:catalog");
                assert_eq!(snapshot.version, 3);
            }
            other => panic!("unexpected apply model: {other:?}"),
        }
    }

    #[test]
    fn default_and_middleware_storage_keys_are_stable() {
        let default = DefaultConfigUpsert {
            namespace: "demo".to_string(),
            name: "account-a".to_string(),
            enabled: true,
            config: sample_envelope("account.default"),
        };
        let middleware = MiddlewareUpsert {
            namespace: "demo".to_string(),
            name: "download-cache".to_string(),
            middleware_type: MiddlewareType::Download,
            enabled: true,
            config: sample_envelope("middleware.config"),
            weight: 20,
        };

        assert_eq!(
            default
                .storage_key(DefaultConfigScope::Account)
                .expect("default key should build"),
            "demo:default:account:account-a"
        );
        assert_eq!(
            middleware
                .storage_key()
                .expect("middleware key should build"),
            "demo:middleware:download:download-cache"
        );
    }

    #[test]
    fn validation_rejects_duplicate_or_empty_bindings() {
        let error = TaskProfileUpsert {
            identity: sample_identity(),
            enabled: true,
            common: sample_common(),
            node_configs: BTreeMap::new(),
            download_middleware: vec![
                MiddlewareBinding {
                    name: "cache".to_string(),
                    middleware_type: MiddlewareType::Download,
                    weight: 1,
                },
                MiddlewareBinding {
                    name: "cache".to_string(),
                    middleware_type: MiddlewareType::Download,
                    weight: 2,
                },
            ],
            data_middleware: vec![],
            middleware_configs: BTreeMap::new(),
            debug_layers_json: None,
            updated_by: "tester".to_string(),
        }
        .validate()
        .expect_err("duplicate middleware names should fail");

        assert_eq!(
            error,
            ProfileValidationError::DuplicateBinding {
                field: "download_middleware",
                name: "cache".to_string(),
            }
        );
    }
}
