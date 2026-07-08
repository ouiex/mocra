//! 状态机命令与结果。
//!
//! `Cmd` 是**可序列化**的:将来经 Raft 日志复制到多数派,再由每个节点确定性地 `apply`。
//! 因此时间(`now_ms`)由调用方在命令里携带,而非在 `apply` 内部读时钟 —— 保证确定性。

use serde::{Deserialize, Serialize};

/// 复制状态机命令。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Cmd {
    /// 写入键值。
    Set { key: Vec<u8>, value: Vec<u8> },
    /// 删除键。
    Delete { key: Vec<u8> },
    /// 比较并交换:仅当当前值等于 `expect` 时写入 `value`。返回是否成功。
    Cas {
        key: Vec<u8>,
        expect: Option<Vec<u8>>,
        value: Vec<u8>,
    },
    /// 获取锁:空闲或已过期(或本人持有)则授予,返回单调递增的 fencing token。
    AcquireLock {
        key: String,
        holder: String,
        now_ms: u64,
        ttl_ms: u64,
    },
    /// 续租:仍为持有者且未过期则延长有效期。
    RenewLock {
        key: String,
        holder: String,
        now_ms: u64,
        ttl_ms: u64,
    },
    /// 释放锁(仅持有者可释放)。
    ReleaseLock { key: String, holder: String },
}

/// `apply` 的结果。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CmdResult {
    /// 无返回值的成功(set/delete/release)。
    Ok,
    /// cas / renew 的成功与否。
    Bool(bool),
    /// acquire_lock 的 fencing token(`None` = 未获得锁)。
    Fencing(Option<u64>),
}

/// 锁记录(存于状态机)。fencing token 单调递增,用于防止过期持有者复活后重复处理。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lock {
    pub holder: String,
    pub expire_at_ms: u64,
    pub fencing_token: u64,
}
