//! 运行时的轻量账号/平台信息(不依赖 sea-orm)。
//!
//! 取代 `Task` / `Module` 中原本内嵌的 sea-orm 实体 `AccountModel` / `PlatformModel`,
//! 使运行时数据结构与数据库层解耦 —— 这是把 sea-orm gate 出默认构建的前置(重构 Phase 2)。
//!
//! DB 路径从实体转换填充;无 DB(standalone)路径直接构造。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 账号运行时信息(轻量)。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountInfo {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub config: Value,
}

/// 平台运行时信息(轻量)。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub config: Value,
}
