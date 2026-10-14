//! Lightweight runtime account/platform information (no sea-orm dependency).
//!
//! Replaces the sea-orm entities `AccountModel` / `PlatformModel` that used to be embedded in
//! `Task` / `Module`, decoupling the runtime data structures from the database layer — a
//! prerequisite for gating sea-orm out of the default build (refactor Phase 2).
//!
//! The DB path fills these in by converting from the entities; the no-DB (standalone) path
//! constructs them directly.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Account runtime information (lightweight).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountInfo {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub config: Value,
}

/// Platform runtime information (lightweight).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub id: i32,
    pub name: String,
    #[serde(default)]
    pub config: Value,
}
