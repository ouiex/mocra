//! DAG 调度(`mocra-dag` shim + 宿主 adapter)已抽为独立 crate [`mocra_core`] 的
//! `schedule` 模块。
//!
//! 此模块保留为 re-export shim,使既有 `crate::schedule::*` 与 `crate::schedule::dag::*`
//! 路径继续有效(零改动迁移);新代码可直接 `use mocra_core::schedule::...`。

pub use mocra_core::schedule::*;

/// 兼容旧路径 `crate::schedule::dag::*`。
pub mod dag {
    pub use mocra_core::schedule::dag::*;
}
