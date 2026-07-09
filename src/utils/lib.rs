//! 工具库已抽为独立 crate [`mocra_core`] 的 `utils` 模块。
//!
//! 此模块保留为 re-export shim,使既有 `crate::utils::*` 引用继续有效(零改动迁移);
//! 新代码可直接 `use mocra_core::utils::...`。

pub use mocra_core::utils::*;
