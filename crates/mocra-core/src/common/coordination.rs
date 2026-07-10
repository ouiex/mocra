//! 协调后端抽象已移至 [`crate::utils::coordination`]
//! —— 其主要消费者(分布式锁管理器、限流器)都在 `utils`,放在那里可避免 `common ↔ utils` 环。
//!
//! 此模块保留为 re-export shim,使既有 `crate::common::coordination::*` 路径继续有效。

pub use crate::utils::coordination::*;
