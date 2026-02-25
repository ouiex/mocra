//! Shared domain models, runtime state, and processing infrastructure.
//!
//! This crate centralizes cross-cutting building blocks used by workspace crates,
//! including configuration, storage abstractions, processor chains, and operational
//! policies.

pub mod interface;
pub mod model;
pub mod processors;
pub mod state;
pub mod status_tracker;
pub mod stream_stats;
pub mod registry;
pub mod config;
pub mod storage;
pub mod policy;
