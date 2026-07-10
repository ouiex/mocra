//! Shared domain models, runtime state, and processing infrastructure.
//!
//! This crate centralizes cross-cutting building blocks used by workspace crates,
//! including configuration, storage abstractions, processor chains, and operational
//! policies.

pub mod config;
pub mod context;
pub mod coordination;
pub mod interface;
pub mod metrics;
pub mod model;
pub mod policy;
pub mod processors;
pub mod registry;
pub mod state;
pub mod status_tracker;
pub mod storage;
pub mod stream_stats;
