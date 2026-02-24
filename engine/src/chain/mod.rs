//! Engine processing chains.
//!
//! This module groups queue-facing typed chains used by the runtime:
//! - task ingress (`task_model_chain`),
//! - request download (`download_chain`),
//! - response parsing and storage (`parser_chain`),
//! - websocket download flow (`stream_chain`).

pub mod backpressure;
pub mod task_model_chain;
pub mod download_chain;
pub mod parser_chain;
pub mod stream_chain;

pub use download_chain::*;
pub use parser_chain::*;
pub use task_model_chain::*;