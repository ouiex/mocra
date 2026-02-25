//! mocra: single-package entry point.
//! All former workspace crates are embedded as local modules under `src/`.

pub mod prelude;

#[path = "cacheable/lib.rs"]
pub mod cacheable;
#[path = "common/lib.rs"]
pub mod common;
#[path = "downloader/lib.rs"]
pub mod downloader;
#[path = "engine/lib.rs"]
pub mod engine;
#[path = "errors/lib.rs"]
pub mod errors;
#[path = "proxy/lib.rs"]
pub mod proxy;
#[path = "queue/lib.rs"]
pub mod queue;
#[path = "sync/lib.rs"]
pub mod sync;
#[path = "utils/lib.rs"]
pub mod utils;

#[cfg(feature = "js-v8")]
#[path = "js_v8/lib.rs"]
pub mod js_v8;

#[cfg(feature = "polars")]
pub mod polars {
    pub mod polars {
        pub use ::polars::*;
    }
    pub mod polars_lazy {
        pub use ::polars_lazy::*;
    }
    pub mod polars_ops {
        pub use ::polars_ops::*;
    }
}

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;