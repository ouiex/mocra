//! mocra: Unified entry point for the mocra workspace.
//! This crate re-exports the internal crates so consumers can
//! `use mocra::engine::...`, `mocra::common::...`, etc.

pub mod prelude;

pub mod common { pub use ::common::*; }
pub mod downloader { pub use ::downloader::*; }
pub mod engine { pub use ::engine::*; }
pub mod queue { pub use ::queue::*; }
pub mod sync { pub use ::sync::*; }
pub mod utils { pub use ::utils::*; }
pub mod proxy { pub use ::proxy::*; }
pub mod errors { pub use ::errors::*; }
pub mod cacheable { pub use ::cacheable::*; }

#[cfg(feature = "js-v8")]
pub mod js_v8{
    pub use ::js_v8::*;
}

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