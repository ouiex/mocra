mod backend;
mod cache_able;
mod local_backend;
pub mod raft_backend;
mod service;

pub use cache_able::CacheAble;
pub use service::CacheService;

#[cfg(test)]
mod tests;
