use std::sync::Arc;
use common::interface::{DataMiddleware, DataStoreMiddleware, DownloadMiddleware};

pub fn register_download_middlewares() -> Vec<Arc<dyn DownloadMiddleware>> {
    Vec::new()
}

pub fn register_data_middlewares() -> Vec<Arc<dyn DataMiddleware>> {
    Vec::new()
}
pub fn register_data_store_middlewares() -> Vec<Arc<dyn DataStoreMiddleware>> {
    Vec::new()
}
