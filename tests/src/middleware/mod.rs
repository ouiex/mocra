use std::sync::Arc;
use mocra::common::interface::{DataMiddleware, DataStoreMiddleware, DownloadMiddleware};

mod object_store;

pub fn register_download_middlewares() -> Vec<Arc<dyn DownloadMiddleware>> {
    Vec::new()
}

pub fn register_data_middlewares() -> Vec<Arc<dyn DataMiddleware>> {
    Vec::new()
}
pub fn register_data_store_middlewares() -> Vec<Arc<dyn DataStoreMiddleware>> {
    vec![<object_store::ObjectStoreMiddleware as DataStoreMiddleware>::default_arc()]
}
