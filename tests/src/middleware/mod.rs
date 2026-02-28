use mocra::common::interface::{
    DataMiddlewareHandle, DataStoreMiddleware, DataStoreMiddlewareHandle,
    DownloadMiddlewareHandle,
};

mod object_store;

pub fn register_download_middlewares() -> Vec<DownloadMiddlewareHandle> {
    Vec::new()
}

pub fn register_data_middlewares() -> Vec<DataMiddlewareHandle> {
    Vec::new()
}
pub fn register_data_store_middlewares() -> Vec<DataStoreMiddlewareHandle> {
    vec![<object_store::ObjectStoreMiddleware as DataStoreMiddleware>::default_arc()]
}
