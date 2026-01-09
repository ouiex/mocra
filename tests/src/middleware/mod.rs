use crate::middleware::data_middleware::to_file_middleware::ToFileMiddleware;
use crate::middleware::data_middleware::to_postgresql_middleware::ToPostgreSQLMiddleware;

use std::sync::Arc;
use common::interface::{DataMiddleware, DataStoreMiddleware, DownloadMiddleware};
use crate::middleware::data_middleware::object_store::ObjectStoreMiddleware;
use crate::middleware::data_middleware::to_pgsql_update_record::RecordUpdate;

mod data_middleware;
pub mod download_middleware;
mod entity;

pub fn register_download_middlewares() -> Vec<Arc<dyn DownloadMiddleware>> {
    let middleware_vec: Vec<Arc<dyn DownloadMiddleware>> = vec![
        download_middleware::binary_store_middleware::BinaryStoreMiddleware::default_arc(),
        download_middleware::taobao_sign_middleware::TaobaoSignMiddleware::default_arc(),
        download_middleware::douyin_encrypt_middleware::DouyinEncryptMiddleware::default_arc(),
        download_middleware::jdsz_encrypt_middleware::JdszEncryptMiddleware::default_arc(),
        download_middleware::random_useragent::RandomUserAgentMiddleware::default_arc(),
    ];

    middleware_vec
}

pub fn register_data_middlewares() -> Vec<Arc<dyn DataMiddleware>> {
    let middleware_vec: Vec<Arc<dyn DataMiddleware>> =
        vec![
            <RecordUpdate as DataMiddleware>::default_arc(),
        ];

    middleware_vec
}
pub fn register_data_store_middlewares() -> Vec<Arc<dyn DataStoreMiddleware>> {
    let middleware_vec: Vec<Arc<dyn DataStoreMiddleware>> = vec![
        <ToFileMiddleware as DataStoreMiddleware>::default_arc(),
        <ToPostgreSQLMiddleware as DataStoreMiddleware>::default_arc(),
        <ObjectStoreMiddleware as DataStoreMiddleware>::default_arc(),
    ];
    middleware_vec
}
