use crate::middleware::data_middleware::store_df_pgsql::StoreDfPgsqlMiddleware;
use async_trait::async_trait;
use common::interface::{DataMiddleware, DataStoreMiddleware};
use common::model::data::Data;
use common::model::ModuleConfig;
use errors::{DataStoreError, Result};
use std::sync::Arc;
use utils::connector::postgres_connection;

pub struct ToPostgreSQLMiddleware;
#[async_trait]
impl DataMiddleware for ToPostgreSQLMiddleware {
    fn name(&self) -> String {
        "to_postgresql".to_string()
    }

    async fn handle_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Data {
        data
    }

    fn default_arc() -> Arc<dyn DataMiddleware>
    where
        Self: Sized,
    {
        Arc::new(ToPostgreSQLMiddleware)
    }
}
#[async_trait]
impl DataStoreMiddleware for ToPostgreSQLMiddleware {
    async fn store_data(&self, data: Data, config: &Option<ModuleConfig>) -> Result<()> {
        if let Some(module_config) = config
            && let Some(config) = module_config.get_postgres_config()
        {
            let connection = postgres_connection(
                &config.database_host,
                config.database_port,
                &config.database_name,
                &config.database_schema,
                &config.database_user,
                &config.database_password,
            )
            .await
            .ok_or(DataStoreError::ConnectionFailed(
                format!("cannot connect to db: {config:?}").into(),
            ))?;
            let store = StoreDfPgsqlMiddleware::new(Arc::new(connection));
            store.update_from_df(data).await
        } else {
            Ok(())
        }
    }

    fn default_arc() -> Arc<dyn DataStoreMiddleware>
    where
        Self: Sized,
    {
        Arc::new(ToPostgreSQLMiddleware)
    }
}
