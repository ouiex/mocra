use super::super::entity::prelude::*;
use async_trait::async_trait;

use common::interface::DataMiddleware;
use common::model::data::{Data, DataType};
use common::model::ModuleConfig;
use errors::DataStoreError;
use polars::prelude::SerReader;
use utils::connector::postgres_connection;
use sea_orm::EntityTrait;
use sea_orm::QueryFilter;
use sea_orm::{ActiveModelTrait, ColumnTrait, IntoActiveModel};
use std::sync::Arc;

pub struct RecordUpdate;
#[async_trait]
impl DataMiddleware for RecordUpdate {
    fn name(&self) -> String {
        "to_pgsql_update_record".to_string()
    }

    async fn handle_data(&self, data: Data, config: &Option<ModuleConfig>) -> Data {
        let conn = if let Some(module_config) = config
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
            ));
            if let Ok(conn) = connection {
                conn
            } else {
                return data;
            }
        } else {
            return data;
        };
        if let DataType::DataFrame(byte_data) = &data.data {
            let schema = byte_data.schema.clone();
            let table = byte_data.table.clone();
            let df = {
                let cursor = std::io::Cursor::new(byte_data.data.to_owned());
                match polars::io::ipc::IpcReader::new(cursor).finish() {
                    Ok(df) => df,
                    Err(e) => {
                        log::error!("Failed to read IPC data: {}", e);
                        return data;
                    }
                }
            };
            let columns = df
                .get_column_names()
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>();
            if columns.contains(&"date".to_string())
                && let Ok(series) = df.column("date")
                && let Ok(casted) = series.cast(&polars::prelude::DataType::Date)
                && let Ok(date_chunked) = casted.date()
            {
                if let Some(max_date) = date_chunked.as_date_iter().max() {
                    let record = DataTableUpdateRecord::find()
                        .filter(
                            DataTableUpdateColumn::Schema
                                .eq(&schema)
                                .and(DataTableUpdateColumn::TableName.eq(&table))
                                .and(DataTableUpdateColumn::Account.eq(data.account.clone()))
                                .and(DataTableUpdateColumn::Module.eq(data.module.clone()))
                                .and(DataTableUpdateColumn::Platform.eq(data.platform.clone())),
                        )
                        .one(&conn)
                        .await;
                    if let Ok(Some(record)) = record {
                        // Only update when the incoming max_date is greater than what's stored
                        let record_max_date = record.max_date.clone();
                        let mut am: DataTableUpdateRecordActiveModel = record.into_active_model();
                        if max_date > record_max_date {
                            am.max_date = sea_orm::Set(max_date);
                        } else {
                            // Optional: verbose log to help diagnose "updated 0 rows" scenarios
                            log::debug!(
                                "Skip update: incoming max_date ({:?}) <= stored max_date ({:?}).",
                                max_date,
                                record_max_date
                            );
                        }

                        am.update_at = sea_orm::Set(chrono::Local::now().naive_local());
                        if let Err(e) = am.update(&conn).await {
                            log::error!("Failed to update record: {}", e);
                        }
                    } else {
                        let new_record = DataTableUpdateRecordActiveModel {
                            schema: sea_orm::Set(schema),
                            table_name: sea_orm::Set(table),
                            account: sea_orm::Set(data.account.clone()),
                            module: sea_orm::Set(data.module.clone()),
                            platform: sea_orm::Set(data.platform.clone()),
                            max_date: sea_orm::Set(max_date),
                            update_at: sea_orm::Set(chrono::Local::now().naive_local()),
                        };
                        if let Err(e) = new_record.insert(&conn).await {
                            log::error!("Failed to insert record: {}", e);
                        }
                    }
                }
            }
        }

        data
    }

    fn default_arc() -> Arc<dyn DataMiddleware>
    where
        Self: Sized,
    {
        Arc::new(RecordUpdate)
    }
}
