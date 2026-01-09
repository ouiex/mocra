#![allow(dead_code)]
use log::trace;
use common::model::data::{Data, DataType};
use errors::{DataStoreError, Result};
use polars::io::ipc::IpcReader;
use polars::io::SerReader;
use polars::prelude::AnyValue;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait, Value};
use std::sync::Arc;

pub struct StoreDfPgsqlMiddleware {
    postgres: Arc<DatabaseConnection>,
}
impl StoreDfPgsqlMiddleware {
    pub fn new(pg: Arc<DatabaseConnection>) -> StoreDfPgsqlMiddleware {
        StoreDfPgsqlMiddleware { postgres: pg }
    }

    async fn columns(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let query = r#"
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2;
        "#;
        let rows = self
            .postgres
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                [schema.into(), table.into()],
            ))
            .await
            .map_err(|e| errors::OrmError::QueryExecutionError(e.into()))?;
        let columns = rows
            .iter()
            .map(|row| row.try_get::<String>("", "column_name").unwrap_or_default())
            .collect();
        Ok(columns)
    }
    async fn get_primary_key_columns(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let query = r#"
            SELECT a.attname
            FROM pg_attribute a
            JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON i.indrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r' AND i.indisprimary = true
            AND n.nspname = $1 AND c.relname = $2;
        "#;
        let rows = self
            .postgres
            .query_all(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                [schema.into(), table.into()],
            ))
            .await
            .map_err(|e| errors::OrmError::QueryExecutionError(e.into()))?;
        let pk_columns = rows
            .iter()
            .map(|row| row.try_get::<String>("", "attname").unwrap_or_default())
            .collect();
        Ok(pk_columns)
    }
    pub async fn update_from_df(&self, data: Data) -> Result<()> {
        let schema;
        let table;
        let df = match data.data {
            DataType::DataFrame(v) => {
                let cursor = std::io::Cursor::new(v.data);
                schema = v.schema.clone();
                table = v.table.clone();
                match IpcReader::new(cursor).finish() {
                    Ok(df) => df,
                    Err(e) => return Err(DataStoreError::InvalidData(e.to_string().into()).into()),
                }
            }
            _ => {
                return Err(DataStoreError::InvalidData("Not DataFrame".to_string().into()).into());
            }
        };

        // 获取表的所有列名
        let columns = self.columns(&schema, &table).await?;

        // 获取主键列名
        let pk_columns = self.get_primary_key_columns(&schema, &table).await?;

        if pk_columns.is_empty() {
            return Err(errors::Error::new(
                errors::ErrorKind::DataStore,
                Some(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Table has no primary key columns",
                )) as errors::BoxError),
            ));
        }

        // 获取DataFrame的列名，只处理存在于目标表中的列
        let df_columns: Vec<String> = df
            .get_column_names()
            .iter()
            .filter(|col| columns.contains(&col.to_string()))
            .map(|col| col.to_string())
            .collect();

        if df_columns.is_empty() {
            return Ok(()); // 没有匹配的列，直接返回
        }

        // 使用 lazy evaluation 预处理数据，只选择需要的列
        let processed_df = if df_columns.len() == df.get_column_names().len() {
            // 如果所有列都需要，直接使用原 DataFrame
            df.clone()
        } else {
            // 只选择需要的列
            df.select(&df_columns).map_err(|e| {
                errors::Error::new(
                    errors::ErrorKind::DataStore,
                    Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Failed to select columns: {e}"),
                    )) as errors::BoxError),
                )
            })?
        };

        // 批量构建 SQL 和参数
        let batch_size = 2000; // 每批处理 2000 行
        let total_rows = processed_df.height();

        // 开始事务
        let txn = self
            .postgres
            .begin()
            .await
            .map_err(|e| errors::OrmError::TransactionError(e.into()))?;

        // 预先确定主键列和非主键列
        let pk_col_indices: Vec<usize> = df_columns
            .iter()
            .enumerate()
            .filter(|(_, col)| pk_columns.contains(col))
            .map(|(idx, _)| idx)
            .collect();

        let non_pk_col_indices: Vec<usize> = df_columns
            .iter()
            .enumerate()
            .filter(|(_, col)| !pk_columns.contains(col))
            .map(|(idx, _)| idx)
            .collect();

        // 构建基础 SQL 模板
        let columns_str = df_columns.join(", ");
        let conflict_columns: Vec<String> = pk_col_indices
            .iter()
            .map(|&idx| df_columns[idx].clone())
            .collect();
        let conflict_str = conflict_columns.join(", ");

        // 分批处理
        for batch_start in (0..total_rows).step_by(batch_size) {
            let batch_end = std::cmp::min(batch_start + batch_size, total_rows);

            // 构建批量插入的 SQL
            let mut all_values: Vec<Value> = Vec::new();
            let mut value_groups: Vec<String> = Vec::new();

            for row_idx in batch_start..batch_end {
                let mut row_values: Vec<Value> = Vec::new();
                let mut row_placeholders: Vec<String> = Vec::new();

                // 处理每一列
                for col_name in &df_columns {
                    if let Ok(series) = processed_df.column(col_name) {
                        let value = series.get(row_idx).map_err(|e| {
                            errors::Error::new(
                                errors::ErrorKind::DataStore,
                                Some(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!(
                                        "Failed to get value at row {row_idx}, column {col_name}: {e}"
                                    ),
                                )) as errors::BoxError),
                            )
                        })?;
                        // 将Polars AnyValue转换为sea_orm Value
                        let db_value = convert_any_value_to_sea_orm_value(value);
                        row_values.push(db_value);
                        row_placeholders.push(format!("${}", all_values.len() + row_values.len()));
                    }
                }

                if !row_values.is_empty() {
                    all_values.extend(row_values);
                    value_groups.push(format!("({})", row_placeholders.join(", ")));
                }
            }

            if !value_groups.is_empty() {
                // 构建批量 SQL
                let values_str = value_groups.join(", ");

                let sql = if non_pk_col_indices.is_empty() {
                    // 如果没有非主键列需要更新，使用DO NOTHING
                    format!(
                        "INSERT INTO {schema}.{table} ({columns_str}) VALUES {values_str} ON CONFLICT ({conflict_str}) DO NOTHING"
                    )
                } else {
                    // 构建UPDATE SET子句
                    let set_clauses: Vec<String> = non_pk_col_indices
                        .iter()
                        .map(|&idx| format!("{} = EXCLUDED.{}", df_columns[idx], df_columns[idx]))
                        .collect();
                    let set_str = set_clauses.join(", ");

                    format!(
                        "INSERT INTO {schema}.{table} ({columns_str}) VALUES {values_str} ON CONFLICT ({conflict_str}) DO UPDATE SET {set_str}"
                    )
                };
                trace!("Executing SQL: {sql}");
                trace!("Values: {all_values:?}");
                // 执行批量SQL
                txn.execute(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    &sql,
                    all_values,
                ))
                .await
                .map_err(|e| errors::OrmError::QueryExecutionError(e.into()))?;
            }
        }

        // 提交事务
        txn.commit()
            .await
            .map_err(|e| errors::OrmError::TransactionError(e.into()))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use common::model::config::PostgresConfig;
    use common::model::data::Data;
    use common::interface::StoreTrait;
    use polars::prelude::*;
    use std::sync::Arc;
    use utils::connector::postgres_connection;

    #[tokio::test]
    async fn test() {
        let config = PostgresConfig {
            database_host: "localhost".to_string(),
            database_port: 5432,
            database_user: "eason".to_string(),
            database_password: "Qaz.123456".to_string(),
            database_name: "crawler".to_string(),
            database_schema: "base".to_string(),
        };
        let connection = postgres_connection(
            &config.database_host,
            config.database_port,
            &config.database_name,
            &config.database_schema,
            &config.database_user,
            &config.database_password,
        )
        .await
        .unwrap();
        let db = Arc::new(connection);
        let pk_columns = super::StoreDfPgsqlMiddleware::new(db)
            .get_primary_key_columns("base", "account")
            .await
            .unwrap();
        println!("pk_columns: {:?}", pk_columns);
    }

    #[tokio::test]
    async fn test_update_from_df_on_douyin_table() {
        // 1) Connect and prepare schema/table
        let config = PostgresConfig {
            database_host: "localhost".to_string(),
            database_port: 5432,
            database_user: "eason".to_string(),
            database_password: "Qaz.123456".to_string(),
            database_name: "crawler".to_string(),
            database_schema: "douyin".to_string(),
        };

        let connection = postgres_connection(
            &config.database_host,
            config.database_port,
            &config.database_name,
            &config.database_schema,
            &config.database_user,
            &config.database_password,
        )
        .await
        .unwrap();
        let db = Arc::new(connection);

        let df: DataFrame = df!(
            "product_id" => ["p1", "p2"],
            "product_name" => ["new1", "new2"],
            "product_vector" => ["v1", "v2"],
            "shop_id" => [1i32, 1i32],
            "field_name" => ["view", "view"],
            "field_value" => [11.0f64, 22.5f64],
            "source_type" => ["s1", "s1"],
            "date" => [chrono::NaiveDate::from_ymd_opt(2025,9,1), chrono::NaiveDate::from_ymd_opt(2025,9,2)],
        )
        .unwrap();

        let data = Data::default()
            .with_df(df)
            .with_schema("douyin".to_string())
            .with_table("douyin_compass_item_details".to_string())
            .build();

        // 3) Run update_from_df
        let mw = super::StoreDfPgsqlMiddleware::new(db.clone());
        mw.update_from_df(data).await.unwrap();
    }
}
/// 将 Polars AnyValue 转换为 sea_orm Value
fn convert_any_value_to_sea_orm_value(value: AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::String(None),
        AnyValue::Boolean(b) => Value::Bool(Some(b)),
        AnyValue::String(s) => Value::String(Some(Box::new(s.to_string()))),
        AnyValue::StringOwned(s) => Value::String(Some(Box::new(s.to_string()))),
        AnyValue::Int32(i) => Value::Int(Some(i)),
        AnyValue::Int64(i) => Value::BigInt(Some(i)),
        AnyValue::Float32(f) => Value::Float(Some(f)),
        AnyValue::Float64(f) => Value::Double(Some(f)),
        AnyValue::UInt64(u) => Value::BigUnsigned(Some(u)),
        AnyValue::UInt32(u) => Value::Unsigned(Some(u)),
        AnyValue::UInt16(u) => Value::SmallUnsigned(Some(u)),
        AnyValue::UInt8(u) => Value::TinyUnsigned(Some(u)),
        AnyValue::Int16(i) => Value::SmallInt(Some(i)),
        AnyValue::Int8(i) => Value::TinyInt(Some(i)),
        AnyValue::Date(d) => {
            // Polars 的日期是自 1970-01-01 以来的天数
            let days_since_epoch = d;
            let base_date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let target_date = base_date + chrono::Duration::days(days_since_epoch as i64);
            Value::ChronoDate(Some(Box::new(target_date)))
        }
        AnyValue::Datetime(dt, time_unit, _) => {
            // 根据时间单位进行转换
            let timestamp_micros = match time_unit {
                polars::prelude::TimeUnit::Nanoseconds => dt / 1_000, // ns -> μs
                polars::prelude::TimeUnit::Microseconds => dt,        // μs
                polars::prelude::TimeUnit::Milliseconds => dt * 1_000, // ms -> μs
            };
            let naive_dt = chrono::DateTime::from_timestamp(
                timestamp_micros / 1_000_000,
                ((timestamp_micros % 1_000_000) * 1000) as u32,
            )
            .unwrap_or_default()
            .naive_utc();
            Value::ChronoDateTime(Some(Box::new(naive_dt)))
        }
        AnyValue::Time(time) => {
            // 时间类型转换，time 是纳秒数
            let total_seconds = time / 1_000_000_000;
            let hours = (total_seconds / 3600) as u32;
            let minutes = ((total_seconds % 3600) / 60) as u32;
            let seconds = (total_seconds % 60) as u32;
            let nanoseconds = (time % 1_000_000_000) as u32;

            if let Some(naive_time) =
                chrono::NaiveTime::from_hms_nano_opt(hours, minutes, seconds, nanoseconds)
            {
                Value::ChronoTime(Some(Box::new(naive_time)))
            } else {
                Value::String(Some(Box::new(format!(
                    "{hours}:{minutes}:{seconds}.{nanoseconds}"
                ))))
            }
        }
        AnyValue::Duration(duration, time_unit) => {
            // 持续时间类型，转换为字符串表示
            let duration_str = match time_unit {
                polars::prelude::TimeUnit::Nanoseconds => format!("{duration}ns"),
                polars::prelude::TimeUnit::Microseconds => format!("{duration}μs"),
                polars::prelude::TimeUnit::Milliseconds => format!("{duration}ms"),
            };
            Value::String(Some(Box::new(duration_str)))
        }
        AnyValue::Binary(bytes) => {
            // 二进制数据转换为字节数组
            Value::Bytes(Some(Box::new(bytes.to_vec())))
        }
        AnyValue::List(series) => {
            // 列表类型转换为JSON字符串
            let json_str = format!("{series:?}");
            Value::Json(Some(Box::new(serde_json::Value::String(json_str))))
        }
        AnyValue::Struct(_, _, fields) => {
            // 结构体类型转换为JSON
            let mut json_obj = serde_json::Map::new();
            for (i, field_value) in fields.iter().enumerate() {
                json_obj.insert(
                    format!("field_{i}"),
                    serde_json::Value::String(format!("{field_value:?}")),
                );
            }
            Value::Json(Some(Box::new(serde_json::Value::Object(json_obj))))
        }
        // 其他未匹配的类型都转换为字符串
        _ => Value::String(Some(Box::new(value.to_string()))),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::PlSmallStr;
    #[test]
    fn test_convert() {
        let av = AnyValue::StringOwned(PlSmallStr::from("7562127427772189479"));
        let val = convert_any_value_to_sea_orm_value(av);
        println!("{:#?}", val);
    }
}
