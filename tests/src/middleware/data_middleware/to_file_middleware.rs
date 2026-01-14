use async_trait::async_trait;
use common::interface::{DataMiddleware, DataStoreMiddleware};
use common::model::data::{Data, DataType};
use common::model::ModuleConfig;
use errors::{DataStoreError, Result};
use log::info;
use polars::io::SerReader;
use polars::io::ipc::IpcReader;
use polars::prelude::SerWriter;
use std::sync::Arc;
use chrono::prelude::*;

pub struct ToFileMiddleware;
#[async_trait]
impl DataMiddleware for ToFileMiddleware {
    fn name(&self) -> String {
        "to_file".to_string()
    }

    async fn handle_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Data {
        data
    }

    fn default_arc() -> Arc<dyn DataMiddleware>
    where
        Self: Sized,
    {
        Arc::new(ToFileMiddleware)
    }
}
#[async_trait]
/// fixed: if ParserData has more than one Data, file name need to be changed, file name is {schema}_{table}_{request_id}.csv
/// but request_id is same for all Data in one ParserData, consider add a suffix with timestamp
impl DataStoreMiddleware for ToFileMiddleware {
    async fn store_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Result<()> {

        let table;
        // ensure output directory exists
        let out_dir = std::path::Path::new("data");
        if !out_dir.exists() {
            std::fs::create_dir_all(out_dir)?;
        }

        // write to CSV file
       
        // write UTF-8 BOM to make encoding utf-8-sig
        let mut df = match data.data {
            DataType::DataFrame(v) => {
                let cursor = std::io::Cursor::new(v.data);
                table =  format!("{}_{}", v.schema, v.table);
                match IpcReader::new(cursor).finish() {
                    Ok(df) => df,
                    Err(e) => return Err(DataStoreError::InvalidData(e.to_string().into()).into()),
                }
            }
            _ => {
                return Err(DataStoreError::InvalidData("Not DataFrame".to_string().into()).into());
            }
        };
        let file_name = format!("{}_{}_{}.csv", table, data.request_id, Local::now().timestamp_millis());
        let file_path = out_dir.join(&file_name);
        let mut file = std::fs::File::create(&file_path)?;
        let writer = polars::prelude::CsvWriter::new(&mut file);
        writer
            .include_bom(true)
            .finish(&mut df)
            .map_err(|e| DataStoreError::SaveFailed(e.into()))?;
        info!("Data saved to CSV: {file_path:?}");
        Ok(())
    }

    fn default_arc() -> Arc<dyn DataStoreMiddleware>
    where
        Self: Sized,
    {
        Arc::new(ToFileMiddleware)
    }
}
