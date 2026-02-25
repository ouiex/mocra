use crate::common::interface::StoreTrait;
use crate::common::model::Response;
use crate::common::model::meta::MetaData;
use polars::io::SerWriter;
use polars::io::ipc::IpcWriter;
use polars::prelude::*;
use std::fmt::Debug;
use uuid::Uuid;
/// Storage context carrying task/module identity.
#[derive(Clone, Debug, Default)]
pub struct StoreContext {
    /// Unique request identifier.
    pub request_id: Uuid,
    /// Platform identifier.
    pub platform: String,
    /// Account identifier.
    pub account: String,
    /// Module identifier.
    pub module: String,
    /// Metadata.
    pub meta: MetaData,
    /// Data middleware list.
    pub data_middleware: Vec<String>,
}
impl StoreContext {
    /// Builds task identifier (`account-platform`).
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.account, self.platform)
    }
    /// Builds module identifier (`account-platform-module`).
    pub fn module_id(&self) -> String {
        format!("{}-{}-{}", self.account, self.platform, self.module)
    }
}

/// File storage structure for binary file content.
#[derive(Clone, Debug, Default)]
pub struct FileStore {
    /// Context information.
    pub ctx: StoreContext,
    /// File name.
    pub file_name: String,
    /// File path.
    pub file_path: String,
    /// File content.
    pub content: Vec<u8>,
}

impl StoreTrait for FileStore {
    fn build(&self) -> Data {
        // Preserve all metadata by cloning the current FileStore into the enum variant.
        Data {
            request_id: self.ctx.request_id,
            platform: self.ctx.platform.clone(),
            account: self.ctx.account.clone(),
            module: self.ctx.module.clone(),
            meta: self.ctx.meta.clone(),
            data: DataType::File(self.clone()),
            data_middleware: self.ctx.data_middleware.clone(),
        }
    }
}


impl From<FileStore> for Data {
    fn from(value: FileStore) -> Self {
        let ctx_clone = value.ctx.clone();
        Data {
            request_id: ctx_clone.request_id,
            platform: ctx_clone.platform.clone(),
            account: ctx_clone.account.clone(),
            module: ctx_clone.module.clone(),
            meta: ctx_clone.meta.clone(),
            data: DataType::File(
                FileStore::default()
                    .with_ctx(ctx_clone.clone())
                    .with_content(value.content)
                    .with_name(value.file_name)
                    .with_path(value.file_path),
            ),
            data_middleware: ctx_clone.data_middleware.clone(),
        }
    }
}
impl From<Data> for FileStore {
    fn from(value: Data) -> Self {
        match value.data {
            DataType::File(f) => f,
            _ => FileStore::default(),
        }
    }
}

impl FileStore {
    /// Sets file content.
    pub fn with_content(mut self, content: Vec<u8>) -> Self {
        self.content = content;
        self
    }
    /// Sets context information.
    pub fn with_ctx(mut self, ctx: StoreContext) -> Self {
        self.ctx = ctx;
        self
    }
    /// Sets file name.
    pub fn with_name(mut self, file_name: impl AsRef<str>) -> Self {
        self.file_name = file_name.as_ref().to_string();
        self
    }
    /// Sets file path.
    pub fn with_path(mut self, file_path: impl AsRef<str>) -> Self {
        self.file_path = file_path.as_ref().to_string();
        self
    }
    /// Sets file name (alias).
    pub fn with_file_name(self, file_name: impl AsRef<str>) -> Self {
        self.with_name(file_name)
    }
    /// Sets file path (alias).
    pub fn with_file_path(self, file_path: impl AsRef<str>) -> Self {
        self.with_path(file_path)
    }
}

/// DataFrame storage structure for tabular data.
#[derive(Clone, Debug, Default)]
pub struct DataFrameStore {
    /// Context information.
    ctx: StoreContext,
    /// Serialized DataFrame payload (IPC format).
    pub data: Vec<u8>,
    /// Database schema.
    pub schema: String,
    /// Database table name.
    pub table: String,
}

impl StoreTrait for DataFrameStore {
    fn build(&self) -> Data {
        // Clone to preserve full metadata.
        Data {
            request_id: self.ctx.request_id,
            platform: self.ctx.platform.clone(),
            account: self.ctx.account.clone(),
            module: self.ctx.module.clone(),
            meta: self.ctx.meta.clone(),
            data: DataType::DataFrame(self.clone()),
            data_middleware: self.ctx.data_middleware.clone(),
        }
    }
}
impl From<DataFrameStore> for Data {
    fn from(value: DataFrameStore) -> Self {
        Data {
            request_id: value.ctx.request_id,
            platform: value.ctx.platform.clone(),
            account: value.ctx.account.clone(),
            module: value.ctx.module.clone(),
            meta: value.ctx.meta.clone(),
            data_middleware: value.ctx.data_middleware.clone(),
            data: DataType::DataFrame(value),
        }
    }
}


impl DataFrameStore {
    /// Sets DataFrame data and serializes to IPC format.
    pub fn with_data(mut self, data: DataFrame) -> Self {
        let mut buffer = Vec::new();
        let mut df = data;
        let mut writer = IpcWriter::new(&mut buffer); // default options
        writer.finish(&mut df).expect("serialize DataFrame to IPC");
        self.data = buffer;
        self
    }
    /// Sets schema.
    pub fn with_schema(mut self, schema: impl AsRef<str>) -> Self {
        self.schema = schema.as_ref().to_string();
        self
    }
    /// Sets table name.
    pub fn with_table(mut self, table: impl AsRef<str>) -> Self {
        self.table = table.as_ref().to_string();
        self
    }
}

/// Data type enum.
#[derive(Debug, Clone)]
pub enum DataType {
    /// Structured tabular data.
    DataFrame(DataFrameStore),
    /// File data.
    File(FileStore),
}

/// Generic data transfer object wrapping metadata and concrete payload.
#[derive(Debug, Clone)]
pub struct Data {
    /// Unique request identifier.
    pub request_id: Uuid,
    /// Platform identifier.
    pub platform: String,
    /// Account identifier.
    pub account: String,
    /// Module identifier.
    pub module: String,
    /// Metadata.
    pub meta: MetaData,
    /// Payload.
    pub data: DataType,
    /// Data middleware to execute.
    pub data_middleware: Vec<String>,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            request_id: Default::default(),
            platform: "".to_string(),
            account: "".to_string(),
            module: "".to_string(),
            meta: Default::default(),
            data: DataType::DataFrame(DataFrameStore::default()),
            data_middleware: vec![],
        }
    }
}
impl Data {
    /// Builds `Data` from `Response`.
    pub fn from(response: &Response) -> Self {
        Data {
            request_id: response.id,
            platform: response.platform.clone(),
            account: response.account.clone(),
            module: response.module.clone(),
            meta: response.metadata.clone(),
            data: DataType::DataFrame(DataFrameStore::default()),
            data_middleware: response.data_middleware.clone(),
        }
    }
    /// Sets middleware list.
    pub fn with_middlewares(mut self, middleware: Vec<String>) -> Self {
        self.data_middleware = middleware;
        self
    }
    /// Adds one middleware item.
    pub fn with_middleware(mut self, middleware: impl AsRef<str>) -> Self {
        self.data_middleware.push(middleware.as_ref().into());
        self
    }
    /// Returns task ID (`account-platform`).
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.account, self.platform)
    }
    /// Returns module ID (`account-platform-module`).
    pub fn module_id(&self) -> String {
        format!("{}-{}-{}", self.account, self.platform, self.module)
    }
    /// Converts into `DataFrameStore`.
    pub fn with_df(self, data: DataFrame) -> DataFrameStore {
        DataFrameStore::default().with_data(data)
    }
    /// Converts into `FileStore`.
    pub fn with_file(self, data: Vec<u8>) -> FileStore {
        // Transfer CrawlData context into a FileStore builder preserving metadata.
        FileStore {
            ctx: StoreContext {
                request_id: self.request_id,
                platform: self.platform,
                account: self.account,
                module: self.module,
                meta: self.meta,
                data_middleware: self.data_middleware,
            },
            file_name: String::new(),
            file_path: String::new(),
            content: data,
        }
    }
    /// Returns payload size (bytes or rows).
    pub fn size(&self) -> usize {
        match &self.data {
            DataType::DataFrame(df_store) => df_store.data.len(),
            DataType::File(file_store) => file_store.content.len(),
        }
    }
}
impl From<(DataFrame, &Response)> for Data {
    fn from(value: (DataFrame, &Response)) -> Self {
        let (data, response) = value;
        Data {
            request_id: response.id,
            platform: response.platform.clone(),
            account: response.account.clone(),
            module: response.module.clone(),
            meta: response.metadata.clone(),
            data: DataType::DataFrame(DataFrameStore::default().with_data(data)),
            data_middleware: response.data_middleware.clone(),
        }
    }
}
impl StoreTrait for Data {
    fn build(&self) -> Data {
        self.clone()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_file_store_builder() {
        let store = FileStore::default()
            .with_name("test.txt")
            .with_path("/tmp/test.txt")
            .with_content(vec![1, 2, 3]);

        assert_eq!(store.file_name, "test.txt");
        assert_eq!(store.file_path, "/tmp/test.txt");
        assert_eq!(store.content, vec![1, 2, 3]);
    }

    #[test]
    fn test_serde_examples() {
        #[derive(Serialize, Deserialize, Debug)]
        struct Item {
            key: String,
            value: serde_json::Value,
        }

        // Method 1: define only required fields; serde ignores unknown fields.
        #[derive(Serialize, Deserialize, Debug)]
        struct RespPartial {
            data: Vec<Item>,
            // `is_success` is not defined and will be ignored.
        }

        // Method 2: use `Option` for optional fields.
        #[derive(Serialize, Deserialize, Debug)]
        struct RespWithOptional {
            data: Vec<Item>,
            is_success: Option<bool>, // Optional field.
            #[serde(default)] // Use default when field is missing.
            extra_field: String,
        }

        // Test JSON with additional fields.
        let complex_json = r#"
        {
            "data": [
                {"key": "name", "value": "Alice"},
                {"key": "age", "value": "30"},
                {"key": "city", "value": "New York"}
            ],
            "is_success": true,
            "timestamp": "2023-01-01T00:00:00Z",
            "metadata": {
                "total_count": 100,
                "page": 1,
                "limit": 10
            },
            "debug_info": "some debug data",
            "version": "1.0.0"
        }"#;

        // Method 1: parse only required fields.
        let resp_partial: RespPartial = serde_json::from_str(complex_json).unwrap();
        assert_eq!(resp_partial.data.len(), 3);

        // Method 2: handle optional fields via `Option`.
        let resp_optional: RespWithOptional = serde_json::from_str(complex_json).unwrap();
        assert_eq!(resp_optional.is_success, Some(true));
        assert_eq!(resp_optional.extra_field, "");

        // Method 4: dynamic parsing with `serde_json::Value`.
        let value: serde_json::Value = serde_json::from_str(complex_json).unwrap();
        if let Some(data) = value.get("data") {
            let items: Vec<Item> = serde_json::from_value(data.clone()).unwrap();
            assert_eq!(items.len(), 3);
        }

        // Extract specific fields.
        let is_success = value
            .get("is_success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let version = value
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        
        assert!(is_success);
        assert_eq!(version, "1.0.0");

        // Polars DataFrame test
        let df: DataFrame = df!(
            "name" => ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
            "birthdate" => [
                NaiveDate::from_ymd_opt(1997, 1, 10).unwrap(),
                NaiveDate::from_ymd_opt(1985, 2, 15).unwrap(),
                NaiveDate::from_ymd_opt(1983, 3, 22).unwrap(),
                NaiveDate::from_ymd_opt(1981, 4, 30).unwrap(),
            ],
            "weight" => [57.9, 72.5, 53.6, 83.1],  // (kg)
            "height" => [1.56, 1.77, 1.65, 1.75],  // (m)
        )
        .unwrap();
        
        assert_eq!(df.height(), 4);
        assert_eq!(df.width(), 4);
    }
}
