use crate::model::meta::MetaData;
use crate::model::Response;
use polars::io::ipc::IpcWriter;
use polars::io::SerWriter;
use polars::prelude::*;
use std::fmt::Debug;
use uuid::Uuid;
use crate::interface::StoreTrait;
#[derive(Clone, Debug, Default)]
pub struct StoreContext {
    pub request_id: Uuid,
    pub platform: String,
    pub account: String,
    pub module: String,
    pub meta: MetaData,
    pub data_middleware: Vec<String>,
}
impl StoreContext {
    pub fn task_id(&self) -> String { format!("{}-{}", self.account, self.platform) }
    pub fn module_id(&self) -> String { format!("{}-{}-{}", self.account, self.platform, self.module) }
}
#[derive(Clone, Debug)]
pub struct FileStore {
    pub ctx: StoreContext,
    pub file_name: String,
    pub file_path: String,
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
impl Default for FileStore {
    fn default() -> Self {
        Self { ctx: StoreContext::default(), file_name: String::new(), file_path: String::new(), content: vec![] }
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
    pub fn with_content(mut self, content: Vec<u8>) -> Self {
        self.content = content;
        self
    }
    pub fn with_ctx(mut self, ctx: StoreContext) -> Self { self.ctx = ctx; self }
    pub fn with_name(mut self, file_name: impl AsRef<str>) -> Self {
        self.file_name = file_name.as_ref().to_string();
        self
    }
    pub fn with_path(mut self, file_path: impl AsRef<str>) -> Self {
        self.file_path = file_path.as_ref().to_string();
        self
    }
    // Convenience alias for clarity in chaining.
    pub fn with_file_name(self, file_name: impl AsRef<str>) -> Self { self.with_name(file_name) }
    pub fn with_file_path(self, file_path: impl AsRef<str>) -> Self { self.with_path(file_path) }
}

#[derive(Clone, Debug)]
pub struct DataFrameStore {
    ctx: StoreContext,
    pub data: Vec<u8>,
    pub schema: String,
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
            request_id: value.ctx.request_id.clone(),
            platform: value.ctx.platform.clone(),
            account: value.ctx.account.clone(),
            module: value.ctx.module.clone(),
            meta: value.ctx.meta.clone(),
            data_middleware: value.ctx.data_middleware.clone(),
            data: DataType::DataFrame(value),
        }
    }
}

impl Default for DataFrameStore {
    fn default() -> Self {
        Self { ctx: StoreContext::default(), data: vec![], schema: String::new(), table: String::new() }
    }
}
impl DataFrameStore {
    pub fn with_data(mut self, data: DataFrame) -> Self {
        let mut buffer = Vec::new();
        let mut df = data;
        let mut writer = IpcWriter::new(&mut buffer); // default options
        writer.finish(&mut df).expect("serialize DataFrame to IPC");
        self.data = buffer;
        self
    }
    pub fn with_schema(mut self, schema: impl AsRef<str>) -> Self {
        self.schema = schema.as_ref().to_string();
        self
    }
    pub fn with_table(mut self, table: impl AsRef<str>) -> Self {
        self.table = table.as_ref().to_string();
        self
    }
}

#[derive(Debug,Clone)]
pub enum DataType {
    DataFrame(DataFrameStore),
    File(FileStore),
}
#[derive(Debug,Clone)]
pub struct Data {
    pub request_id: Uuid,
    pub platform: String,
    pub account: String,
    pub module: String,
    pub meta: MetaData,
    pub data: DataType,
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
    pub fn from(response:&Response) -> Self {
        Data{
            request_id: response.id,
            platform: response.platform.clone(),
            account: response.account.clone(),
            module: response.module.clone(),
            meta: response.metadata.clone(),
            data: DataType::DataFrame(DataFrameStore::default()),
            data_middleware: response.data_middleware.clone(),
        }
    }
    pub fn with_middlewares(mut self, middleware: Vec<String>) -> Self {
        self.data_middleware = middleware;
        self
    }
    pub fn with_middleware(mut self, middleware: impl AsRef<str>) -> Self {
        self.data_middleware.push(middleware.as_ref().into());
        self
    }
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.account, self.platform)
    }
    pub fn module_id(&self) -> String {
        format!("{}-{}-{}", self.account, self.platform, self.module)
    }
    pub fn with_df(self, data: DataFrame) -> DataFrameStore {
        DataFrameStore::default().with_data(data)
    }
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
#[test]
fn test() {
    use chrono::NaiveDate;
    use polars::prelude::*;
    use serde::{Deserialize, Serialize};
    #[derive(Serialize, Deserialize, Debug)]
    struct Item {
        key: String,
        value: serde_json::Value,
    }

    // 方法1: 只定义需要的字段，serde会自动忽略其他字段
    #[derive(Serialize, Deserialize, Debug)]
    struct RespPartial {
        data: Vec<Item>,
        // 不定义is_success字段，它会被忽略
    }

    // 方法2: 使用Option来处理可选字段
    #[derive(Serialize, Deserialize, Debug)]
    struct RespWithOptional {
        data: Vec<Item>,
        is_success: Option<bool>, // 可选字段
        #[serde(default)] // 如果字段不存在，使用默认值
        extra_field: String,
    }

    // 方法3: 使用serde的rename和skip属性
    #[derive(Serialize, Deserialize, Debug)]
    struct RespCustom {
        #[serde(rename = "data")]
        items: Vec<Item>,

        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<bool>,

        #[serde(skip)] // 完全跳过这个字段
        _ignored: String,
    }

    // 测试包含更多字段的复杂JSON
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

    // 方法1: 只解析需要的字段
    println!("=== 方法1: 只定义需要的字段 ===");
    let resp_partial: RespPartial = serde_json::from_str(complex_json).unwrap();
    println!("部分解析结果: {:?}", resp_partial);

    // 方法2: 使用Option处理可选字段
    println!("\n=== 方法2: 使用Option处理可选字段 ===");
    let resp_optional: RespWithOptional = serde_json::from_str(complex_json).unwrap();
    println!("可选字段解析: {:?}", resp_optional);

    // 方法4: 使用serde_json::Value进行动态解析
    println!("\n=== 方法4: 动态解析特定字段 ===");
    let value: serde_json::Value = serde_json::from_str(complex_json).unwrap();
    if let Some(data) = value.get("data") {
        let items: Vec<Item> = serde_json::from_value(data.clone()).unwrap();
        println!("动态解析的data字段: {:?}", items);
    }

    // 提取特定字段
    let is_success = value
        .get("is_success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let version = value
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    println!(
        "提取的字段 - is_success: {}, version: {}",
        is_success, version
    );

    // 方法5: 使用serde的flatten和untagged来处理复杂嵌套
    println!("\n=== 方法5: 处理嵌套结构的部分解析 ===");
    #[derive(Serialize, Deserialize, Debug)]
    struct MetadataPartial {
        total_count: Option<i32>,
        page: Option<i32>,
        // 忽略limit字段
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ComplexRespPartial {
        data: Vec<Item>,

        #[serde(default)]
        metadata: Option<MetadataPartial>,

        // 使用flatten将嵌套的字段提升到顶层（如果需要的话）
        #[serde(flatten)]
        extra: std::collections::HashMap<String, serde_json::Value>,
    }

    let complex_resp: ComplexRespPartial = serde_json::from_str(complex_json).unwrap();
    println!("复杂嵌套解析: {:?}", complex_resp);

    let mut df: DataFrame = df!(
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
    println!("{df}");
}
