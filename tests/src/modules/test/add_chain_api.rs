use async_trait::async_trait;
use mocra::common::interface::{ModuleNodeTrait, ModuleTrait, StoreTrait, SyncBoxStream, ToSyncBoxStream};
use mocra::common::model::cron_config::CronConfig;
use mocra::common::model::data::Data;
use mocra::common::model::login_info::LoginInfo;
use mocra::common::model::message::{ParserData, ParserTaskModel};
use mocra::common::model::request::RequestMethod;
use mocra::common::model::{Headers, ModuleConfig, Request, Response};
use mocra::errors::{Error, ErrorKind, Result};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const NODE_COUNT: usize = 100;
const META_SUM_KEY: &str = "current_sum";
const META_STEP_KEY: &str = "current_step";
const RANDOM_FAIL_PERCENT: u32 = 20;

pub struct AddChainApiModule;

#[async_trait]
impl ModuleTrait for AddChainApiModule {
    fn should_login(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        "test.add.chain.api".to_string()
    }

    fn version(&self) -> i32 {
        1
    }

    async fn headers(&self) -> Headers {
        Headers::new().add("accept", "application/json,text/plain,*/*")
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(AddChainApiModule)
    }

    fn cron(&self) -> Option<CronConfig> {
        Some(CronConfig::right_now())
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        let base_url = std::env::var("ADD_API_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3888/add".to_string());

        (0..NODE_COUNT)
            .map(|idx| {
                let value = std::env::var(format!("ADD_NODE_{}_VALUE", idx + 1))
                    .ok()
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or((idx + 1) as i64);

                Arc::new(AddChainNode {
                    step_idx: idx,
                    value,
                    url: base_url.clone(),
                }) as Arc<dyn ModuleNodeTrait>
            })
            .collect()
    }

}

struct AddChainNode {
    step_idx: usize,
    value: i64,
    url: String,
}

#[async_trait]
impl ModuleNodeTrait for AddChainNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        if should_random_fail("generate", self.step_idx) {
            return Err(new_stage_error(
                "generate",
                self.step_idx,
                "randomly injected generate error".to_string(),
            ));
        }

        let previous = parse_meta_i64(&params, META_SUM_KEY)
            .or_else(|| {
                std::env::var("ADD_CHAIN_INITIAL")
                    .ok()
                    .and_then(|v| v.parse::<i64>().ok())
            })
            .unwrap_or(0);

        let request_url = format!("{}?a={}&b={}", self.url, previous, self.value);
        let request = Request::new(request_url, RequestMethod::Get);
        vec![request].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<ParserData> {
        if should_random_fail("parser", self.step_idx) {
            return Err(new_stage_error(
                "parser",
                self.step_idx,
                "randomly injected parser error".to_string(),
            ));
        }

        let sum = parse_add_result(&response.content)?;

        log::info!(
            "[add-chain] step={} value={} api_sum={}",
            self.step_idx + 1,
            self.value,
            sum
        );

        if self.step_idx + 1 >= NODE_COUNT {
            let payload = serde_json::to_vec(&serde_json::json!({
                "module": "test.add.chain.api",
                "steps": NODE_COUNT,
                "final_sum": sum,
                "last_node_value": self.value
            }))
            .map_err(|e| Error::new(ErrorKind::Parser, Some(e)))?;
            let data = Data::from(&response)
                .with_middleware("object_store")
                .with_file(payload)
                .with_name("add_chain_result.json")
                .with_path("./data/add_chain")
                .build();
            return Ok(ParserData::default().with_data(vec![data]));
        }

        let task = ParserTaskModel::from(&response)
            .add_meta(META_SUM_KEY, sum)
            .add_meta(META_STEP_KEY, self.step_idx + 1);

        Ok(ParserData::default().with_task(task))
    }
}

fn parse_meta_i64(params: &Map<String, Value>, key: &str) -> Option<i64> {
    let value = params.get(key)?;
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(s) = value.as_str() {
        return s.parse::<i64>().ok();
    }
    None
}

fn should_random_fail(stage: &str, step_idx: usize) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let stage_seed = if stage == "generate" { 97u128 } else { 193u128 };
    let mixed = now
        ^ ((step_idx as u128 + 1) * 1103515245u128)
        ^ stage_seed
        ^ ((now >> 13) & 0xffffu128);
    let roll = (mixed % 100) as u32;
    roll < RANDOM_FAIL_PERCENT
}

fn new_stage_error(stage: &str, step_idx: usize, message: String) -> Error {
    log::warn!(
        "[add-chain] {} stage random failure injected at step={} detail={}",
        stage,
        step_idx + 1,
        message
    );
    new_parser_error(message)
}

fn parse_add_result(content: &[u8]) -> Result<i64> {
    let text = String::from_utf8_lossy(content).trim().to_string();
    if text.is_empty() {
        return Err(new_parser_error("add api response is empty"));
    }

    let json = serde_json::from_str::<Value>(&text)
        .map_err(|e| new_parser_error(format!("invalid add api json response: {e}")))?;

    extract_result_from_json(&json)
        .ok_or_else(|| new_parser_error(format!("add api json missing numeric `result`: {}", text)))
}

fn new_parser_error(message: impl AsRef<str>) -> Error {
    Error::new(
        ErrorKind::Parser,
        Some(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message.as_ref().to_string(),
        )),
    )
}

fn extract_result_from_json(v: &Value) -> Option<i64> {
    let result = v.get("result")?;
    if let Some(n) = result.as_i64() {
        return Some(n);
    }
    if let Some(s) = result.as_str() {
        return s.parse::<i64>().ok();
    }
    None
}
