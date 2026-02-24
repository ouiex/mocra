use async_trait::async_trait;
use common::model::cron_config::{CronConfig, CronInterval};
use common::interface::{ModuleTrait, ModuleNodeTrait, SyncBoxStream, StoreTrait, ToSyncBoxStream};
use common::model::{Headers, ModuleConfig, Request, Response};
use common::model::login_info::LoginInfo;
use common::model::request::RequestMethod;
use common::model::message::ParserData;
use common::model::data::Data;
use errors::Result;
use log::{info, warn};

use serde_json::{Map, Value};
use std::sync::Arc;

pub struct MocDevModule {}

#[async_trait]
impl ModuleTrait for MocDevModule {
    fn should_login(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        "moc.dev".to_string()
    }
    fn version(&self) -> i32 {
        1
    }
    async fn headers(&self) -> Headers {
        Headers::new()
            .add("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
            .add("accept-language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7")
            .add("cache-control", "no-cache")
            .add("pragma", "no-cache")
            .add("priority", "u=0, i")
            .add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
    }
    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(MocDevModule {})
    }
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(MocDevNode {
            url: "http://127.0.0.1:3000".to_string(),
            request_count: 1000,
        })]
    }
    fn cron(&self) -> Option<CronConfig> {
        Some(CronConfig::right_now())
    }
}

struct MocDevNode {
    url: String,
    request_count: usize,
}
#[async_trait]
impl ModuleNodeTrait for MocDevNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        login_info: Option<LoginInfo>,
    ) ->  Result<SyncBoxStream<'static, Request>> {
        if let Some(info) = login_info.as_ref() {
            info!("moc_dev login_info loaded: cookies={}, ua={}", info.cookies.len(), info.useragent);
        } else {
            warn!("moc_dev login_info missing");
        }
        let mut requests = Vec::with_capacity(self.request_count);
        for i in 0..self.request_count {
           let request = Request::new(format!("{}?/msg={i}",self.url), RequestMethod::Get);
            requests.push(request);
        }
        requests.into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) ->  Result<ParserData> {
        // Just extract basic info, don't store files
        let status = response.status_code;
        
        // Build file store with response content
        let data = Data::from(&response)
            .with_file(response.content.clone())
            .with_name(format!("moc_dev_response_{}.html", status))
            .with_path("./data/moc_dev")
            .build();

        Ok(ParserData::default().with_data(vec![data]))
    }

}
