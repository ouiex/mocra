use async_trait::async_trait;

use common::interface::{ModuleTrait, ModuleNodeTrait, SyncBoxStream, StoreTrait, ToSyncBoxStream};
use common::model::{CronConfig, CronInterval, Headers, ModuleConfig, Request, Response};
use common::model::login_info::LoginInfo;
use common::model::request::RequestMethod;
use common::model::message::ParserData;
use common::model::data::Data;
use errors::Result;

use serde_json::{Map, Value};
use std::sync::Arc;

pub struct MocDevModule {}

#[async_trait]
impl ModuleTrait for MocDevModule {
    fn should_login(&self) -> bool {
        false
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
            url: "http://127.0.0.1:8888".to_string(),
        })]
    }
    fn cron(&self) -> Option<CronConfig> {
        Some(CronConfig::every(CronInterval::Custom("0 3/2 * * * ?".to_string())).build())
    }
}

struct MocDevNode {
    url: String,
}
#[async_trait]
impl ModuleNodeTrait for MocDevNode {
    async fn generate(
        &self,
        _config: ModuleConfig,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) ->  Result<SyncBoxStream<'static, Request>> {
        let mut requests = vec![];
        for _i in 0..1 {
            let request = Request::new(self.url.clone(), RequestMethod::Get);
            requests.push(request);
        }

        requests.into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<ModuleConfig>,
    ) ->  Result<ParserData> {
        let data = Data::default()
            .with_file(response.content.clone())
            .with_name("moc.dat".to_string())
            .with_path("./data")
            .build();

        Ok( ParserData::default().with_data(vec![data]))
    }
}
