use async_trait::async_trait;
use common::interface::{ModuleNodeTrait, ModuleTrait};
use common::interface::SyncBoxStream;
use common::model::login_info::LoginInfo;
use common::model::message::ParserData;
use common::model::request::RequestMethod;
use common::model::{ModuleConfig, Request, Response};
use errors::Result;
use log::info;
use serde_json::{Map, Value};
use std::sync::Arc;

pub struct WssTest {}
#[async_trait]
impl ModuleTrait for WssTest {
    fn should_login(&self) -> bool {
        false
    }
    fn name(&self) -> String {
        "wss_test".to_string()
    }

    fn version(&self) -> i32 {
        1
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(WssTest {})
    }
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(WssTestNode {})]
    }
}
struct WssTestNode;
#[async_trait]
impl ModuleNodeTrait for WssTestNode {
    async fn generate(
        &self,
        _config: ModuleConfig,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) ->  Result<SyncBoxStream<'static, Request>> {
        let url = "ws://localhost:8765".to_string();
        let start_ts = chrono::Utc::now().timestamp();
        let request =
            Request::new(url, RequestMethod::Wss).with_trait_config("wss_start_ts", start_ts);

        Ok(Box::pin(futures::stream::iter(vec![request])))
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<ModuleConfig>,
    ) ->  Result<ParserData> {
        let start_ts = response.get_trait_config::<i64>("wss_start_ts").unwrap();
        let data = String::from_utf8_lossy(&response.content);
        info!("{}", data);
        info!("{}",start_ts);
        info!("{}",chrono::Utc::now().timestamp() - start_ts);
        if chrono::Utc::now().timestamp() - start_ts > 10 {
            return Ok(ParserData::default().with_stop(true));
        }
        Ok(ParserData::default())
    }
}
