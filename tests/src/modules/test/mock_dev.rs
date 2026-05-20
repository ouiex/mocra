use async_trait::async_trait;

use mocra::common::interface::{
    ModuleNodeTrait, ModuleTrait, NodeGenerateContext, NodeParseContext, SyncBoxStream,
    ToSyncBoxStream,
};
use mocra::common::model::{NodeParseOutput, Request, Response};
use mocra::common::model::request::RequestMethod;
use mocra::errors::Result;

use std::sync::Arc;

pub struct MockDevModule {}

#[async_trait]
impl ModuleTrait for MockDevModule {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "mock.dev"
    }

    fn version(&self) -> i32 {
        1
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(MockDevModule {})
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        let url = std::env::var("MOCK_DEV_URL").unwrap_or_else(|_| "http://127.0.0.1:9009/test".to_string());
        let request_count = std::env::var("MOCK_DEV_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2000);

        vec![Arc::new(MockDevNode { url, request_count })]
    }
}

struct MockDevNode {
    url: String,
    request_count: usize,
}

#[async_trait]
impl ModuleNodeTrait for MockDevNode {
    async fn generate(
        &self,
        _ctx: NodeGenerateContext<'_>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut requests = Vec::with_capacity(self.request_count);
        for i in 0..self.request_count {
            let url = format!("{}?_t={}&_i={}", self.url, std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(), i);
            let request = Request::new(url, RequestMethod::Get);
            requests.push(request);
        }
        requests.into_stream_ok()
    }

    async fn parser(
        &self,
        _response: Response,
        _ctx: NodeParseContext<'_>,
    ) -> Result<NodeParseOutput> {
        Ok(NodeParseOutput::default())
    }
}
