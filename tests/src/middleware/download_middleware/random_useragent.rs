use async_trait::async_trait;
use common::interface::DownloadMiddleware;
use common::model::{ModuleConfig, Request, Response};
use rand::prelude::IndexedRandom;
use rand::Rng;
use std::sync::Arc;

pub struct RandomUserAgentMiddleware {
    user_agent: Vec<String>,
}

impl RandomUserAgentMiddleware {
    fn new(user_agent: Vec<String>) -> Self {
        RandomUserAgentMiddleware { user_agent }
    }
    fn generate_user_agents() -> Vec<String> {
        let mut agents = Vec::new();

        let os_list = ["Windows NT 10.0; Win64; x64",
            "Windows NT 6.1; Win64; x64",
            "Macintosh; Intel Mac OS X 10_15_7",
            "Macintosh; Intel Mac OS X 11_2_3",
            "Macintosh; Intel Mac OS X 12_6_1",
            "Macintosh; Intel Mac OS X 13_4_1",
            "Macintosh; Intel Mac OS X 14_0_1"];

        let mut rng = rand::rng();

        // For each OS token, generate several UA variants across browsers
        for os_tok in os_list.iter() {
            for _ in 0..20 {
                let choice = rng.random_range(0..3);
                let ua = match choice {
                    0 => {
                        // Chrome-like UA
                        let major = rng.random_range(90..=120);
                        let build = rng.random_range(0..=5999);
                        format!(
                            "Mozilla/5.0 ({}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{}.0.{}.0 Safari/537.36",
                            os_tok, major, build
                        )
                    }
                    1 => {
                        // Edge UA includes Chrome token + Edg token
                        let chrome_major = rng.random_range(90..=140);
                        let chrome_build = rng.random_range(0..=5999);
                        let edg_major = rng.random_range(90..=140);
                        let edg_build = rng.random_range(0..=5999);
                        format!(
                            "Mozilla/5.0 ({}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{}.0.{}.0 Safari/537.36 Edg/{}.0.{}.0",
                            os_tok, chrome_major, chrome_build, edg_major, edg_build
                        )
                    }
                    _ => {
                        // Firefox UA
                        let major = rng.random_range(80..=140);
                        let minor = rng.random_range(0..=9);
                        format!(
                            "Mozilla/5.0 ({}) Gecko/20100101 Firefox/{}.{}",
                            os_tok, major, minor
                        )
                    }
                };
                agents.push(ua);
            }
        }

        agents
    }

    fn random_user_agent(&self) -> String {
        let mut rng = rand::rng();
        // Safe fallback in case list is empty
        self.user_agent.choose(&mut rng)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Mozilla/5.0".to_string())
    }
}

#[async_trait]
impl DownloadMiddleware for RandomUserAgentMiddleware {
    fn name(&self) -> String {
        "random_useragent".to_string()
    }

    async fn handle_request(&self, request: Request, _config: &Option<ModuleConfig>) -> Request {
        let mut req = request;
        let ua = self.random_user_agent();
        if req.headers.contains("user-agent") {
            req.headers
                .headers
                .iter_mut()
                .for_each(|h| {
                    if h.key.eq_ignore_ascii_case("user-agent") {
                        h.value = ua.clone();
                    }
                });
        } else {
            req.headers = req.headers.add("user-agent", &ua);
        }
        req
    }

    async fn handle_response(&self, response: Response, _config: &Option<ModuleConfig>) -> Response {
        response
    }

    fn default_arc() -> Arc<dyn DownloadMiddleware>
    where
        Self: Sized,
    {
        Arc::new(RandomUserAgentMiddleware::new(Self::generate_user_agents()))
    }
}
