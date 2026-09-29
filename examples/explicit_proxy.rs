//! Use one proxy for a Spider seed request. This example needs a real, reachable proxy.
//!
//! Run:
//! `MOCRA_PROXY_URL=http://127.0.0.1:8080 MOCRA_TARGET_URL=https://example.org/ cargo run --example explicit_proxy`
//!
//! `Request::use_proxy` is an explicit, fixed choice. It does not enable managed pool feedback or
//! automatic rotation; those require an engine proxy pool and module config `enable_proxy=true`.

use async_trait::async_trait;
use mocra::prelude::proxy::{DirectProxy, PoolConfig, ProxyConfig, ProxyEnum, ProxyManager};
use mocra::prelude::*;

struct ProxySpider {
    target: String,
    proxy: ProxyEnum,
}

#[async_trait]
impl Spider for ProxySpider {
    type Item = (u16, usize);

    fn name(&self) -> &str {
        "explicit-proxy-example"
    }

    async fn start(&self, seeds: &mut Seeds) {
        seeds.get(&self.target).use_proxy(self.proxy.clone());
    }

    async fn parse(&self, response: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit((response.status_code, response.content.len()));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let proxy_url = std::env::var("MOCRA_PROXY_URL")?;
    let target =
        std::env::var("MOCRA_TARGET_URL").unwrap_or_else(|_| "https://example.org/".to_string());

    let mut pool_config = PoolConfig::default();
    pool_config.health_check_interval_secs = 0;
    let config = ProxyConfig {
        direct: Some(vec![DirectProxy {
            name: Some("chosen-proxy".into()),
            url: proxy_url,
            rate_limit: Some(0.0),
            expire_time: None,
        }]),
        tunnel: None,
        ip_provider: None,
        pool_config: Some(pool_config),
    };
    let manager = ProxyManager::from_proxy_config(&config).await?;
    let proxy = manager.get_proxy_for_attempt(None).await?;

    Mocra::builder()
        .spider(
            ProxySpider { target, proxy },
            on_item(|(status, bytes)| async move {
                println!("response through explicit proxy: status={status}, bytes={bytes}");
            }),
        )
        .run()
        .await?;
    Ok(())
}
