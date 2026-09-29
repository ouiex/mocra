//! Offline proxy-pool example: configure two direct proxies, select one, report a simulated
//! failed attempt, then select a different proxy for the retry. No network requests are made.
//!
//! Run: `cargo run --example proxy_pool`
//!
//! This uses `ProxyManager` as a standalone library. In a standalone TOML string passed to
//! `ProxyManager::from_config`, direct proxies use `[[direct]]`. The crawler's `config.toml`
//! uses `[[proxy.direct]]`; its module JSON config also needs `{"enable_proxy": true}`.

use mocra::prelude::proxy::{DirectProxy, PoolConfig, ProxyConfig, ProxyManager};

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut pool_config = PoolConfig::default();
    pool_config.health_check_interval_secs = 0; // No background probes in this offline example.

    let config = ProxyConfig {
        direct: Some(vec![
            DirectProxy {
                name: Some("proxy-a".into()),
                url: "http://127.0.0.1:18080".into(),
                rate_limit: Some(0.0),
                expire_time: None,
            },
            DirectProxy {
                name: Some("proxy-b".into()),
                url: "http://127.0.0.1:18081".into(),
                rate_limit: Some(0.0),
                expire_time: None,
            },
        ]),
        tunnel: None,
        ip_provider: None,
        pool_config: Some(pool_config),
    };

    let manager = ProxyManager::from_proxy_config(&config).await?;

    // The engine normally calls begin_proxy_attempt immediately before a real download and
    // reports the actual outcome afterwards. Here we simulate a failure to show the API flow.
    let first = manager.get_proxy_for_attempt(None).await?;
    manager.begin_proxy_attempt(&first).await?;
    manager.report_failure(&first).await?;
    println!("first attempt used {first}; simulated failure");

    // Exclude the failed proxy within the same retry budget.
    let next = manager.get_proxy_for_attempt(Some(&first)).await?;
    manager.begin_proxy_attempt(&next).await?;
    manager.report_success(&next, None).await?;
    println!("retry selected {next}; simulated success");
    println!("pool status: {:?}", manager.get_status().await);

    Ok(())
}
