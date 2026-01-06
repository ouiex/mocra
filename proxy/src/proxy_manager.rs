use crate::proxy_pool::*;
use errors::{Error, Result};
use std::time::Duration;

pub struct ProxyManager {
    pool: ProxyPool,
}

impl ProxyManager {
    pub async fn from_config(config_str: &str) -> Result<Self> {
        let proxy_setting = ProxyConfig::load_from_toml(config_str)?;
        let pool = proxy_setting.build_proxy_pool().await;
        Ok(Self { pool })
    }

    pub fn new() -> Self {
        let pool = ProxyPool::new(PoolConfig::default());
        Self { pool }
    }

    pub fn with_config(config: PoolConfig) -> Self {
        let pool = ProxyPool::new(config);
        Self { pool }
    }

    pub async fn get_proxy(&self, provider_name: Option<&str>) -> Result<ProxyEnum> {
        self.pool.get_proxy(provider_name).await
    }
    pub async fn get_tunnel(&self) -> Result<ProxyEnum> {
        self.pool
            .get_best_tunnel()
            .await
            .ok_or_else(Error::proxy_not_found)
    }

    pub async fn report_proxy_result(
        &self,
        proxy: &ProxyEnum,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        // 直接调用代理池的report_proxy_result方法
        // 系统会自动根据代理类型进行相应的处理
        self.pool
            .report_proxy_result(proxy, success, response_time)
            .await
    }

    pub async fn report_success(
        &self,
        proxy: &ProxyEnum,
        response_time: Option<Duration>,
    ) -> Result<()> {
        self.report_proxy_result(proxy, true, response_time).await
    }

    pub async fn report_failure(&self, proxy: &ProxyEnum) -> Result<()> {
        self.report_proxy_result(proxy, false, None).await
    }

    pub async fn get_status(&self) -> std::collections::HashMap<String, usize> {
        self.pool.get_pool_status().await
    }

    pub async fn get_detailed_stats(&self) -> PoolStats {
        self.pool.get_stats().await
    }

    pub async fn health_check(&self) -> Result<()> {
        self.pool.health_check().await
    }

    pub async fn add_ip_provider(&mut self, provider: Box<dyn IpProxyLoader>) {
        self.pool.add_ip_provider(provider).await;
    }
    pub async fn add_tunnel(&mut self, tunnel: Tunnel) {
        self.pool.add_tunnel(tunnel).await;
    }
}

impl Default for ProxyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use crate::{ProxyConfig, ProxyManager};
    use tokio::fs;

    #[tokio::test]
    async fn test() {
        let config =
            ProxyConfig::load_from_toml(&fs::read_to_string("proxy.toml").await.unwrap()).unwrap();
        let mut manager = ProxyManager::new();
        if let Some(tunnel) = config.tunnel {
            for t in tunnel {
                manager.add_tunnel(t).await;
            }
        }
        let proxy = manager.get_tunnel().await.unwrap();
        println!("{:?}", proxy.to_string());
    }
}
