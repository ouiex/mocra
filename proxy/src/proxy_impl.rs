use crate::{IpProvider, IpProxy, IpProxyLoader};
use async_trait::async_trait;
use log::info;
use std::time::Duration;

async fn fetch_text_proxies(config: &IpProvider) -> errors::Result<Vec<IpProxy>> {
    info!("Fetching proxies from {}: {}", config.name, config.url);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout))
        .build()
        .map_err(|e| errors::Error::from(errors::ProxyError::GetProxy(Box::new(e))))?;

    let resp = client
        .get(&config.url)
        .send()
        .await
        .map_err(|e| errors::Error::from(errors::ProxyError::GetProxy(Box::new(e))))?
        .text()
        .await
        .map_err(|e| errors::Error::from(errors::ProxyError::GetProxy(Box::new(e))))?;

    let mut proxies = Vec::new();
    for line in resp.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Parse IP:PORT or IP:PORT:USER:PASS
        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() >= 2
            && let Ok(port) = parts[1].parse::<u16>()
        {
            proxies.push(IpProxy {
                ip: parts[0].to_string(),
                port,
                username: if parts.len() >= 4 {
                    Some(parts[2].to_string())
                } else {
                    None
                },
                password: if parts.len() >= 4 {
                    Some(parts[3].to_string())
                } else {
                    None
                },
                proxy_type: Some("http".to_string()),
                rate_limit: config.rate_limit,
            });
        }
    }
    info!("Fetched {} proxies from {}", proxies.len(), config.name);
    Ok(proxies)
}

/// 快代理加载器
pub(crate) struct KuaiDaiLiLoader {
    pub(crate) config: IpProvider,
}

#[async_trait]
impl IpProxyLoader for KuaiDaiLiLoader {
    async fn get_ip_proxies(&self) -> errors::Result<Vec<IpProxy>> {
        fetch_text_proxies(&self.config).await
    }
    fn is_retry_code(&self, code: &u16) -> bool {
        self.config.retry_codes.contains(code)
    }

    fn get_name(&self) -> String {
        self.config.name.clone()
    }

    fn get_weight(&self) -> u32 {
        self.config.weight.unwrap_or(1)
    }

    fn get_config(&self) -> &IpProvider {
        &self.config
    }

    async fn health_check(&self, proxy: &IpProxy) -> bool {
        check_proxy_health(proxy).await
    }
}

/// 芝麻代理加载器
pub(crate) struct ZmLoader {
    pub(crate) config: IpProvider,
}

#[async_trait]
impl IpProxyLoader for ZmLoader {
    async fn get_ip_proxies(&self) -> errors::Result<Vec<IpProxy>> {
        fetch_text_proxies(&self.config).await
    }

    fn is_retry_code(&self, code: &u16) -> bool {
        self.config.retry_codes.contains(code)
    }

    fn get_name(&self) -> String {
        self.config.name.clone()
    }

    fn get_weight(&self) -> u32 {
        self.config.weight.unwrap_or(1)
    }

    fn get_config(&self) -> &IpProvider {
        &self.config
    }

    async fn health_check(&self, proxy: &IpProxy) -> bool {
        check_proxy_health(proxy).await
    }
}

/// Generic text-list proxy loader (IP:PORT[:USER:PASS])
pub(crate) struct TextListLoader {
    pub(crate) config: IpProvider,
}

#[async_trait]
impl IpProxyLoader for TextListLoader {
    async fn get_ip_proxies(&self) -> errors::Result<Vec<IpProxy>> {
        fetch_text_proxies(&self.config).await
    }

    fn is_retry_code(&self, code: &u16) -> bool {
        self.config.retry_codes.contains(code)
    }

    fn get_name(&self) -> String {
        self.config.name.clone()
    }

    fn get_weight(&self) -> u32 {
        self.config.weight.unwrap_or(1)
    }

    fn get_config(&self) -> &IpProvider {
        &self.config
    }

    async fn health_check(&self, proxy: &IpProxy) -> bool {
        check_proxy_health(proxy).await
    }
}

pub(crate) fn build_ip_proxy_loader(config: IpProvider) -> Box<dyn IpProxyLoader> {
    let name = config.name.to_lowercase();
    match name.as_str() {
        "kuaidaili" => Box::new(KuaiDaiLiLoader { config }),
        "zm" | "zhima" => Box::new(ZmLoader { config }),
        _ => Box::new(TextListLoader { config }),
    }
}

async fn check_proxy_health(proxy: &IpProxy) -> bool {
    let proxy_url = proxy.to_string();
    if let Ok(p) = reqwest::Proxy::all(&proxy_url)
        && let Ok(client) = reqwest::Client::builder()
            .proxy(p)
            .timeout(Duration::from_secs(5))
            .build()
        {
             // Use a lightweight check
            return client.head("http://www.baidu.com").send().await.is_ok();
        }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IpProvider;

    #[test]
    fn test_kuaidaili_config() {
        let config = IpProvider {
            name: "kuaidaili".to_string(),
            url: "http://example.com".to_string(),
            retry_codes: vec![429, 503],
            timeout: 10,
            rate_limit: 10.0,
            provider_expire_time: None,
            proxy_expire_time: 300,
            weight: Some(10),
        };
        let loader = KuaiDaiLiLoader { config: config.clone() };
        assert_eq!(loader.get_name(), "kuaidaili");
        assert_eq!(loader.get_weight(), 10);
        assert!(loader.is_retry_code(&429));
        assert!(!loader.is_retry_code(&200));
    }

    #[test]
    fn test_zm_config() {
        let config = IpProvider {
            name: "zm".to_string(),
            url: "http://example.com".to_string(),
            retry_codes: vec![],
            timeout: 10,
            rate_limit: 5.0,
            provider_expire_time: None,
            proxy_expire_time: 300,
            weight: None,
        };
        let loader = ZmLoader { config };
        assert_eq!(loader.get_name(), "zm");
        assert_eq!(loader.get_weight(), 1); // Default
    }
}
