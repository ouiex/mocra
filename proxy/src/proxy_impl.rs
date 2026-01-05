#![allow(unused)]

use crate::{IpProvider, IpProxy, IpProxyLoader};
use async_trait::async_trait;

/// 快代理加载器
pub(crate) struct KuaiDaiLiLoader {
    pub(crate) config: IpProvider,
}

#[async_trait]
impl IpProxyLoader for KuaiDaiLiLoader {
    async fn get_ip_proxies(&self) -> errors::Result<Vec<IpProxy>> {
        // TODO: 实现快代理API调用
        // 这里应该调用快代理API获取代理列表
        Ok(vec![])
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

    async fn health_check(&self, _proxy: &IpProxy) -> bool {
        // TODO: 实现健康检查
        true
    }
}

/// 芝麻代理加载器
struct ZmLoader {
    config: IpProvider,
}

#[async_trait]
impl IpProxyLoader for ZmLoader {
    async fn get_ip_proxies(&self) -> errors::Result<Vec<IpProxy>> {
        // TODO: 实现芝麻代理API调用
        Ok(vec![])
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

    async fn health_check(&self, _proxy: &IpProxy) -> bool {
        // TODO: 实现健康检查
        true
    }
}
