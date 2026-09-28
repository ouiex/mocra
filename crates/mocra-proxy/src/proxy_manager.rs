use crate::error::{ProxyError, Result};
use crate::proxy_pool::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct ProxyManager {
    pool: Arc<ProxyPool>,
    health_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ProxyManager {
    fn with_pool(pool: ProxyPool) -> Self {
        let manager = Self {
            pool: Arc::new(pool),
            health_task: Mutex::new(None),
        };
        manager.start_health_checks();
        manager
    }

    pub async fn from_proxy_config(proxy_config: &ProxyConfig) -> Result<Self> {
        let pool = proxy_config.build_proxy_pool().await;
        Ok(Self::with_pool(pool))
    }

    pub async fn from_config(config_str: &str) -> Result<Self> {
        let proxy_setting = ProxyConfig::load_from_toml(config_str)?;
        let pool = proxy_setting.build_proxy_pool().await;
        Ok(Self::with_pool(pool))
    }

    pub fn new() -> Self {
        let pool = ProxyPool::new(PoolConfig::default());
        Self::with_pool(pool)
    }

    pub fn with_config(config: PoolConfig) -> Self {
        let pool = ProxyPool::new(config);
        Self::with_pool(pool)
    }

    /// Starts periodic health checks once. An interval of zero disables scheduling.
    /// Returns false when no Tokio runtime is active or a task is already running.
    pub fn start_health_checks(&self) -> bool {
        let seconds = self.pool.config.health_check_interval_secs;
        if seconds == 0 {
            return false;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return false;
        };
        let mut task = self.health_task.lock().unwrap_or_else(|e| e.into_inner());
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return false;
        }
        let pool = self.pool.clone();
        *task = Some(runtime.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(seconds)).await;
                if let Err(error) = pool.health_check().await {
                    log::warn!("scheduled proxy health check failed: {error}");
                }
            }
        }));
        true
    }

    /// Cancels the periodic task and any in-flight health probes.
    pub async fn stop_health_checks(&self) {
        let task = self
            .health_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    pub async fn get_proxy(&self, provider_name: Option<&str>) -> Result<ProxyEnum> {
        self.pool.get_proxy(provider_name).await
    }

    pub async fn get_proxy_for_attempt(&self, excluded: Option<&ProxyEnum>) -> Result<ProxyEnum> {
        self.pool.get_proxy_for_attempt(excluded).await
    }

    pub async fn begin_proxy_attempt(&self, proxy: &ProxyEnum) -> Result<()> {
        self.pool.begin_proxy_attempt(proxy).await
    }

    pub async fn is_retry_code(&self, proxy: &ProxyEnum, code: u16) -> bool {
        self.pool.is_retry_code(proxy, code).await
    }
    pub async fn get_tunnel(&self) -> Result<ProxyEnum> {
        self.pool
            .get_best_tunnel()
            .await
            .ok_or_else(|| ProxyError::ProxyNotFound)
    }

    pub async fn report_proxy_result(
        &self,
        proxy: &ProxyEnum,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        // Call the proxy pool's report_proxy_result directly; it dispatches on the proxy
        // type automatically.
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

    pub fn selection_wait_stats(&self) -> ProxySelectionWaitStats {
        self.pool.selection_wait_stats()
    }

    pub fn attempt_stats(&self) -> ProxyAttemptStats {
        self.pool.attempt_stats()
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

impl Drop for ProxyManager {
    fn drop(&mut self) {
        if let Some(task) = self
            .health_task
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
    }
}

impl Default for ProxyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        IpProvider, IpProxy, IpProxyLoader, PoolConfig, ProxyConfig, ProxyManager, Result,
    };
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::fs;
    use tokio::sync::Notify;

    struct BlockingHealthLoader {
        config: IpProvider,
        entered: Arc<Notify>,
        active: Arc<AtomicUsize>,
    }

    struct ProbeGuard(Arc<AtomicUsize>);
    impl Drop for ProbeGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl IpProxyLoader for BlockingHealthLoader {
        async fn get_ip_proxies(&self) -> Result<Vec<IpProxy>> {
            Ok(vec![IpProxy {
                ip: "127.0.0.1".into(),
                port: 8080,
                username: None,
                password: None,
                proxy_type: Some("http".into()),
                rate_limit: 0.0,
            }])
        }
        fn is_retry_code(&self, _: &u16) -> bool {
            false
        }
        fn get_name(&self) -> String {
            self.config.name.clone()
        }
        fn get_weight(&self) -> u32 {
            1
        }
        fn get_config(&self) -> &IpProvider {
            &self.config
        }
        async fn health_check(&self, _: &IpProxy) -> bool {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _guard = ProbeGuard(self.active.clone());
            self.entered.notify_one();
            std::future::pending::<()>().await;
            true
        }
    }

    #[tokio::test]
    async fn scheduled_health_checks_stop_and_cancel_inflight_probe() {
        let mut manager = ProxyManager::with_config(PoolConfig {
            min_size: 1,
            max_size: 1,
            health_check_interval_secs: 1,
            health_check_concurrency: 1,
            ..PoolConfig::default()
        });
        assert!(!manager.start_health_checks());
        let entered = Arc::new(Notify::new());
        let active = Arc::new(AtomicUsize::new(0));
        manager
            .add_ip_provider(Box::new(BlockingHealthLoader {
                config: IpProvider {
                    name: "scheduled".into(),
                    url: String::new(),
                    retry_codes: vec![],
                    timeout: 5,
                    rate_limit: 0.0,
                    provider_expire_time: None,
                    proxy_expire_time: 60,
                    weight: None,
                },
                entered: entered.clone(),
                active: active.clone(),
            }))
            .await;
        manager.get_proxy(Some("scheduled")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 1);
        manager.stop_health_checks().await;
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert!(manager.start_health_checks());
        manager.stop_health_checks().await;
    }

    #[tokio::test]
    #[ignore = "requires proxy.toml"]
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
