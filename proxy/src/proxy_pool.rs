use async_trait::async_trait;
use errors::ProxyError;
use errors::Result;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, PartialEq};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{Mutex, RwLock};
#[derive(Clone)]
pub struct RateLimitTracker {
    requests_in_window: u32,
    window_start: Instant,     // 窗口开始（单调时钟）
    window_duration: Duration, // 窗口长度
}

impl RateLimitTracker {
    /// 创建新的限速跟踪器（毫秒级）
    pub fn new() -> Self {
        Self {
            requests_in_window: 0,
            window_start: Instant::now(),
            window_duration: Duration::from_millis(1000), // 固定 1 秒窗口
        }
    }

    /// 记录一个请求
    pub fn record_request(&mut self) {
        // 如果时间窗口已过期，重置计数器
        if self.window_start.elapsed() >= self.window_duration {
            self.requests_in_window = 0;
            self.window_start = Instant::now();
        }
        self.requests_in_window += 1;
    }

    /// 检查是否达到限速（毫秒级）
    pub fn is_rate_limited(&mut self, rate_limit: f32) -> bool {
        // 非正限速视为不限制
        if rate_limit <= 0.0 {
            return false;
        }
        // 若窗口已过，视为未限
        if self.window_start.elapsed() >= self.window_duration {
            self.requests_in_window = 0;
            self.window_start = Instant::now();
            return false;
        }
        let cap = rate_limit.floor() as u32;
        self.requests_in_window >= cap
    }

    /// 获取当前请求频率（毫秒级）
    pub fn get_current_rate(&self) -> f32 {
        let elapsed = self.window_start.elapsed();
        if elapsed >= self.window_duration || elapsed.as_millis() == 0 {
            return 0.0;
        }
        self.requests_in_window as f32 / elapsed.as_secs_f32()
    }

    /// 剩余窗口时长（用于等待）
    pub fn remaining_in_window(&self) -> Duration {
        let elapsed = self.window_start.elapsed();
        if elapsed >= self.window_duration {
            Duration::from_millis(0)
        } else {
            self.window_duration - elapsed
        }
    }

    /// 当前窗口内已计数的请求数（若窗口已过期则返回0）
    pub fn current_window_count(&self) -> u32 {
        if self.window_start.elapsed() >= self.window_duration {
            0
        } else {
            self.requests_in_window
        }
    }
}

impl Default for RateLimitTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IpProvider {
    pub name: String,
    pub url: String,
    pub retry_codes: Vec<u16>,
    pub timeout: u64,
    pub rate_limit: f32,
    pub provider_expire_time: Option<String>, // 提供商的过期时间
    pub proxy_expire_time: u64,               // 当前供应商的代理过期时间
    pub weight: Option<u32>,                  // 添加权重支持
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IpProxy {
    pub ip: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub proxy_type: Option<String>, // http, socks5, etc
    pub rate_limit: f32,            // 每秒最大请求数
}

impl Display for IpProxy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // 格式化为reqwest可以直接使用的代理URL格式
        let proxy_type = self.proxy_type.as_deref().unwrap_or("http");
        match (&self.username, &self.password) {
            (Some(username), Some(password)) => {
                write!(
                    f,
                    "{}://{}:{}@{}:{}",
                    proxy_type, username, password, self.ip, self.port
                )
            }
            (Some(username), None) => {
                write!(f, "{}://{}@{}:{}", proxy_type, username, self.ip, self.port)
            }
            _ => {
                write!(f, "{}://{}:{}", proxy_type, self.ip, self.port)
            }
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tunnel {
    pub name: String,
    pub endpoint: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tunnel_type: String,
    pub expire_time: String,
    pub rate_limit: f32, // 每秒最大请求数
}

impl Display for Tunnel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // 格式化为reqwest可以直接使用的代理URL格式
        match (&self.username, &self.password) {
            (Some(username), Some(password)) => {
                write!(
                    f,
                    "{}://{}:{}@{}",
                    self.tunnel_type, username, password, self.endpoint
                )
            }
            (Some(username), None) => {
                write!(f, "{}://{}@{}", self.tunnel_type, username, self.endpoint)
            }
            _ => {
                write!(f, "{}://{}", self.tunnel_type, self.endpoint)
            }
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProxyEnum {
    Tunnel(Tunnel),
    IpProxy(IpProxy),
}

impl Display for ProxyEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            ProxyEnum::Tunnel(tunnel) => tunnel.to_string(),
            ProxyEnum::IpProxy(ip_proxy) => ip_proxy.to_string(),
        };
        write!(f, "{str}")
    }
}
impl PartialEq<Tunnel> for ProxyEnum {
    fn eq(&self, other: &Tunnel) -> bool {
        if let ProxyEnum::Tunnel(tunnel) = self {
            tunnel.endpoint == other.endpoint
                && tunnel.username == other.username
                && tunnel.password == other.password
                && tunnel.tunnel_type == other.tunnel_type
        } else {
            false
        }
    }
}
impl PartialEq<IpProxy> for ProxyEnum {
    fn eq(&self, other: &IpProxy) -> bool {
        if let ProxyEnum::IpProxy(ip_proxy) = self {
            ip_proxy.ip == other.ip
                && ip_proxy.port == other.port
                && ip_proxy.username == other.username
                && ip_proxy.password == other.password
                && ip_proxy.proxy_type == other.proxy_type
                && (ip_proxy.rate_limit - other.rate_limit).abs() < f32::EPSILON // 比较浮点数时使用容差
        } else {
            false
        }
    }
}
impl PartialEq for ProxyEnum {
    fn eq(&self, other: &ProxyEnum) -> bool {
        match self {
            ProxyEnum::Tunnel(tunnel) => {
                if let ProxyEnum::Tunnel(other_tunnel) = other {
                    tunnel.endpoint == other_tunnel.endpoint
                        && tunnel.username == other_tunnel.username
                        && tunnel.password == other_tunnel.password
                        && tunnel.tunnel_type == other_tunnel.tunnel_type
                } else {
                    false
                }
            }
            ProxyEnum::IpProxy(ip_proxy) => {
                if let ProxyEnum::IpProxy(other_ip_proxy) = other {
                    ip_proxy.ip == other_ip_proxy.ip
                        && ip_proxy.port == other_ip_proxy.port
                        && ip_proxy.password == other_ip_proxy.password
                        && ip_proxy.username == other_ip_proxy.username
                        && ip_proxy.proxy_type == other_ip_proxy.proxy_type
                } else {
                    false
                }
            }
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProxyConfig {
    pub tunnel: Option<Vec<Tunnel>>,
    pub ip_provider: Option<Vec<IpProvider>>,
    pub pool_config: Option<PoolConfig>,
}

impl ProxyConfig {
    pub fn load_from_toml(toml_str: &str) -> Result<Self> {
        toml::from_str(toml_str).map_err(|e| ProxyError::InvalidConfig(e.to_string().into()).into())
    }

    pub async fn build_proxy_pool(&self) -> ProxyPool {
        let config = self.pool_config.clone().unwrap_or_default();
        let mut builder = ProxyPoolBuilder::new(config);

        if let Some(tunnels) = &self.tunnel {
            builder = builder.with_tunnels(tunnels.clone());
        }

        if let Some(providers) = &self.ip_provider {
            builder = builder.with_ip_providers(providers.clone());
        }

        builder.build().await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PoolConfig {
    pub min_size: usize,
    pub max_size: usize,
    pub max_errors: u32,
    pub health_check_interval_secs: u64,
    pub refill_threshold: f32, // 当池大小低于这个比例时触发补充
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 5,
            max_size: 50,
            max_errors: 3,
            health_check_interval_secs: 300,
            refill_threshold: 0.3,
        }
    }
}

#[async_trait]
pub trait IpProxyLoader: Send + Sync {
    async fn get_ip_proxies(&self) -> Result<Vec<IpProxy>>;
    fn is_retry_code(&self, code: &u16) -> bool;
    fn get_name(&self) -> String;
    fn get_weight(&self) -> u32;
    fn get_config(&self) -> &IpProvider;
    async fn health_check(&self, proxy: &IpProxy) -> bool;
}

/// 代理池构建器
pub struct ProxyPoolBuilder {
    config: PoolConfig,
    tunnels: Vec<Tunnel>,
    ip_providers: Vec<IpProvider>,
}

impl ProxyPoolBuilder {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            tunnels: Vec::new(),
            ip_providers: Vec::new(),
        }
    }

    pub fn with_tunnels(mut self, tunnels: Vec<Tunnel>) -> Self {
        self.tunnels = tunnels;
        self
    }

    pub fn with_tunnel(mut self, tunnel: Tunnel) -> Self {
        self.tunnels.push(tunnel);
        self
    }

    pub fn with_ip_providers(mut self, providers: Vec<IpProvider>) -> Self {
        self.ip_providers = providers;
        self
    }

    pub async fn build(self) -> ProxyPool {
        let pool = ProxyPool::new(self.config);
        for tunnel in &self.tunnels {
            pool.add_tunnel(tunnel.clone()).await;
        }
        for provider in &self.ip_providers {
            let loader = crate::proxy_impl::build_ip_proxy_loader(provider.clone());
            pool.add_ip_provider(loader).await;
        }
        pool
    }
}
#[derive(Clone)]
pub struct ProxyItem {
    pub proxy: ProxyEnum,
    pub error_count: u32,
    pub success_count: u32,
    pub last_used: Option<Duration>,
    pub expire_time: Duration,
    pub provider_name: String,
    pub response_time: Option<Duration>,      // 响应时间
    pub success_rate: f32,                    // 成功率
    pub rate_limit_tracker: RateLimitTracker, // 限速跟踪器
    pub provider_rate_limit: f32,             // 提供商设置的限速值
}

impl ProxyItem {
    pub fn new_for_tunnel(tunnel: Tunnel) -> Self {
        let expire_time = if let Ok(datetime) = OffsetDateTime::parse(&tunnel.expire_time, &Rfc3339)
        {
            (datetime - OffsetDateTime::from_unix_timestamp(0).unwrap_or(OffsetDateTime::UNIX_EPOCH))
                .unsigned_abs()
        } else {
            Duration::from_secs(360 * 24 * 60 * 60)
                + SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
        };

        let rate = tunnel.rate_limit; // 默认每秒10个请求
        let name = tunnel.name.clone();
        Self {
            proxy: ProxyEnum::Tunnel(tunnel),
            error_count: 0,
            success_count: 0,
            last_used: None,
            expire_time,
            provider_name: name,
            response_time: None,
            success_rate: 1.0,
            rate_limit_tracker: RateLimitTracker::new(),
            provider_rate_limit: rate, // 使用隧道的限速值
        }
    }
    pub fn new_for_ip_proxy(ip_proxy: IpProxy, ip_provider: &IpProvider) -> Self {
        let expire_time = Duration::from_secs(ip_provider.proxy_expire_time)
            + SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default(); // 默认5分钟
        Self {
            proxy: ProxyEnum::IpProxy(ip_proxy),
            error_count: 0,
            success_count: 0,
            last_used: None,
            expire_time,
            provider_name: ip_provider.name.clone(),
            response_time: None,
            success_rate: 1.0,
            rate_limit_tracker: RateLimitTracker::new(),
            provider_rate_limit: ip_provider.rate_limit, // 使用提供商的限速值
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        now > self.expire_time
    }

    pub fn is_valid(&self, max_errors: u32) -> bool {
        self.error_count < max_errors && !self.is_expired()
    }

    pub fn record_success(&mut self, response_time: Duration) {
        self.success_count += 1;
        self.response_time = Some(response_time);
        self.last_used = Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default());
        self.update_success_rate();
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.last_used = Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default());
        self.update_success_rate();
        // 失败同样计入本窗口请求数，保持与选择时的一致性
        self.rate_limit_tracker.record_request();
    }
    fn update_success_rate(&mut self) {
        let total = self.success_count + self.error_count;
        if total > 0 {
            self.success_rate = self.success_count as f32 / total as f32;
        }
    }

    pub fn quality_score(&self) -> f32 {
        let mut score = self.success_rate * 100.0;

        // 响应时间影响分数
        if let Some(response_time) = self.response_time {
            let response_ms = response_time.as_millis() as f32;
            score -= response_ms / 100.0; // 响应时间越长，分数越低
        }

        // 最近使用时间影响分数
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let time_since_last_use = if let Some(last_used) = self.last_used {
            (now - last_used).as_secs()
        } else {
            0
        };
        score -= time_since_last_use as f32 / 3600.0; // 越久未使用，分数越低

        score.max(0.0)
    }

    pub fn is_rate_limited(&mut self) -> bool {
        // 确定使用的限速值
        let actual_rate_limit = match &self.proxy {
            ProxyEnum::IpProxy(ip_proxy) => {
                if ip_proxy.rate_limit > 0.0 {
                    ip_proxy.rate_limit
                } else {
                    self.provider_rate_limit
                }
            }
            ProxyEnum::Tunnel(tunnel) => tunnel.rate_limit,
        };
        let limited = self.rate_limit_tracker.is_rate_limited(actual_rate_limit);
        if limited {
            let remaining = self.rate_limit_tracker.remaining_in_window();
            let count = self.rate_limit_tracker.current_window_count();
            log::warn!(
                "Proxy rate limited: provider={}, proxy={}, limit={:.2}/s, count_in_window={}, remaining={:?}",
                self.provider_name,
                self.proxy,
                actual_rate_limit,
                count,
                remaining
            );
        }
        limited
    }
}

impl PartialEq for ProxyItem {
    fn eq(&self, other: &Self) -> bool {
        match &self.proxy {
            ProxyEnum::IpProxy(ip_proxy) => {
                if let ProxyEnum::IpProxy(other_ip_proxy) = &other.proxy {
                    ip_proxy.ip == other_ip_proxy.ip && ip_proxy.port == other_ip_proxy.port
                } else {
                    false
                }
            }
            ProxyEnum::Tunnel(tunnel) => {
                if let ProxyEnum::Tunnel(other_tunnel) = &other.proxy {
                    tunnel.endpoint == other_tunnel.endpoint
                } else {
                    false
                }
            }
        }
    }
}

impl Eq for ProxyItem {}

impl PartialOrd for ProxyItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProxyItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .quality_score()
            .partial_cmp(&self.quality_score())
            .unwrap_or(Ordering::Equal)
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_proxies: usize,
    pub valid_proxies: usize,
    pub error_proxies: usize,
    pub expired_proxies: usize,
    pub avg_success_rate: f32,
    pub providers: HashMap<String, ProviderStats>,
}

#[derive(Debug, Clone)]
pub struct ProviderStats {
    pub name: String,
    pub total_proxies: usize,
    pub valid_proxies: usize,
    pub avg_success_rate: f32,
    pub avg_response_time: Option<Duration>,
}

type IpProvidersMap = HashMap<String, Arc<Box<dyn IpProxyLoader>>>;

pub struct ProxyPool {
    pub config: PoolConfig,
    pub pools: Arc<RwLock<HashMap<String, Vec<ProxyItem>>>>,
    pub ip_providers: Arc<Mutex<IpProvidersMap>>,
    pub stats: Arc<RwLock<PoolStats>>,
}

impl ProxyPool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            pools: Arc::new(RwLock::new(HashMap::new())),
            ip_providers: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(RwLock::new(PoolStats {
                total_proxies: 0,
                valid_proxies: 0,
                error_proxies: 0,
                expired_proxies: 0,
                avg_success_rate: 0.0,
                providers: HashMap::new(),
            })),
        }
    }
    pub async fn add_tunnel(&self, tunnel: Tunnel) {
        let proxy_name = tunnel.name.clone();
        let proxy_item = ProxyItem::new_for_tunnel(tunnel);
        let mut pools = self.pools.write().await;
        pools
            .entry(proxy_name)
            .or_insert_with(Vec::new)
            .push(proxy_item);
    }
    pub async fn add_ip_provider(&self, provider: Box<dyn IpProxyLoader>) {
        let name = provider.get_name();
        let mut ip_providers = self.ip_providers.lock().await;
        ip_providers.insert(name.clone(), Arc::new(provider));
        let mut pools = self.pools.write().await;
        pools.insert(name.clone(), Vec::new());
    }

    /// 获取代理，支持负载均衡和故障转移
    pub async fn get_proxy(&self, provider_name: Option<&str>) -> Result<ProxyEnum> {
        if let Some(name) = provider_name {
            return self.get_ip_proxy_from_provider(name).await;
        }
        // 首先尝试获取最佳隧道代理
        if let Some(tunnel) = self.get_best_tunnel().await {
            return Ok(tunnel);
        }

        // 如果没有可用的隧道代理，处理IP代理
        self.get_best_ip_proxy().await
    }

    /// 获取质量最好的隧道代理
    pub async fn get_best_tunnel(&self) -> Option<ProxyEnum> {
        // 获取写锁，直接操作pools中的数据
        let mut pools = self.pools.write().await;

        // 收集所有隧道代理并按质量分数排序
        let mut tunnel_items = Vec::new();
        for (_, pool) in pools.iter_mut() {
            for item in pool.iter_mut() {
                if matches!(item.proxy, ProxyEnum::Tunnel(_)) {
                    tunnel_items.push(item);
                }
            }
        }

        if tunnel_items.is_empty() {
            return None;
        }
        tunnel_items.sort();
        // 按质量分数排序（降序）
        // tunnel_items.sort_by(|a, b| {
        //     b.quality_score()
        //         .partial_cmp(&a.quality_score())
        //         .unwrap_or(std::cmp::Ordering::Equal)
        // });

        // 尝试找到未达到限速的最佳隧道代理

        for item in tunnel_items.iter_mut() {
            if !item.is_rate_limited() {
                item.rate_limit_tracker.record_request();
                return Some(item.proxy.clone());
            }
        }

        // 全部被限，计算最短剩余时间等待
        if let Some(min_remaining) = tunnel_items
            .iter()
            .map(|i| i.rate_limit_tracker.remaining_in_window())
            .min()
        {
            let sleep_dur = if min_remaining > Duration::from_millis(0) {
                min_remaining
            } else {
                Duration::from_millis(50)
            };
            tokio::time::sleep(sleep_dur).await;
            // 重试一次（不递归多次，交给调用方后续节奏）
            for item in tunnel_items.iter_mut() {
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    return Some(item.proxy.clone());
                }
            }
        }

        None
    }

    /// 从指定提供商获取代理
    async fn get_ip_proxy_from_provider(&self, provider_name: &str) -> Result<ProxyEnum> {
        self.ensure_pool_size(provider_name).await?;

        // 先获取代理池的副本，避免锁跨越await
        {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(provider_name).ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?;

            // 移除过期和无效的代理
            pool.retain(|item| item.is_valid(self.config.max_errors));

            // 按质量分数排序
            pool.sort();
            // 尝试找到未达到限速的最佳代理
            for item in pool.iter_mut() {
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    return Ok(item.proxy.clone());
                }
            }
        }

        // 如果所有代理都达到限速，尝试获取新的代理
        self.refill_pool(provider_name).await?;

        {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(provider_name).ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?;

            // 移除过期和无效的代理
            pool.retain(|item| item.is_valid(self.config.max_errors));

            // 按质量分数排序
            pool.sort();
            // 尝试找到未达到限速的最佳代理
            for item in pool.iter_mut() {
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    return Ok(item.proxy.clone());
                }
            }
            // 如果仍然没有可用代理，计算最短剩余时间等待后重试一次
            if let Some(min_remaining) = pool
                .iter()
                .map(|i| i.rate_limit_tracker.remaining_in_window())
                .min()
            {
                let sleep_dur = if min_remaining > Duration::from_millis(0) {
                    min_remaining
                } else {
                    Duration::from_millis(50)
                };
                tokio::time::sleep(sleep_dur).await;
                for item in pool.iter_mut() {
                    if !item.is_rate_limited() {
                        item.rate_limit_tracker.record_request();
                        return Ok(item.proxy.clone());
                    }
                }
            }
        }

        Err(ProxyError::InvalidConfig("No valid proxy available".into()).into())
    }

    /// 获取最佳代理（跨所有提供商）
    async fn get_best_ip_proxy(&self) -> Result<ProxyEnum> {
        let mut pools = self.pools.write().await;
        let mut proxy_items = pools
            .iter_mut()
            .flat_map(|(_, v)| v)
            .filter(|x| matches!(x.proxy, ProxyEnum::IpProxy(_)))
            .collect::<Vec<_>>();
        proxy_items.sort();
        for item in proxy_items.iter_mut() {
            if !item.is_rate_limited() {
                item.rate_limit_tracker.record_request();
                return Ok(item.proxy.clone());
            }
        }

        // 如果所有代理都达到限速，获取权重最高的IpProvider 执行self.get_ip_proxy_from_provider

        let mut providers: Vec<_> = {
            let providers = self.ip_providers.lock().await;
            providers
                .iter()
                .map(|(name, provider)| (name.clone(), provider.get_weight()))
                .collect()
        };
        providers.sort_by(|a, b| b.1.cmp(&a.1)); // 按权重降序排序
        if let Some((provider_name, _)) = providers.first() {
            // 计算全局最短剩余时间并等待一次
            if let Some(min_remaining) = proxy_items
                .iter()
                .map(|i| i.rate_limit_tracker.remaining_in_window())
                .min()
            {
                let sleep_dur = if min_remaining > Duration::from_millis(0) {
                    min_remaining
                } else {
                    Duration::from_millis(50)
                };
                tokio::time::sleep(sleep_dur).await;
            }
            self.get_ip_proxy_from_provider(provider_name).await
        } else if !proxy_items.is_empty() {
            // 再尝试一次是否已解除限速（需要可变借用）
            let mut_idx = 0usize;
            if !proxy_items[mut_idx].is_rate_limited() {
                proxy_items[mut_idx].rate_limit_tracker.record_request();
                Ok(proxy_items[mut_idx].proxy.clone())
            } else {
                Err(ProxyError::InvalidConfig("No valid proxy available".into()).into())
            }
        } else {
            Err(ProxyError::InvalidConfig("No valid proxy available".into()).into())
        }
    }

    /// 报告代理使用结果
    pub async fn report_proxy_result(
        &self,
        proxy: &ProxyEnum,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        match proxy {
            ProxyEnum::Tunnel(tunnel) => {
                self.report_tunnel_result(tunnel, success, response_time)
                    .await
            }
            ProxyEnum::IpProxy(ip_proxy) => {
                self.report_ip_proxy_result(ip_proxy, success, response_time)
                    .await
            }
        }
    }

    /// 报告隧道代理使用结果
    async fn report_tunnel_result(
        &self,
        tunnel: &Tunnel,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        let mut found = false;
        {
            let mut pools = self.pools.write().await;
            for (_name, pool) in pools.iter_mut() {
                for item in pool.iter_mut() {
                    if let ProxyEnum::Tunnel(ref t) = item.proxy
                        && t.endpoint == tunnel.endpoint {
                            if success {
                                item.record_success(
                                    response_time.unwrap_or(Duration::from_millis(1000)),
                                );
                            } else {
                                item.record_error();
                            }
                            found = true;
                        }
                }
            }
        } // 写锁作用域提前结束
        if !found {
            return Err(ProxyError::InvalidConfig(
                format!("Tunnel {} not found", tunnel.endpoint).into(),
            )
            .into());
        }
        // 更新统计信息
        self.update_stats().await;
        Ok(())
    }

    /// 报告IP代理使用结果
    async fn report_ip_proxy_result(
        &self,
        proxy: &IpProxy,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        let mut proxy_found = false;
        {
            // 遍历所有提供商的池查找代理
            let mut pools = self.pools.write().await;
            for (_, pool) in pools.iter_mut() {
                for item in pool.iter_mut() {
                    if item.proxy.eq(proxy) {
                        if success {
                            item.record_success(
                                response_time.unwrap_or(Duration::from_millis(1000)),
                            );
                        } else {
                            item.record_error();
                        }
                        proxy_found = true;
                        break;
                    }
                }
                if proxy_found {
                    break;
                }
            }

            // 清理所有池中的无效代理
            for (_, pool) in pools.iter_mut() {
                pool.retain(|item| item.is_valid(self.config.max_errors));
            }
        } // 写锁作用域提前结束
        // 如果没有找到代理，返回错误
        if !proxy_found {
            return Err(ProxyError::InvalidConfig(
                format!(
                    "Proxy {}:{} not found in any provider",
                    proxy.ip, proxy.port
                )
                .into(),
            )
            .into());
        }
        // 更新统计信息
        self.update_stats().await;
        Ok(())
    }

    /// 报告代理成功使用
    pub async fn report_success(
        &self,
        proxy: &ProxyEnum,
        response_time: Option<Duration>,
    ) -> Result<()> {
        self.report_proxy_result(proxy, true, response_time).await
    }

    /// 报告代理失败使用
    pub async fn report_failure(&self, proxy: &ProxyEnum) -> Result<()> {
        self.report_proxy_result(proxy, false, None).await
    }

    /// 确保池大小满足要求，具体补充多少由对应的struct决定
    async fn ensure_pool_size(&self, provider_name: &str) -> Result<()> {
        let current_size = {
            let pools = self.pools.read().await;
            pools.get(provider_name).map(|p| p.len()).unwrap_or(0)
        };

        let threshold = (self.config.max_size as f32 * self.config.refill_threshold) as usize;

        if current_size < self.config.min_size || current_size < threshold {
            self.refill_pool(provider_name).await?;
        }

        Ok(())
    }

    /// 当所有代理IP超限了，获取新一批的代理并添加进去，之前的代理仍然存在
    async fn refill_pool(&self, provider_name: &str) -> Result<()> {
        // await 前 clone Arc 指针
        let provider: Arc<Box<dyn IpProxyLoader>> = {
            let providers = self.ip_providers.lock().await;
            providers.get(provider_name).cloned().ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?
        };
        // check provider is available
        if let Some(expire_time) = &provider.get_config().provider_expire_time {
            let now = OffsetDateTime::now_utc().unix_timestamp();
            if let Ok(expire_timestamp) = OffsetDateTime::parse(expire_time, &Rfc3339) {
                if now >= expire_timestamp.unix_timestamp() {
                    return Err(ProxyError::ProxyProviderExpired.into());
                }
            } else {
                return Err(ProxyError::InvalidConfig(
                    format!(
                        "Provider {} expire time is not available",
                        provider.get_config().name
                    )
                    .into(),
                )
                .into());
            };
        }
        let new_proxies = provider.get_ip_proxies().await?;
        {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(provider_name).unwrap();
            for proxy in new_proxies {
                pool.push(ProxyItem::new_for_ip_proxy(proxy, provider.get_config()));
            }
        }
        Ok(())
    }

    /// 更新统计信息
    async fn update_stats(&self) {
        let pools = self.pools.read().await;
        let mut stats = self.stats.write().await;

        stats.total_proxies = 0;
        stats.valid_proxies = 0;
        stats.error_proxies = 0;
        stats.expired_proxies = 0;
        stats.providers.clear();

        let mut total_success_rate = 0.0;
        let mut total_providers = 0;

        for (provider_name, pool) in pools.iter() {
            let mut provider_stats = ProviderStats {
                name: provider_name.clone(),
                total_proxies: pool.len(),
                valid_proxies: 0,
                avg_success_rate: 0.0,
                avg_response_time: None,
            };

            let mut provider_success_rate = 0.0;
            let mut response_times = Vec::new();

            for item in pool.iter() {
                stats.total_proxies += 1;

                if item.is_valid(self.config.max_errors) {
                    stats.valid_proxies += 1;
                    provider_stats.valid_proxies += 1;
                } else if item.is_expired() {
                    stats.expired_proxies += 1;
                } else {
                    stats.error_proxies += 1;
                }

                provider_success_rate += item.success_rate;
                if let Some(response_time) = item.response_time {
                    response_times.push(response_time);
                }
            }

            if !pool.is_empty() {
                provider_stats.avg_success_rate = provider_success_rate / pool.len() as f32;
                total_success_rate += provider_stats.avg_success_rate;
                total_providers += 1;
            }

            if !response_times.is_empty() {
                let avg_ms = response_times.iter().map(|d| d.as_millis()).sum::<u128>()
                    / response_times.len() as u128;
                provider_stats.avg_response_time = Some(Duration::from_millis(avg_ms as u64));
            }

            stats
                .providers
                .insert(provider_name.clone(), provider_stats);
        }

        if total_providers > 0 {
            stats.avg_success_rate = total_success_rate / total_providers as f32;
        }
    }

    /// 获取池状态
    pub async fn get_pool_status(&self) -> HashMap<String, usize> {
        let pools = self.pools.read().await;
        pools
            .iter()
            .map(|(name, pool)| (name.clone(), pool.len()))
            .collect()
    }

    /// 获取详细统计信息
    pub async fn get_stats(&self) -> PoolStats {
        self.stats.read().await.clone()
    }

    /// 执行健康检查
    pub async fn health_check(&self) -> Result<()> {
        let providers: Vec<String> = {
            let providers = self.ip_providers.lock().await;
            providers.keys().cloned().collect()
        };
        for provider_name in providers {
            let provider: Option<Arc<Box<dyn IpProxyLoader>>> = {
                let providers = self.ip_providers.lock().await;
                providers.get(&provider_name).cloned()
            };
            if let Some(provider) = provider {
                let pool = {
                    let pools = self.pools.read().await;
                    pools.get(&provider_name).unwrap_or(&Vec::new()).clone()
                };
                let mut healthy_proxies = Vec::new();
                for item in pool.into_iter() {
                    if let ProxyEnum::IpProxy(ref proxy) = item.proxy {
                        // 执行健康检查
                        if provider.health_check(proxy).await {
                            healthy_proxies.push(item.clone());
                        }
                    }
                }
                {
                    let mut pools = self.pools.write().await;
                    if let Some(pool) = pools.get_mut(&provider_name) {
                        *pool = healthy_proxies;
                    }
                }
            }
        }
        self.update_stats().await;
        Ok(())
    }
}
