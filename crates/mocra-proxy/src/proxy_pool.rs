use crate::error::ProxyError;
use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, PartialEq};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{Mutex, RwLock};
use url::Url;
#[derive(Clone)]
pub struct RateLimitTracker {
    requests_in_window: u32,
    window_start: Instant,     // Window start (monotonic clock)
    window_duration: Duration, // Window length
}

impl RateLimitTracker {
    /// Creates a new rate-limit tracker (millisecond resolution).
    pub fn new() -> Self {
        Self {
            requests_in_window: 0,
            window_start: Instant::now(),
            window_duration: Duration::from_millis(1000), // Fixed 1-second window
        }
    }

    /// Records a single request.
    pub fn record_request(&mut self) {
        // Reset the counter if the time window has expired.
        if self.window_start.elapsed() >= self.window_duration {
            self.requests_in_window = 0;
            self.window_start = Instant::now();
        }
        self.requests_in_window += 1;
    }

    /// Checks whether the rate limit has been reached (millisecond resolution).
    pub fn is_rate_limited(&mut self, rate_limit: f32) -> bool {
        // A non-positive rate limit means no limiting.
        if rate_limit <= 0.0 {
            return false;
        }
        // If the window has elapsed, treat as not limited.
        if self.window_start.elapsed() >= self.window_duration {
            self.requests_in_window = 0;
            self.window_start = Instant::now();
            return false;
        }
        let cap = rate_limit.floor() as u32;
        self.requests_in_window >= cap
    }

    /// Returns the current request rate (millisecond resolution).
    pub fn get_current_rate(&self) -> f32 {
        let elapsed = self.window_start.elapsed();
        if elapsed >= self.window_duration || elapsed.as_millis() == 0 {
            return 0.0;
        }
        self.requests_in_window as f32 / elapsed.as_secs_f32()
    }

    /// Remaining duration of the current window (used for waiting).
    pub fn remaining_in_window(&self) -> Duration {
        let elapsed = self.window_start.elapsed();
        if elapsed >= self.window_duration {
            Duration::from_millis(0)
        } else {
            self.window_duration - elapsed
        }
    }

    /// Number of requests counted in the current window (returns 0 if the window has expired).
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
    pub provider_expire_time: Option<String>, // Provider expiry time
    pub proxy_expire_time: u64,               // Expiry time of this provider's proxies
    pub weight: Option<u32>,                  // Weight support
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct IpProxy {
    pub ip: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub proxy_type: Option<String>, // http, socks5, etc
    pub rate_limit: f32,            // Maximum requests per second
}

impl Display for IpProxy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Format as a proxy URL that reqwest can consume directly.
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
    pub rate_limit: f32, // Maximum requests per second
}

impl Display for Tunnel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Format as a proxy URL that reqwest can consume directly.
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
                && (ip_proxy.rate_limit - other.rate_limit).abs() < f32::EPSILON // Float tolerance
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
    pub direct: Option<Vec<DirectProxy>>,
    pub ip_provider: Option<Vec<IpProvider>>,
    pub pool_config: Option<PoolConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirectProxy {
    pub name: Option<String>,
    pub url: String,
    pub rate_limit: Option<f32>,
    pub expire_time: Option<String>,
}

impl DirectProxy {
    fn to_static_ip_proxy(&self, index: usize) -> Result<StaticIpProxyEntry> {
        let parsed = Url::parse(&self.url).map_err(|e| {
            ProxyError::InvalidConfig(
                format!("invalid direct proxy url '{}': {e}", self.url).into(),
            )
        })?;

        let scheme = parsed.scheme().to_ascii_lowercase();
        let proxy_type = match scheme.as_str() {
            "http" => "http",
            "https" => "https",
            // WebSocket proxies are normalized to the http/https proxy protocol.
            "ws" => "http",
            "wss" => "https",
            _ => {
                return Err(ProxyError::InvalidConfig(
                    format!(
                        "unsupported direct proxy scheme '{}', expected http/https/ws/wss",
                        scheme
                    )
                    .into(),
                ));
            }
        }
        .to_string();

        let host = parsed.host_str().ok_or_else(|| {
            ProxyError::InvalidConfig(format!("direct proxy missing host: {}", self.url).into())
        })?;
        let port = parsed.port_or_known_default().ok_or_else(|| {
            ProxyError::InvalidConfig(format!("direct proxy missing port: {}", self.url).into())
        })?;

        let username = if parsed.username().is_empty() {
            None
        } else {
            Some(parsed.username().to_string())
        };

        Ok(StaticIpProxyEntry {
            provider_name: self
                .name
                .clone()
                .unwrap_or_else(|| format!("direct_{}", index)),
            proxy: IpProxy {
                ip: host.to_string(),
                port,
                username,
                password: parsed.password().map(|x| x.to_string()),
                proxy_type: Some(proxy_type),
                rate_limit: self.rate_limit.unwrap_or(10.0),
            },
            rate_limit: self.rate_limit.unwrap_or(10.0),
        })
    }
}

#[derive(Debug, Clone)]
struct StaticIpProxyEntry {
    provider_name: String,
    proxy: IpProxy,
    rate_limit: f32,
}

impl StaticIpProxyEntry {
    fn into_proxy_item(self) -> ProxyItem {
        ProxyItem::new_for_static_ip_proxy(self.proxy, self.provider_name, self.rate_limit)
    }
}

impl ProxyConfig {
    pub fn load_from_toml(toml_str: &str) -> Result<Self> {
        toml::from_str(toml_str).map_err(|e| ProxyError::InvalidConfig(e.to_string().into()))
    }

    pub async fn build_proxy_pool(&self) -> ProxyPool {
        let config = self.pool_config.clone().unwrap_or_default();
        let mut builder = ProxyPoolBuilder::new(config);

        if let Some(tunnels) = &self.tunnel {
            builder = builder.with_tunnels(tunnels.clone());
        }

        if let Some(direct) = &self.direct {
            builder = builder.with_direct_proxies(direct.clone());
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
    pub refill_threshold: f32, // Refill is triggered when the pool falls below this ratio
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

/// Proxy pool builder.
pub struct ProxyPoolBuilder {
    config: PoolConfig,
    tunnels: Vec<Tunnel>,
    direct_proxies: Vec<DirectProxy>,
    ip_providers: Vec<IpProvider>,
}

impl ProxyPoolBuilder {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            tunnels: Vec::new(),
            direct_proxies: Vec::new(),
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

    pub fn with_direct_proxies(mut self, proxies: Vec<DirectProxy>) -> Self {
        self.direct_proxies = proxies;
        self
    }

    pub async fn build(self) -> ProxyPool {
        let pool = ProxyPool::new(self.config);
        for tunnel in &self.tunnels {
            pool.add_tunnel(tunnel.clone()).await;
        }
        for (idx, direct) in self.direct_proxies.into_iter().enumerate() {
            match direct.to_static_ip_proxy(idx) {
                Ok(entry) => {
                    pool.add_static_ip_proxy(entry.into_proxy_item()).await;
                }
                Err(e) => {
                    log::warn!(
                        "[ProxyConfig] skip invalid direct proxy '{}': {}",
                        direct.url,
                        e
                    );
                }
            }
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
    pub response_time: Option<Duration>,      // Response time
    pub success_rate: f32,                    // Success rate
    pub rate_limit_tracker: RateLimitTracker, // Rate-limit tracker
    pub provider_rate_limit: f32,             // Rate limit configured by the provider
}

impl ProxyItem {
    pub fn new_for_tunnel(tunnel: Tunnel) -> Self {
        let expire_time = if let Ok(datetime) = OffsetDateTime::parse(&tunnel.expire_time, &Rfc3339)
        {
            (datetime
                - OffsetDateTime::from_unix_timestamp(0).unwrap_or(OffsetDateTime::UNIX_EPOCH))
            .unsigned_abs()
        } else {
            Duration::from_secs(360 * 24 * 60 * 60)
                + SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
        };

        let rate = tunnel.rate_limit; // Defaults to 10 requests per second
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
            provider_rate_limit: rate, // Use the tunnel's rate limit
        }
    }
    pub fn new_for_ip_proxy(ip_proxy: IpProxy, ip_provider: &IpProvider) -> Self {
        let expire_time = Duration::from_secs(ip_provider.proxy_expire_time)
            + SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(); // Defaults to 5 minutes
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
            provider_rate_limit: ip_provider.rate_limit, // Use the provider's rate limit
        }
    }

    pub fn new_for_static_ip_proxy(
        ip_proxy: IpProxy,
        provider_name: String,
        rate_limit: f32,
    ) -> Self {
        let expire_time = Duration::from_secs(360 * 24 * 60 * 60)
            + SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
        Self {
            proxy: ProxyEnum::IpProxy(ip_proxy),
            error_count: 0,
            success_count: 0,
            last_used: None,
            expire_time,
            provider_name,
            response_time: None,
            success_rate: 1.0,
            rate_limit_tracker: RateLimitTracker::new(),
            provider_rate_limit: rate_limit,
        }
    }

    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        now > self.expire_time
    }

    pub fn is_valid(&self, max_errors: u32) -> bool {
        self.error_count < max_errors && !self.is_expired()
    }

    pub fn record_success(&mut self, response_time: Duration) {
        self.success_count += 1;
        self.response_time = Some(response_time);
        self.last_used = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
        );
        self.update_success_rate();
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
        self.last_used = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
        );
        self.update_success_rate();
        // Failures also count toward this window's requests, consistent with selection.
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

        // Response time affects the score.
        if let Some(response_time) = self.response_time {
            let response_ms = response_time.as_millis() as f32;
            score -= response_ms / 100.0; // The slower the response, the lower the score
        }

        // Time since last use affects the score.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let time_since_last_use = if let Some(last_used) = self.last_used {
            (now - last_used).as_secs()
        } else {
            0
        };
        score -= time_since_last_use as f32 / 3600.0; // Longer unused, lower score

        score.max(0.0)
    }

    pub fn is_rate_limited(&mut self) -> bool {
        // Determine which rate limit applies.
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

    pub async fn add_static_ip_proxy(&self, item: ProxyItem) {
        let mut pools = self.pools.write().await;
        pools
            .entry(item.provider_name.clone())
            .or_insert_with(Vec::new)
            .push(item);
    }

    /// Gets a proxy, with load balancing and failover.
    pub async fn get_proxy(&self, provider_name: Option<&str>) -> Result<ProxyEnum> {
        if let Some(name) = provider_name {
            return self.get_ip_proxy_from_provider(name).await;
        }
        // First try to get the best tunnel proxy.
        if let Some(tunnel) = self.get_best_tunnel().await {
            return Ok(tunnel);
        }

        // Fall back to IP proxies if no tunnel proxy is available.
        self.get_best_ip_proxy().await
    }

    /// Gets the highest-quality tunnel proxy.
    pub async fn get_best_tunnel(&self) -> Option<ProxyEnum> {
        // Take the write lock and operate on the pool data directly.
        let mut pools = self.pools.write().await;

        // Collect all tunnel proxies and sort them by quality score.
        let mut tunnel_items = Vec::new();
        for pool in pools.values_mut() {
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
        // Sort by quality score (descending).
        // tunnel_items.sort_by(|a, b| {
        //     b.quality_score()
        //         .partial_cmp(&a.quality_score())
        //         .unwrap_or(std::cmp::Ordering::Equal)
        // });

        // Try to find the best tunnel proxy that has not hit its rate limit.

        for item in tunnel_items.iter_mut() {
            if !item.is_rate_limited() {
                item.rate_limit_tracker.record_request();
                return Some(item.proxy.clone());
            }
        }

        // All are rate limited: wait for the shortest remaining window.
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
            // Retry once (no deep recursion; leave further pacing to the caller).
            for item in tunnel_items.iter_mut() {
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    return Some(item.proxy.clone());
                }
            }
        }

        None
    }

    /// Gets a proxy from a specific provider.
    async fn get_ip_proxy_from_provider(&self, provider_name: &str) -> Result<ProxyEnum> {
        self.ensure_pool_size(provider_name).await?;

        // Copy out of the proxy pool first so the lock is not held across an await.
        {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(provider_name).ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?;

            // Drop expired and invalid proxies.
            pool.retain(|item| item.is_valid(self.config.max_errors));

            // Sort by quality score.
            pool.sort();
            // Try to find the best proxy that has not hit its rate limit.
            for item in pool.iter_mut() {
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    return Ok(item.proxy.clone());
                }
            }
        }

        // If every proxy is rate limited, try to fetch new ones.
        self.refill_pool(provider_name).await?;

        {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(provider_name).ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?;

            // Drop expired and invalid proxies.
            pool.retain(|item| item.is_valid(self.config.max_errors));

            // Sort by quality score.
            pool.sort();
            // Try to find the best proxy that has not hit its rate limit.
            for item in pool.iter_mut() {
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    return Ok(item.proxy.clone());
                }
            }
            // If none are available, wait the shortest remaining window and retry once.
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

        Err(ProxyError::InvalidConfig("No valid proxy available".into()))
    }

    /// Gets the best proxy across all providers.
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

        // If every proxy is rate limited, call self.get_ip_proxy_from_provider on the
        // highest-weighted IpProvider.

        let mut providers: Vec<_> = {
            let providers = self.ip_providers.lock().await;
            providers
                .iter()
                .map(|(name, provider)| (name.clone(), provider.get_weight()))
                .collect()
        };
        providers.sort_by_key(|x| std::cmp::Reverse(x.1)); // Sort by weight, descending
        if let Some((provider_name, _)) = providers.first() {
            // Compute the global shortest remaining window and wait once.
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
            // Check once more whether the rate limit has lifted (needs a mutable borrow).
            let mut_idx = 0usize;
            if !proxy_items[mut_idx].is_rate_limited() {
                proxy_items[mut_idx].rate_limit_tracker.record_request();
                Ok(proxy_items[mut_idx].proxy.clone())
            } else {
                Err(ProxyError::InvalidConfig("No valid proxy available".into()))
            }
        } else {
            Err(ProxyError::InvalidConfig("No valid proxy available".into()))
        }
    }

    /// Reports the outcome of using a proxy.
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

    /// Reports the outcome of using a tunnel proxy.
    async fn report_tunnel_result(
        &self,
        tunnel: &Tunnel,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        let mut found = false;
        {
            let mut pools = self.pools.write().await;
            for pool in pools.values_mut() {
                for item in pool.iter_mut() {
                    if let ProxyEnum::Tunnel(ref t) = item.proxy
                        && t.endpoint == tunnel.endpoint
                    {
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
        } // End the write-lock scope early
        if !found {
            return Err(ProxyError::InvalidConfig(
                format!("Tunnel {} not found", tunnel.endpoint).into(),
            ));
        }
        // Update statistics.
        self.update_stats().await;
        Ok(())
    }

    /// Reports the outcome of using an IP proxy.
    async fn report_ip_proxy_result(
        &self,
        proxy: &IpProxy,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        let mut proxy_found = false;
        {
            // Scan every provider's pool looking for the proxy.
            let mut pools = self.pools.write().await;
            for pool in pools.values_mut() {
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

            // Purge invalid proxies from every pool.
            for pool in pools.values_mut() {
                pool.retain(|item| item.is_valid(self.config.max_errors));
            }
        } // End the write-lock scope early
        // Return an error if the proxy was not found.
        if !proxy_found {
            return Err(ProxyError::InvalidConfig(
                format!(
                    "Proxy {}:{} not found in any provider",
                    proxy.ip, proxy.port
                )
                .into(),
            ));
        }
        // Update statistics.
        self.update_stats().await;
        Ok(())
    }

    /// Reports a successful proxy use.
    pub async fn report_success(
        &self,
        proxy: &ProxyEnum,
        response_time: Option<Duration>,
    ) -> Result<()> {
        self.report_proxy_result(proxy, true, response_time).await
    }

    /// Reports a failed proxy use.
    pub async fn report_failure(&self, proxy: &ProxyEnum) -> Result<()> {
        self.report_proxy_result(proxy, false, None).await
    }

    /// Ensures the pool meets the required size; how much to refill is decided by the
    /// corresponding struct.
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

    /// When every proxy IP is over its limit, fetches a fresh batch and adds it; existing
    /// proxies are kept.
    async fn refill_pool(&self, provider_name: &str) -> Result<()> {
        // Clone the Arc pointer before awaiting.
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
                    return Err(ProxyError::ProxyProviderExpired);
                }
            } else {
                return Err(ProxyError::InvalidConfig(
                    format!(
                        "Provider {} expire time is not available",
                        provider.get_config().name
                    )
                    .into(),
                ));
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

    /// Updates the statistics.
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

    /// Gets the pool status.
    pub async fn get_pool_status(&self) -> HashMap<String, usize> {
        let pools = self.pools.read().await;
        pools
            .iter()
            .map(|(name, pool)| (name.clone(), pool.len()))
            .collect()
    }

    /// Gets detailed statistics.
    pub async fn get_stats(&self) -> PoolStats {
        self.stats.read().await.clone()
    }

    /// Runs a health check.
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
                        // Run the health check.
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
