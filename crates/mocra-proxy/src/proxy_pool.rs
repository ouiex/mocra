use crate::error::ProxyError;
use crate::error::Result;
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, PartialEq};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
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
        // Fractional rates mean one request per multiple seconds.
        let window = if rate_limit < 1.0 {
            Duration::from_secs_f64((1.0 / f64::from(rate_limit)).min(365.0 * 24.0 * 3600.0))
        } else {
            Duration::from_secs(1)
        };
        self.window_duration = window;
        // If the window has elapsed, treat as not limited.
        if self.window_start.elapsed() >= self.window_duration {
            self.requests_in_window = 0;
            self.window_start = Instant::now();
            return false;
        }
        let cap = rate_limit.floor().max(1.0) as u32;
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
            expire_time: self
                .expire_time
                .as_ref()
                .map(|value| {
                    OffsetDateTime::parse(value, &Rfc3339)
                        .map(|date| Duration::from_secs(date.unix_timestamp().max(0) as u64))
                        .map_err(|error| {
                            ProxyError::InvalidConfig(
                                format!("invalid direct proxy expiry '{value}': {error}").into(),
                            )
                        })
                })
                .transpose()?,
        })
    }
}

#[derive(Debug, Clone)]
struct StaticIpProxyEntry {
    provider_name: String,
    proxy: IpProxy,
    rate_limit: f32,
    expire_time: Option<Duration>,
}

impl StaticIpProxyEntry {
    fn into_proxy_item(self) -> ProxyItem {
        let mut item =
            ProxyItem::new_for_static_ip_proxy(self.proxy, self.provider_name, self.rate_limit);
        if let Some(expire_time) = self.expire_time {
            item.expire_time = expire_time;
        }
        item
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
    /// Maximum number of dynamically loaded IP proxies per provider.
    pub max_size: usize,
    pub max_errors: u32,
    pub health_check_interval_secs: u64,
    /// Maximum simultaneous provider health probes (default 8, capped at 64).
    #[serde(default = "default_health_check_concurrency")]
    pub health_check_concurrency: usize,
    pub refill_threshold: f32, // Refill is triggered when the pool falls below this ratio
}

fn default_health_check_concurrency() -> usize {
    8
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_size: 5,
            max_size: 50,
            max_errors: 3,
            health_check_interval_secs: 300,
            health_check_concurrency: default_health_check_concurrency(),
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

#[derive(Debug, Clone, Copy)]
pub struct ProxySelectionWaitStats {
    pub count: u64,
    pub total_wait_ns: u64,
    pub max_wait_ns: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct ProxyAttemptStats {
    pub started: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub rate_limited: u64,
}

pub struct ProxyPool {
    pub config: PoolConfig,
    pub pools: Arc<RwLock<HashMap<String, Vec<ProxyItem>>>>,
    pub ip_providers: Arc<Mutex<IpProvidersMap>>,
    refill_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    health_check_lock: Mutex<()>,
    /// Last diagnostics snapshot. Use `get_stats()` for a current snapshot.
    pub stats: Arc<RwLock<PoolStats>>,
    selection_wait_count: AtomicU64,
    selection_wait_total_ns: AtomicU64,
    selection_wait_max_ns: AtomicU64,
    attempts_started: AtomicU64,
    attempts_succeeded: AtomicU64,
    attempts_failed: AtomicU64,
    attempts_rate_limited: AtomicU64,
}

impl ProxyPool {
    pub fn new(config: PoolConfig) -> Self {
        let mut config = config;
        if config.max_size == 0 || config.min_size > config.max_size {
            log::warn!(
                "invalid proxy pool sizes: min_size={}, max_size={}; clamping to a valid range",
                config.min_size,
                config.max_size
            );
        }
        config.max_size = config.max_size.max(1);
        config.min_size = config.min_size.min(config.max_size);
        Self {
            config,
            pools: Arc::new(RwLock::new(HashMap::new())),
            ip_providers: Arc::new(Mutex::new(HashMap::new())),
            refill_locks: Arc::new(Mutex::new(HashMap::new())),
            health_check_lock: Mutex::new(()),
            stats: Arc::new(RwLock::new(PoolStats {
                total_proxies: 0,
                valid_proxies: 0,
                error_proxies: 0,
                expired_proxies: 0,
                avg_success_rate: 0.0,
                providers: HashMap::new(),
            })),
            selection_wait_count: AtomicU64::new(0),
            selection_wait_total_ns: AtomicU64::new(0),
            selection_wait_max_ns: AtomicU64::new(0),
            attempts_started: AtomicU64::new(0),
            attempts_succeeded: AtomicU64::new(0),
            attempts_failed: AtomicU64::new(0),
            attempts_rate_limited: AtomicU64::new(0),
        }
    }

    /// Time spent waiting for the pool write lock during proxy selection.
    pub fn selection_wait_stats(&self) -> ProxySelectionWaitStats {
        ProxySelectionWaitStats {
            count: self.selection_wait_count.load(AtomicOrdering::Relaxed),
            total_wait_ns: self.selection_wait_total_ns.load(AtomicOrdering::Relaxed),
            max_wait_ns: self.selection_wait_max_ns.load(AtomicOrdering::Relaxed),
        }
    }

    /// Cumulative attempt accounting for this pool instance.
    pub fn attempt_stats(&self) -> ProxyAttemptStats {
        ProxyAttemptStats {
            started: self.attempts_started.load(AtomicOrdering::Relaxed),
            succeeded: self.attempts_succeeded.load(AtomicOrdering::Relaxed),
            failed: self.attempts_failed.load(AtomicOrdering::Relaxed),
            rate_limited: self.attempts_rate_limited.load(AtomicOrdering::Relaxed),
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
        self.get_proxy_inner(provider_name, true, None).await
    }

    /// Selects a candidate for a download attempt without charging its rate window.
    /// `begin_proxy_attempt` charges the window when the downloader actually starts.
    pub async fn get_proxy_for_attempt(&self, excluded: Option<&ProxyEnum>) -> Result<ProxyEnum> {
        self.get_proxy_inner(None, false, excluded).await
    }

    async fn get_proxy_inner(
        &self,
        provider_name: Option<&str>,
        reserve: bool,
        excluded: Option<&ProxyEnum>,
    ) -> Result<ProxyEnum> {
        if let Some(name) = provider_name {
            return self
                .get_ip_proxy_from_provider(name, reserve, excluded)
                .await;
        }
        // First try to get the best tunnel proxy.
        if let Some(tunnel) = self.get_best_tunnel_inner(reserve, excluded).await {
            return Ok(tunnel);
        }

        // Fall back to IP proxies if no tunnel proxy is available.
        self.get_best_ip_proxy(reserve, excluded).await
    }

    /// Gets the highest-quality tunnel proxy.
    pub async fn get_best_tunnel(&self) -> Option<ProxyEnum> {
        self.get_best_tunnel_inner(true, None).await
    }

    async fn get_best_tunnel_inner(
        &self,
        reserve: bool,
        excluded: Option<&ProxyEnum>,
    ) -> Option<ProxyEnum> {
        let (selected, wait) = self.select_available(None, true, reserve, excluded).await;
        if selected.is_some() {
            return selected;
        }
        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
            return self.select_available(None, true, reserve, excluded).await.0;
        }
        None
    }

    /// Gets a proxy from a specific provider.
    async fn get_ip_proxy_from_provider(
        &self,
        provider_name: &str,
        reserve: bool,
        excluded: Option<&ProxyEnum>,
    ) -> Result<ProxyEnum> {
        self.ensure_pool_size(provider_name).await?;
        let (selected, _) = self
            .select_available(Some(provider_name), false, reserve, excluded)
            .await;
        if let Some(proxy) = selected {
            return Ok(proxy);
        }

        // Fetch only when there is room. A provider fetch and the subsequent pool update
        // share a per-provider lock; other providers and feedback remain unblocked.
        let observed_size = self
            .pools
            .read()
            .await
            .get(provider_name)
            .map_or(0, Vec::len);
        if let Err(error) = self.refill_pool(provider_name, true, observed_size).await {
            log::warn!("proxy refill failed for {provider_name}: {error}");
        }
        let (selected, wait) = self
            .select_available(Some(provider_name), false, reserve, excluded)
            .await;
        if let Some(proxy) = selected {
            return Ok(proxy);
        }
        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
            if let Some(proxy) = self
                .select_available(Some(provider_name), false, reserve, excluded)
                .await
                .0
            {
                return Ok(proxy);
            }
        }
        Err(ProxyError::ProxyNotFound)
    }

    /// Gets the best proxy across all providers.
    async fn get_best_ip_proxy(
        &self,
        reserve: bool,
        excluded: Option<&ProxyEnum>,
    ) -> Result<ProxyEnum> {
        let (selected, wait) = self.select_available(None, false, reserve, excluded).await;
        if let Some(proxy) = selected {
            return Ok(proxy);
        }
        let mut providers: Vec<_> = {
            let providers = self.ip_providers.lock().await;
            providers
                .iter()
                .map(|(name, provider)| (name.clone(), provider.get_weight()))
                .collect()
        };
        providers.sort_by_key(|x| std::cmp::Reverse(x.1)); // Sort by weight, descending
        if let Some((provider_name, _)) = providers.first() {
            self.get_ip_proxy_from_provider(provider_name, reserve, excluded)
                .await
        } else {
            if let Some(wait) = wait {
                tokio::time::sleep(wait).await;
                if let Some(proxy) = self
                    .select_available(None, false, reserve, excluded)
                    .await
                    .0
                {
                    return Ok(proxy);
                }
            }
            Err(ProxyError::ProxyNotFound)
        }
    }

    /// Select a proxy under one short critical section, optionally reserving its rate slot.
    async fn select_available(
        &self,
        provider_name: Option<&str>,
        tunnel: bool,
        reserve: bool,
        excluded: Option<&ProxyEnum>,
    ) -> (Option<ProxyEnum>, Option<Duration>) {
        let lock_started = Instant::now();
        let mut pools = self.pools.write().await;
        let wait_ns = lock_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.selection_wait_count
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.selection_wait_total_ns
            .fetch_add(wait_ns, AtomicOrdering::Relaxed);
        self.selection_wait_max_ns
            .fetch_max(wait_ns, AtomicOrdering::Relaxed);
        // Gather compact indices, sample two distinct eligible proxies, then
        // score only those two. This avoids random draws and quality scoring
        // for every proxy while holding the pool lock.
        let mut rng = rand::rng();
        let mut candidates = Vec::new();
        let mut wait: Option<Duration> = None;
        for (provider_index, (name, pool)) in pools.iter_mut().enumerate() {
            if provider_name.is_some_and(|requested| requested != name) {
                continue;
            }
            pool.retain(|item| item.is_valid(self.config.max_errors));
            for (index, item) in pool.iter_mut().enumerate() {
                if matches!(item.proxy, ProxyEnum::Tunnel(_)) != tunnel {
                    continue;
                }
                if excluded.is_some_and(|excluded| item.proxy == *excluded) {
                    continue;
                }
                if item.is_rate_limited() {
                    let remaining = item.rate_limit_tracker.remaining_in_window();
                    wait = Some(wait.map_or(remaining, |current| current.min(remaining)));
                    continue;
                }
                candidates.push((provider_index, index));
            }
        }
        let selected = match candidates.len() {
            0 => None,
            1 => Some(candidates[0]),
            count => {
                let first = rng.random_range(0..count);
                let mut second = rng.random_range(0..count - 1);
                if second >= first {
                    second += 1;
                }
                let first = candidates[first];
                let second = candidates[second];
                let score = |(provider, index): (usize, usize)| {
                    pools
                        .values()
                        .nth(provider)
                        .expect("candidate provider exists")[index]
                        .quality_score()
                };
                let first_score = score(first);
                let second_score = score(second);
                if first_score > second_score
                    || (first_score == second_score && rng.random_bool(0.5))
                {
                    Some(first)
                } else {
                    Some(second)
                }
            }
        };
        if let Some((provider, index)) = selected {
            let item = &mut pools
                .values_mut()
                .nth(provider)
                .expect("selected provider exists")[index];
            if reserve {
                item.rate_limit_tracker.record_request();
            }
            return (Some(item.proxy.clone()), None);
        }
        (None, wait)
    }

    /// Charges a managed proxy's rate window immediately before a download attempt.
    pub async fn begin_proxy_attempt(&self, proxy: &ProxyEnum) -> Result<()> {
        for pass in 0..2 {
            let wait = {
                let mut pools = self.pools.write().await;
                let item = pools
                    .values_mut()
                    .flat_map(|pool| pool.iter_mut())
                    .find(|item| item.proxy == *proxy && item.is_valid(self.config.max_errors))
                    .ok_or(ProxyError::ProxyNotFound)?;
                if !item.is_rate_limited() {
                    item.rate_limit_tracker.record_request();
                    self.attempts_started.fetch_add(1, AtomicOrdering::Relaxed);
                    return Ok(());
                }
                self.attempts_rate_limited
                    .fetch_add(1, AtomicOrdering::Relaxed);
                item.rate_limit_tracker.remaining_in_window()
            };
            if pass == 0 {
                tokio::time::sleep(wait).await;
            }
        }
        Err(ProxyError::ProxyNotFound)
    }

    /// Whether this provider explicitly treats an HTTP status as a proxy failure.
    pub async fn is_retry_code(&self, proxy: &ProxyEnum, code: u16) -> bool {
        let provider_name = {
            let pools = self.pools.read().await;
            pools
                .values()
                .flat_map(|pool| pool.iter())
                .find(|item| item.proxy == *proxy)
                .map(|item| item.provider_name.clone())
        };
        let Some(provider_name) = provider_name else {
            return false;
        };
        let providers = self.ip_providers.lock().await;
        providers
            .get(&provider_name)
            .is_some_and(|provider| provider.is_retry_code(&code))
    }

    /// Reports the outcome of using a proxy.
    pub async fn report_proxy_result(
        &self,
        proxy: &ProxyEnum,
        success: bool,
        response_time: Option<Duration>,
    ) -> Result<()> {
        let result = match proxy {
            ProxyEnum::Tunnel(tunnel) => {
                self.report_tunnel_result(tunnel, success, response_time)
                    .await
            }
            ProxyEnum::IpProxy(ip_proxy) => {
                self.report_ip_proxy_result(ip_proxy, success, response_time)
                    .await
            }
        };
        if result.is_ok() {
            if success {
                self.attempts_succeeded
                    .fetch_add(1, AtomicOrdering::Relaxed);
            } else {
                self.attempts_failed.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
        result
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
            'providers: for pool in pools.values_mut() {
                for item in pool.iter_mut() {
                    if item.proxy.eq(tunnel) {
                        if success {
                            item.record_success(
                                response_time.unwrap_or(Duration::from_millis(1000)),
                            );
                        } else {
                            item.record_error();
                        }
                        found = true;
                        break 'providers;
                    }
                }
            }
        } // End the write-lock scope early
        if !found {
            return Err(ProxyError::InvalidConfig(
                format!("Tunnel {} not found", tunnel.endpoint).into(),
            ));
        }
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
            // Find the used proxy, then prune only its own pool. A full-pool
            // retain here would make every download feedback O(all proxies).
            let mut pools = self.pools.write().await;
            for pool in pools.values_mut() {
                let Some(index) = pool.iter().position(|item| item.proxy.eq(proxy)) else {
                    continue;
                };
                let item = &mut pool[index];
                if success {
                    item.record_success(response_time.unwrap_or(Duration::from_millis(1000)));
                } else {
                    item.record_error();
                }
                if !item.is_valid(self.config.max_errors) {
                    pool.swap_remove(index);
                }
                proxy_found = true;
                break;
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
            pools
                .get(provider_name)
                .map(|p| {
                    p.iter()
                        .filter(|item| item.is_valid(self.config.max_errors))
                        .count()
                })
                .unwrap_or(0)
        };

        let threshold = (self.config.max_size as f32 * self.config.refill_threshold) as usize;

        if (current_size < self.config.min_size || current_size < threshold)
            && let Err(error) = self.refill_pool(provider_name, false, current_size).await
        {
            if current_size == 0 {
                return Err(error);
            }
            log::warn!("proxy refill failed for {provider_name}: {error}");
        }

        Ok(())
    }

    /// When every proxy IP is over its limit, fetches a fresh batch and adds it; existing
    /// proxies are kept.
    async fn refill_pool(
        &self,
        provider_name: &str,
        force: bool,
        observed_size: usize,
    ) -> Result<()> {
        let refill_lock = {
            let mut locks = self.refill_locks.lock().await;
            locks
                .entry(provider_name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _refill_guard = refill_lock.lock().await;
        // A concurrent caller may have filled the pool while we waited for this provider.
        let current_size = {
            let mut pools = self.pools.write().await;
            let pool = pools.get_mut(provider_name).ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?;
            pool.retain(|item| item.is_valid(self.config.max_errors));
            pool.len()
        };
        let threshold = (self.config.max_size as f32 * self.config.refill_threshold) as usize;
        if current_size > observed_size
            || current_size >= self.config.max_size
            || (!force && current_size >= self.config.min_size && current_size >= threshold)
        {
            return Ok(());
        }
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
            let pool = pools.get_mut(provider_name).ok_or_else(|| {
                ProxyError::InvalidConfig(format!("Provider {provider_name} not found").into())
            })?;
            pool.retain(|item| item.is_valid(self.config.max_errors));
            for proxy in new_proxies {
                if pool.len() >= self.config.max_size {
                    break;
                }
                let candidate = ProxyItem::new_for_ip_proxy(proxy, provider.get_config());
                if pool.iter().any(|item| item.proxy == candidate.proxy) {
                    continue;
                }
                pool.push(candidate);
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
        // Feedback changes one proxy at a time. Rebuild only for a diagnostics read,
        // so request throughput does not pay for a full-pool scan on every attempt.
        self.update_stats().await;
        self.stats.read().await.clone()
    }

    /// Runs a health check.
    pub async fn health_check(&self) -> Result<()> {
        // Manual and scheduled checks share one budget; overlapping runs would
        // otherwise multiply the configured number of concurrent probes.
        let _check_guard = self.health_check_lock.lock().await;
        let providers = self.ip_providers.lock().await.clone();
        let mut pending = VecDeque::new();
        {
            let pools = self.pools.read().await;
            for (name, provider) in providers {
                if let Some(pool) = pools.get(&name) {
                    for item in pool {
                        if let ProxyEnum::IpProxy(proxy) = &item.proxy {
                            pending.push_back((
                                name.clone(),
                                provider.clone(),
                                proxy.clone(),
                                item.expire_time,
                            ));
                        }
                    }
                }
            }
        }

        let concurrency = self.config.health_check_concurrency.clamp(1, 64);
        let mut running = tokio::task::JoinSet::new();
        let mut unhealthy = Vec::new();
        loop {
            while running.len() < concurrency {
                let Some((name, provider, proxy, expiry)) = pending.pop_front() else {
                    break;
                };
                running.spawn(async move {
                    let timeout = Duration::from_secs(provider.get_config().timeout.max(1));
                    let healthy = tokio::time::timeout(timeout, provider.health_check(&proxy))
                        .await
                        .unwrap_or(false);
                    (!healthy).then_some((name, ProxyEnum::IpProxy(proxy), expiry))
                });
            }
            match running.join_next().await {
                Some(Ok(Some(failed))) => unhealthy.push(failed),
                Some(Ok(None)) => {}
                Some(Err(error)) => log::warn!("proxy health probe failed: {error}"),
                None => break,
            }
        }

        if !unhealthy.is_empty() {
            let mut pools = self.pools.write().await;
            for (name, proxy, expiry) in unhealthy {
                if let Some(pool) = pools.get_mut(&name) {
                    pool.retain(|item| item.proxy != proxy || item.expire_time != expiry);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn fractional_rate_uses_multi_second_window() {
        let mut tracker = RateLimitTracker::new();
        tracker.record_request();
        assert!(tracker.is_rate_limited(0.5));
        assert!(tracker.remaining_in_window() > Duration::from_secs(1));
    }

    struct FakeLoader {
        config: IpProvider,
        calls: Arc<AtomicUsize>,
    }

    struct SlowHealthLoader {
        config: IpProvider,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    struct PeakHealthLoader {
        config: IpProvider,
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl IpProxyLoader for PeakHealthLoader {
        async fn get_ip_proxies(&self) -> Result<Vec<IpProxy>> {
            Ok(Vec::new())
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
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            true
        }
    }

    #[async_trait]
    impl IpProxyLoader for SlowHealthLoader {
        async fn get_ip_proxies(&self) -> Result<Vec<IpProxy>> {
            Ok(Vec::new())
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
            self.entered.notify_one();
            self.release.notified().await;
            true
        }
    }

    #[async_trait]
    impl IpProxyLoader for FakeLoader {
        async fn get_ip_proxies(&self) -> Result<Vec<IpProxy>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(vec![IpProxy {
                ip: "127.0.0.1".into(),
                port: 8080,
                username: None,
                password: None,
                proxy_type: Some("http".into()),
                rate_limit: 0.0,
            }])
        }
        fn is_retry_code(&self, code: &u16) -> bool {
            self.config.retry_codes.contains(code)
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
            true
        }
    }

    #[tokio::test]
    async fn concurrent_empty_pool_fetches_once_without_deadlock() {
        let pool = Arc::new(ProxyPool::new(PoolConfig::default()));
        let calls = Arc::new(AtomicUsize::new(0));
        pool.add_ip_provider(Box::new(FakeLoader {
            config: IpProvider {
                name: "fake".into(),
                url: String::new(),
                retry_codes: vec![],
                timeout: 1,
                rate_limit: 0.0,
                provider_expire_time: None,
                proxy_expire_time: 60,
                weight: None,
            },
            calls: calls.clone(),
        }))
        .await;
        tokio::time::timeout(Duration::from_secs(1), async {
            let (a, b) = tokio::join!(pool.get_proxy(None), pool.get_proxy(None));
            assert!(a.is_ok());
            assert!(b.is_ok());
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(pool.get_pool_status().await["fake"], 1);
    }

    #[tokio::test]
    async fn rate_limit_wait_does_not_block_feedback() {
        let pool = Arc::new(ProxyPool::new(PoolConfig::default()));
        pool.add_tunnel(Tunnel {
            name: "tunnel".into(),
            endpoint: "localhost:8080".into(),
            username: None,
            password: None,
            tunnel_type: "http".into(),
            expire_time: "2999-01-01T00:00:00Z".into(),
            rate_limit: 1.0,
        })
        .await;
        let selected = pool.get_best_tunnel().await.unwrap();
        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.get_best_tunnel().await }
        });
        tokio::task::yield_now().await;
        tokio::time::timeout(
            Duration::from_millis(200),
            pool.report_success(&selected, None),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .unwrap()
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn direct_proxy_expiry_is_enforced() {
        let pool = ProxyPoolBuilder::new(PoolConfig::default())
            .with_direct_proxies(vec![DirectProxy {
                name: Some("old".into()),
                url: "http://127.0.0.1:8080".into(),
                rate_limit: None,
                expire_time: Some("2020-01-01T00:00:00Z".into()),
            }])
            .build()
            .await;
        assert!(pool.get_proxy(None).await.is_err());
    }

    #[tokio::test]
    async fn equally_scored_direct_proxies_share_selection() {
        let pool = ProxyPoolBuilder::new(PoolConfig::default())
            .with_direct_proxies(vec![
                DirectProxy {
                    name: Some("first".into()),
                    url: "http://127.0.0.1:8080".into(),
                    rate_limit: Some(10.0),
                    expire_time: None,
                },
                DirectProxy {
                    name: Some("second".into()),
                    url: "http://127.0.0.1:8081".into(),
                    rate_limit: Some(10.0),
                    expire_time: None,
                },
            ])
            .build()
            .await;
        let mut counts = HashMap::new();
        for _ in 0..256 {
            let proxy = pool.get_proxy_for_attempt(None).await.unwrap();
            *counts.entry(proxy.to_string()).or_insert(0usize) += 1;
        }
        assert_eq!(counts.len(), 2);
        assert!(counts.values().all(|count| *count > 70));
    }

    // Run separately before/after a selection-policy change:
    // cargo test -p mocra-proxy --lib selection_policy_workload -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "local proxy selection policy measurement"]
    async fn selection_policy_workload() {
        let proxies = (0..32)
            .map(|index| DirectProxy {
                name: Some(format!("p_{index}")),
                url: format!("http://127.0.0.1:{}", 8100 + index),
                rate_limit: Some(0.0),
                expire_time: None,
            })
            .collect();
        let pool = Arc::new(
            ProxyPoolBuilder::new(PoolConfig::default())
                .with_direct_proxies(proxies)
                .build()
                .await,
        );
        {
            let mut pools = pool.pools.write().await;
            for item in pools.values_mut().flatten() {
                let ProxyEnum::IpProxy(proxy) = &item.proxy else {
                    continue;
                };
                item.success_rate = match proxy.port - 8100 {
                    0..=7 => 0.99,
                    8..=23 => 0.85,
                    _ => 0.60,
                };
            }
        }
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                let mut samples = Vec::with_capacity(500);
                for _ in 0..500 {
                    let started = Instant::now();
                    let selected = pool.get_proxy_for_attempt(None).await.unwrap();
                    let ProxyEnum::IpProxy(proxy) = selected else {
                        panic!("expected direct IP proxy");
                    };
                    samples.push((started.elapsed().as_micros(), proxy.port - 8100));
                }
                samples
            }));
        }
        let mut latencies = Vec::new();
        let mut selections = [0usize; 32];
        for task in tasks {
            for (latency, index) in task.await.unwrap() {
                latencies.push(latency);
                selections[index as usize] += 1;
            }
        }
        latencies.sort_unstable();
        let high: usize = selections[..8].iter().sum();
        let medium: usize = selections[8..24].iter().sum();
        let low: usize = selections[24..].iter().sum();
        let expected_success =
            (high as f64 * 0.99 + medium as f64 * 0.85 + low as f64 * 0.60) / 4000.0;
        let relative_cost = (high * 3 + medium * 2 + low) as f64 / 4000.0;
        println!(
            "selection_workload wall_ms={} p95_us={} p99_us={} distinct={} max_share_pct={:.1} high={} medium={} low={} expected_success={:.3} relative_cost={:.3}",
            started.elapsed().as_millis(),
            latencies[latencies.len() * 95 / 100],
            latencies[latencies.len() * 99 / 100],
            selections.iter().filter(|count| **count > 0).count(),
            selections.iter().max().copied().unwrap_or(0) as f64 * 100.0 / 4000.0,
            high,
            medium,
            low,
            expected_success,
            relative_cost,
        );
    }

    #[tokio::test]
    async fn attempt_selection_only_charges_when_started_and_excludes_failure() {
        let pool = ProxyPoolBuilder::new(PoolConfig::default())
            .with_direct_proxies(vec![
                DirectProxy {
                    name: Some("a".into()),
                    url: "http://127.0.0.1:8080".into(),
                    rate_limit: Some(10.0),
                    expire_time: None,
                },
                DirectProxy {
                    name: Some("b".into()),
                    url: "http://127.0.0.1:8081".into(),
                    rate_limit: Some(10.0),
                    expire_time: None,
                },
            ])
            .build()
            .await;
        let first = pool.get_proxy_for_attempt(None).await.unwrap();
        assert_eq!(pool.attempt_stats().started, 0);
        {
            let pools = pool.pools.read().await;
            assert!(
                pools
                    .values()
                    .flatten()
                    .all(|item| item.rate_limit_tracker.current_window_count() == 0)
            );
        }
        pool.begin_proxy_attempt(&first).await.unwrap();
        let second = pool.get_proxy_for_attempt(Some(&first)).await.unwrap();
        assert_ne!(first.to_string(), second.to_string());
        pool.report_failure(&first).await.unwrap();
        let attempts = pool.attempt_stats();
        assert_eq!(attempts.started, 1);
        assert_eq!(attempts.failed, 1);
        assert_eq!(attempts.succeeded, 0);
        assert_eq!(attempts.rate_limited, 0);
        assert!(pool.selection_wait_stats().count >= 2);
        let pools = pool.pools.read().await;
        assert_eq!(
            pools
                .values()
                .flatten()
                .map(|item| item.rate_limit_tracker.current_window_count())
                .sum::<u32>(),
            1
        );
    }

    #[tokio::test]
    async fn provider_retry_codes_are_scoped_to_its_proxy() {
        let pool = ProxyPool::new(PoolConfig::default());
        pool.add_ip_provider(Box::new(FakeLoader {
            config: IpProvider {
                name: "retry_provider".into(),
                url: String::new(),
                retry_codes: vec![429],
                timeout: 1,
                rate_limit: 0.0,
                provider_expire_time: None,
                proxy_expire_time: 60,
                weight: None,
            },
            calls: Arc::new(AtomicUsize::new(0)),
        }))
        .await;
        let proxy = pool.get_proxy_for_attempt(None).await.unwrap();
        assert!(pool.is_retry_code(&proxy, 429).await);
        assert!(!pool.is_retry_code(&proxy, 500).await);
    }

    #[tokio::test]
    async fn refill_deduplicates_and_respects_provider_capacity() {
        let pool = ProxyPool::new(PoolConfig {
            min_size: 2,
            max_size: 2,
            ..PoolConfig::default()
        });
        let calls = Arc::new(AtomicUsize::new(0));
        pool.add_ip_provider(Box::new(FakeLoader {
            config: IpProvider {
                name: "bounded".into(),
                url: String::new(),
                retry_codes: vec![],
                timeout: 1,
                rate_limit: 0.0,
                provider_expire_time: None,
                proxy_expire_time: 60,
                weight: None,
            },
            calls,
        }))
        .await;
        for _ in 0..3 {
            assert!(pool.get_proxy(Some("bounded")).await.is_ok());
        }
        assert_eq!(pool.get_pool_status().await["bounded"], 1);
    }

    #[tokio::test]
    async fn health_check_preserves_concurrent_pool_updates() {
        let pool = Arc::new(ProxyPool::new(PoolConfig::default()));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let config = IpProvider {
            name: "health".into(),
            url: String::new(),
            retry_codes: vec![],
            timeout: 1,
            rate_limit: 0.0,
            provider_expire_time: None,
            proxy_expire_time: 60,
            weight: None,
        };
        pool.add_ip_provider(Box::new(SlowHealthLoader {
            config: config.clone(),
            entered: entered.clone(),
            release: release.clone(),
        }))
        .await;
        let proxy = |port| IpProxy {
            ip: "127.0.0.1".into(),
            port,
            username: None,
            password: None,
            proxy_type: Some("http".into()),
            rate_limit: 0.0,
        };
        pool.pools
            .write()
            .await
            .get_mut("health")
            .unwrap()
            .push(ProxyItem::new_for_ip_proxy(proxy(8080), &config));
        let checking = tokio::spawn({
            let pool = pool.clone();
            async move { pool.health_check().await }
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        pool.pools
            .write()
            .await
            .get_mut("health")
            .unwrap()
            .push(ProxyItem::new_for_ip_proxy(proxy(8081), &config));
        pool.report_success(&ProxyEnum::IpProxy(proxy(8080)), None)
            .await
            .unwrap();
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), checking)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let pools = pool.pools.read().await;
        let items = &pools["health"];
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].success_count, 1);
    }

    #[tokio::test]
    async fn health_checks_obey_concurrency_limit() {
        let pool = ProxyPool::new(PoolConfig {
            health_check_concurrency: 2,
            ..PoolConfig::default()
        });
        let config = IpProvider {
            name: "bounded_health".into(),
            url: String::new(),
            retry_codes: vec![],
            timeout: 1,
            rate_limit: 0.0,
            provider_expire_time: None,
            proxy_expire_time: 60,
            weight: None,
        };
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        pool.add_ip_provider(Box::new(PeakHealthLoader {
            config: config.clone(),
            active: active.clone(),
            peak: peak.clone(),
        }))
        .await;
        for port in 8000..8006 {
            pool.pools
                .write()
                .await
                .get_mut("bounded_health")
                .unwrap()
                .push(ProxyItem::new_for_ip_proxy(
                    IpProxy {
                        ip: "127.0.0.1".into(),
                        port,
                        username: None,
                        password: None,
                        proxy_type: Some("http".into()),
                        rate_limit: 0.0,
                    },
                    &config,
                ));
        }
        let (first, second) = tokio::join!(pool.health_check(), pool.health_check());
        first.unwrap();
        second.unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn statistics_snapshot_includes_additions_and_feedback() {
        let pool = ProxyPoolBuilder::new(PoolConfig::default())
            .with_direct_proxies(vec![
                DirectProxy {
                    name: Some("a".into()),
                    url: "http://127.0.0.1:8080".into(),
                    rate_limit: None,
                    expire_time: None,
                },
                DirectProxy {
                    name: Some("b".into()),
                    url: "http://127.0.0.1:8081".into(),
                    rate_limit: None,
                    expire_time: None,
                },
            ])
            .build()
            .await;
        assert_eq!(pool.get_stats().await.total_proxies, 2);
        let failed = pool.get_proxy_for_attempt(None).await.unwrap();
        pool.report_failure(&failed).await.unwrap();
        let snapshot = pool.get_stats().await;
        assert_eq!(snapshot.total_proxies, 2);
        assert_eq!(snapshot.avg_success_rate, 0.5);
    }
}
