//! High-level `Spider` facade (refactor Phase 1).
//!
//! A simple entry point covering 80% of use cases: implement a [`Spider`], get it running in three
//! steps with [`Mocra::builder`], and receive typed data through [`DataSink`] / [`on_item`] — no
//! need to implement `DataStoreMiddleware`.
//!
//! The existing [`ModuleTrait`] + DAG remains the advanced path;
//! a `Spider` is adapted into a single-node module and compiled into the existing engine.
//!
//! # Current status (v0)
//! - `run()` currently reuses `State::try_new(path)` and still requires a config (including
//!   `db.url`). "DB-less L0" and "programmatic default config" land in refactor Phase 2.
//! - `Ctx::follow` feeds requests back into the same node's `generate` via `TaskParserEvent`
//!   metadata, which is how pagination / follow-up is implemented.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream};
use crate::common::model::login_info::LoginInfo;
use crate::common::model::message::{TaskOutputEvent, TaskParserEvent};
use crate::common::model::request::RequestMethod;
use crate::common::model::{ModuleConfig, Request, Response};
use crate::common::state::State;
use crate::engine::engine::Engine;
use crate::errors::{Error, ErrorKind, Result};

use crate::common::config::ConfigProvider;
use crate::common::config::file::FileConfigProvider;
use crate::common::model::config::{
    CacheConfig, ChannelConfig, Config, CrawlerConfig, DatabaseConfig, DownloadConfig,
};
use crate::common::model::message::TaskEvent;
use crate::queue::QueuedItem;
use tokio::sync::watch;

/// Metadata key used when `Ctx::follow` feeds a request URL back in (an internal convention).
const FOLLOW_URL_KEY: &str = "__mocra_spider_follow_url";

/// A unit of collection: defines what to fetch ([`start`](Spider::start)) and how to parse it
/// ([`parse`](Spider::parse)).
///
/// ```ignore
/// use mocra::prelude::*;
///
/// #[derive(serde::Serialize)]
/// struct Story { title: String }
///
/// struct HackerNews;
///
/// #[async_trait::async_trait]
/// impl Spider for HackerNews {
///     type Item = Story;
///     fn name(&self) -> &str { "hn" }
///     async fn start(&self, s: &mut Seeds) { s.get("https://news.ycombinator.com/"); }
///     async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
///         cx.emit(Story { title: res.text()?.len().to_string() });
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Spider: Send + Sync + 'static {
    /// The typed item this spider emits, delivered through a [`DataSink`].
    type Item: Send + 'static;

    /// Unique name (used for the queue topic, metric labels, and the deduplication namespace).
    fn name(&self) -> &str;

    /// Seed the initial requests.
    async fn start(&self, seeds: &mut Seeds);

    /// Parse a single response: `cx.emit` emits data, `cx.follow` queues follow-up requests.
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()>;

    /// Whether a login flow is required (defaults to `false`).
    fn should_login(&self) -> bool {
        false
    }

    /// Version number (defaults to `1`).
    fn version(&self) -> i32 {
        1
    }
}

/// Collector for seed requests, passed to [`Spider::start`].
#[derive(Default)]
pub struct Seeds {
    reqs: Vec<Request>,
}

impl Seeds {
    /// Convenience GET: queues a request and returns a mutable reference so you can go on to set
    /// headers / metadata.
    pub fn get(&mut self, url: impl AsRef<str>) -> &mut Request {
        self.reqs.push(Request::new(url, RequestMethod::Get));
        self.reqs.last_mut().expect("just pushed")
    }

    /// Queue a request with any method.
    pub fn add(&mut self, req: Request) -> &mut Request {
        self.reqs.push(req);
        self.reqs.last_mut().expect("just pushed")
    }

    fn into_vec(self) -> Vec<Request> {
        self.reqs
    }
}

/// Parse context, passed to [`Spider::parse`]: emit data and queue follow-up requests.
pub struct Ctx<Item> {
    items: Vec<Item>,
    follows: Vec<Request>,
}

impl<Item> Ctx<Item> {
    fn new() -> Self {
        Self {
            items: Vec::new(),
            follows: Vec::new(),
        }
    }

    /// Emit one typed item (delivered to the [`DataSink`]).
    pub fn emit(&mut self, item: Item) {
        self.items.push(item);
    }

    /// Queue a follow-up request (once downloaded it re-enters this spider's `parse`).
    pub fn follow(&mut self, req: Request) {
        self.follows.push(req);
    }

    /// Convenience GET version of [`follow`](Ctx::follow).
    pub fn follow_get(&mut self, url: impl AsRef<str>) {
        self.follows.push(Request::new(url, RequestMethod::Get));
    }
}

/// Data outlet: how typed items leave the system (callback / channel / MQ / custom).
///
/// Replaces the old route that required implementing `DataStoreMiddleware` and mounting a relation
/// in the database.
#[async_trait]
pub trait DataSink<Item>: Send + Sync {
    /// Deliver a single item.
    async fn write(&self, item: Item) -> Result<()>;
}

/// A closure-backed sink (constructed by [`on_item`]).
pub struct ClosureSink<Item, F> {
    f: F,
    _p: PhantomData<fn(Item)>,
}

#[async_trait]
impl<Item, Fut, F> DataSink<Item> for ClosureSink<Item, F>
where
    Item: Send + 'static,
    F: Fn(Item) -> Fut + Send + Sync,
    Fut: Future<Output = ()> + Send,
{
    async fn write(&self, item: Item) -> Result<()> {
        (self.f)(item).await;
        Ok(())
    }
}

/// Build a [`DataSink`] from an async closure: `on_item(|item| async move { ... })`.
pub fn on_item<Item, Fut, F>(f: F) -> ClosureSink<Item, F>
where
    Item: Send + 'static,
    F: Fn(Item) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    ClosureSink { f, _p: PhantomData }
}

/// A [`DataSink`] that pushes every item into a tokio channel.
///
/// Handy for handing collected data to a downstream task: `ChannelSink::new(tx)`, with the
/// consumer calling `rx.recv().await`.
pub struct ChannelSink<Item> {
    tx: tokio::sync::mpsc::Sender<Item>,
}

impl<Item> ChannelSink<Item> {
    /// Construct from an `mpsc::Sender`.
    pub fn new(tx: tokio::sync::mpsc::Sender<Item>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl<Item: Send + 'static> DataSink<Item> for ChannelSink<Item> {
    async fn write(&self, item: Item) -> Result<()> {
        self.tx.send(item).await.map_err(|_| {
            Error::new(
                ErrorKind::Service,
                Some("ChannelSink: receiver dropped".to_string()),
            )
        })?;
        Ok(())
    }
}

// ---- Spider → ModuleTrait / ModuleNodeTrait adapter ----

struct SpiderModule<S: Spider> {
    spider: Arc<S>,
    sink: Arc<dyn DataSink<S::Item>>,
    store_outcomes: Arc<crate::engine::runner::StageCounter>,
}

#[async_trait]
impl<S: Spider> ModuleTrait for SpiderModule<S> {
    fn name(&self) -> String {
        self.spider.name().to_string()
    }

    fn version(&self) -> i32 {
        self.spider.version()
    }

    fn should_login(&self) -> bool {
        self.spider.should_login()
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        panic!("SpiderModule requires a spider instance; use Mocra::builder().spider(..)");
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(SpiderNode {
            spider: self.spider.clone(),
            sink: self.sink.clone(),
            store_outcomes: self.store_outcomes.clone(),
        })]
    }
}

struct SpiderNode<S: Spider> {
    spider: Arc<S>,
    sink: Arc<dyn DataSink<S::Item>>,
    /// Store outcomes for the sink, so the dashboard's store stage reflects a `Spider`'s writes.
    ///
    /// A `Spider` delivers items through its [`DataSink`], never through `DataStoreMiddleware`, so
    /// it bypasses the engine's `DataStoreProcessor` entirely — the stage would read 0 forever
    /// without this. `MocraBuilder::run` hands the engine this same counter, so both routes add up.
    store_outcomes: Arc<crate::engine::runner::StageCounter>,
}

#[async_trait]
impl<S: Spider> ModuleNodeTrait for SpiderNode<S> {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        // Follow-up request: fed back in through metadata by the previous parse round's follow.
        if let Some(url) = params.get(FOLLOW_URL_KEY).and_then(|v| v.as_str()) {
            let req = Request::new(url, RequestMethod::Get);
            return Ok(Box::pin(futures::stream::iter(vec![req])));
        }
        // First round: seeding.
        let mut seeds = Seeds::default();
        self.spider.start(&mut seeds).await;
        Ok(Box::pin(futures::stream::iter(seeds.into_vec())))
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let mut cx = Ctx::new();
        self.spider.parse(response.clone(), &mut cx).await?;

        // Data → sink.
        for item in cx.items {
            match self.sink.write(item).await {
                Ok(()) => self.store_outcomes.record_success(),
                Err(e) => {
                    self.store_outcomes.record_failure();
                    return Err(e);
                }
            }
        }

        // Follow-up requests → fed back as parser tasks (re-entering this node's generate).
        //
        // `stay_current_step` is mandatory here: `SpiderNode` is a single node (both the entry
        // point and a leaf). Without the marker the task takes the "auto-route to successor"
        // branch, and a leaf node has no successor — so it would be silently dropped (see the leaf
        // branch in `module_dag_processor::execute_parse`), making `Ctx::follow` a no-op.
        let mut out = TaskOutputEvent::default();
        for req in cx.follows {
            let task = TaskParserEvent::from(&response)
                .add_meta(FOLLOW_URL_KEY, req.url)
                .stay_current_step();
            out = out.with_task(task);
        }
        Ok(out)
    }

    fn stable_node_key(&self) -> &'static str {
        "spider"
    }
}

// ---- Mocra builder ----

/// The high-level entry point. Use [`Mocra::builder`] to register spiders, configure, and run.
pub struct Mocra;

impl Mocra {
    /// Create a builder.
    pub fn builder() -> MocraBuilder {
        MocraBuilder::default()
    }
}

/// Embedded cluster configuration (requires the `cluster-embedded` feature).
#[cfg(feature = "cluster-embedded")]
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's unique id.
    pub node_id: u64,
    /// The address this node binds and advertises (e.g. `127.0.0.1:7001`).
    pub http_addr: String,
    /// Directory for the redb state machine + log.
    pub data_dir: String,
    /// Seed node addresses; empty = this node bootstraps a new cluster (the first core node),
    /// non-empty = join via the seeds.
    pub seeds: Vec<String>,
    /// Optional Raft timing tuning (the defaults suit a LAN; enlarge for WAN / high latency).
    pub raft_tuning: Option<mocra_cluster::RaftTuning>,
}

#[cfg(feature = "cluster-embedded")]
impl ClusterConfig {
    /// **Bootstrap** the first core node of a new cluster (`seeds` is empty).
    ///
    /// ```ignore
    /// Mocra::builder().spider(s, sink)
    ///     .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/node-1"))
    ///     .run().await?;
    /// ```
    pub fn bootstrap(
        node_id: u64,
        http_addr: impl Into<String>,
        data_dir: impl Into<String>,
    ) -> Self {
        Self {
            node_id,
            http_addr: http_addr.into(),
            data_dir: data_dir.into(),
            seeds: Vec::new(),
            raft_tuning: None,
        }
    }

    /// **Join** an existing cluster: enter the network through any known node's (seed) address, as
    /// a learner.
    ///
    /// ```ignore
    /// .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/node-2", "127.0.0.1:7001"))
    /// ```
    pub fn join(
        node_id: u64,
        http_addr: impl Into<String>,
        data_dir: impl Into<String>,
        seed: impl Into<String>,
    ) -> Self {
        Self {
            node_id,
            http_addr: http_addr.into(),
            data_dir: data_dir.into(),
            seeds: vec![seed.into()],
            raft_tuning: None,
        }
    }

    /// Read the cluster config from environment variables — convenient for **containerized
    /// deployments**: the same image across nodes, with only the env vars changing.
    ///
    /// - `MOCRA_NODE_ID` (required, u64)
    /// - `MOCRA_HTTP_ADDR` (required, this node's advertised address)
    /// - `MOCRA_DATA_DIR` (optional, defaults to `./mocra-data/node-{id}`)
    /// - `MOCRA_SEEDS` (optional, comma-separated seed addresses; empty = bootstrap)
    pub fn from_env() -> std::result::Result<Self, String> {
        Self::from_vars(
            std::env::var("MOCRA_NODE_ID").ok(),
            std::env::var("MOCRA_HTTP_ADDR").ok(),
            std::env::var("MOCRA_DATA_DIR").ok(),
            std::env::var("MOCRA_SEEDS").ok(),
        )
    }

    /// The pure parsing core of [`from_env`](Self::from_env) (unit-test friendly; reads no
    /// environment).
    fn from_vars(
        node_id: Option<String>,
        http_addr: Option<String>,
        data_dir: Option<String>,
        seeds: Option<String>,
    ) -> std::result::Result<Self, String> {
        let node_id = node_id
            .ok_or("MOCRA_NODE_ID not set")?
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("MOCRA_NODE_ID invalid: {e}"))?;
        let http_addr = http_addr
            .filter(|s| !s.trim().is_empty())
            .ok_or("MOCRA_HTTP_ADDR not set")?
            .trim()
            .to_string();
        let data_dir = data_dir
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("./mocra-data/node-{node_id}"));
        let seeds = seeds
            .map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(Self {
            node_id,
            http_addr,
            data_dir,
            seeds,
            raft_tuning: None,
        })
    }

    /// Customize the Raft timing (WAN / high-latency clusters); the defaults suit a LAN.
    pub fn with_raft_tuning(mut self, tuning: mocra_cluster::RaftTuning) -> Self {
        self.raft_tuning = Some(tuning);
        self
    }
}

/// Start the embedded redb+Raft control plane and return its
/// [`CoordinationBackend`](crate::sync::CoordinationBackend).
///
/// `seeds` empty → bootstrap a new cluster and wait for leader election; non-empty → join via the
/// first seed (as a learner).
#[cfg(feature = "cluster-embedded")]
async fn start_embedded_cluster(
    cluster: &ClusterConfig,
) -> Result<Arc<dyn crate::sync::CoordinationBackend>> {
    use crate::sync::RaftCoordinationBackend;
    use mocra_cluster::RaftControlPlane;
    let svc_err =
        |e: mocra_cluster::ControlError| Error::new(ErrorKind::Service, Some(e.to_string()));

    let cp = match cluster.raft_tuning.clone() {
        Some(tuning) => RaftControlPlane::start_cluster_node_with(
            cluster.node_id,
            &cluster.data_dir,
            cluster.http_addr.clone(),
            tuning,
        )
        .await
        .map_err(svc_err)?,
        None => RaftControlPlane::start_cluster_node(
            cluster.node_id,
            &cluster.data_dir,
            cluster.http_addr.clone(),
        )
        .await
        .map_err(svc_err)?,
    };

    if cluster.seeds.is_empty() {
        let mut init = std::collections::BTreeMap::new();
        init.insert(cluster.node_id, cluster.http_addr.clone());
        cp.init_cluster(init).await.map_err(svc_err)?;
        cp.wait_leader(std::time::Duration::from_secs(10))
            .await
            .map_err(svc_err)?;
        log::info!(
            "cluster: bootstrapped new Raft cluster as node {} @ {}",
            cluster.node_id,
            cluster.http_addr
        );
    } else {
        RaftControlPlane::join_cluster(&cluster.seeds[0], cluster.node_id, &cluster.http_addr)
            .await
            .map_err(svc_err)?;
        log::info!(
            "cluster: node {} joined via seed {}",
            cluster.node_id,
            cluster.seeds[0]
        );
    }
    Ok(Arc::new(RaftCoordinationBackend::new(cp)))
}

/// Builder for [`Mocra`].
#[derive(Default)]
pub struct MocraBuilder {
    config_path: Option<String>,
    modules: Vec<Arc<dyn ModuleTrait>>,
    /// Store outcomes for every spider's sink. Created here because a `SpiderModule` is built by
    /// [`spider`](MocraBuilder::spider), before `run` builds the engine; `run` then hands this same
    /// counter to the engine so the observability API reports the sink's writes.
    store_outcomes: Arc<crate::engine::runner::StageCounter>,
    /// User-registered named downloaders (routed by the `config.downloader` name).
    downloaders: Vec<Box<dyn crate::downloader::Downloader>>,
    /// Overrides the global default downloader (reqwest by default).
    default_downloader: Option<Box<dyn crate::downloader::Downloader>>,
    #[cfg(feature = "cluster-embedded")]
    cluster: Option<ClusterConfig>,
    #[cfg(feature = "dashboard")]
    dashboard_port: Option<u16>,
}

impl MocraBuilder {
    /// Load the config from a TOML file.
    ///
    /// Required in v0; programmatic default config and DB-less runs land in refactor Phase 2.
    pub fn from_toml(mut self, path: impl Into<String>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    /// Register a spider along with its data outlet.
    ///
    /// ```ignore
    /// Mocra::builder()
    ///     .from_toml("config.toml")
    ///     .spider(HackerNews, on_item(|s: Story| async move { println!("{s:?}"); }))
    ///     .run()
    ///     .await?;
    /// ```
    pub fn spider<S, K>(mut self, spider: S, sink: K) -> Self
    where
        S: Spider,
        K: DataSink<S::Item> + 'static,
    {
        let sink: Arc<dyn DataSink<S::Item>> = Arc::new(sink);
        let module: Arc<dyn ModuleTrait> = Arc::new(SpiderModule {
            spider: Arc::new(spider),
            sink,
            store_outcomes: self.store_outcomes.clone(),
        });
        self.modules.push(module);
        self
    }

    /// Register a custom downloader (implementing the
    /// [`Downloader`](crate::downloader::Downloader) trait).
    ///
    /// A module routes to it by setting `DownloadConfig.downloader` to that downloader's `name()`;
    /// anything unmatched still uses the default downloader. Multiple downloaders can be
    /// registered. Useful for picking a different download strategy per module.
    ///
    /// ```ignore
    /// Mocra::builder()
    ///     .spider(MySpider, on_item(|_| async {}))
    ///     .downloader(MyDownloader::new())   // used when config.downloader = "<its name()>"
    ///     .run().await?;
    /// ```
    pub fn downloader<D: crate::downloader::Downloader>(mut self, downloader: D) -> Self {
        self.downloaders.push(Box::new(downloader));
        self
    }

    /// Replace the **global default** downloader (reqwest by default). It handles any request
    /// whose `config.downloader` matches no registered downloader — useful for swapping out
    /// reqwest wholesale (e.g. browser rendering / proxy rotation / custom retry).
    pub fn default_downloader<D: crate::downloader::Downloader>(mut self, downloader: D) -> Self {
        self.default_downloader = Some(Box::new(downloader));
        self
    }

    /// Enable the embedded redb+Raft control plane (requires the `cluster-embedded` feature).
    ///
    /// - `seeds` empty → this node bootstraps a new cluster (the first core node, option A).
    /// - `seeds` non-empty → join via a seed node (as a learner).
    ///
    /// Once enabled, the engine's leader election / locks go through the Raft cluster — no
    /// external coordinator required.
    #[cfg(feature = "cluster-embedded")]
    pub fn cluster(mut self, cluster: ClusterConfig) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// Enable the admin / monitoring dashboard (requires the `dashboard` feature).
    ///
    /// The engine exposes an admin + observability HTTP API on `port` for the admin page to
    /// consume:
    /// - `GET /metrics` (Prometheus metrics), `GET /health`
    /// - `GET /observability/cluster` (cluster status), `GET /observability/engine` (queue/engine)
    /// - `GET /observability/system` (host CPU/memory/swap), `GET /observability/logs?limit=N`
    ///   (recent logs)
    /// - `GET /nodes`, `GET /dlq`, `POST /control/pause|resume`, `POST /start_work`
    ///
    /// It also serves a built-in single-file frontend page at `GET /`: open that port in a browser
    /// and you get the metrics / logs / tasks / performance panels (same-origin, so they point at
    /// this engine automatically — no endpoint to fill in by hand). The read-only observability
    /// endpoints (`/`, `/metrics`, `/health`, `/observability/*`) need no API key and have CORS
    /// enabled, so a standalone frontend can consume them cross-origin; write endpoints still
    /// require authentication.
    ///
    /// In single-node mode this additionally switches the engine into "service mode": idle stop is
    /// disabled (otherwise it would exit after 30s idle and the dashboard would go dark), and log
    /// collection is enabled by default when logging was not configured explicitly, so the "logs"
    /// panel has data out of the box.
    ///
    /// It can also be enabled through the `[api]` section of `from_toml`; this method is for
    /// enabling it programmatically (without TOML).
    #[cfg(feature = "dashboard")]
    pub fn dashboard(mut self, port: u16) -> Self {
        self.dashboard_port = Some(port);
        self
    }

    /// Build the State + Engine, register every spider, and start the engine (blocks until
    /// shutdown).
    ///
    /// - [`from_toml`](MocraBuilder::from_toml) provided → use that config (may include a DB).
    /// - Not provided → **DB-less / single-node** default config, with one seed task injected
    ///   automatically per spider.
    /// - [`cluster`](MocraBuilder::cluster) provided → start the embedded Raft control plane and
    ///   route coordination through Raft.
    pub async fn run(self) -> Result<()> {
        let standalone = self.config_path.is_none();
        let provider: Box<dyn ConfigProvider> = match &self.config_path {
            Some(path) => Box::new(FileConfigProvider::new(path)),
            None => Box::new(StaticConfigProvider {
                config: default_standalone_config("mocra"),
            }),
        };

        // Start the cluster before State: once the control plane is ready we hand the coordination
        // backend to State, so distributed locks / leader election go through Raft from
        // construction time (rather than falling back to in-process locks).
        let coordination: Option<Arc<dyn crate::sync::CoordinationBackend>> = {
            #[cfg(feature = "cluster-embedded")]
            {
                match &self.cluster {
                    Some(cluster) => Some(start_embedded_cluster(cluster).await?),
                    None => None,
                }
            }
            #[cfg(not(feature = "cluster-embedded"))]
            {
                None
            }
        };

        let state = State::try_new_with_provider_and_coordination(provider, coordination)
            .await
            .map_err(|e| Error::new(ErrorKind::Service, Some(e.to_string())))?;

        let state = Arc::new(state);
        // dashboard: when enabled programmatically, write the port into config.api so the engine
        // brings up the admin HTTP API from it on start.
        #[cfg(feature = "dashboard")]
        if let Some(port) = self.dashboard_port {
            let mut cfg = state.config.write().await;
            cfg.api = Some(crate::common::model::config::Api {
                port,
                api_key: None,
                rate_limit: None,
            });
            // A dashboard implies "service mode": single-node defaults to stopping after 30s idle,
            // which would take the monitoring panel down with the process. Enabling the dashboard
            // programmatically therefore disables single-node idle stop so the observability
            // endpoints stay up (explicit from_toml config is left untouched).
            if standalone {
                cfg.crawler.idle_stop_secs = None;
                // When logging was not configured explicitly, enable log collection by default so
                // the dashboard's "logs" panel has data out of the box: LogSinkLayer feeds
                // log/tracing events into an in-memory ring buffer for `/observability/logs`.
                if cfg.logger.is_none() {
                    use crate::common::model::logger_config::{LogOutputConfig, LoggerConfig};
                    cfg.logger = Some(LoggerConfig {
                        enabled: Some(true),
                        level: Some("info".to_string()),
                        format: None,
                        include: None,
                        buffer: None,
                        flush_interval_ms: None,
                        outputs: vec![LogOutputConfig::Console],
                        prometheus: None,
                    });
                }
            }
            drop(cfg);
            log::info!("dashboard: admin/observability API on http://127.0.0.1:{port}");
        }
        // Capture the coordination backend handle before state moves into the engine: used for the
        // leader gate that keeps seeding to exactly once, and for graceful shutdown after the
        // engine stops.
        let coordination = state.coordination.clone();
        // Seed exactly once in a cluster: only the leader injects, which avoids every node
        // injecting the same seeds → N× duplicate crawling.
        let should_seed = coordination.as_ref().map(|c| c.is_leader()).unwrap_or(true);
        let mut engine = Engine::new(state, None).await?;
        // Point the engine's store stage at the counter the spiders' sinks already write to, so
        // both persistence routes (DataSink and DataStoreMiddleware) add up to one number. Must
        // happen before `start`, which clones this into the parser chain.
        engine.outcomes.store = self.store_outcomes.clone();

        // User-registered downloaders / default downloader override (wired up before the engine
        // starts; the chains and the manager share the same Arc).
        for downloader in self.downloaders {
            engine.downloader_manager.register(downloader).await;
        }
        if let Some(default_downloader) = self.default_downloader {
            engine
                .downloader_manager
                .set_default_downloader(default_downloader)
                .await;
        }

        let spider_names: Vec<String> = self.modules.iter().map(|m| m.name()).collect();
        for module in self.modules {
            engine.register_module(module).await;
        }

        // Single-node / cluster leader: inject one seed task per spider (otherwise the engine idles
        // with nothing to do). Cluster followers skip this to avoid N× duplicate injection. How
        // seeds spread across nodes depends on the data-plane MQ: with a distributed MQ
        // (Kafka/NATS) they fan out to every node; with the default in-memory queue they stay on
        // the leader.
        if standalone && should_seed {
            let task_tx = engine.queue_manager.get_task_push_channel();
            for name in spider_names {
                let ev = TaskEvent {
                    account: "default".to_string(),
                    platform: "default".to_string(),
                    module: Some(vec![name]),
                    priority: Default::default(),
                    run_id: uuid::Uuid::now_v7(),
                };
                if let Err(e) = task_tx.send(QueuedItem::new(ev)).await {
                    log::warn!("failed to inject seed task: {e}");
                }
            }
        }

        let started = engine.start().await;

        // Gracefully shut the cluster control plane down after the engine stops (releasing the redb
        // handle and the Raft background tasks). This runs even when startup failed, so an error
        // such as an unbindable dashboard port does not strand the redb handle.
        if let Some(c) = &coordination {
            c.shutdown().await;
        }
        started
    }
}

/// Programmatic default config for DB-less / single-node runs (in-memory queue).
fn default_standalone_config(name: &str) -> Config {
    Config {
        name: name.to_string(),
        db: DatabaseConfig {
            url: None,
            database_schema: None,
            pool_size: None,
            tls: None,
        },
        download_config: DownloadConfig {
            downloader_expire: 3600,
            timeout: 30,
            rate_limit: 0.0,
            enable_session: false,
            enable_locker: false,
            enable_rate_limit: false,
            cache_ttl: 60,
            wss_timeout: 30,
            pool_size: Some(100),
            max_response_size: Some(10 * 1024 * 1024),
        },
        cache: CacheConfig {
            ttl: 60,
            compression_threshold: None,
            enable_l1: None,
            l1_ttl_secs: None,
            l1_max_entries: None,
        },
        crawler: CrawlerConfig {
            request_max_retries: 2,
            task_max_errors: 100,
            module_max_errors: 50,
            module_locker_ttl: 5,
            node_id: None,
            task_concurrency: Some(32),
            publish_concurrency: None,
            parser_concurrency: None,
            error_task_concurrency: None,
            backpressure_retry_delay_ms: None,
            dedup_ttl_secs: None,
            // Single-node: stop automatically once the queue has been idle for 30s (a finite crawl
            // exits when done; spiders that keep producing tasks are unaffected).
            idle_stop_secs: Some(30),
        },
        scheduler: None,
        sync: None,
        channel_config: ChannelConfig {
            blob_storage: None,
            kafka: None,
            nats: None,
            minid_time: 12,
            capacity: 10000,
            queue_codec: None,
            batch_concurrency: None,
            compression_threshold: None,
            nack_max_retries: None,
            nack_backoff_ms: None,
        },
        proxy: None,
        api: None,
        event_bus: None,
        logger: None,
        policy: None,
    }
}

/// Supplies a fixed [`Config`] to [`State`](crate::common::state::State) (file-less,
/// programmatic).
struct StaticConfigProvider {
    config: Config,
}

#[async_trait]
impl ConfigProvider for StaticConfigProvider {
    async fn load_config(&self) -> std::result::Result<Config, String> {
        Ok(self.config.clone())
    }

    async fn watch(&self) -> std::result::Result<watch::Receiver<Config>, String> {
        // Static config: no change watching (the sender is dropped immediately, so State's watch
        // loop exits right away).
        let (_tx, rx) = watch::channel(self.config.clone());
        Ok(rx)
    }
}

#[cfg(all(test, feature = "cluster-embedded"))]
mod cluster_config_tests {
    use super::ClusterConfig;

    #[test]
    fn bootstrap_has_no_seeds_join_has_one() {
        let boot = ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./d1");
        assert_eq!(boot.node_id, 1);
        assert_eq!(boot.http_addr, "127.0.0.1:7001");
        assert!(
            boot.seeds.is_empty(),
            "a bootstrap node has no seeds (it self-bootstraps)"
        );

        let joined = ClusterConfig::join(2, "127.0.0.1:7002", "./d2", "127.0.0.1:7001");
        assert_eq!(joined.node_id, 2);
        assert_eq!(joined.seeds, vec!["127.0.0.1:7001".to_string()]);
    }

    #[test]
    fn from_vars_parses_env_style_config() {
        // Full case: multiple comma-separated seeds (join).
        let c = ClusterConfig::from_vars(
            Some("3".into()),
            Some("127.0.0.1:7003".into()),
            Some("/data/n3".into()),
            Some("127.0.0.1:7001, 127.0.0.1:7002".into()),
        )
        .unwrap();
        assert_eq!(c.node_id, 3);
        assert_eq!(c.http_addr, "127.0.0.1:7003");
        assert_eq!(c.data_dir, "/data/n3");
        assert_eq!(
            c.seeds,
            vec!["127.0.0.1:7001".to_string(), "127.0.0.1:7002".to_string()]
        );

        // No seeds + omitted data_dir → bootstrap + default directory.
        let boot =
            ClusterConfig::from_vars(Some("1".into()), Some("127.0.0.1:7001".into()), None, None)
                .unwrap();
        assert!(boot.seeds.is_empty());
        assert_eq!(boot.data_dir, "./mocra-data/node-1");

        // Missing required vars / invalid id → error (not a panic).
        assert!(ClusterConfig::from_vars(None, Some("a".into()), None, None).is_err());
        assert!(ClusterConfig::from_vars(Some("x".into()), Some("a".into()), None, None).is_err());
        assert!(ClusterConfig::from_vars(Some("1".into()), None, None, None).is_err());
    }
}

#[cfg(all(test, feature = "dashboard"))]
mod dashboard_tests {
    use super::*;

    struct Noop;

    #[async_trait]
    impl Spider for Noop {
        type Item = ();
        fn name(&self) -> &str {
            "noop"
        }
        async fn start(&self, _s: &mut Seeds) {}
        async fn parse(&self, _r: Response, _c: &mut Ctx<Self::Item>) -> Result<()> {
            Ok(())
        }
    }

    /// End-to-end: `.dashboard(port)` really does serve the observability HTTP API on that port.
    #[tokio::test]
    async fn dashboard_serves_observability_endpoints() {
        let port = 17673u16;
        let server = tokio::spawn(async move {
            let _ = Mocra::builder()
                .spider(Noop, on_item(|_: ()| async {}))
                .dashboard(port)
                .run()
                .await;
        });

        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();

        // Poll /health until the server binds.
        let mut ready = false;
        for _ in 0..60 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(r) = client.get(format!("{base}/health")).send().await
                && r.status().is_success()
            {
                ready = true;
                break;
            }
        }
        assert!(ready, "dashboard /health did not come up");

        // /observability/engine → engine/queue snapshot.
        let engine: serde_json::Value = client
            .get(format!("{base}/observability/engine"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(engine["single_node"], serde_json::json!(true));
        assert_eq!(engine["namespace"], serde_json::json!("mocra"));
        assert!(engine["pending"]["total"].is_number());

        // /observability/cluster → null (no cluster coordination backend).
        let cluster: serde_json::Value = client
            .get(format!("{base}/observability/cluster"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(cluster.is_null());

        // /observability/system → 200 + JSON (snapshot is null or an object); also checks the CORS
        // header.
        let sys = client
            .get(format!("{base}/observability/system"))
            .header("Origin", "http://localhost:3000")
            .send()
            .await
            .unwrap();
        assert!(sys.status().is_success());
        assert!(
            sys.headers().contains_key("access-control-allow-origin"),
            "CORS header missing — cross-origin frontend requests would be blocked"
        );
        let _sys_json: serde_json::Value = sys.json().await.unwrap();

        // /observability/logs → 200 + a JSON array.
        let logs: serde_json::Value = client
            .get(format!("{base}/observability/logs?limit=50"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(logs.is_array(), "the logs endpoint should return an array");

        // GET / → the built-in single-file dashboard page (HTML).
        let page = client.get(format!("{base}/")).send().await.unwrap();
        assert!(page.status().is_success());
        let html = page.text().await.unwrap();
        assert!(
            html.contains("mocra dashboard"),
            "the root path should serve the built-in dashboard page"
        );

        server.abort();
    }
}

#[cfg(test)]
mod downloader_tests {
    use super::{ChannelSink, Ctx, Mocra, Seeds, Spider};
    use async_trait::async_trait;
    use semver::Version;

    use crate::common::model::download_config::DownloadConfig;
    use crate::common::model::{Cookies, Request, Response};
    use crate::downloader::Downloader;
    use crate::errors::Result;

    /// A custom downloader: ignores the network and returns a fixed response — used to prove the
    /// pipeline really calls it (rather than reqwest).
    #[derive(Clone)]
    struct FakeDownloader;

    #[async_trait]
    impl Downloader for FakeDownloader {
        async fn set_config(&self, _id: &str, _config: DownloadConfig) {}
        async fn set_limit(&self, _id: &str, _limit: f32) {}
        fn name(&self) -> String {
            "fake_downloader".to_string()
        }
        fn version(&self) -> Version {
            Version::new(1, 0, 0)
        }
        async fn download(&self, request: Request) -> Result<Response> {
            Ok(Response {
                id: request.id,
                platform: request.platform,
                account: request.account,
                module: request.module,
                status_code: 299,
                cookies: Cookies { cookies: vec![] },
                content: b"FAKE_DOWNLOADER_BODY".to_vec(),
                storage_path: None,
                headers: vec![],
                task_retry_times: request.task_retry_times,
                metadata: request.meta,
                download_middleware: request.download_middleware,
                data_middleware: request.data_middleware,
                task_finished: request.task_finished,
                context: request.context,
                run_id: request.run_id,
                prefix_request: request.prefix_request,
                request_hash: None,
                priority: request.priority,
            })
        }
        async fn health_check(&self) -> Result<()> {
            Ok(())
        }
    }

    struct ProbeSpider;

    #[async_trait]
    impl Spider for ProbeSpider {
        type Item = u16;
        fn name(&self) -> &str {
            "downloader_probe"
        }
        async fn start(&self, s: &mut Seeds) {
            // Deliberately unresolvable domain: real reqwest would fail on it, so only the fake
            // downloader can return the fixed body.
            s.get("https://this-host-does-not-resolve.invalid/x");
        }
        async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
            if res.text_lossy().contains("FAKE_DOWNLOADER_BODY") {
                cx.emit(res.status_code);
            }
            Ok(())
        }
    }

    /// End-to-end: the custom downloader injected via `Mocra::builder().default_downloader(..)` is
    /// really used by the pipeline.
    #[tokio::test]
    async fn custom_default_downloader_serves_requests() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u16>(4);
        let server = tokio::spawn(async move {
            let _ = Mocra::builder()
                .spider(ProbeSpider, ChannelSink::new(tx))
                .default_downloader(FakeDownloader)
                .run()
                .await;
        });

        let got = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv()).await;
        server.abort();

        assert_eq!(
            got.ok().flatten(),
            Some(299),
            "the custom default downloader should have served this request (status 299 + fake \
             body); None means reqwest was still used"
        );
    }
}
