//! High-level `Spider` facade.
//!
//! A simple entry point covering 80% of use cases: implement a [`Spider`], get it running in three
//! steps with [`Mocra::builder`], and receive typed data through [`DataSink`] / [`on_item`] — no
//! need to implement `DataStoreMiddleware`.
//!
//! The existing [`ModuleTrait`] + DAG remains the advanced path;
//! a `Spider` is adapted into a single-node module and compiled into the existing engine.
//!
//! # Runtime
//! - `run()` uses an in-memory, database-free configuration unless `from_toml` is provided.
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

/// Legacy metadata key for follow-up tasks queued before full requests were preserved.
const FOLLOW_URL_KEY: &str = "__mocra_spider_follow_url";
/// Metadata key used to carry the full follow-up request through the parser task queue.
const FOLLOW_REQUEST_KEY: &str = "__mocra_spider_follow_request";

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
        self.add(Request::new(url, RequestMethod::Get))
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
        self.follow(Request::new(url, RequestMethod::Get));
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
        if let Some(value) = params.get(FOLLOW_REQUEST_KEY) {
            let req = serde_json::from_value::<Request>(value.clone()).map_err(|error| {
                Error::new(
                    ErrorKind::Service,
                    Some(format!("invalid Spider follow-up request: {error}")),
                )
            })?;
            return Ok(Box::pin(futures::stream::iter(vec![req])));
        }
        // Accept URL-only tasks that were queued by older versions.
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
        for mut req in cx.follows {
            if let Some(meta) = req.meta.task.as_object_mut() {
                meta.remove(FOLLOW_URL_KEY);
                meta.remove(FOLLOW_REQUEST_KEY);
            }
            let priority = req.priority;
            let request_value = serde_json::to_value(req).map_err(|error| {
                Error::new(
                    ErrorKind::Service,
                    Some(format!(
                        "failed to serialize Spider follow-up request: {error}"
                    )),
                )
            })?;
            let mut task = TaskParserEvent::from(&response);
            // TaskParserEvent inherits the previous request's task metadata. Remove the old
            // follow-up payload before adding the next one to avoid retaining a request chain.
            task.metadata.remove(FOLLOW_URL_KEY);
            task.metadata.remove(FOLLOW_REQUEST_KEY);
            task.metadata
                .insert(FOLLOW_REQUEST_KEY.to_string(), request_value);
            task.account_task.priority = priority;
            let task = task.stay_current_step();
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
    /// An initialized data directory is restored directly without using the seeds.
    pub seeds: Vec<String>,
    /// Optional Raft timing tuning (the defaults suit a LAN; enlarge for WAN / high latency).
    pub raft_tuning: Option<crate::cluster::RaftTuning>,
}

#[cfg(feature = "cluster-embedded")]
impl ClusterConfig {
    /// **Bootstrap** the first core node of a new cluster (`seeds` is empty), or restore its
    /// initialized data directory on restart.
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
    pub fn with_raft_tuning(mut self, tuning: crate::cluster::RaftTuning) -> Self {
        self.raft_tuning = Some(tuning);
        self
    }
}

/// Start the embedded redb+Raft control plane and return its
/// [`CoordinationBackend`](crate::sync::CoordinationBackend).
///
/// Restore existing Raft state; otherwise bootstrap when `seeds` is empty, or try the seeds in
/// order to join as a learner.
#[cfg(feature = "cluster-embedded")]
async fn start_embedded_cluster(
    cluster: &ClusterConfig,
) -> Result<Arc<dyn crate::sync::CoordinationBackend>> {
    use crate::cluster::RaftControlPlane;
    use crate::sync::RaftCoordinationBackend;
    let svc_err =
        |e: crate::cluster::ControlError| Error::new(ErrorKind::Service, Some(e.to_string()));

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

    let startup = async {
        if cp.raft().is_initialized().await.map_err(|e| {
            Error::new(
                ErrorKind::Service,
                Some(format!("read Raft initialization state: {e}")),
            )
        })? {
            cp.wait_ready(std::time::Duration::from_secs(10))
                .await
                .map_err(svc_err)?;
            log::info!(
                "cluster: restored node {} from {}",
                cluster.node_id,
                cluster.data_dir
            );
            return Ok(());
        }
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
            let mut errors = Vec::new();
            for seed in &cluster.seeds {
                match RaftControlPlane::join_cluster(seed, cluster.node_id, &cluster.http_addr)
                    .await
                {
                    Ok(()) => {
                        let node_id = cluster.node_id;
                        cp.raft()
                            .wait(Some(std::time::Duration::from_secs(10)))
                            .metrics(
                                move |m| {
                                    m.membership_config
                                        .membership()
                                        .get_node(&node_id)
                                        .is_some()
                                        && m.current_leader.is_some()
                                },
                                "joined node receives membership",
                            )
                            .await
                            .map_err(|e| Error::new(ErrorKind::Service, Some(e.to_string())))?;
                        log::info!("cluster: node {} joined via seed {seed}", cluster.node_id);
                        return Ok(());
                    }
                    Err(error) => errors.push(error.to_string()),
                }
            }
            return Err(Error::new(
                ErrorKind::Service,
                Some(format!(
                    "cluster join failed for every seed: {}",
                    errors.join("; ")
                )),
            ));
        }
        Ok(())
    }
    .await;
    if let Err(error) = startup {
        // A failed join must release its listener and storage before callers can retry startup.
        if let Err(shutdown_error) = cp.shutdown().await {
            log::warn!("cluster: failed startup cleanup: {shutdown_error}");
        }
        return Err(error);
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
    /// Without this method, the builder uses the single-node, database-free defaults.
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
    /// enabling it programmatically. When combined with `from_toml`, it overrides only the port,
    /// preserving the configured API key and rate limit.
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
    /// - `.cluster(...)` provided → start the embedded Raft control plane and
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

        let started = async {
            let state =
                State::try_new_with_provider_and_coordination(provider, coordination.clone())
                    .await
                    .map_err(|e| Error::new(ErrorKind::Service, Some(e.to_string())))?;

            let state = Arc::new(state);
            // dashboard: when enabled programmatically, write the port into config.api so the engine
            // brings up the admin HTTP API from it on start.
            #[cfg(feature = "dashboard")]
            if let Some(port) = self.dashboard_port {
                let mut cfg = state.config.write().await;
                cfg.api
                    .get_or_insert(crate::common::model::config::Api {
                        port,
                        api_key: None,
                        rate_limit: None,
                    })
                    .port = port;
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

            engine.start().await
        }
        .await;

        // Gracefully shut the cluster control plane down after the engine stops (releasing the redb
        // handle and the Raft background tasks), including State/Engine initialization errors.
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
            proxy_client_cache_capacity: None,
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

#[cfg(all(test, any(feature = "cluster-embedded", feature = "dashboard")))]
const TEST_CONFIG_TOML: &str = r#"
name = "facade_regression"
[db]
[download_config]
downloader_expire = 3600
timeout = 10
rate_limit = 0.0
enable_session = false
enable_locker = false
enable_rate_limit = false
cache_ttl = 60
wss_timeout = 30
[cache]
ttl = 60
[crawler]
request_max_retries = 0
task_max_errors = 5
module_max_errors = 10
module_locker_ttl = 5
idle_stop_secs = 0
[channel_config]
minid_time = 0
capacity = 16
"#;

#[cfg(all(test, feature = "cluster-embedded"))]
mod cluster_config_tests {
    use super::*;

    fn unused_addr() -> String {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string()
    }

    async fn assert_failed_initialization_releases_cluster(invalid_queue: bool) {
        let dir = std::env::temp_dir().join(format!("mocra-init-error-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        if invalid_queue {
            std::fs::write(
                &path,
                format!(
                    "{}\n[api]\nport = 0\napi_key = \"regression-key\"\nrate_limit = 1.0\n",
                    TEST_CONFIG_TOML.replace("capacity = 16", "capacity = 0")
                ),
            )
            .unwrap();
        }
        let expected = if invalid_queue {
            "queue capacity and batch concurrency must be positive"
        } else {
            "load config failed"
        };
        let config = ClusterConfig::bootstrap(1, unused_addr(), dir.join("raft").to_string_lossy());
        for _ in 0..2 {
            let error = Mocra::builder()
                .cluster(config.clone())
                .from_toml(path.to_str().unwrap())
                .run()
                .await
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
        let recovered = start_embedded_cluster(&config).await.unwrap();
        recovered.set("after-error", b"ready").await.unwrap();
        recovered.shutdown().await;
        drop(recovered);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn failed_state_initialization_releases_cluster() {
        assert_failed_initialization_releases_cluster(false).await;
    }

    #[tokio::test]
    async fn failed_engine_initialization_releases_cluster() {
        assert_failed_initialization_releases_cluster(true).await;
    }

    async fn assert_normal_stop_releases_cluster(api_port: Option<u16>) {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = std::env::temp_dir().join(format!("mocra-stop-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let mut toml = TEST_CONFIG_TOML.replace("idle_stop_secs = 0", "idle_stop_secs = 1");
        if let Some(port) = api_port {
            toml += &format!("\n[api]\nport = {port}\napi_key = \"regression-key\"\n");
        }
        std::fs::write(&path, toml).unwrap();
        let config = ClusterConfig::bootstrap(1, unused_addr(), dir.join("raft").to_string_lossy());
        let first_config = config.clone();
        let first_path = path.clone();
        let first = tokio::spawn(async move {
            Mocra::builder()
                .cluster(first_config)
                .from_toml(first_path.to_str().unwrap())
                .run()
                .await
        });
        let mut sockets = Vec::new();
        if let Some(port) = api_port {
            let address = format!("127.0.0.1:{port}");
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(response) = reqwest::get(format!("http://{address}/health")).await
                        && response.status().is_success()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            for request in [
                "POST /start_work HTTP/1.1\r\nHost: localhost\r\n",
                "POST /start_work HTTP/1.1\r\nHost: localhost\r\nx-api-key: regression-key\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n",
            ] {
                let mut socket = tokio::net::TcpStream::connect(&address).await.unwrap();
                socket.write_all(request.as_bytes()).await.unwrap();
                sockets.push(socket);
            }
        }
        tokio::time::timeout(Duration::from_secs(8), first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(port) = api_port {
            let rebound = std::net::TcpListener::bind(("127.0.0.1", port)).unwrap();
            drop(rebound);
            for mut socket in sockets {
                let mut received = Vec::new();
                let _ =
                    tokio::time::timeout(Duration::from_secs(1), socket.read_to_end(&mut received))
                        .await
                        .expect("stopped API must close accepted connections");
            }
        }
        tokio::time::timeout(
            Duration::from_secs(8),
            Mocra::builder()
                .cluster(config)
                .from_toml(path.to_str().unwrap())
                .run(),
        )
        .await
        .unwrap()
        .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normal_stop_releases_cluster_without_api() {
        assert_normal_stop_releases_cluster(None).await;
    }

    #[cfg(feature = "dashboard")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normal_stop_releases_api_connections_and_cluster() {
        let port = unused_addr().rsplit(':').next().unwrap().parse().unwrap();
        assert_normal_stop_releases_cluster(Some(port)).await;
    }

    #[cfg(feature = "dashboard")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn explicit_shutdown_finishes_before_start_returns() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;

        let dir =
            std::env::temp_dir().join(format!("mocra-explicit-stop-{}", uuid::Uuid::now_v7()));
        let cluster = ClusterConfig::bootstrap(1, unused_addr(), dir.to_string_lossy());
        let coordination = start_embedded_cluster(&cluster).await.unwrap();
        let port = unused_addr().rsplit(':').next().unwrap().parse().unwrap();
        let mut config = default_standalone_config("explicit_stop");
        config.crawler.idle_stop_secs = None;
        config.api = Some(crate::common::model::config::Api {
            port,
            api_key: Some("regression-key".into()),
            rate_limit: None,
        });
        let state = State::try_new_with_provider_and_coordination(
            Box::new(StaticConfigProvider { config }),
            Some(coordination.clone()),
        )
        .await
        .unwrap();
        let engine = Arc::new(Engine::new(Arc::new(state), None).await.unwrap());
        let running_engine = engine.clone();
        let running = tokio::spawn(async move { running_engine.start().await });
        let client = reqwest::Client::new();
        let address = format!("127.0.0.1:{port}");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(response) = client
                    .get(format!("http://{address}/nodes"))
                    .header("x-api-key", "regression-key")
                    .send()
                    .await
                    && response.status().is_success()
                    && !response
                        .json::<Vec<serde_json::Value>>()
                        .await
                        .unwrap()
                        .is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let mut socket = tokio::net::TcpStream::connect(&address).await.unwrap();
        socket
            .write_all(b"POST /start_work HTTP/1.1\r\nHost: localhost\r\nx-api-key: regression-key\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n")
            .await
            .unwrap();
        let stopping_engine = engine.clone();
        let stopping = tokio::spawn(async move { stopping_engine.shutdown().await });
        tokio::time::timeout(Duration::from_secs(6), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(engine);
        coordination.shutdown().await;
        drop(coordination);
        // No extra await on the concurrent shutdown is needed before reopening storage.
        let restored = start_embedded_cluster(&cluster).await.unwrap();
        restored.shutdown().await;
        drop(restored);
        stopping.await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopping_active_spider_releases_cluster_storage() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;
        use tokio::sync::Notify;

        struct ActiveTask(Arc<AtomicBool>);

        impl Drop for ActiveTask {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }

        struct HeldSpider {
            entered: Arc<Notify>,
            active: Arc<AtomicBool>,
        }

        #[async_trait]
        impl Spider for HeldSpider {
            type Item = ();

            fn name(&self) -> &str {
                "active_stop"
            }

            async fn start(&self, _seeds: &mut Seeds) {
                self.active.store(true, Ordering::SeqCst);
                let _active = ActiveTask(self.active.clone());
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }

            async fn parse(&self, _response: Response, _cx: &mut Ctx<()>) -> Result<()> {
                Ok(())
            }
        }

        let dir = std::env::temp_dir().join(format!("mocra-active-stop-{}", uuid::Uuid::now_v7()));
        let cluster = ClusterConfig::bootstrap(1, unused_addr(), dir.to_string_lossy());
        let coordination = start_embedded_cluster(&cluster).await.unwrap();
        let mut config = default_standalone_config("active_stop");
        config.crawler.idle_stop_secs = None;
        let state = State::try_new_with_provider_and_coordination(
            Box::new(StaticConfigProvider { config }),
            Some(coordination.clone()),
        )
        .await
        .unwrap();
        let engine = Arc::new(Engine::new(Arc::new(state), None).await.unwrap());
        let entered = Arc::new(Notify::new());
        let active = Arc::new(AtomicBool::new(false));
        let builder = Mocra::builder().spider(
            HeldSpider {
                entered: entered.clone(),
                active: active.clone(),
            },
            on_item(|_: ()| async {}),
        );
        for module in builder.modules {
            engine.register_module(module).await;
        }
        engine
            .queue_manager
            .get_task_push_channel()
            .send(QueuedItem::new(TaskEvent {
                account: "default".into(),
                platform: "default".into(),
                module: Some(vec!["active_stop".into()]),
                priority: Default::default(),
                run_id: uuid::Uuid::now_v7(),
            }))
            .await
            .unwrap();
        let running_engine = engine.clone();
        let running = tokio::spawn(async move { running_engine.start().await });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("the spider must be active before shutdown");
        assert!(active.load(Ordering::SeqCst));

        tokio::time::timeout(Duration::from_secs(5), engine.shutdown())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            !active.load(Ordering::SeqCst),
            "shutdown must release active spider tasks before start returns"
        );
        assert_eq!(engine.inflight.total(), 0);
        drop(engine);
        coordination.shutdown().await;
        drop(coordination);

        let restored = start_embedded_cluster(&cluster).await.unwrap();
        restored.set("after-active-stop", b"ready").await.unwrap();
        restored.shutdown().await;
        drop(restored);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn config_watch_updates_live_limiters() {
        struct WatchedConfig(tokio::sync::watch::Receiver<Config>);
        #[async_trait]
        impl ConfigProvider for WatchedConfig {
            async fn load_config(&self) -> std::result::Result<Config, String> {
                Ok(self.0.borrow().clone())
            }
            async fn watch(
                &self,
            ) -> std::result::Result<tokio::sync::watch::Receiver<Config>, String> {
                Ok(self.0.clone())
            }
        }
        let mut config = default_standalone_config("config_watch_regression");
        config.api = Some(crate::common::model::config::Api {
            port: 0,
            api_key: Some("regression-key".into()),
            rate_limit: Some(1.0),
        });
        let (tx, rx) = tokio::sync::watch::channel(config.clone());
        let state = State::try_new_with_provider(Box::new(WatchedConfig(rx)))
            .await
            .unwrap();
        config.download_config.rate_limit = 2.0;
        config.api.as_mut().unwrap().rate_limit = Some(3.0);
        tx.send(config).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let download = state.limiter.get_key_config("test").await.unwrap();
                let api = state
                    .api_limiter
                    .as_ref()
                    .unwrap()
                    .get_key_config("test")
                    .await
                    .unwrap();
                if download.max_requests_per_second == 2.0 && api.max_requests_per_second == 3.0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn embedded_cluster_restore_has_a_leader_before_seed_decision() {
        use crate::cluster::{ControlPlane, RaftControlPlane};
        use std::collections::BTreeMap;
        use std::time::Duration;

        let dir = std::env::temp_dir().join(format!("mocra-quorum-{}", uuid::Uuid::now_v7()));
        let configs: Vec<_> = (1..=3)
            .map(|id| {
                ClusterConfig::bootstrap(
                    id,
                    unused_addr(),
                    dir.join(id.to_string()).to_string_lossy(),
                )
            })
            .collect();
        let mut nodes = Vec::new();
        for config in &configs {
            nodes.push(
                RaftControlPlane::start_cluster_node(
                    config.node_id,
                    &config.data_dir,
                    &config.http_addr,
                )
                .await
                .unwrap(),
            );
        }
        nodes[0]
            .init_cluster(BTreeMap::from([(1, configs[0].http_addr.clone())]))
            .await
            .unwrap();
        nodes[0].wait_leader(Duration::from_secs(5)).await.unwrap();
        for config in &configs[1..] {
            nodes[0]
                .add_learner(config.node_id, &config.http_addr)
                .await
                .unwrap();
        }
        nodes[0].change_membership([1, 2, 3].into()).await.unwrap();
        nodes[0].set(b"persist", b"value").await.unwrap();
        for node in &nodes {
            node.shutdown().await.unwrap();
        }
        drop(nodes);

        let isolated =
            RaftControlPlane::start_cluster_node(1, &configs[0].data_dir, &configs[0].http_addr)
                .await
                .unwrap();
        assert!(
            isolated
                .wait_ready(Duration::from_millis(250))
                .await
                .is_err(),
            "remembering its own leadership does not replace a live quorum"
        );
        isolated.shutdown().await.unwrap();
        drop(isolated);

        // The old leader stays offline. Both remaining voters remember it on restart.
        let (second, third) = tokio::join!(
            start_embedded_cluster(&configs[1]),
            start_embedded_cluster(&configs[2])
        );
        let second = second.unwrap();
        let third = third.unwrap();
        let seed_count = usize::from(second.is_leader()) + usize::from(third.is_leader());
        assert_eq!(
            seed_count, 1,
            "the facade must inject seeds on the restored leader"
        );
        let leader = if second.is_leader() { &second } else { &third };
        leader.set("after-restart", b"ready").await.unwrap();
        second.shutdown().await;
        third.shutdown().await;
        drop((second, third));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn embedded_cluster_recovers_on_the_same_address() {
        let dir = std::env::temp_dir().join(format!("mocra-recovery-{}", uuid::Uuid::now_v7()));
        let config = ClusterConfig::bootstrap(1, unused_addr(), dir.to_string_lossy());
        let backend = start_embedded_cluster(&config).await.unwrap();
        backend.set("persist", b"value").await.unwrap();
        backend.shutdown().await;
        drop(backend);

        let restored = start_embedded_cluster(&config).await.unwrap();
        assert_eq!(
            restored.get("persist").await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(restored.cluster_size(), 1);
        restored.shutdown().await;
        drop(restored);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn embedded_cluster_tries_seeds_and_restores_without_them() {
        let dir = std::env::temp_dir().join(format!("mocra-join-{}", uuid::Uuid::now_v7()));
        let boot = ClusterConfig::bootstrap(1, unused_addr(), dir.join("n1").to_string_lossy());
        let leader = start_embedded_cluster(&boot).await.unwrap();
        let missing_seed = unused_addr();
        let mut joined = ClusterConfig::join(
            2,
            unused_addr(),
            dir.join("n2").to_string_lossy(),
            &missing_seed,
        );
        joined.seeds.push(boot.http_addr.clone());
        let follower = start_embedded_cluster(&joined).await.unwrap();
        assert_eq!(leader.cluster_size(), 2);
        follower.shutdown().await;
        drop(follower);

        joined.seeds = vec![missing_seed.clone()];
        let restored = start_embedded_cluster(&joined).await.unwrap();
        restored.set("restored", b"yes").await.unwrap();
        assert_eq!(leader.get("restored").await.unwrap(), Some(b"yes".to_vec()));
        restored.shutdown().await;
        drop(restored);

        let bad = ClusterConfig::join(
            3,
            unused_addr(),
            dir.join("n3").to_string_lossy(),
            &missing_seed,
        );
        let error = match start_embedded_cluster(&bad).await {
            Ok(_) => panic!("joining an unreachable seed must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(&missing_seed));
        // Failed startup must also release the bound HTTP port and data directory.
        let mut retry = bad;
        retry.seeds = vec![boot.http_addr.clone()];
        let recovered = start_embedded_cluster(&retry).await.unwrap();
        recovered.shutdown().await;
        drop(recovered);
        leader.shutdown().await;
        drop(leader);
        std::fs::remove_dir_all(dir).unwrap();
    }

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

    #[tokio::test]
    async fn dashboard_preserves_configured_api_auth_and_rate_limit() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let path = std::env::temp_dir().join(format!("mocra-api-{}.toml", uuid::Uuid::now_v7()));
        std::fs::write(
            &path,
            format!("{TEST_CONFIG_TOML}\n[api]\nport = 0\napi_key = \"regression-key\"\nrate_limit = 1.0\n"),
        )
        .unwrap();
        let config_path = path.clone();
        let server = tokio::spawn(async move {
            Mocra::builder()
                .from_toml(config_path.to_str().unwrap())
                .dashboard(port)
                .run()
                .await
        });
        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();
        let ready = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(response) = client.get(format!("{base}/health")).send().await
                    && response.status().is_success()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await;
        let authorized = client
            .get(format!("{base}/nodes"))
            .header("x-api-key", "regression-key")
            .send()
            .await;
        let limited = client
            .get(format!("{base}/nodes"))
            .header("x-api-key", "regression-key")
            .send()
            .await;
        let unauthorized = client.get(format!("{base}/nodes")).send().await;
        server.abort();
        let _ = server.await;
        std::fs::remove_file(path).unwrap();
        ready.unwrap();
        assert_eq!(authorized.unwrap().status(), reqwest::StatusCode::OK);
        assert_eq!(
            limited.unwrap().status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            unauthorized.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
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
mod follow_request_tests {
    use super::*;
    use crate::common::model::meta::MetaData;
    use crate::engine::runner::StageCounter;
    use crate::proxy::{IpProxy, ProxyEnum};
    use async_trait::async_trait;
    use futures::StreamExt;
    use serde_json::json;

    struct FollowSpider;

    #[async_trait]
    impl Spider for FollowSpider {
        type Item = ();

        fn name(&self) -> &str {
            "follow_request_probe"
        }

        async fn start(&self, _seeds: &mut Seeds) {}

        async fn parse(&self, _response: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
            let mut request = Request::new("https://example.test/next", RequestMethod::Post)
                .with_body(b"payload".to_vec())
                .add_meta("cursor", 2);
            request.id = uuid::Uuid::from_u128(42);
            request.headers =
                crate::common::model::Headers::new().add("Authorization", "Bearer key");
            request.cookies.add("session", "cookie", "example.test");
            request.params = Some(vec![("page".into(), "2".into())]);
            request.timeout = 17;
            request.hash_str = Some("page-2".into());
            request.priority = crate::common::model::Priority::High;
            request.meta.task = json!({
                FOLLOW_REQUEST_KEY: "nested previous request",
                "user_context": "retained",
            });
            request.use_proxy(ProxyEnum::IpProxy(IpProxy {
                ip: "127.0.0.1".into(),
                port: 8080,
                username: Some("proxy-user".into()),
                password: Some("proxy-password".into()),
                proxy_type: Some("http".into()),
                rate_limit: 1.0,
            }));
            cx.follow(request);
            Ok(())
        }
    }

    fn node() -> SpiderNode<FollowSpider> {
        SpiderNode {
            spider: Arc::new(FollowSpider),
            sink: Arc::new(on_item(|_: ()| async {})),
            store_outcomes: Arc::new(StageCounter::default()),
        }
    }

    fn response() -> Response {
        Response {
            id: uuid::Uuid::now_v7(),
            platform: "platform".into(),
            account: "account".into(),
            module: "follow_request_probe".into(),
            status_code: 200,
            cookies: Default::default(),
            content: vec![],
            storage_path: None,
            headers: vec![],
            task_retry_times: 0,
            metadata: MetaData::default().add_task_config(json!({
                FOLLOW_REQUEST_KEY: "previous request",
                "cursor_context": "retained",
            })),
            download_middleware: vec![],
            data_middleware: vec![],
            task_finished: false,
            context: Default::default(),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
            request_hash: None,
            priority: Default::default(),
        }
    }

    #[tokio::test]
    async fn follow_preserves_request_through_parser_task_serialization() {
        let node = node();
        let output = node.parser(response(), None).await.unwrap();
        assert_eq!(output.parser_task.len(), 1);
        let task = &output.parser_task[0];
        assert!(task.context.stay_current_step);
        assert_eq!(
            task.account_task.priority,
            crate::common::model::Priority::High
        );
        assert_eq!(task.metadata["cursor_context"], "retained");
        assert!(task.metadata.get(FOLLOW_URL_KEY).is_none());
        assert_ne!(task.metadata[FOLLOW_REQUEST_KEY], "previous request");

        let bytes = serde_json::to_vec(task).unwrap();
        let task: TaskParserEvent = serde_json::from_slice(&bytes).unwrap();
        let mut requests = node
            .generate(Arc::new(ModuleConfig::default()), task.metadata, None)
            .await
            .unwrap();
        let request = requests.next().await.unwrap();
        assert!(requests.next().await.is_none());
        assert_eq!(request.id, uuid::Uuid::from_u128(42));
        assert_eq!(request.url, "https://example.test/next");
        assert_eq!(request.method, "POST");
        assert_eq!(request.body.as_deref(), Some(b"payload".as_slice()));
        assert_eq!(
            request.headers.get("Authorization").as_deref(),
            Some("Bearer key")
        );
        assert_eq!(request.cookies.cookies[0].value, "cookie");
        assert_eq!(request.params.as_ref().unwrap()[0].1, "2");
        assert_eq!(request.timeout, 17);
        assert_eq!(request.hash_str.as_deref(), Some("page-2"));
        assert_eq!(request.meta.get_trait_config::<i32>("cursor"), Some(2));
        assert_eq!(request.meta.task["user_context"], "retained");
        assert!(request.meta.task.get(FOLLOW_REQUEST_KEY).is_none());
        assert!(
            matches!(request.proxy, Some(ProxyEnum::IpProxy(proxy)) if proxy.port == 8080 && proxy.username.as_deref() == Some("proxy-user"))
        );
    }

    #[tokio::test]
    async fn legacy_url_only_follow_task_still_generates_get() {
        let mut params = Map::new();
        params.insert(FOLLOW_URL_KEY.into(), json!("https://example.test/old"));
        let mut requests = node()
            .generate(Arc::new(ModuleConfig::default()), params, None)
            .await
            .unwrap();
        let request = requests.next().await.unwrap();
        assert_eq!(request.url, "https://example.test/old");
        assert_eq!(request.method, "GET");
    }
}

#[cfg(test)]
mod downloader_tests {
    use super::{ChannelSink, Ctx, Mocra, Seeds, Spider};
    use async_trait::async_trait;
    use semver::Version;

    use crate::common::model::download_config::DownloadConfig;
    use crate::common::model::request::RequestMethod;
    use crate::common::model::{Cookies, Headers, Request, Response};
    use crate::downloader::Downloader;
    use crate::errors::Result;
    use crate::proxy::{IpProxy, ProxyEnum};

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

    #[derive(Clone)]
    struct CaptureDownloader {
        seen: tokio::sync::mpsc::UnboundedSender<Request>,
    }

    #[async_trait]
    impl Downloader for CaptureDownloader {
        async fn set_config(&self, _id: &str, _config: DownloadConfig) {}
        async fn set_limit(&self, _id: &str, _limit: f32) {}
        fn name(&self) -> String {
            "capture_downloader".into()
        }
        fn version(&self) -> Version {
            Version::new(1, 0, 0)
        }
        async fn download(&self, request: Request) -> Result<Response> {
            let _ = self.seen.send(request.clone());
            let mut response = FakeDownloader.download(request.clone()).await?;
            response.content = request.url.into_bytes();
            Ok(response)
        }
        async fn health_check(&self) -> Result<()> {
            Ok(())
        }
    }

    struct FollowProbeSpider;

    #[async_trait]
    impl Spider for FollowProbeSpider {
        type Item = ();
        fn name(&self) -> &str {
            "follow_downloader_probe"
        }
        async fn start(&self, seeds: &mut Seeds) {
            seeds.get("https://example.test/seed");
        }
        async fn parse(&self, response: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
            if response.text()?.ends_with("/seed") {
                let mut next = Request::new("https://example.test/next", RequestMethod::Post)
                    .with_body(b"follow body".to_vec());
                next.headers = Headers::new().add("X-Follow", "preserved");
                next.use_proxy(ProxyEnum::IpProxy(IpProxy {
                    ip: "127.0.0.1".into(),
                    port: 8080,
                    username: None,
                    password: None,
                    proxy_type: Some("http".into()),
                    rate_limit: 1.0,
                }));
                cx.follow(next);
            } else {
                cx.emit(());
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn follow_request_reaches_downloader_with_method_headers_and_body() {
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel::<Request>();
        let (item_tx, mut item_rx) = tokio::sync::mpsc::channel::<()>(1);
        let server = tokio::spawn(async move {
            let _ = Mocra::builder()
                .spider(FollowProbeSpider, ChannelSink::new(item_tx))
                .default_downloader(CaptureDownloader { seen: seen_tx })
                .run()
                .await;
        });

        let completed = tokio::time::timeout(std::time::Duration::from_secs(10), item_rx.recv())
            .await
            .ok()
            .flatten();
        server.abort();
        assert_eq!(completed, Some(()));

        let first = seen_rx.recv().await.unwrap();
        let follow = seen_rx.recv().await.unwrap();
        assert_eq!(first.method, "GET");
        assert_eq!(follow.url, "https://example.test/next");
        assert_eq!(follow.method, "POST");
        assert_eq!(follow.body.as_deref(), Some(b"follow body".as_slice()));
        assert_eq!(follow.headers.get("X-Follow").as_deref(), Some("preserved"));
        assert!(matches!(follow.proxy, Some(ProxyEnum::IpProxy(proxy)) if proxy.port == 8080));
    }
}
