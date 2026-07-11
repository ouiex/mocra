//! High-level `Spider` facade (重构 Phase 1).
//!
//! 面向 80% 场景的简单入口:实现一个 [`Spider`],用 [`Mocra::builder`] 三步跑起来,
//! 通过 [`DataSink`] / [`on_item`] 拿到类型化数据,无需实现 `DataStoreMiddleware`。
//!
//! 现有的 [`ModuleTrait`] + DAG 仍是进阶路径;
//! `Spider` 会被适配成一个单节点模块编译进现有引擎。
//!
//! 详见 `docs/refactor/02-target-api.md`。
//!
//! # 现状(v0)
//! - `run()` 目前复用 `State::try_new(path)`,仍需一个 config(含 `db.url`)。
//!   「无 DB 的 L0」与「程序化默认配置」在重构 Phase 2 落地(见 `docs/refactor/04-roadmap.md`)。
//! - `Ctx::follow` 通过 `TaskParserEvent` 元数据回灌同一节点的 `generate`,实现翻页/跟进。

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

/// `Ctx::follow` 回灌请求 URL 时使用的元数据键(内部约定)。
const FOLLOW_URL_KEY: &str = "__mocra_spider_follow_url";

/// 一个采集单元:定义抓什么([`start`](Spider::start))与怎么解析([`parse`](Spider::parse))。
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
    /// 类型化的产出项,经 [`DataSink`] 交付。
    type Item: Send + 'static;

    /// 唯一名称(用于队列 topic、指标标签、去重命名空间)。
    fn name(&self) -> &str;

    /// 播种初始请求。
    async fn start(&self, seeds: &mut Seeds);

    /// 解析单个响应:`cx.emit` 产出数据,`cx.follow` 追加后继请求。
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()>;

    /// 是否需要登录流程(默认 `false`)。
    fn should_login(&self) -> bool {
        false
    }

    /// 版本号(默认 `1`)。
    fn version(&self) -> i32 {
        1
    }
}

/// 种子请求收集器,传给 [`Spider::start`]。
#[derive(Default)]
pub struct Seeds {
    reqs: Vec<Request>,
}

impl Seeds {
    /// 便捷 GET:入队一个请求并返回可变引用以便继续设置头/元数据。
    pub fn get(&mut self, url: impl AsRef<str>) -> &mut Request {
        self.reqs.push(Request::new(url, RequestMethod::Get));
        self.reqs.last_mut().expect("just pushed")
    }

    /// 入队任意方法的请求。
    pub fn add(&mut self, req: Request) -> &mut Request {
        self.reqs.push(req);
        self.reqs.last_mut().expect("just pushed")
    }

    fn into_vec(self) -> Vec<Request> {
        self.reqs
    }
}

/// 解析上下文,传给 [`Spider::parse`]:产出数据、追加后继请求。
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

    /// 产出一条类型化数据(交付给 [`DataSink`])。
    pub fn emit(&mut self, item: Item) {
        self.items.push(item);
    }

    /// 追加一个后继请求(下载后重新进入本 spider 的 `parse`)。
    pub fn follow(&mut self, req: Request) {
        self.follows.push(req);
    }

    /// 便捷 GET 版 [`follow`](Ctx::follow)。
    pub fn follow_get(&mut self, url: impl AsRef<str>) {
        self.follows.push(Request::new(url, RequestMethod::Get));
    }
}

/// 数据出口:类型化 Item 如何离开系统(回调 / channel / MQ / 自定义)。
///
/// 取代「必须实现 `DataStoreMiddleware` + 库中 relation 挂载」的老路。
#[async_trait]
pub trait DataSink<Item>: Send + Sync {
    /// 交付一条数据。
    async fn write(&self, item: Item) -> Result<()>;
}

/// 闭包 sink(由 [`on_item`] 构造)。
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

/// 用一个异步闭包构造 [`DataSink`]:`on_item(|item| async move { ... })`。
pub fn on_item<Item, Fut, F>(f: F) -> ClosureSink<Item, F>
where
    Item: Send + 'static,
    F: Fn(Item) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    ClosureSink { f, _p: PhantomData }
}

/// 把每条 Item 送进一个 tokio channel 的 [`DataSink`]。
///
/// 便于把采集数据交给下游任务处理:`ChannelSink::new(tx)`,消费端 `rx.recv().await`。
pub struct ChannelSink<Item> {
    tx: tokio::sync::mpsc::Sender<Item>,
}

impl<Item> ChannelSink<Item> {
    /// 用一个 `mpsc::Sender` 构造。
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

// ---- Spider → ModuleTrait / ModuleNodeTrait 适配器 ----

struct SpiderModule<S: Spider> {
    spider: Arc<S>,
    sink: Arc<dyn DataSink<S::Item>>,
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
        panic!("SpiderModule 需要一个 spider 实例,请使用 Mocra::builder().spider(..)");
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(SpiderNode {
            spider: self.spider.clone(),
            sink: self.sink.clone(),
        })]
    }
}

struct SpiderNode<S: Spider> {
    spider: Arc<S>,
    sink: Arc<dyn DataSink<S::Item>>,
}

#[async_trait]
impl<S: Spider> ModuleNodeTrait for SpiderNode<S> {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        // 跟进请求:由上一轮 parse 的 follow 通过元数据回灌。
        if let Some(url) = params.get(FOLLOW_URL_KEY).and_then(|v| v.as_str()) {
            let req = Request::new(url, RequestMethod::Get);
            return Ok(Box::pin(futures::stream::iter(vec![req])));
        }
        // 首轮:播种。
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

        // 数据 → sink。
        for item in cx.items {
            self.sink.write(item).await?;
        }

        // 跟进请求 → 回灌 parser task(重新进入本节点 generate)。
        let mut out = TaskOutputEvent::default();
        for req in cx.follows {
            let task = TaskParserEvent::from(&response).add_meta(FOLLOW_URL_KEY, req.url);
            out = out.with_task(task);
        }
        Ok(out)
    }

    fn stable_node_key(&self) -> &'static str {
        "spider"
    }
}

// ---- Mocra 构建器 ----

/// 高层入口。用 [`Mocra::builder`] 注册 spider、配置并运行。
pub struct Mocra;

impl Mocra {
    /// 创建一个构建器。
    pub fn builder() -> MocraBuilder {
        MocraBuilder::default()
    }
}

/// 内嵌集群配置(需 `cluster-embedded` 特性)。
#[cfg(feature = "cluster-embedded")]
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// 本节点唯一 id。
    pub node_id: u64,
    /// 本节点绑定并对外的地址(如 `127.0.0.1:7001`)。
    pub http_addr: String,
    /// redb 状态机 + 日志目录。
    pub data_dir: String,
    /// 种子节点地址;为空 = 本节点自举一个新集群(首个核心),非空 = 向种子 join。
    pub seeds: Vec<String>,
    /// 可选 Raft 时序调参(默认适配局域网;广域网 / 高延迟可放大)。
    pub raft_tuning: Option<mocra_cluster::RaftTuning>,
}

#[cfg(feature = "cluster-embedded")]
impl ClusterConfig {
    /// **自举**一个新集群的首个核心节点(`seeds` 为空)。
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

    /// **加入**已有集群:通过任意一个已知节点(种子)地址入网(作 learner)。
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

    /// 从环境变量读取集群配置 —— 便于**容器化部署**:同一镜像跨节点,仅换环境变量。
    ///
    /// - `MOCRA_NODE_ID`(必填,u64)
    /// - `MOCRA_HTTP_ADDR`(必填,本节点对外地址)
    /// - `MOCRA_DATA_DIR`(选填,缺省 `./mocra-data/node-{id}`)
    /// - `MOCRA_SEEDS`(选填,逗号分隔的种子地址;为空 = 自举)
    pub fn from_env() -> std::result::Result<Self, String> {
        Self::from_vars(
            std::env::var("MOCRA_NODE_ID").ok(),
            std::env::var("MOCRA_HTTP_ADDR").ok(),
            std::env::var("MOCRA_DATA_DIR").ok(),
            std::env::var("MOCRA_SEEDS").ok(),
        )
    }

    /// [`from_env`](Self::from_env) 的纯解析核心(便于单测,不读环境)。
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

    /// 自定义 Raft 时序(广域网 / 高延迟集群);默认适配局域网。
    pub fn with_raft_tuning(mut self, tuning: mocra_cluster::RaftTuning) -> Self {
        self.raft_tuning = Some(tuning);
        self
    }
}

/// 启动内嵌 redb+Raft 控制面并返回其 [`CoordinationBackend`](crate::sync::CoordinationBackend)。
///
/// `seeds` 空 → 自举新集群并等待选主;非空 → 向首个种子 join(作 learner)。
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

/// [`Mocra`] 的构建器。
#[derive(Default)]
pub struct MocraBuilder {
    config_path: Option<String>,
    modules: Vec<Arc<dyn ModuleTrait>>,
    /// 用户注册的具名下载器(按 `config.downloader` 名字路由)。
    downloaders: Vec<Box<dyn crate::downloader::Downloader>>,
    /// 替换全局默认下载器(缺省 reqwest)。
    default_downloader: Option<Box<dyn crate::downloader::Downloader>>,
    #[cfg(feature = "cluster-embedded")]
    cluster: Option<ClusterConfig>,
    #[cfg(feature = "dashboard")]
    dashboard_port: Option<u16>,
}

impl MocraBuilder {
    /// 从 TOML 文件加载配置。
    ///
    /// v0 必需;程序化默认配置与无 DB 运行在重构 Phase 2 落地。
    pub fn from_toml(mut self, path: impl Into<String>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    /// 注册一个 spider 及其数据出口。
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
        });
        self.modules.push(module);
        self
    }

    /// 注册一个自定义下载器(实现 [`Downloader`](crate::downloader::Downloader) trait)。
    ///
    /// 模块把 `DownloadConfig.downloader` 设为该下载器的 `name()` 即可路由到它;
    /// 未匹配时仍用默认下载器。可注册多个。适合按模块选择不同下载策略。
    ///
    /// ```ignore
    /// Mocra::builder()
    ///     .spider(MySpider, on_item(|_| async {}))
    ///     .downloader(MyDownloader::new())   // 模块 config.downloader = "<其 name()>" 时启用
    ///     .run().await?;
    /// ```
    pub fn downloader<D: crate::downloader::Downloader>(mut self, downloader: D) -> Self {
        self.downloaders.push(Box::new(downloader));
        self
    }

    /// 替换**全局默认**下载器(缺省是 reqwest)。当请求的 `config.downloader` 未匹配任何
    /// 已注册下载器时走它 —— 适合整体换掉 reqwest(如浏览器渲染 / 代理轮换 / 自定义重试)。
    pub fn default_downloader<D: crate::downloader::Downloader>(mut self, downloader: D) -> Self {
        self.default_downloader = Some(Box::new(downloader));
        self
    }

    /// 启用内嵌 redb+Raft 控制面(需 `cluster-embedded` 特性)。
    ///
    /// - `seeds` 为空 → 本节点自举一个新集群(首个核心,方案 A)。
    /// - `seeds` 非空 → 向种子节点 join(作 learner)。
    ///
    /// 开启后,引擎的选举 / 锁走 Raft 集群,无需外部协调器。
    #[cfg(feature = "cluster-embedded")]
    pub fn cluster(mut self, cluster: ClusterConfig) -> Self {
        self.cluster = Some(cluster);
        self
    }

    /// 启用后台管理 / 监控 dashboard(需 `dashboard` 特性)。
    ///
    /// 引擎在 `port` 上暴露 admin + 可观测 HTTP API,供后台管理页面消费:
    /// - `GET /metrics`(Prometheus 指标)、`GET /health`
    /// - `GET /observability/cluster`(集群状态)、`GET /observability/engine`(队列/引擎)
    /// - `GET /observability/system`(主机 CPU/内存/swap)、`GET /observability/logs?limit=N`(近期日志)
    /// - `GET /nodes`、`GET /dlq`、`POST /control/pause|resume`、`POST /start_work`
    ///
    /// 另在 `GET /` 内置一个单文件前端页面:浏览器打开该端口即见 指标 / 日志 / 任务 / 性能
    /// 面板(同源自动指向本引擎,无需手填 endpoint)。只读可观测端点(`/`、`/metrics`、
    /// `/health`、`/observability/*`)免 API key 且开启 CORS,独立前端也可跨域消费;写操作端点仍需鉴权。
    ///
    /// 单机模式下额外把引擎切到「服务态」:关闭空闲自停(否则空闲 30s 退出、面板失联),
    /// 并在未显式配日志时默认开启日志采集,让「日志」面板开箱即有数据。
    ///
    /// 从 `from_toml` 的 `[api]` 配置也能开启;此方法用于程序化(免 TOML)开启。
    #[cfg(feature = "dashboard")]
    pub fn dashboard(mut self, port: u16) -> Self {
        self.dashboard_port = Some(port);
        self
    }

    /// 构建 State + Engine,注册所有 spider,启动引擎(阻塞至关闭)。
    ///
    /// - 提供了 [`from_toml`](MocraBuilder::from_toml) → 用该 config(可含 DB)。
    /// - 未提供 → **无 DB / 单机** 默认配置,并为每个 spider 自动注入一个种子任务。
    /// - 提供了 [`cluster`](MocraBuilder::cluster) → 起内嵌 Raft 控制面,协调走 Raft。
    pub async fn run(self) -> Result<()> {
        let standalone = self.config_path.is_none();
        let provider: Box<dyn ConfigProvider> = match &self.config_path {
            Some(path) => Box::new(FileConfigProvider::new(path)),
            None => Box::new(StaticConfigProvider {
                config: default_standalone_config("mocra"),
            }),
        };

        // 先于 State 起集群:控制面就绪后把协调后端交给 State,使分布式锁 / 选举
        // 从构造起即走 Raft(而非退化的进程内锁)。
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
        // dashboard:程序化开启时把端口写进 config.api,引擎 start 时据此起后台管理 HTTP API。
        #[cfg(feature = "dashboard")]
        if let Some(port) = self.dashboard_port {
            let mut cfg = state.config.write().await;
            cfg.api = Some(crate::common::model::config::Api {
                port,
                api_key: None,
                rate_limit: None,
            });
            // dashboard 即「服务态」:单机默认会空闲 30s 自停,那样监控面板会随进程退出而失联。
            // 程序化开启 dashboard 时关闭单机空闲自停,让可观测端点常驻(from_toml 的显式配置不受影响)。
            if standalone {
                cfg.crawler.idle_stop_secs = None;
                // 未显式配日志时,默认开启日志采集,让 dashboard 的「日志」面板开箱即有数据:
                // LogSinkLayer 把 log/tracing 事件送入内存环形缓冲,供 `/observability/logs` 读取。
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
        // 在 state 移入 engine 前捕获协调后端句柄:用于「种子只注入一次」的 leader 门,
        // 以及引擎停机后的优雅关闭。
        let coordination = state.coordination.clone();
        // 集群下「种子只注入一次」:仅 leader 注入,避免每节点重复注入 → N× 重复抓取。
        let should_seed = coordination.as_ref().map(|c| c.is_leader()).unwrap_or(true);
        let engine = Engine::new(state, None).await?;

        // 用户自定义下载器 / 替换默认下载器(在引擎启动前接线;chains 与 manager 共享同一 Arc)。
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

        // 单机 / 集群 leader:为每个 spider 注入一个种子任务(否则引擎空转)。
        // 集群 follower 跳过,避免 N× 重复注入。种子的跨节点分发取决于数据面 MQ:
        // 配了分布式 MQ(Kafka/NATS)则扇出到各节点;默认内存队列则留在 leader。
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

        engine.start().await;

        // 引擎停机后优雅关闭集群控制面(释放 redb 句柄与 Raft 后台任务)。
        if let Some(c) = &coordination {
            c.shutdown().await;
        }
        Ok(())
    }
}

/// 无 DB / 单机的程序化默认配置(内存队列)。
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
            // 单机:队列空闲 30s 后自动停止(有限抓取跑完即退;持续产任务的 spider 不受影响)。
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

/// 把一份固定 [`Config`] 提供给 [`State`](crate::common::state::State)(无文件、程序化)。
struct StaticConfigProvider {
    config: Config,
}

#[async_trait]
impl ConfigProvider for StaticConfigProvider {
    async fn load_config(&self) -> std::result::Result<Config, String> {
        Ok(self.config.clone())
    }

    async fn watch(&self) -> std::result::Result<watch::Receiver<Config>, String> {
        // 静态配置:不监听变更(发送端立即释放,State 的监听循环随即退出)。
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
        assert!(boot.seeds.is_empty(), "bootstrap 节点无种子(自举)");

        let joined = ClusterConfig::join(2, "127.0.0.1:7002", "./d2", "127.0.0.1:7001");
        assert_eq!(joined.node_id, 2);
        assert_eq!(joined.seeds, vec!["127.0.0.1:7001".to_string()]);
    }

    #[test]
    fn from_vars_parses_env_style_config() {
        // 完整:含逗号分隔的多种子(join)。
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

        // 无种子 + 省略 data_dir → 自举 + 默认目录。
        let boot =
            ClusterConfig::from_vars(Some("1".into()), Some("127.0.0.1:7001".into()), None, None)
                .unwrap();
        assert!(boot.seeds.is_empty());
        assert_eq!(boot.data_dir, "./mocra-data/node-1");

        // 缺必填项 / 非法 id → 报错(不 panic)。
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

    /// 端到端:`.dashboard(port)` 真的在该端口提供可观测 HTTP API。
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

        // 轮询 /health 直到 server 绑定。
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

        // /observability/engine → 引擎/队列快照。
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

        // /observability/cluster → null(无集群协调后端)。
        let cluster: serde_json::Value = client
            .get(format!("{base}/observability/cluster"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(cluster.is_null());

        // /observability/system → 200 + JSON(快照 null 或对象);顺带验证 CORS 头。
        let sys = client
            .get(format!("{base}/observability/system"))
            .header("Origin", "http://localhost:3000")
            .send()
            .await
            .unwrap();
        assert!(sys.status().is_success());
        assert!(
            sys.headers().contains_key("access-control-allow-origin"),
            "CORS header missing — 前端跨域会被拦"
        );
        let _sys_json: serde_json::Value = sys.json().await.unwrap();

        // /observability/logs → 200 + JSON 数组。
        let logs: serde_json::Value = client
            .get(format!("{base}/observability/logs?limit=50"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(logs.is_array(), "logs 端点应返回数组");

        // GET / → 内置单文件 dashboard 页面(HTML)。
        let page = client.get(format!("{base}/")).send().await.unwrap();
        assert!(page.status().is_success());
        let html = page.text().await.unwrap();
        assert!(
            html.contains("mocra dashboard"),
            "根路径应托管内置 dashboard 页面"
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

    /// 自定义下载器:忽略网络,返回固定响应 —— 用于证明它被管线真正调用(而非 reqwest)。
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
            // 故意用不可解析的域名:若走真实 reqwest 会失败;走 fake 才能拿到固定响应体。
            s.get("https://this-host-does-not-resolve.invalid/x");
        }
        async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
            if res.text_lossy().contains("FAKE_DOWNLOADER_BODY") {
                cx.emit(res.status_code);
            }
            Ok(())
        }
    }

    /// 端到端:`Mocra::builder().default_downloader(..)` 注入的自定义下载器真的被管线使用。
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
            "自定义默认下载器应已服务该请求(状态 299 + fake body);若为 None 说明仍走了 reqwest"
        );
    }
}
