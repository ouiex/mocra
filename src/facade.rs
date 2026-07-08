//! High-level `Spider` facade (重构 Phase 1).
//!
//! 面向 80% 场景的简单入口:实现一个 [`Spider`],用 [`Mocra::builder`] 三步跑起来,
//! 通过 [`DataSink`] / [`on_item`] 拿到类型化数据,无需实现 `DataStoreMiddleware`。
//!
//! 现有的 [`ModuleTrait`](crate::common::interface::ModuleTrait) + DAG 仍是进阶路径;
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
    ClosureSink {
        f,
        _p: PhantomData,
    }
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

/// [`Mocra`] 的构建器。
#[derive(Default)]
pub struct MocraBuilder {
    config_path: Option<String>,
    modules: Vec<Arc<dyn ModuleTrait>>,
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

    /// 构建 State + Engine,注册所有 spider,启动引擎(阻塞至关闭)。
    ///
    /// - 提供了 [`from_toml`](MocraBuilder::from_toml) → 用该 config(可含 DB / Redis)。
    /// - 未提供 → **无 DB / 单机** 默认配置,并为每个 spider 自动注入一个种子任务。
    pub async fn run(self) -> Result<()> {
        let (state, standalone) = match &self.config_path {
            Some(path) => (
                State::try_new(path)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Service, Some(e.to_string())))?,
                false,
            ),
            None => {
                let provider = StaticConfigProvider {
                    config: default_standalone_config("mocra"),
                };
                (
                    State::try_new_with_provider(Box::new(provider))
                        .await
                        .map_err(|e| Error::new(ErrorKind::Service, Some(e.to_string())))?,
                    true,
                )
            }
        };
        let state = Arc::new(state);
        let engine = Engine::new(state, None).await?;

        let spider_names: Vec<String> = self.modules.iter().map(|m| m.name()).collect();
        for module in self.modules {
            engine.register_module(module).await;
        }

        // 单机模式:为每个 spider 注入一个种子任务(否则引擎空转)。
        if standalone {
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
        Ok(())
    }
}

/// 无 DB / 单机的程序化默认配置(无 redis、内存队列)。
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
            redis: None,
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
        cookie: None,
        channel_config: ChannelConfig {
            blob_storage: None,
            redis: None,
            kafka: None,
            compensator: None,
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
