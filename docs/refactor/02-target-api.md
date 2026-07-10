# 02 · 目标 API

> 目标:一个 `Spider` trait + `Mocra` 构建器覆盖 80% 场景,数据库化的 `account × platform × module` 模型降级为「多租户」选配。
>
> 下列签名为**提案草案**,用于锁定 API 形状,细节在实现时收敛。

## 设计原则:渐进式披露

同一内核,三个入口层级。用户从零依赖起步,随需求逐层解锁,且下层能力永不牺牲。

| 层 | 面向 | 依赖 | 集群化改动 |
|---|---|---|---|
| **L0 · 零配置** | 脚本、原型、单机抓取 | 核心运行时(无 DB/Redis/MQ) | —— |
| **L1 · 分布式** | 生产集群 | + cluster(redb+raft)+ MQ 特性 | 一行 `.cluster()` / `.queue()` |
| **L2 · 多租户(选配)** | 多账号采集平台 | + store 特性(sea-orm) | 开启 `store` 特性 |

## Spider trait —— 80% 场景的主入口

把常见情形(单一抓取逻辑 + 类型化输出)收敛成一个 trait,取代 `ModuleTrait` + 单 `ModuleNodeTrait` 的样板。

```rust
use mocra::prelude::*;

#[async_trait]
pub trait Spider: Send + Sync + 'static {
    /// 类型化的产出项(自动可序列化,走 DataSink 出口)
    type Item: Serialize + Send + 'static;

    /// 唯一名称(用于队列 topic、指标标签、去重命名空间)
    fn name(&self) -> &str;

    /// 种子请求
    async fn start(&self, s: &mut Seeds);

    /// 解析单个响应:产出 Item、追加后继请求
    async fn parse(&self, res: Response, cx: &mut Ctx<Self>) -> Result<()>;

    // ---- 以下均有默认实现,按需覆盖 ----

    /// 并发上限(也可在 builder 覆盖)
    fn concurrency(&self) -> usize { 16 }

    /// 每域限速(每秒请求数)
    fn rate_limit(&self) -> Option<f32> { None }

    /// 默认请求头
    fn headers(&self) -> Headers { Headers::default() }

    /// Cron 表达式:周期性重新播种
    fn cron(&self) -> Option<&str> { None }

    /// 登录钩子:返回的 LoginInfo 注入到每次 parse 的 Ctx
    async fn login(&self) -> Result<Option<LoginInfo>> { Ok(None) }
}
```

### Seeds / Ctx —— 请求播种与产出

```rust
/// 种子请求收集器
impl Seeds {
    pub fn get(&mut self, url: impl Into<Url>) -> &mut Request;   // 便捷 GET
    pub fn add(&mut self, req: Request) -> &mut Request;          // 任意方法
}

/// 解析上下文:产出 Item、追加后继请求、读取登录态与元数据
pub struct Ctx<S: Spider + ?Sized> { /* ... */ }
impl<S: Spider> Ctx<S> {
    pub fn emit(&mut self, item: S::Item);                 // → DataSink
    pub fn follow(&mut self, req: impl Into<Request>);     // 入队后继请求
    pub fn login(&self) -> Option<&LoginInfo>;
    pub fn meta(&self) -> &Meta;                           // 跨请求传递的元数据
    pub fn set_meta(&mut self, k: &str, v: impl Serialize);
}
```

### Response 便捷解析

为常见抓取提供开箱即用的选择器(HTML/JSON),避免每个 parser 手搓 serde:

```rust
impl Response {
    pub fn text(&self) -> Result<&str>;
    pub fn json<T: DeserializeOwned>(&self) -> Result<T>;
    pub fn css(&self, sel: &str) -> Result<Vec<Node>>;      // HTML 选择器
    pub fn css_first(&self, sel: &str) -> Result<Option<Node>>;
    pub fn join(&self, href: &str) -> Url;                  // 相对 URL 解析
}
```

## Mocra 构建器 —— 取代 State→Engine→register→start 四步

```rust
#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(HackerNews)                                  // 可多次调用,注册多个 spider
        .concurrency(200)
        .on_item(|s: Story| async move { println!("{s:?}"); })  // L0 简单出口:回调
        .run()                                               // 内存队列,无 DB、无 Redis
        .await
}
```

关键变化:

- **配置程序化,带合理默认,TOML 变成众多加载器之一**(`Mocra::from_toml("config.toml")` 仍可用)。
- **`Engine::new` 不再 `process::exit`**;`.run()` 返回 `Result`。
- **`on_item` / `sink` 取代必须实现的 `DataStoreMiddleware`**。
- **`Item` 是类型化输出**,而非 polars-`DataEvent`。

## 完整对比:抓取并拿到数据

**现在:**

```rust
// + config.toml(14 段)
// + DB schema 建表 + 种 account/platform 行
// + 实现 DataStoreMiddleware 才能拿到数据
let state  = Arc::new(State::new("config.toml").await);
let engine = Engine::new(state, None).await?;
engine.register_module(My::default_arc()).await;   // 还要 impl ModuleTrait + ModuleNodeTrait
engine.start().await;                               // 还要注入 TaskEvent 才会真正开始
```

**提案(L0,零基础设施):**

```rust
use mocra::prelude::*;

#[derive(Debug, Serialize)]
struct Story { title: String, url: String }

struct HackerNews;

#[async_trait]
impl Spider for HackerNews {
    type Item = Story;
    fn name(&self) -> &str { "hn" }

    async fn start(&self, s: &mut Seeds) {
        s.get("https://news.ycombinator.com/");
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self>) -> Result<()> {
        for row in res.css(".athing")? {
            cx.emit(Story {
                title: row.text(".titleline")?,
                url:   row.attr("a", "href")?,
            });
        }
        if let Some(n) = res.css_first(".morelink")? {
            cx.follow(res.join(&n.attr("", "href")?));
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(HackerNews)
        .concurrency(50)
        .on_item(|s: Story| async move { println!("{} — {}", s.title, s.url); })
        .run()
        .await
}
```

## L1 · 分布式 —— 加一行,同一二进制起集群

**已实现**(`cluster-embedded` 特性):

```rust
// 首个核心节点:自举新集群
Mocra::builder()
    .spider(HackerNews, on_item(|s: Story| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "10.0.0.1:7001", "/var/lib/mocra/n1"))
    .run().await?;

// 其余节点:注册到任意已知节点即入网(作 learner)
Mocra::builder()
    .spider(HackerNews, on_item(|s: Story| async move { /* ... */ }))
    .cluster(ClusterConfig::join(2, "10.0.0.2:7001", "/var/lib/mocra/n2", "10.0.0.1:7001"))
    .run().await?;
```

- 控制面(选举 / 锁 / 成员 / 分区归属)跑在内嵌 **redb + Raft**,自组网,**无需 Redis**。
- 容器化:`ClusterConfig::from_env()`(读 `MOCRA_*`);广域网:`.with_raft_tuning(RaftTuning { .. })`。
- 数据面 MQ 经现有 config 的 `channel_config`(Kafka / Redis / 内存)配置;任务按 `hash(account)` 路由实现跨节点消费亲和。
- **待糖化**:`.queue(..)` / `.sink(..)` / `.api(port)` 链式配置(数据面 / 出口 / 控制 HTTP)是后续项;当前经 config 或 `DataSink` 提供。

## DataSink —— 数据出口的可插拔缝

取代「必须实现 `DataStoreMiddleware` + 库中 relation 挂载」的老路。这是用户「消费数据」的入口。

```rust
#[async_trait]
pub trait DataSink<Item>: Send + Sync {
    async fn write(&self, item: Item) -> Result<()>;
    async fn flush(&self) -> Result<()> { Ok(()) }
}

// 内置实现:
//   on_item(closure)        —— 回调(L0 默认)
//   ChannelSink             —— 送进 tokio channel
//   KafkaSink / NatsSink    —— 送进 MQ 供下游消费
//   自定义:impl DataSink for MyDbWriter
```

`emit` 出的 Item 依次经过(可选的)数据中间件后落到 sink。多个 sink 可叠加(如同时写库 + 发 Kafka)。

## 与现有 ModuleTrait / DAG 的关系

`Spider` 是**主路径,不是替代**。进阶能力保留:

- **多阶段扇出/汇合** → 继续用 `ModuleTrait::dag_definition()`(见 [dag-guide](../dag-guide.md))。`Spider` 内部会被适配成一个单节点或线性 DAG。
- **多租户 account × platform** → L2 的 `store` 选配特性,`TaskEvent`/`ModuleTrait` 语义不变。
- 换言之:**`Spider` 编译进现有引擎,而非另起炉灶**——这让 Phase 1 能作为薄层快速落地(见 [04 · 实施路线](04-roadmap.md))。

---

上一篇:[01 · 现状评估](01-assessment.md) · 下一篇:[03 · 集群架构](03-cluster-architecture.md)
