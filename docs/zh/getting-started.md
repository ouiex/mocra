# 快速上手

本指南带你在几分钟内从空项目跑起一个爬虫。你只需实现一个 **`Spider`**，交给
`Mocra::builder()`，再调用 `.run()` —— 单机、内存模式，**无需数据库**。

## 前置条件

- **Rust 1.85+**（edition 2024）。

快速上手就这一项。mocra 的默认构建是单机模式，不依赖任何外部服务。

> **数据库**（PostgreSQL / SQLite）是*可选*的 —— 它只在进阶的多阶段
> 与分布式路径上才用到（`store` 特性、TOML 配置，或内嵌 `cluster-embedded` 控制面）。本指南**无需**它。

## 安装

在 `Cargo.toml` 中添加：

```toml
[dependencies]
mocra = "0.4"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }

# 可选：mocra 返回原始 HTTP Response —— 解析器自备。
scraper = "0.20"   # CSS 选择器，用于 HTML 目标
```

mocra 把原始 `Response` 交给你，由你决定如何解析（HTML 用 `scraper`，JSON 接口用
`res.json()` —— 框架不内置任何解析器）。

## 你的第一个 Spider

一个 `Spider` 回答两个问题：**抓什么**（`start`）与**怎么解析每个响应**（`parse`）。
产出的数据流向你提供的**出口（sink）**。下面是一个完整程序：抓取一个 URL，并为每个响应打印一条类型化记录：

```rust
use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Page {
    url: String,
    status: u16,
}

struct Httpbin;

#[async_trait]
impl Spider for Httpbin {
    type Item = Page;

    fn name(&self) -> &str {
        "httpbin"
    }

    // 播种初始请求。
    async fn start(&self, s: &mut Seeds) {
        s.get("https://httpbin.org/get");
    }

    // 解析单个下载到的响应；`cx.emit` 产出一条类型化数据。
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(Page {
            url: res.module_id(),
            status: res.status_code,
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(Httpbin, on_item(|p: Page| async move {
            println!("[item] {} -> {}", p.url, p.status);
        }))
        .run()
        .await
}
```

## 运行

```bash
cargo run
```

无需配置文件，也无需启动任何服务。在没有 TOML 配置时，`.run()` 会启动一个单机、内存引擎，
**自动为每个已注册的 spider 注入种子任务**，并在**队列空闲后自动退出** —— 非常适合一次性抓取。

## 刚刚发生了什么

- **`name()`** —— 唯一 id，用于队列 topic、指标标签与去重。
- **`start(&mut Seeds)`** —— `s.get(url)` 入队一个 GET（返回 `&mut Request`，可继续设置头、超时等）；
  `s.add(Request)` 入队任意方法的请求。
- **`parse(Response, &mut Ctx)`** —— `cx.emit(item)` 产出一条类型化数据；
  `cx.follow_get(url)` / `cx.follow(Request)` 追加后继请求，其响应会重新进入 `parse`
  （用于翻页、详情页）。记得加终止条件，让跟进能停下来。
- **出口（sink）** —— `on_item(|item| async move { … })` 为每条产出的数据运行你的异步闭包
  （打印、写文件、入库、下发下游）。

## 解析响应

mocra 返回原始 `Response`。常用访问方式：

| 访问方式 | 返回 | 用途 |
|---|---|---|
| `res.status_code` | `u16` | HTTP 状态码 |
| `res.text()` | `Result<&str>` | UTF-8 正文（非法 UTF-8 时报错） |
| `res.text_lossy()` | `Cow<str>` | 有损 UTF-8 —— 面对杂乱 HTML 更稳 |
| `res.json::<T>()` | `Result<T>` | 反序列化 JSON 正文 |
| `res.content` | `Vec<u8>` | 原始字节 |

用 `scraper` 解析 HTML：

```rust
async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    let html = res.text_lossy();
    let doc = scraper::Html::parse_document(&html);
    let sel = scraper::Selector::parse("h2 a").unwrap();
    for a in doc.select(&sel) {
        cx.emit(Item {
            title: a.text().collect::<String>().trim().to_string(),
            url: a.value().attr("href").unwrap_or_default().to_string(),
        });
    }
    Ok(())
}
```

JSON 接口 —— 直接反序列化成你的类型：

```rust
#[derive(serde::Deserialize)]
struct ApiPage { results: Vec<Row> }
#[derive(serde::Deserialize)]
struct Row { id: u64, name: String }

async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    let page: ApiPage = res.json()?;
    for r in page.results {
        cx.emit(Item { id: r.id, name: r.name });
    }
    Ok(())
}
```

## 取回数据

`on_item` 是最快的路径。如果你想把结果从运行中**取出**、而非就地处理，用 `ChannelSink`：

```rust
let (tx, mut rx) = tokio::sync::mpsc::channel::<Item>(256);
let engine = tokio::spawn(async move {
    Mocra::builder().spider(MySpider, ChannelSink::new(tx)).run().await
});

let mut all = Vec::new();
while let Some(item) = rx.recv().await { all.push(item); }  // 运行结束后自然停止
engine.await.ok();
```

或者自己实现 `DataSink<Item>` 以完全掌控。

## 可选特性

默认全部关闭，按需开启，例如
`mocra = { version = "0.4", features = ["dashboard"] }`。

| 特性 | 解锁 |
|---|---|
| `dashboard` | 只读可观测 HTTP API + 内置 Web UI（`.dashboard(port)`） |
| `cluster-embedded` | 内嵌 Raft + redb 控制面（`.cluster(…)`）—— 无需外部协调器 |
| `store` | 基于数据库的 账号 × 平台 × 模块 模型（sea-orm） |
| `queue-kafka` / `queue-nats` | Kafka / NATS JetStream 数据面队列后端 |
| `polars` / `excel` | DataFrame / Excel 数据处理 |
| `js-v8` | 内嵌 V8 JS 运行时，用于 parse 脚本 |
| `mimalloc` | mimalloc 全局分配器（**默认开启**） |

## 下一步

- [系统架构](architecture.md) —— 队列驱动的流水线与 DAG 引擎如何协作。
- [模块开发](module-development.md) —— **进阶**路径：实现 `ModuleTrait` / `ModuleNodeTrait`，
  构建多阶段流水线、登录流程与自定义中间件。
- [DAG 执行](dag-guide.md) —— 扇出 / 汇合图与推进门（advance gate）。
- [配置参考](configuration.md) —— 完整 TOML 参考（数据库、队列、API）。
- [`examples/`](../../examples/) 下的可运行示例：
  - [`spider_quickstart.rs`](../../examples/spider_quickstart.rs) —— 上文的最小 `Spider`（无 DB）。
  - [`custom_downloader.rs`](../../examples/custom_downloader.rs) —— 实现 `Downloader` trait 并用 `.default_downloader()` 注入。
  - [`dashboard.rs`](../../examples/dashboard.rs) —— 内置可观测 dashboard（`--features dashboard`）。
  - [`cluster_quickstart.rs`](../../examples/cluster_quickstart.rs) —— 自组织内嵌集群（`--features cluster-embedded`）。
