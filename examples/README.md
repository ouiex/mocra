# Runnable examples / 可运行示例

Run commands from the repository root. Most examples without a feature flag use the single-node,
in-memory `Spider` facade. It stops after the configured idle period (30 seconds by default).
`proxy_pool` demonstrates the standalone `ProxyManager` API.

从仓库根目录运行。无需特性开关的 `Spider` 示例使用单机内存模式；默认在队列空闲 30 秒后退出。

| Example / 示例 | Command / 命令 | Needs / 前置条件 |
| --- | --- | --- |
| [spider_quickstart.rs](spider_quickstart.rs) | `cargo run --example spider_quickstart` | Internet access / 可访问外网 |
| [custom_downloader.rs](custom_downloader.rs) | `cargo run --example custom_downloader` | None; mock downloader / 离线模拟下载器 |
| [follow_request.rs](follow_request.rs) | `cargo run --example follow_request` | None; demonstrates GET → POST with headers, body, cookie / 离线验证后续请求字段 |
| [proxy_pool.rs](proxy_pool.rs) | `cargo run --example proxy_pool` | None; proxy outcomes are simulated / 离线演示代理池选择与反馈 |
| [explicit_proxy.rs](explicit_proxy.rs) | `MOCRA_PROXY_URL=http://127.0.0.1:8080 cargo run --example explicit_proxy` | A reachable HTTP proxy and target / 可用代理与目标站 |
| [quotes_scraper.rs](quotes_scraper.rs) | `cargo run --example quotes_scraper` | Internet access; writes `data/quotes/*.jsonl` / 外网和本地写入权限 |
| [quotes_project.rs](quotes_project.rs) + [config](quotes_project.toml) | `cargo run --example quotes_project` | Internet access; writes a new `data/quotes_project/<run-id>/` directory / 外网和本地写入权限 |
| [dashboard.rs](dashboard.rs) | `cargo run --example dashboard --features dashboard` | Internet access; open `http://127.0.0.1:12800` / 外网与浏览器 |
| [cluster_quickstart.rs](cluster_quickstart.rs) | `cargo run --example cluster_quickstart --features cluster-embedded -- 1 127.0.0.1:7001` | Internet access, local port, writable `mocra-data/` / 外网、可用端口与数据目录 |

## Advanced quotes project / 进阶引文项目

`quotes_project` is a complete single-node `Engine` example. A `ModuleTrait`/`ModuleNodeTrait`
starts at the first listing page, follows `Next` links up to **50 listing pages**, and fetches
each author page once. It stops earlier when the site has no next page. Its download middleware
sets common request headers; its data middleware normalizes and validates parsed records; its
store middleware writes `quotes.jsonl` and `authors.jsonl`. The TOML config limits request rate
and queue capacity. Each run uses a new output directory and prints stage counts on completion.
The `Spider`/`DataSink` facade in `quotes_scraper` is the simpler alternative; typed sink items
do not pass through the engine's data middleware.

`quotes_project` 是完整的单机 `Engine` 示例：模块从第一页出发，最多抓取 **50 个列表页**，
没有“下一页”时提前结束，并且每位作者的详情页只请求一次。下载中间件统一设置请求头，数据
中间件清洗和校验记录，存储中间件输出两个 JSONL 文件；TOML 配置限制请求速率与队列容量。
每次运行都会生成新的输出目录，并在完成时打印各阶段计数。简易的 `quotes_scraper` 使用
`Spider`/`DataSink`；其类型化输出不会经过引擎的数据中间件。

`explicit_proxy` sets one request's proxy through `Request::use_proxy`. It stays fixed by default.
For managed proxy selection in the DB-backed task model, add `[[proxy.direct]]` (or a provider)
to the engine TOML and set `{"enable_proxy": true}` in the module JSON config. The simple
DB-less `Spider` builder does not expose that module setting yet.

`explicit_proxy` 通过 `Request::use_proxy` 为请求指定固定代理。数据库任务模型要自动选代理，
还需在引擎 TOML 中配置 `[[proxy.direct]]`（或提供商），并在模块 JSON 配置中设置
`{"enable_proxy": true}`；目前无数据库的简易 `Spider` builder 尚未暴露该开关。

`cluster_quickstart` demonstrates the embedded coordination plane. Its default in-memory queue
does not share crawl tasks across processes; use a distributed queue to demonstrate a shared
data plane. `cluster_quickstart` 演示内嵌协调面；默认内存队列不会在进程间共享爬取任务。

For request fields and proxy behavior, see [Follow-up Requests](../docs/follow-up-requests.md)
and [Proxies and Downloaders](../docs/proxies-and-downloaders.md).

请求字段和代理行为详见[后续请求](../docs/zh/follow-up-requests.md)与[代理与下载器](../docs/zh/proxies-and-downloaders.md)。
