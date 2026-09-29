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
| [dashboard.rs](dashboard.rs) | `cargo run --example dashboard --features dashboard` | Internet access; open `http://127.0.0.1:12800` / 外网与浏览器 |
| [cluster_quickstart.rs](cluster_quickstart.rs) | `cargo run --example cluster_quickstart --features cluster-embedded -- 1 127.0.0.1:7001` | Internet access, local port, writable `mocra-data/` / 外网、可用端口与数据目录 |

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
