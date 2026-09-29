# 代理与下载器

> **English version:** [Proxies and downloaders](../proxies-and-downloaders.md)

mocra 可以为请求指定固定代理、从托管代理池选取代理，或使用自定义下载器。这些方式的配置与反馈行为不同。

## 选择代理方式

| 需求 | API 或配置 | 行为 |
| --- | --- | --- |
| 为某个 `Spider` 请求指定代理 | `Request::use_proxy(proxy)` | 使用固定代理；默认没有托管池反馈或自动轮换 |
| 引擎托管选择与重试轮换 | 引擎 TOML 的 `[proxy]` 加模块 JSON `{"enable_proxy": true}` | 从池中选择、记录尝试，并可能在重试时换代理 |
| 在爬虫外独立使用代理池 | `ProxyManager::from_config` 或 `from_proxy_config` | 调用者自行选取代理并报告结果 |

无数据库的简易 `Spider` builder 目前没有设置 `enable_proxy` 的模块配置接口。可直接使用的示例是 [`examples/explicit_proxy.rs`](../../examples/explicit_proxy.rs)，它需要在 `MOCRA_PROXY_URL` 中指定可用代理。离线演示代理池选择可运行 `cargo run --example proxy_pool`。

## 配置引擎托管代理池

在其他必需字段已齐备的引擎 TOML 中加入：

```toml
[[proxy.direct]]
name = "primary"
url = "http://127.0.0.1:8080"
rate_limit = 10.0

[[proxy.direct]]
name = "secondary"
url = "http://127.0.0.1:8081"
rate_limit = 10.0

[proxy.pool_config]
max_errors = 3
health_check_interval_secs = 300
```

然后在数据库模块的 JSON 配置中设置 `{"enable_proxy": true}`。请求若已显式指定代理，引擎会保留该代理。独立传给 `ProxyManager::from_config` 的 TOML 则在根层使用 `[[direct]]`，而非 `[[proxy.direct]]`。隧道和 IP 提供商等完整字段见[配置参考](configuration.md#proxy)。

## 失败反馈与轮换

引擎在下载真正开始时记录托管代理尝试，完成后报告成功或失败。网络与超时错误、代理握手 407、提供商配置的重试状态码可标记代理失败。普通目标站 HTTP 错误不会自动归咎于代理。重试使用已有的请求预算（`crawler.request_max_retries`）；轮换不会额外增加尝试次数。

自动轮换仅用于 `GET`、`HEAD`、`OPTIONS`、`PUT`、`DELETE` 和 `WSS`。`POST` 不会自动经另一代理重放。下一次选择会尽量排除失败代理；没有替代代理时，剩余重试仍受原预算约束。

`Request::use_proxy` 默认固定代理。在有数据库的模块中，如果同时有引擎代理池，配置 `{"auto_rotate_explicit_proxy": true}` 可允许可重放的显式代理请求在失败重试时切换到池中代理。显式代理尝试本身不计入托管池反馈；切换到池中代理后的尝试才会计入。是否允许轮换应符合目标请求的重试语义。

## 运行中更换代理配置

文件配置提供器约每五秒检查一次改动，并更新引擎共享配置及限速值。但引擎只在**启动时**根据代理配置创建一次 `ProxyManager`；修改 TOML 中的 `[[proxy.direct]]`、提供商或池策略不会重建正在使用的代理池。要应用这些代理变更，需要重启引擎。新生成的请求仍可通过 `Request::use_proxy` 指定不同的显式代理。独立的 `ProxyManager` 提供 `add_ip_provider` 和 `add_tunnel` 以便程序化增加来源，但没有通用的直连代理列表替换接口。

独立使用 `ProxyManager` 时，先调用 `get_proxy_for_attempt(excluded)`，真正发送前调用 `begin_proxy_attempt(&proxy)`，再按实际结果调用 `report_success(&proxy, latency)` 或 `report_failure(&proxy)`。仅选中代理不算开始尝试。[`examples/proxy_pool.rs`](../../examples/proxy_pool.rs) 展示调用顺序；其中的成败是模拟结果。

## 代理池与 Client 缓存调优

- `proxy.pool_config.max_size` 限制每个提供商动态加载的 IP 代理数，**不**限制静态 `[[proxy.direct]]` 条目数。`health_check_interval_secs = 0` 关闭定时健康检查；`health_check_concurrency` 限制并行探测数。
- 代理池从合格候选中采样两个，优先选质量评分较高者。隧道代理仍优先；选择前会考虑过期、错误上限、速率限制与重试排除。取舍见[本地选择对比](../../benchmarks/proxy-selection.md)。
- `download_config.proxy_client_cache_capacity` 限制代理专用 HTTP Client 缓存（默认 1000；`0` 关闭）。调整前对比命中/未命中、新建、绕过、淘汰、空闲连接与 RSS。见[本地缓存测量](../../benchmarks/proxy-client-cache.md)。
- 开启 dashboard API 后，可在 `/metrics` 查看 `mocra_download_attempts_total`、`mocra_proxy_faults_total`、`mocra_proxy_rotations_total`、`mocra_proxy_selection_errors_total` 及代理 Client 缓存计数器。独立管理器提供 `selection_wait_stats()` 与 `attempt_stats()`。

## 自定义下载器

实现 `mocra::prelude::downloader::Downloader`，用 `.default_downloader(d)` 全局替换 reqwest。`.downloader(d)` 注册由模块配置 `downloader` 选取的具名实现；无数据库的简易 `Spider` builder 没有设置该模块配置的接口。浏览器或 Servo 可通过此接口接入，但仓库没有内置浏览器实现。

自定义下载器需要自行决定如何处理 `Request.proxy`、请求头、Cookie 和超时；注册下载器不会让 reqwest 代替它处理这些字段。

自定义下载器返回的 `Response` 必须复制请求的关联字段（`id`、账号、平台、模块、元数据、运行/执行上下文、中间件列表、重试次数、优先级和前置请求）。状态码、正文、响应头及 Cookie 则来自实际下载结果。完整的离线实现见 [`examples/custom_downloader.rs`](../../examples/custom_downloader.rs)。
