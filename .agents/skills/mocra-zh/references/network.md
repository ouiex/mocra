# 代理与下载器

适用于显式代理、代理池选择与失败反馈、替换 HTTP 下载器或对接浏览器。先确认任务需要固定代理、引擎自动轮换，还是独立使用 `mocra-proxy`。

## 三种代理用法

| 场景 | 做法 | 边界 |
| --- | --- | --- |
| 某个请求指定代理 | `Request::use_proxy(proxy)`；见 `examples/explicit_proxy.rs` | 固定选择，本身不启用池反馈与自动轮换 |
| 引擎托管选择 | 在引擎 TOML 中配置 `[[proxy.direct]]` 或 provider，并在数据库模块 JSON 中设置 `{"enable_proxy": true}` | 无数据库的简易 `Spider` builder 当前没有模块配置 setter |
| 独立操作池 | `ProxyManager::from_proxy_config` / `from_config`；见 `examples/proxy_pool.rs` | 调用者负责反馈实际结果 |

引擎 TOML 的直接代理片段：

```toml
[[proxy.direct]]
name = "primary"
url = "http://127.0.0.1:8080"

[proxy.pool_config]
health_check_interval_secs = 300
```

传给独立 `ProxyManager::from_config` 的 TOML 根节点使用 `[[direct]]`，不是 `[[proxy.direct]]`。具体字段及默认值见 `docs/configuration.md` 的 `[proxy]` 部分和 `crates/mocra-proxy/src/proxy_pool.rs`。
引擎仅在启动时构建代理池；文件监听更新不会重建它。回答运行中修改代理配置的问题前，请查看 `docs/zh/proxies-and-downloaders.md`。

## 代理尝试与反馈

独立管理器的调用顺序是：`get_proxy_for_attempt(excluded)` 选取代理；真正开始发送前 `begin_proxy_attempt(&proxy)`；按结果调用 `report_success(&proxy, latency)` 或 `report_failure(&proxy)`。一次请求重试时可将失败代理传给下一次 `get_proxy_for_attempt(Some(&failed))`。选中代理并不等于已发起下载，不应据此扣限额或报告失败。引擎中的对应流程在 `crates/mocra-core/src/engine/chain/proxy_attempt.rs`；配置值和选择结果均需沿下载链传递。

运行 `cargo run --example proxy_pool` 可以离线检查 API 调用顺序；示例的失败与成功均为模拟。`examples/explicit_proxy.rs` 需要真实代理，可用 `MOCRA_PROXY_URL=... cargo run --example explicit_proxy` 启动。

## 自定义 Downloader

`Downloader` 位于 `mocra::prelude::downloader`，实现者需要可克隆，并实现 `name`、`version`、`set_config`、`set_limit`、`health_check`、`download`。`close` 有默认实现。使用 `.default_downloader(d)` 替换默认 reqwest 下载器；`.downloader(d)` 注册命名下载器，由模块配置 `downloader` 选择。无数据库的简易 `Spider` builder 当前不提供该模块配置 setter；不要声称设置 `Request.downloader` 就能完成命名路由。

在 `download(Request) -> Result<Response>` 中保留关联字段：`id`、`platform`、`account`、`module`、`task_retry_times`、`meta → metadata`、middleware 列表、`task_finished`、`context`、`run_id`、`prefix_request` 和 `priority`。响应状态、正文、headers 与 cookies 则来自实际下载结果。遗漏关联字段会让后续任务与解析不可靠。完整可编译构造见 `examples/custom_downloader.rs`。

浏览器或 Servo 可以通过此接口作为外部下载实现；仓库没有内置浏览器下载器。接口适配时还要明确导航超时、Cookie/headers、代理、响应状态及清理资源的语义，先用离线 mock 或本地测试站验证，再接真实目标。
