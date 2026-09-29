# Proxies and downloaders

Use this reference for explicit proxies, proxy-pool selection and feedback, replacing the HTTP downloader, or integrating a browser. First identify whether the task needs a fixed proxy, engine-managed rotation, or standalone `mocra-proxy` usage.

## Three proxy paths

| Scenario | Approach | Boundary |
| --- | --- | --- |
| Set a proxy on one request | `Request::use_proxy(proxy)`; see `examples/explicit_proxy.rs` | A fixed choice; it does not enable pool feedback or rotation |
| Let the engine select proxies | Configure `[[proxy.direct]]` or a provider in engine TOML and set `{"enable_proxy": true}` in database module JSON | The simple DB-less `Spider` builder has no module-config setter yet |
| Operate a pool directly | `ProxyManager::from_proxy_config` / `from_config`; see `examples/proxy_pool.rs` | The caller must report real outcomes |

An engine TOML proxy fragment:

```toml
[[proxy.direct]]
name = "primary"
url = "http://127.0.0.1:8080"

[proxy.pool_config]
health_check_interval_secs = 300
```

A standalone TOML string passed to `ProxyManager::from_config` uses root-level `[[direct]]`, not `[[proxy.direct]]`. See the `[proxy]` section in `docs/configuration.md` and `crates/mocra-proxy/src/proxy_pool.rs` for fields and defaults.
The engine creates its pool at startup; file-watch updates do not rebuild it. See `docs/proxies-and-downloaders.md` before promising live proxy changes.

## Attempts and feedback

With a standalone manager, call `get_proxy_for_attempt(excluded)` to select a proxy, `begin_proxy_attempt(&proxy)` immediately before sending, then `report_success(&proxy, latency)` or `report_failure(&proxy)` according to the actual outcome. A retry may pass the failed proxy to `get_proxy_for_attempt(Some(&failed))`. Selection alone is not an attempt: do not charge a rate limit or report failure before download starts. The engine path is in `crates/mocra-core/src/engine/chain/proxy_attempt.rs`; trace configuration and selection through the download chain when changing it.

`cargo run --example proxy_pool` checks the API sequence offline; its outcomes are simulated. `examples/explicit_proxy.rs` needs a real proxy: run it with `MOCRA_PROXY_URL=... cargo run --example explicit_proxy`.

## Custom Downloader

`Downloader` is in `mocra::prelude::downloader`. Its implementor must be cloneable and implement `name`, `version`, `set_config`, `set_limit`, `health_check`, and `download`; `close` has a default implementation. `.default_downloader(d)` replaces the default reqwest downloader. `.downloader(d)` registers a named downloader, selected by the module's `downloader` config. The simple DB-less `Spider` builder has no setter for that module config; setting `Request.downloader` alone does not provide named routing.

In `download(Request) -> Result<Response>`, carry over the correlation fields: `id`, `platform`, `account`, `module`, `task_retry_times`, `meta → metadata`, middleware lists, `task_finished`, `context`, `run_id`, `prefix_request`, and `priority`. Set status, body, headers, and cookies from the actual download. Missing correlation fields can break follow-up task handling and parsing. `examples/custom_downloader.rs` contains a complete, compiling constructor.

A browser or Servo can be integrated through this interface as an external downloader; this repository does not include a built-in browser downloader. Define navigation timeouts, cookies and headers, proxies, response status, and resource cleanup, then verify the adapter with a mock or local test site before a live target.
