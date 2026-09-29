# Proxies and downloaders

> **中文版：** [代理与下载器](zh/proxies-and-downloaders.md)

mocra can attach a fixed proxy to a request, select one from a managed pool, or use a custom downloader. These paths have different configuration and feedback behavior.

## Choose a proxy path

| Need | API or setting | Behavior |
| --- | --- | --- |
| One fixed proxy on a `Spider` request | `Request::use_proxy(proxy)` | Sends through that proxy; no managed pool feedback or default rotation |
| Engine-managed selection and retry rotation | `[proxy]` in engine TOML plus module JSON `{"enable_proxy": true}` | Selects from the pool, reports attempts, and may choose another proxy on retry |
| A proxy pool outside the crawler | `ProxyManager::from_config` or `from_proxy_config` | Caller selects proxies and reports outcomes |

The simple, DB-less `Spider` builder does not currently expose a module-config setter for `enable_proxy`. Its directly usable proxy example is [`examples/explicit_proxy.rs`](../examples/explicit_proxy.rs); it needs a reachable proxy in `MOCRA_PROXY_URL`. To exercise pool selection without a network, run `cargo run --example proxy_pool`.

## Configure an engine-managed pool

Add a proxy section to an otherwise valid engine TOML file:

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

Then set `{"enable_proxy": true}` in the database module's JSON configuration. The engine leaves an explicitly set request proxy in place. A standalone TOML string passed to `ProxyManager::from_config` uses root-level `[[direct]]` rather than `[[proxy.direct]]`. The complete schema, including tunnel and IP providers, is in [Configuration](configuration.md#proxy).

## Failure feedback and rotation

The engine accounts for a managed proxy attempt when download starts, then reports success or failure. Network and timeout errors, proxy handshake 407, and provider-configured retry status codes can mark a proxy failure. An ordinary target HTTP error is not automatically a proxy fault. Retrying uses the existing request retry budget (`crawler.request_max_retries`); rotation does not add attempts beyond that budget.

Automatic retry rotation is limited to `GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`, and `WSS`. A `POST` is not automatically replayed through another proxy. The next selection excludes the failed proxy when possible; if no alternate is available, the remaining retry still follows the configured budget.

`Request::use_proxy` is fixed by default. In a DB-backed module, `{"auto_rotate_explicit_proxy": true}` permits a replayable failed explicit request to switch to an engine pool proxy on retry when a pool is configured. Explicit attempts do not enter managed pool feedback until a retry has switched to a pool proxy. The choice should match the target's retry semantics.

## Changing proxy configuration while running

The file config provider checks for changes about every five seconds and updates the engine's shared config, including rate limits. The engine creates its `ProxyManager` from the proxy section **once at startup**; changing `[[proxy.direct]]`, providers, or pool policy in the TOML file does not rebuild that active pool. Restart the engine to apply those proxy changes. Newly created requests can still specify a different explicit proxy with `Request::use_proxy`. A standalone `ProxyManager` exposes `add_ip_provider` and `add_tunnel` for programmatic additions; it does not expose a general replacement API for direct proxies.

When using `ProxyManager` directly, call `get_proxy_for_attempt(excluded)`, then `begin_proxy_attempt(&proxy)` immediately before sending, and finally `report_success(&proxy, latency)` or `report_failure(&proxy)` from the real result. Merely selecting a proxy does not consume an attempt. See [`examples/proxy_pool.rs`](../examples/proxy_pool.rs) for the API sequence; its outcomes are simulated.

## Pool and client-cache tuning

- `proxy.pool_config.max_size` limits dynamically loaded IP proxies **per provider**; it does not cap the number of static `[[proxy.direct]]` entries. `health_check_interval_secs = 0` disables scheduled checks; `health_check_concurrency` caps simultaneous probes.
- The pool samples two eligible candidates and favors the higher quality score. Tunnel proxies retain priority; expiry, error limits, rate limits, and retry exclusion are applied before selection. See the [selection comparison](../benchmarks/proxy-selection.md) for the local tradeoff.
- `download_config.proxy_client_cache_capacity` bounds cached proxy-specific HTTP clients (default 1000; `0` disables caching). Compare cache hit/miss, client creation, bypass, eviction, idle connections, and RSS before changing it. See the [local cache measurement](../benchmarks/proxy-client-cache.md).
- The engine exposes `mocra_download_attempts_total`, `mocra_proxy_faults_total`, `mocra_proxy_rotations_total`, `mocra_proxy_selection_errors_total`, and proxy-client-cache counters at `/metrics` when the dashboard API is enabled. The standalone manager provides `selection_wait_stats()` and `attempt_stats()`.

## Custom downloaders

Implement `mocra::prelude::downloader::Downloader` and register it with `.default_downloader(d)` to replace reqwest globally. `.downloader(d)` registers a named implementation chosen by the module's `downloader` config; the DB-less `Spider` builder has no module-config setter for this route. A browser or Servo can be integrated this way, but this repository does not provide an included browser implementation.

A custom downloader must decide how to honor `Request.proxy`, headers, cookies, and timeout; registering it does not make reqwest handle those fields on its behalf.

A custom downloader's `Response` must copy the incoming request's correlation fields (`id`, account, platform, module, metadata, run/context, middleware lists, retry count, priority, and prefix request). Fill status, body, headers, and cookies from the actual fetch. The complete, offline implementation in [`examples/custom_downloader.rs`](../examples/custom_downloader.rs) is the reference.
