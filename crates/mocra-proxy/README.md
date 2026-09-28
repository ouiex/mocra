# mocra-proxy

Configuration-driven **proxy pool / manager** for the [mocra](https://github.com/ouiex/mocra)
distributed crawler framework — standalone, zero-`State`, usable on its own.

## Features

- Multi-provider proxy pool: tunnels, IP providers, health checks, rotation, expiry recovery.
- TOML configuration (`ProxyConfig::load_from_toml`).
- Own error type [`ProxyError`] / `Result` — no reverse dependency on the host crate; the
  host maps it via `From<ProxyError>`.

## Example

```rust,ignore
use mocra_proxy::{ProxyConfig, ProxyManager};

let manager = ProxyManager::from_config(&toml_str).await?;
let proxy = manager.get_proxy(None).await?;      // pick a proxy
manager.report_success(&proxy, None).await?;     // feedback for health tracking
```

`pool_config.max_size` caps dynamically loaded IP proxies **per provider**. Invalid
`min_size`/`max_size` values are clamped to a valid range with a warning. Direct
proxies can set an RFC 3339 `expire_time`. A `rate_limit` between 0 and 1 means
one request every `1 / rate_limit` seconds.

Health checks run every `health_check_interval_secs` seconds when `ProxyManager`
is created inside Tokio (default 300; zero disables scheduling). Probes are bounded
by `health_check_concurrency` (default 8), and the task is cancelled when the
manager is dropped or `stop_health_checks()` is called. Pool statistics are
recomputed when requested, so proxy feedback does not scan the whole pool.

Proxy selection samples two eligible candidates and prefers the higher quality
score. Tunnel proxies still take priority over IP proxies; expiry, error limits,
rate limits and failed-proxy exclusion apply before sampling.
`ProxyManager::selection_wait_stats()` reports cumulative selection write-lock
wait time, and `attempt_stats()` reports started requests, successful and failed
feedback, and rate-limit events for the manager's lifetime.

Part of the [mocra](https://github.com/ouiex/mocra) workspace.

## License

Licensed under either of MIT or Apache-2.0 at your option.
