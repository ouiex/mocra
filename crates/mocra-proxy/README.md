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

Part of the [mocra](https://github.com/ouiex/mocra) workspace.

## License

Licensed under either of MIT or Apache-2.0 at your option.
