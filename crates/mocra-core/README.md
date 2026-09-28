# mocra-core

Shared runtime types for the [mocra](https://github.com/ouiex/mocra) crawler framework.

Currently this crate hosts the framework's error types (`mocra_core::errors`). The rest of the
shared runtime (domain models, cache service, the crawling pipeline) is being migrated here
incrementally so the host `mocra` crate can become a thin facade over reusable,
independently-compilable crates.

Not intended for direct use yet — depend on `mocra` instead.

For local queues, `channel_config.capacity` is the capacity of each channel,
including the log channel, and must be greater than zero. `queue_codec` defaults
to MessagePack and is configured independently for each `QueueManager`.

Managed proxy downloads report one result for each HTTP or WebSocket attempt and
rotate after proxy transport, authentication, or provider retry-code failures.
Rotation uses the existing download retry budget. An explicitly set `Request.proxy`
stays fixed by default; set `auto_rotate_explicit_proxy = true` in the module config
to allow rotation to a pool proxy. The counters `mocra_download_attempts_total`,
`mocra_proxy_faults_total`, `mocra_proxy_rotations_total`, and
`mocra_proxy_selection_errors_total` distinguish attempts from final requests.

`download_config.proxy_client_cache_capacity` bounds cached proxy-specific HTTP
clients (default 1000; zero disables caching). Cache hit, creation, bypass and
eviction counters allow capacity decisions from production traffic.
