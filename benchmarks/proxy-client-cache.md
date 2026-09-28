# Proxy Client cache measurement

Run the ignored local workload with:

```bash
cargo test -p mocra-core --lib proxy_client_cache_workload -- --ignored --nocapture
```

The workload starts 16 local keep-alive HTTP proxies and sends three sequential
requests through each. Five fresh test processes were run on 2026-09-28 in a
debug build. The table shows medians; counts were identical in all five runs.

| Cache capacity | Hits / 48 | Clients created | Connections accepted | Idle proxy connections | Process RSS (KiB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 0 | 48 | 48 | 0 | 25,468 |
| 8 | 16 | 32 | 32 | 8 | 31,916 |
| 16 | 32 | 16 | 16 | 16 | 37,148 |

RSS includes the test binary and local proxy servers. Each row runs after the
previous row in the same process, so the RSS difference is a workload observation,
not a per-Client allocation estimate. The local servers have negligible latency;
this test does not establish production throughput or an ideal cache size.

The default capacity remains 1000 for compatibility. Operators can set
`download_config.proxy_client_cache_capacity` to a smaller value and compare
cache hits, Client creations, bypasses, idle connections and RSS on their actual
proxy population. A simple LRU policy would churn on this 16-proxy loop with
capacity 8, while the current bounded admission policy retained 16 hits. The
existing one-hour idle eviction remains in place; the workload does not justify
replacing it with LRU or a shorter TTL yet.
