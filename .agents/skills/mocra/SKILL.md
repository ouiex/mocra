---
name: mocra
description: Default English guidance for building, debugging, and extending Rust crawlers with mocra. Use for Spider flows, follow-up requests, proxies, downloaders, modules, DAGs, and this repository; use mocra-zh for Chinese instructions.
---

# Developing with mocra

The [Chinese version](../mocra-zh/SKILL.md) covers the same workflows. This skill applies to the current mocra repository and projects using its API. Check `Cargo.toml` for the version, identify whether the task concerns a crawler, download capability, or the runtime, then read only the relevant reference. The user's requirements and existing code take priority over examples here.

## Choose a path

| Task | Read first | Source or example |
| --- | --- | --- |
| Build or fix a `Spider`, parsing, pagination, or output | [Spider and request flow](references/spider.md) | `src/facade.rs`, `examples/spider_quickstart.rs`, `examples/follow_request.rs` |
| Proxy selection, failure rotation, custom or browser downloader | [Proxies and downloaders](references/network.md) | `crates/mocra-proxy/`, `crates/mocra-core/src/engine/chain/proxy_attempt.rs`, `examples/custom_downloader.rs` |
| Modules, DAGs, queues, clusters, configuration, or runtime performance | [Runtime and validation](references/runtime.md) | `crates/mocra-core/`, `docs/architecture.md`, `docs/configuration.md` |

Read multiple references only when the task crosses those paths. `docs/README.md` indexes the project documentation, including the [follow-up request](../../../docs/follow-up-requests.md), [proxy/downloader](../../../docs/proxies-and-downloaders.md), and [runtime tuning](../../../docs/runtime-tuning.md) guides. `examples/README.md` lists runnable examples and prerequisites. If docs disagree with the code, verify the current type definitions, call path, and tests, then correct the affected docs.

## Implementation decisions

1. For ordinary crawlers, start with `mocra::prelude::*`, `Spider`, and `Mocra::builder()`. The default run uses one process, in-memory queues, and no database. Use the lower-level module API for the account × platform × module task model or a custom DAG.
2. Preserve caller-specified fields on follow-up requests. `Ctx::follow(Request)` queues a complete request; `follow_get(url)` creates a new GET. Changes to this path should cover method, headers, body, cookies, timeout, metadata, explicit proxy, and priority. See `examples/follow_request.rs`.
3. Distinguish an explicit proxy from a managed pool. `Request::use_proxy` fixes the proxy for one request; managed selection also needs engine proxy configuration and module JSON with `enable_proxy`. A standalone `ProxyManager` example does not demonstrate automatic `Spider` rotation.
4. A custom `Downloader` must carry request correlation fields into its `Response`; otherwise the parser may lose the task association. Follow the complete construction in `examples/custom_downloader.rs`.
5. When changing a public API, inspect the affected claims in `README.md`, `README.zh.md`, `docs/`, and `examples/`. Check old snippets against current source before reusing them.

## Validate the outcome

Choose checks that match the change: `cargo fmt --all -- --check`, relevant crate tests, `cargo check --examples`, and checks for affected features. For a new crawler, confirm output using a small live target or local response. If network access is unavailable, state whether verification was compilation-only or offline. Examples requiring a real proxy, external site, or distributed service are not offline tests.
