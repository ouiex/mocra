# mocra Documentation

mocra is a distributed, event-driven crawling and data-collection framework for Rust. Most
projects use the **facade**: implement a `Spider`, run it with `Mocra::builder().run()` — a
single-node, in-memory engine with no database. The same code scales to a
multi-stage DAG and a distributed cluster when you need it. Start with **Getting Started**; the
guides below go deeper into the runtime, the advanced module API, and operations.

> **中文版:** [docs/zh/README.md](zh/README.md)

## Documentation Index

| Document | Description |
|---|---|
| [Getting Started](getting-started.md) | Install mocra, write your first `Spider`, and run it — no DB |
| [Architecture](architecture.md) | The queue-driven pipeline, DAG execution engine, and single-node vs distributed |
| [Module Development](module-development.md) | **Advanced path** — `ModuleTrait` / `ModuleNodeTrait`, multi-node pipelines, passing data between nodes |
| [DAG Guide](dag-guide.md) | DAG definition, fan-out / fan-in graphs, and advance gates |
| [Middleware](middleware-guide.md) | Download, data-transformation, and storage middleware |
| [Configuration](configuration.md) | Full TOML reference (database, queues, control API) |
| [API Reference](api-reference.md) | Built-in HTTP control plane and Prometheus metrics endpoints |
| [Deployment](deployment.md) | Single-node vs distributed, monitoring, and operations |

## Runnable examples

Prefer to read code? The [`examples/`](../examples/) directory has complete, runnable programs:

- [`spider_quickstart.rs`](../examples/spider_quickstart.rs) — the minimal `Spider` (no DB).
- [`quotes_scraper.rs`](../examples/quotes_scraper.rs) — a real end-to-end crawl of [quotes.toscrape.com](https://quotes.toscrape.com): pagination, detail-page fan-out, dedup, typed items, and a custom `DataSink` writing JSONL.
- [`custom_downloader.rs`](../examples/custom_downloader.rs) — implement the `Downloader` trait and inject it with `.default_downloader()` (offline, deterministic).
- [`dashboard.rs`](../examples/dashboard.rs) — the built-in observability dashboard (`--features dashboard`).
- [`cluster_quickstart.rs`](../examples/cluster_quickstart.rs) — a self-organizing embedded cluster (`--features cluster-embedded`).

## Design & Refactoring (设计与重构)

| Document | Description |
|---|---|
| [Refactoring Plan (中文)](refactor/README.md) | Design notes on the evolution into an embeddable distributed crawler library: target API, cluster architecture (redb + Raft control plane / pluggable MQ data plane), and roadmap |

## Quick Links

- **Repository:** <https://github.com/ouiex/mocra>
- **API Docs (docs.rs):** <https://docs.rs/mocra>
- **Crate (crates.io):** <https://crates.io/crates/mocra>
