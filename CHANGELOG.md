# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.1] — 2026-09-30

### Changed

- Upgrade the Polars dependencies from 0.52.0 to 0.54.4 and align all six workspace
  crates and internal dependency requirements at 0.5.1.

### Compatibility

- Raise the facade/core minimum Rust version to 1.95 for Polars 0.54.4; enable
  Parquet alongside IPC and migrate Excel DataFrame construction to the new API.

### Fixed

- Restore Polars/Excel documentation builds on recent nightly Rust compilers by
  using the upstream fix that removes the unstable internal Unicode API dependency.

## [0.5.0] — 2026-09-29

### Added

- Offline examples for complete `Spider` follow-up requests and the standalone proxy-manager
  feedback flow, plus an explicit-proxy example that accepts `MOCRA_PROXY_URL`.
- MIT and Apache-2.0 license text files are now included in the repository.
- Repository skills in English (`$mocra`) and Chinese (`$mocra-zh`), and bilingual guides for
  follow-up requests, proxies/downloaders, and runtime tuning.

### Changed

- All six workspace crates now use version 0.5.0, including `mocra-store`.
- The facade and core crate now require Rust 1.89 for Polars/Excel. `mocra-cluster`,
  `mocra-dag`, and `mocra-proxy` require Rust 1.88 for their current dependencies or syntax.
- Local channels now honor `channel_config.capacity`, including the log channel. Batch dispatch
  waits for its concurrency permit and drains accepted work on shutdown instead of accumulating
  unbounded waiting tasks.
- The proxy pool bounds dynamically loaded IPs per provider and selects between two eligible
  candidates, favoring the higher score. Proxy-specific HTTP clients use a bounded cache
  (`download_config.proxy_client_cache_capacity`, default 1000; `0` disables it).
- The standalone `mocra-dag` scheduler waits for completion events and, when run-state storage is
  enabled, defaults to a full checkpoint every 16 successful nodes. The interval is configurable.
- DAG execution state is isolated per run; distributed task state uses the coordination backend.
- File-system blob storage can use a shared path for cluster deployments.

### Compatibility

- `mocra-cluster` 0.5.0 persists Raft state-machine metadata and snapshots. Existing 0.4.1
  data directories lack this metadata and must be backed up and migrated before upgrade; an
  in-place restart now fails explicitly rather than risking incorrect recovery.
- `PoolConfig` and `BlobStorageConfig` gained public fields. Downstream struct literals must
  account for `health_check_concurrency` and `shared_path`, respectively.
- File-system blob storage now returns relative keys from `put`; workers using a remote queue
  must share the configured path to read those keys.

### Fixed

- Embedded Raft nodes can recover after a snapshot and confirmed log purge; snapshot restore
  preserves the fencing counter.
- Quickstart examples now label `Response::module_id()` as a module identifier; configuration
  guides link to maintained samples instead of removed test fixtures.
- Managed proxy attempts report actual success or failure, with best-effort failure feedback for
  cancellation while the runtime is active. Within the existing retry budget, replayable requests
  can rotate away from a failed proxy; explicit proxies remain fixed unless a DB-backed module
  opts into `auto_rotate_explicit_proxy`.
- `Ctx::follow(Request)` now preserves the full request across the parser task queue, including
  method, headers, body, cookies, metadata, explicit proxy, and priority. Previously queued
  URL-only follow-up tasks still decode as GET requests.
- Cookie deserialization accepts `httpOnly: null` in a serialized follow-up request.
- Kafka and NATS queue startup now reports connection failures instead of leaving an unusable
  backend running.

Local [runtime acceptance results](benchmarks/runtime-acceptance.md) cover bounded queues and
managed proxy retries against local HTTP proxies. A staging or production canary and real
Kafka/NATS pressure test have not yet been run.

## [0.4.1] — 2026-07-17 — dashboard and observability

### Added

- Engine observability reports work in flight, per-stage throughput, and success rates alongside
  queue depth; the dashboard displays these values. Added the end-to-end quotes scraper example.

### Changed

- Removed Redis from the runtime; embedded Raft + redb is the distributed coordination backend.
  Rewrote the English and Chinese user guides for the 0.4 facade and translated comments and
  rustdoc to English. The dashboard example uses port 12800 by default.

### Fixed

- A configured API port that cannot be bound now fails startup. The dashboard no longer freezes
  on a stalled poll, and `Ctx::follow` no longer silently discards follow-up tasks.

## [0.4.0] — 2026-07-10 — embeddable-library refactor

Breaking, structural refactor turning mocra into a genuinely usable third-party library and a
Cargo workspace. No backward compatibility with `0.2.x` internals.

### Added

- **Simple facade API** — implement a `Spider`, run with `Mocra::builder().spider(s, on_item(..)).run()`;
  typed output via `DataSink` / `on_item`. Runs with **no DB** on a single node.
- **Embedded cluster (`cluster-embedded`)** — a self-organizing **Raft + redb** control plane
  ([`mocra-cluster`]): strongly-consistent leader election, distributed locks with monotonic
  **fencing tokens**, KV/CAS, membership + `/cluster/join`, dynamic scale up/down, leader
  failover, snapshot/log compaction, crash recovery, and **partition ownership** (rendezvous
  hashing + Raft-fenced leases). No external coordinator (ZooKeeper / etcd) required.
  Any node accepts writes (auto-forwarded to the leader). Facade: `.cluster(ClusterConfig)`.
- **NATS (JetStream) data-plane backend** (`queue-nats`) — persistent, at-least-once queue with
  ack + nack retry/DLQ, integration-tested against a real server.
- **Formal `MetadataStore` trait** — DB metadata access behind `Arc<dyn MetadataStore>` instead
  of a concrete repository; DB is optional.
- **Pluggable downloaders from the facade** — `Mocra::builder().downloader(impl Downloader)`
  registers a named downloader (routed when a request's `config.downloader` matches its
  `name()`); `.default_downloader(impl Downloader)` replaces the global default (reqwest) for
  swapping the download strategy wholesale (browser rendering, proxy rotation, custom retry, …).
  The `Downloader` trait was always pluggable; this wires user downloaders into the simple API.
- **Admin dashboard (`dashboard`)** — enabling the feature exposes a read-only, CORS-enabled
  observability HTTP API (engine/queue stats, host CPU/memory/swap, recent structured logs, Raft
  cluster status) **and** a built-in single-file web dashboard served at `GET /` — open the
  endpoint in a browser to see metrics / logs / tasks / performance, no frontend build required.
  Facade: `.dashboard(port)` (keeps a standalone engine alive and captures logs so the panels have
  data out of the box). See [`examples/dashboard.rs`].

### Changed

- **Split into a Cargo workspace** of reusable, independently-publishable crates that never
  depend back on the host: `mocra-core` (the entire runtime — errors, cache, utils, models,
  downloader, queue, sync, scheduler, engine + admin API), [`mocra-cluster`], [`mocra-dag`]
  (generic distributed DAG engine), [`mocra-proxy`] (proxy pool/manager), [`mocra-store`]
  (multi-tenant sea-orm entities).
- **The `mocra` crate is now a thin facade** — it re-exports `mocra-core` (via the `prelude`
  and per-layer shims) and adds the ergonomic `Mocra::builder()` API. Its direct dependencies
  dropped from ~65 to 12 (feature switches now forward to `mocra-core/<feature>` instead of
  re-declaring `sea-orm` / `rdkafka` / `async-nats` / `polars` / `calamine` / `tower-http`).
- **Default dependencies slimmed** — `sea-orm`, `rdkafka`, `polars`, `calamine` moved behind
  feature flags (`store`, `queue-kafka`, `polars`, `excel`); default build no longer compiles them.
- Rate limiting shares the global limit by live cluster member count when clustered.
- Distributed locks route through the coordination backend (Raft) when clustered.

### Removed

- Dead `ModuleProcessorWithChain` executor (superseded by the queue-driven DAG processor).
- **Vestigial "shadow" DAG execution path** — the parallel `mocra-dag`-`Dag`-compilation machinery
  that was precompiled/cached at module registration but never actually executed at runtime:
  the placeholder `ModuleNodeDagAdapter`, `ModuleDagCompiler`, the orchestrator's `compile_*`/
  `execute_dag` methods, `TaskManager`'s per-module compiled-DAG cache + `DagCutoverStateTracker`,
  and the public **`Engine::get_module_dag`** (BREAKING). Module DAGs now have a single path:
  `ModuleDagOrchestrator::build_definition` → the queue-backed `ModuleDagProcessor`. This also
  removes wasted DAG precompilation on every module registration.

### Fixed

- Cross-node pub/sub message loss and duplicate seed-task injection in cluster mode.

[`mocra-cluster`]: crates/mocra-cluster
[`mocra-dag`]: crates/mocra-dag
[`mocra-proxy`]: crates/mocra-proxy
[`mocra-store`]: crates/mocra-store
[`examples/dashboard.rs`]: examples/dashboard.rs
