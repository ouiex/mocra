# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — embeddable-library refactor

Breaking, structural refactor turning mocra into a genuinely usable third-party library and a
Cargo workspace. No backward compatibility with `0.2.x` internals.

### Added

- **Simple facade API** — implement a `Spider`, run with `Mocra::builder().spider(s, on_item(..)).run()`;
  typed output via `DataSink` / `on_item`. Runs with **no DB and no Redis** on a single node.
- **Embedded cluster (`cluster-embedded`)** — a self-organizing **Raft + redb** control plane
  ([`mocra-cluster`]): strongly-consistent leader election, distributed locks with monotonic
  **fencing tokens**, KV/CAS, membership + `/cluster/join`, dynamic scale up/down, leader
  failover, snapshot/log compaction, crash recovery, and **partition ownership** (rendezvous
  hashing + Raft-fenced leases). No external coordinator (ZooKeeper / etcd / Redis) required.
  Any node accepts writes (auto-forwarded to the leader). Facade: `.cluster(ClusterConfig)`.
- **NATS (JetStream) data-plane backend** (`queue-nats`) — persistent, at-least-once queue with
  ack + nack retry/DLQ, integration-tested against a real server.
- **Formal `MetadataStore` trait** — DB metadata access behind `Arc<dyn MetadataStore>` instead
  of a concrete repository; DB is optional.

### Changed

- **Split into a Cargo workspace** of reusable, independently-publishable crates that never
  depend back on the host: [`mocra-cluster`], [`mocra-dag`] (generic distributed DAG engine),
  [`mocra-proxy`] (proxy pool/manager), [`mocra-store`] (multi-tenant sea-orm entities).
- **Default dependencies slimmed** — `sea-orm`, `rdkafka`, `polars`, `calamine` moved behind
  feature flags (`store`, `queue-kafka`, `polars`, `excel`); default build no longer compiles them.
- Rate limiting shares the global limit by live cluster member count when clustered (no Redis).
- Distributed locks route through the coordination backend (Raft) when clustered.

### Removed

- Dead `ModuleProcessorWithChain` executor (superseded by the queue-driven DAG processor).

### Fixed

- Cross-node pub/sub message loss and duplicate seed-task injection in cluster mode.

[`mocra-cluster`]: crates/mocra-cluster
[`mocra-dag`]: crates/mocra-dag
[`mocra-proxy`]: crates/mocra-proxy
[`mocra-store`]: crates/mocra-store
