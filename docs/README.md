# mocra Documentation

Welcome to the mocra documentation. This directory contains the full technical documentation for the framework.

> **中文版:** [docs/zh/README.md](zh/README.md)

## Documentation Index

| Document | Description |
|---|---|
| [Getting Started](getting-started.md) | Installation, minimal example, and first run |
| [Architecture](architecture.md) | System architecture, processing pipeline, and design decisions |
| [Module Development](module-development.md) | How to implement `ModuleTrait` and `ModuleNodeTrait` |
| [DAG Execution](dag-guide.md) | DAG definition, fan-out/fan-in, and execution semantics |
| [Middleware](middleware-guide.md) | Download, data, and storage middleware development |
| [Configuration](configuration.md) | Full configuration reference (TOML) |
| [API Reference](api-reference.md) | Built-in HTTP control plane and metrics |
| [Deployment](deployment.md) | Single-node vs distributed mode, monitoring, and operations |

## Design & Refactoring (设计与重构)

| Document | Description |
|---|---|
| [Refactoring Plan (中文)](refactor/README.md) | 从企业级采集平台重构为可嵌入的分布式爬虫库:现状评估、目标 API、集群架构(redb+Raft 控制面 / 可插拔 MQ 数据面)、实施路线 |

## Quick Links

- **Repository:** <https://github.com/ouiex/mocra>
- **API Docs (docs.rs):** <https://docs.rs/mocra>
- **Crate (crates.io):** <https://crates.io/crates/mocra>
