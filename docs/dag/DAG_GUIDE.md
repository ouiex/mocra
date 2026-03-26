# DAG Guide Index / DAG 文档索引

## Overview / 概览

EN:
The original single DAG guide has been split into two bilingual documents for maintainability and clearer ownership.

ZH:
原先单体 DAG 指南已拆分为两份双语文档，便于维护并明确职责边界。

## Documents / 文档列表

1. API reference (developer-facing) / API 参考（开发向）
   - [docs/dag/DAG_API_REFERENCE.md](docs/dag/DAG_API_REFERENCE.md)
2. Operational runbook (test/SRE/on-call-facing) / 运行手册（测试/SRE/值班向）
   - [docs/dag/DAG_RUNBOOK.md](docs/dag/DAG_RUNBOOK.md)

## Reading Order / 阅读顺序

EN:
Read API reference first, then runbook for operations.

ZH:
建议先阅读 API 参考，再阅读运行手册。

## Ownership Suggestion / 维护建议

EN:
- API reference: maintained by scheduler and node-runtime developers.
- Runbook: maintained by test, platform, and on-call owners.

ZH:
- API 参考：由调度器与节点运行时开发同学维护。
- 运行手册：由测试、平台与值班同学共同维护。

## Update Policy / 更新策略

EN:
For scheduler semantic changes, update both documents in the same PR.

ZH:
涉及调度语义变更时，要求同一 PR 同步更新两份文档。