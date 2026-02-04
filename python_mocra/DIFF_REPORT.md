# Mocra Python vs Rust 差异报告

> 生成日期：2026-02-03

本报告对比 Rust mocra 与 python_mocra 当前代码结构，记录缺失/不完整的模块与行为。

## 1) 工具层（utils）差异

| Rust utils 模块 | Python 对应 | 现状 | 说明 |
|---|---|---|---|
| distributed_rate_limit.rs | utils/distributed_rate_limit.py | ✅ 已补全 | 增加分布式平滑限流与暂停/等待机制。 |
| batch_buffer.rs | utils/batch_buffer.py | ✅ 已补全 | 异步批量缓冲器（按大小/超时 flush）。 |
| connector.rs | utils/connector.py | ✅ 已补全 | 数据库连接池缓存与 Redis 连接池构建。 |
| string_case.rs | utils/string_case.py | ✅ 已补全 | 多种字符串 case 转换。 |
| to_numeric.rs | utils/to_numeric.py | ✅ 已补全 | 中文数字/单位解析与百分号扩展。 |
| type_convert.rs | utils/type_convert.py | ✅ 已补全 | JSON/Excel 值 -> Polars Series。 |
| excel_dataframe.rs | utils/excel_dataframe.py | ✅ 已补全 | Excel -> DataFrame 解析（依赖 openpyxl/xlrd）。 |
| storage.rs | utils/storage.py | ✅ 已补全 | BlobStorage/Offloadable 抽象与文件实现。 |

## 2) 通用存储层（common/storage）差异

| Rust common 模块 | Python 对应 | 现状 | 说明 |
|---|---|---|---|
| common/interface/storage.rs | common/interfaces/storage.py | ✅ 已补全 | 导出 storage 抽象接口。 |
| common/storage/file.rs | common/storage/file.py | ✅ 已补全 | FileBlobStorage 文件落盘实现。 |

## 3) 代理系统（proxy）差异

| Rust proxy 模块 | Python 对应 | 现状 | 说明 |
|---|---|---|---|
| proxy_pool.rs | proxy/pool.py | ✅ 已补全 | 代理池、失败统计、淘汰、状态统计。 |
| proxy_manager.rs | proxy/manager.py | ✅ 已增强 | 使用代理池并支持结果上报。 |

## 4) 其他差异/待确认

- Rust utils 还有部分事务/类型转换细节可进一步优化（可选）。
- 分布式限流已补全，但仍可继续优化脚本原子性与监控指标（可选）。

## 5) 下一步补全计划（对应 TODO）

详见 [TODO.md](TODO.md)。
