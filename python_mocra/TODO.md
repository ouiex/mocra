# Mocra Python TODO

> 生成日期：2026-02-03

## 对齐缺失模块

- [x] 新增 utils/distributed_rate_limit.py（分布式平滑限流）
- [x] 新增 utils/batch_buffer.py（批量缓冲器）
- [x] 新增 utils/connector.py（Redis/DB 连接池工具）
- [x] 新增 utils/string_case.py（多种 case 转换）
- [x] 新增 utils/to_numeric.py（中文数字/单位解析）
- [x] 新增 utils/type_convert.py（JSON/Excel 值 -> Polars Series）
- [x] 新增 utils/excel_dataframe.py（Excel -> DataFrame）
- [x] 新增 utils/storage.py（BlobStorage/Offloadable 抽象）

## 通用存储层

- [x] 新增 common/interfaces/storage.py
- [x] 新增 common/storage/file.py 与 common/storage/__init__.py

## 代理系统

- [x] 新增 proxy/pool.py（代理池、统计、失败淘汰）
- [x] 扩展 proxy/manager.py 使用代理池与结果上报

## 校验

- [x] 更新 utils/__init__.py（导出新增工具）
- [ ] 视情况补充测试（基础路径可选）
