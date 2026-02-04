> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Utils Module Design

## Overview
The `utils` module is a collection of general-purpose utility functions, helper structs, and shared logic that doesn't fit into a specific domain domain (like engine or downloader). It promotes code reuse and separation of concerns.

## Key Components

### 1. Distributed Primitives
- **`DistributedSlidingWindowRateLimiter`**: Implements a rate limiter using Redis Lua scripts to track usage across multiple nodes. Uses a sliding window algorithm for precision.
- **`DistributedLockManager`**: Provides distributed locking capabilities using Redis (Redlock algorithm simplified).
- **`BatchBuffer`**: A utility to collect items and flush them in batches based on size or time (used for high-performance database inserts).

### 2. Infrastructure Helpers
- **`connector`**: Functions to establish connections to PostgreSQL (`sea-orm`) and Redis (`deadpool-redis`), handling configuration and pool creation.
- **`logger`**: Configures the application's logging system. Built on `tracing-subscriber`, supports JSON/text output, file rotation, and optional queue forwarding.
	- **简化初始化**: 使用 `init_app_logger(namespace)` 一行初始化，默认写入 `logs/mocra.{namespace}`。
	- **环境变量**:
		- `MOCRA_LOG_LEVEL` (默认: `info,engine=debug;sqlx=warn,sea_orm=warn,h2=warn,hyper=warn`)
		- `MOCRA_LOG_FILE` (设为 `none/off/false` 可关闭文件输出)
		- `MOCRA_LOG_CONSOLE` (`true/false/1/0`)
		- `MOCRA_LOG_JSON` (`true/false/1/0`)
		- `DISABLE_LOGS` / `MOCRA_DISABLE_LOGS` (禁用所有日志)
- **`device_info`**: Retrieves system information (CPU, Memory, OS) for monitoring or fingerprinting.

### 3. Data Manipulation
- **`date_utils`**: Helpers for parsing and formatting dates, handling various formats.
- **`type_convert`**: Robust conversion between different data types (String to Int, etc.), handling edge cases.
- **`string_case`**: Utilities for converting string cases (camelCase, snake_case, etc.).
- **`encrypt`**: Basic encryption/hashing helpers (MD5, SHA1).

### 4. Data Science / Analysis
- **`polars_utils`**: Extensions and helpers for working with `Polars` DataFrames.
- **`excel_dataframe`**: Utilities to read/write Excel files into DataFrames.

## Design
The `utils` module is designed to be:
- **Dependency-free** (mostly): It should not depend on `engine` or `common` to avoid circular dependencies.
- **Stateless**: Functions are pure where possible.
- **Robust**: Extensive error handling in conversion logic.
