> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Errors 模块设计

## 概览
`errors` 模块提供统一的错误表示与分类，作为全局的错误协议入口。它基于 `thiserror` 派生错误类型，保证所有 crate 的错误一致可追踪、可分类、可治理。

## 核心结构

### `Error` 结构体
系统内部统一使用的错误类型。通过内部 `ErrorInner` 间接持有内容，保证 `Error` 体积稳定。
```rust
pub struct Error {
    pub inner: Box<ErrorInner>,
}

pub struct ErrorInner {
    pub kind: ErrorKind,
    pub source: Option<BoxError>,
    pub message: Option<String>,
}
```

### `ErrorKind` 枚举
按领域对错误进行归类，便于策略决策（重试/退避/DLQ/告警）。当前主要类别包括：
- `Request`, `Response`, `Download`
- `Proxy`, `Queue`, `Orm`, `CacheService`
- `Task`, `Module`, `ProcessorChain`
- `Parser`, `DataMiddleware`, `DataStore`
- `Service`, `Command`, `RateLimit`, `DynamicLibrary`

### 具体错误枚举
各领域提供更细粒度的错误枚举，并通过 `From` 转换为统一的 `Error`：
- `RequestError`（InvalidUrl, Timeout...）
- `ProxyError`（ProxyNotFound, AuthenticationFailed...）
- `DownloadError`（NetworkError, FileWriteError...）
- `OrmError`（ConnectionError, NotFound...）

## 设计原则

1. **统一出口**：所有公共 API 返回 `Result<T, errors::Error>`。
2. **上下文保留**：保留 `source`（底层错误）与 `message`（业务描述）。
3. **清晰分类**：通过 `ErrorKind` 支持策略解析与指标聚合。
4. **易用性**：提供构造器与辅助方法，减少样板代码。

## 使用示例
```rust
// 具体错误转换
if !found {
    return Err(Error::from(ProxyError::ProxyNotFound));
}

// 携带上下文信息
match some_op() {
    Ok(_) => Ok(()),
    Err(e) => Err(Error::new(ErrorKind::Service, Some(e))),
}

// 按类型处理
if let Err(e) = do_something() {
    if e.is_download() {
        retry();
    }
}
```
