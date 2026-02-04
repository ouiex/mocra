> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Errors Module Design

## Overview
The `errors` module provides a centralized and unified error handling mechanism for the entire framework. It uses the `thiserror` crate to derive error implementations and ensures consistent error reporting and handling across all crates.

## Core Structure

### `Error` Struct
The main error type used throughout the application. It wraps an inner structure to keep the size small (pointer-sized).
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

### `ErrorKind` Enum
Categorizes errors into broad domains for easier pattern matching and logic handling (e.g., deciding whether to retry).
- `Request`, `Response`, `Download`
- `Proxy`, `Cookie`, `Header`
- `Queue`, `Orm`, `CacheService`
- `Service`, `Command`, `System`
- ...and more.

### Specific Error Enums
Detailed error types for specific domains, which are automatically convertible into the generic `Error` type via `From` implementations.
- `RequestError` (InvalidUrl, Timeout...)
- `ProxyError` (ProxyNotFound, AuthenticationFailed...)
- `DownloadError` (NetworkError, FileWriteError...)
- `OrmError` (ConnectionError, NotFound...)

## Design Principles

1. **Unified Type**: All public APIs return `Result<T, errors::Error>`. This simplifies function signatures and error propagation.
2. **Context**: Errors capture the `source` (underlying cause) and optional custom messages.
3. **Categorization**: Helper methods like `is_timeout()`, `is_connect()`, `is_proxy()` allow checking error properties without matching deep nested enums.
4. **Ergonomics**: Helper macros and constructors (`Error::request_timeout()`) make raising errors verbose-free.

## Usage
```rust
// Raising a specific error
if !found {
    return Err(Error::from(ProxyError::ProxyNotFound));
}

// Raising a generic error with context
match some_io_op() {
    Ok(_) => Ok(()),
    Err(e) => Err(Error::new(ErrorKind::IO, Some(e))),
}

// Checking errors
if let Err(e) = do_something() {
    if e.is_timeout() {
        retry();
    }
}
```
