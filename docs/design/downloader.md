> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Downloader Design Document

## Overview
The Downloader module handles all HTTP and WebSocket communications. It is designed to be extensible, reliable, and respectful of target server limits.

## Components

### DownloaderManager
The central registry and factory for downloaders.
-   **Registry**: Manages available downloader implementations.
-   **Task Binding**: Creates and caches downloader instances for specific tasks (modules).
-   **Lifecycle**: automatically cleans up expired/idle downloaders to free resources.

### RequestDownloader
The default HTTP downloader based on `reqwest`.
-   **Connection Pooling**: Shares `reqwest::Client` across requests to optimize TCP connections.
-   **Rate Limiting**: Integrated `DistributedSlidingWindowRateLimiter`.
-   **Distributed Locking**: Optional granular locking (Run ID + Module ID) for sequential tasks.
-   **Caching**: Supports caching responses (Headers, Cookies) via `CacheService`.
-   **Proxy Support**: Dynamic proxy switching and authentication.

### WebSocketDownloader
Handles persistent WebSocket connections for streaming data.

## Features

### Rate Limiting
Uses a distributed sliding window algorithm backed by Redis. Supports handling clock skew via Redis TIME.

### Caching
Implements a smart compression strategy (gzip > 1KB) for caching request/response data in Redis to save bandwidth and storage.
