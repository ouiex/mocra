> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Proxy Design Document

## Overview
The Proxy module manages proxy servers used for rotating IP addresses during crawling.

## Components

### ProxyManager
Loads and manages proxy configurations.

### ProxyPool
Maintains a pool of available proxies.
-   **Health Checking**: Periodically validates proxies.
-   **Scoring**: Tracks proxy performance and reliability.
-   **Strategy**: Picks the highest-scored proxy while enforcing per-proxy rate limits.
-   **Providers**: Builds IP proxy loaders from config and supports generic text lists (IP:PORT[:USER:PASS]).

### Integration
Integrated with `RequestDownloader` to transparently assign proxies to requests based on configuration or rotation rules.
