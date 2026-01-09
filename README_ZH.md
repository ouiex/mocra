# Mocra

Mocra 是一个基于 Rust 编写的高性能、分布式、模块化爬虫框架。它设计用于处理复杂的爬取场景，支持分布式任务编排、动态模块配置以及集成 JavaScript 执行环境。

## 🏗 分布式架构与数据生命周期

Mocra 采用专为高并发和分布式部署设计的模块化流水线架构。

### 1. 系统组件
*   **Engine (引擎)**: 核心协调器。它是全异步运行的，负责从队列中拉取任务并管理请求的整个生命周期。
*   **Queue (任务队列)**: 对底层消息代理（Redis/Kafka）的抽象。负责任务分发、补偿机制（死信队列）以及消息确认 (ACK)。
*   **Downloader Manager (下载管理器)**: 处理 HTTP/WebSocket 数据获取，支持代理管理、Cookie 自动维护和全局限速。
*   **State (状态管理)**: 基于 PostgreSQL 的全局配置与状态管理，支持动态加载账号、模块和平台的配置。

### 2. 数据生命周期与执行流程

数据流决定了一个爬虫任务从初始化到持久化的全过程：

1.  **任务接入 (Task Ingestion)**:
    *   `TaskModel` 对象被推送到 **Queue** (Redis 或 Kafka)。
    *   **Engine** 消费这些任务。

2.  **请求生成 (Request Generation)**:
    *   `TaskModel` 被传递给指定的 **Module (模块)**。
    *   模块的 `generate` 方法执行业务逻辑，生成 `Request` 对象流。

3.  **下载阶段 (Downloading Phase)**:
    *   `Request` 被 **Downloader** 接收。
    *   **Download Middlewares (下载中间件)** 拦截请求（例如：添加签名、Headers）。
    *   下载器（HTTP 或 WebSocket）执行网络请求。
    *   通过 `ProxyManager` 自动轮转和管理 **代理 IP**。

4.  **响应与解析 (Response & Parsing)**:
    *   获取原始 `Response`。
    *   模块的 `parser` 方法处理响应，提取数据或生成后续的请求。
    *   输出被封装为 `ParserData`。

5.  **数据处理与存储 (Data Processing & Storage)**:
    *   `ParserData` 流经 **Data Middlewares (数据中间件)** 进行清洗和校验。
    *   最后，**Data Store Middlewares (存储中间件)** 将数据持久化到配置的后端（PostgreSQL, 文件系统等）。

## 📦 模块概览

Mocra 作为一个 Rust workspace 管理，包含多个功能各异的 crate：

| Crate | 功能描述 |
| :--- | :--- |
| **`engine`** | 中央神经系统。协调事件循环，连接队列与处理器，并管理整体任务生命周期。 |
| **`common`** | 定义核心 Trait (`ModuleTrait`, `Downloader`, `Middleware`) 和共享数据模型 (`Request`, `Response`, `Config`)。这是实现自定义业务逻辑的基础。 |
| **`downloader`** | 提供网络 IO 实现。支持基于 `reqwest` 的 HTTP/HTTPS 请求和基于 `tokio-tungstenite` 的 WebSocket 连接。 |
| **`queue`** | 分布式队列的抽象层。目前支持 **Redis** (Stream/List) 和 **Kafka**。包含任务补偿和可靠性保障逻辑。 |
| **`proxy`** | 管理代理 IP 的生命周期。处理 IP 轮转、有效性验证和评分协议。 |
| **`cacheable`** | 缓存抽象层，支持本地内存 (DashMap) 和分布式存储 (Redis)。用于缓存配置或临时状态。 |
| **`sync`** | 基于 Redis 构建的分布式同步原语（如分布式锁、栅栏），确保跨节点的原子操作。 |
| **`js-v8`** | 集成 V8 JavaScript 引擎。允许爬虫直接在 Rust 中执行 JS 代码，处理复杂的加密参数生成或动态 Token 逻辑。 |
| **`scheduler`** | (可选) 专门的调度逻辑，用于定时或周期性任务。 |
| **`utils`** | 工具库，包含加密算法、日期解析、限流器和数据库交互辅助函数。 |

## ⚙️ 配置与数据库设置

Mocra 依赖 **PostgreSQL** 数据库来管理爬虫模块的动态配置。这使得你可以在不重新编译代码的情况下更新业务限制、账号或目标平台信息。

### 1. 数据库连接配置
在运行系统之前，必须在 `config.dev.toml` (开发环境) 或 `config.toml` 中配置数据库连接：

```toml
[db]
database_host = "localhost"
database_port = 5432
database_user = "mocra_user"
database_password = "password"
database_name = "crawler"
```

### 2. 业务模块配置 (PostgreSQL)
不同于静态配置文件，执行逻辑严重依赖数据库表中的配置。你需要为你的模块设置相应的表数据。

*   **`module` 表**: 注册爬虫模块信息（名称、版本等）。
*   **`platform` 表**: 定义目标站点的配置（域名、Base URL）。
*   **`account` 表**: 如果模块需要认证，在此管理登录凭证。
*   **关联配置表**: 定义模块如何与特定的账号或平台交互（例如：为 VIP 账号配置特殊的限流规则）。

`common/model_config.rs` 中的 `ModuleConfig` 结构体负责在运行时加载这些配置。在启动引擎之前，请确保已在 Postgres 中插入了相应的测试数据，否则模块可能因找不到配置上下文而初始化失败。

## 🚀 快速开始

### 前置要求
*   **Rust**: 最新稳定版工具链。
*   **基础服务**: Redis, PostgreSQL (必需); Kafka (可选)。

### 安装与运行
1.  克隆仓库:
    ```bash
    git clone https://gitlab.ouiex.dev/eason/mocra.git
    cd mocra
    ```
2.  设置数据库:
    *   在 Postgres 中创建 `crawler` 数据库。
    *   导入初始 Schema（参考项目中的 sql 文件或手动建表）。
3.  配置文件:
    *   复制 `tests/config.dev.toml` 并修改连接字符串。
4.  运行测试 Demo:
    ```bash
    cargo run -p tests
    ```

## 📄 许可证
MIT OR Apache-2.0
