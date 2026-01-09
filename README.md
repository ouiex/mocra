# Mocra

Mocra is a high-performance, distributed, modular crawler framework written in Rust. It is built to be scalable and robust, capable of handling complex crawling scenarios including distributed task orchestration, dynamic module configuration, and JavaScript execution.

[中文文档](README_ZH.md)

## 🏗 Distributed Architecture & Data Lifecycle

Mocra adopts a modular pipeline architecture designed for high concurrency and distributed deployment.

### 1. System Components
*   **Engine**: The core orchestrator. It runs asynchronously, pulling tasks from the queue and managing the lifecycle of requests.
*   **Queue (Queue Manager)**: Abstracts underlying message brokers (Redis/Kafka). It handles task distribution, compensation (dead-letter queues), and message acknowledgment.
*   **Downloader Manager**: Handles HTTP/WebSocket data fetching with support for proxies, cookies, and rate limiting.
*   **State & Configuration**: Global state management backed by PostgreSQL for dynamic configuration (accounts, modules, platforms).

### 2. Data Lifecycle & Execution Flow

The data flow determines how a crawl task is processed from initiation to storage.

1.  **Task Ingestion**:
    *   `TaskModel` objects are pushed into the **Queue** (Redis or Kafka).
    *   The **Engine** consumes these tasks.

2.  **Request Generation**:
    *   The `TaskModel` is passed to the specified **Module**.
    *   The Module's `generate` method executes business logic to produce a stream of `Request` objects.

3.  **Downloading Phase**:
    *   `Request`s are processed by the **Downloader**.
    *   **Download Middlewares** intercept requests (e.g., adding headers, signing requests).
    *   The Downloader (HTTP/WSS) fetches the content.
    *   **Proxies** are automatically rotated via `ProxyManager`.

4.  **Response & Parsing**:
    *   The raw `Response` is returned.
    *   The Module's `parser` method processes the `Response`, extracting data or generating new follow-up requests.
    *   Output is encapsulated in `ParserData`.

5.  **Data Processing & Storage**:
    *   `ParserData` flows through **Data Middlewares** (cleaning, validation).
    *   Finally, **Data Store Middlewares** persist the data to the configured backend (PostgreSQL, Files, etc.).

## 📦 Module Overview

Mocra is organized as a Rust workspace containing several specialized crates:

| Crate | Functionality |
| :--- | :--- |
| **`engine`** | The central nervous system. Coordinates the event loop, connects the queue with processors, and manages the overall task lifecycle. |
| **`common`** | Defines core traits (`ModuleTrait`, `Downloader`, `Middleware`) and shared data models (`Request`, `Response`, `Config`). Essential for implementing custom business logic. |
| **`downloader`** | Provides implementation for network IO. Supports `reqwest` for HTTP/HTTPS and `tokio-tungstenite` for WebSocket connections. |
| **`queue`** | Abstraction layer for distributed queues. Currently supports **Redis** (Stream/List) and **Kafka**. Includes logic for task compensation and reliability. |
| **`proxy`** | Manages proxy lifecycles. Handles IP rotation, validation, and scoring protocols. |
| **`cacheable`** | A caching interface supporting local memory (DashMap) and distributed storage (Redis). Used for caching configurations or ephemeral states. |
| **`sync`** | Distributed synchronization primitives (like Locks and Barriers) built on top of Redis, ensuring atomic operations across distributed nodes. |
| **`js-v8`** | Integrates the V8 JavaScript engine. Allows the crawler to execute JS code handling complex encryption or dynamic token generation logic directly within Rust. |
| **`scheduler`** | (Optional) specialized scheduling logic for timed or periodic tasks. |
| **`utils`** | A utility library containing helpers for encryption, date parsing, rate limiting, and database interactions. |

## ⚙️ Configuration & Database Setup

Mocra relies on a **PostgreSQL** database to manage the dynamic configuration of crawling modules. This allows you to update business limits, accounts, or target platforms without recompiling the code.

### 1. Database Configuration
Before running the system, you must configure the connection in `config.tom` (or `config.dev.toml` for development):

```toml
[db]
database_host = "localhost"
database_port = 5432
database_user = "mocra_user"
database_password = "password"
database_name = "crawler"
```

### 2. Business Module Configuration (PostgreSQL)
Unlike static configuration, execution logic is heavily driven by database tables. You need to set up the corresponding tables for your modules.

*   **`module` table**: Registers the crawl module (name, version, etc.).
*   **`platform` table**: Defines target site configurations (domain, base URL).
*   **`account` table**: Manages login credentials if the module requires authentication.
*   **Relationship tables**: Linking tables define how modules interact with specific accounts or platforms (e.g., specific rate limits for a VIP account).

The `ModuleConfig` struct in `common/model_config.rs` attempts to load these configurations at runtime. Ensure your test data is inserted into Postgres before launching the engine, otherwise, the module may fail to initialize or find its configuration context.

## 🚀 Getting Started

### Prerequisites
*   **Rust**: Stable toolchain.
*   **Services**: Redis, PostgreSQL (Required); Kafka (Optional).

### Installation
1.  Clone the repository:
    ```bash
    git clone https://gitlab.ouiex.dev/eason/mocra.git
    cd mocra
    ```
2.  Setup Database:
    *   Create the `crawler` database in Postgres.
    *   Import the initial schema (refer to `schema.sql` if provided or initialize tables manually).
3.  Configure:
    *   Copy `tests/config.dev.toml` and adjust connection strings.
4.  Run:
    ```bash
    cargo run -p tests
    ```

## 📄 License
MIT OR Apache-2.0
