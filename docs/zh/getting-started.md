# 快速上手

本指南将引导你安装 mocra、编写第一个模块并运行引擎。

## 前置条件

- **Rust 1.85+**（edition 2024）
- **PostgreSQL** 或 **SQLite** 用于元数据存储
- **Redis**（可选 — 启用分布式模式、缓存和分布式队列）
- **Kafka**（可选 — 替代队列后端）

## 安装

在 `Cargo.toml` 中添加：

```toml
[dependencies]
mocra = "0.2"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
futures = "0.3"
```

### 可选特性

| 特性 | 说明 |
|---|---|
| `mimalloc` | 使用 mimalloc 分配器（默认启用） |
| `js-v8` | 嵌入 V8 JavaScript 引擎用于脚本执行 |
| `polars` | 重新导出 Polars 用于数据处理 |

## 最小示例

下面是最简单的模块 — 单节点抓取 URL 并解析响应：

```rust
use std::sync::Arc;
use async_trait::async_trait;
use futures::stream;
use serde_json::{Map, Value};

use mocra::prelude::*;
use mocra::common::model::login_info::LoginInfo;
use mocra::common::state::State;
use mocra::engine::engine::Engine;

// 1. 定义节点
struct FetchNode;

#[async_trait]
impl ModuleNodeTrait for FetchNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new(
            "https://httpbin.org/get",
            RequestMethod::Get.as_ref(),
        );
        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        println!("状态码: {}", response.status);
        println!("响应体长度: {}", response.body.len());
        Ok(TaskOutputEvent::default())
    }
}

// 2. 定义模块
struct MyModule;

#[async_trait]
impl ModuleTrait for MyModule {
    fn name(&self) -> String { "my_module".into() }
    fn version(&self) -> i32 { 1 }
    fn should_login(&self) -> bool { false }

    fn default_arc() -> Arc<dyn ModuleTrait> { Arc::new(Self) }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(FetchNode)]
    }
}

// 3. 运行引擎
#[tokio::main]
async fn main() {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(state, None).await;
    engine.register_module(MyModule::default_arc()).await;
    engine.run().await;
}
```

## 最小配置

创建 `config.toml`：

```toml
name = "my_crawler"

[db]
url = "sqlite://data/crawler.db?mode=rwc"

[cache]
ttl = 60

[download_config]
timeout = 30
downloader_expire = 3600
rate_limit = 10
enable_session = false
enable_locker = false
enable_rate_limit = true
cache_ttl = 60
wss_timeout = 30

[crawler]
request_max_retries = 3
task_max_errors = 100
module_max_errors = 10
module_locker_ttl = 60

[channel_config]
minid_time = 12
capacity = 5000
```

## 运行

```bash
cargo run
```

引擎将：
1. 从 `config.toml` 加载配置
2. 初始化数据库、缓存和队列服务
3. 为每个注册模块编译 DAG
4. 启动处理流水线
5. 在配置的端口暴露控制 API（如果设置了 `[api]`）

## 下一步

- [系统架构](architecture.md) — 了解处理流水线
- [模块开发](module-development.md) — 多节点模块、分页、登录
- [DAG 执行](dag-guide.md) — 扇出、分支与合并模式
- [配置参考](configuration.md) — 完整配置参考
