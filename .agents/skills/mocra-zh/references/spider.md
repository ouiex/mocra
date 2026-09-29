# Spider 与请求流

适用于编写爬虫、修复请求字段、解析响应和输出数据。公开用法以 `src/facade.rs`、`src/prelude.rs`、`docs/getting-started.md` 与 `examples/` 为准。

## 最小可运行路径

普通项目在 `Cargo.toml` 中添加 `mocra = "0.4"`、`async-trait = "0.1"`、带 Tokio 宏和运行时的 `tokio`。解析 HTML 时另加 `scraper`；mocra 提供响应字节，不自带 HTML 选择器。需要序列化输出记录时再添加 `serde`。仓库内可从 `examples/spider_quickstart.rs` 直接开始。

```rust
use async_trait::async_trait;
use mocra::prelude::*;

struct BodyLength;

#[async_trait]
impl Spider for BodyLength {
    type Item = usize;

    fn name(&self) -> &str { "body-length" }

    async fn start(&self, seeds: &mut Seeds) {
        seeds.get("https://example.org/");
    }

    async fn parse(&self, response: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(response.content.len());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(BodyLength, on_item(|bytes: usize| async move { println!("{bytes} bytes"); }))
        .run()
        .await
}
```

`Spider::Item` 只要求 `Send + 'static`；序列化是数据出口的需求，不是 `Spider` trait 的要求。`name()` 在模块标识和去重命名空间中使用，多个 spider 应使用不同名称。

## 种子、后续请求与解析

| 操作 | 作用 | 参考 |
| --- | --- | --- |
| `seeds.get(url)` | 加入 GET，返回可修改的 `&mut Request` | `examples/spider_quickstart.rs` |
| `seeds.add(req)` | 加入自行构造的任意方法请求 | `src/facade.rs` |
| `cx.emit(item)` | 将一条类型化记录交给 sink | `examples/quotes_scraper.rs` |
| `cx.follow_get(url)` | 新建后续 GET，其响应再次进入同一 `parse` | `examples/quotes_scraper.rs` |
| `cx.follow(req)` | 入队完整 `Request`，可携带 POST、headers、body、cookies 等 | `examples/follow_request.rs` |

例如后续 POST：

```rust
let mut next = Request::new("https://example.org/api/next", RequestMethod::Post)
    .with_body(b"page=2".to_vec());
next.headers = Headers::new().add("content-type", "application/x-www-form-urlencoded");
next.timeout = 20;
cx.follow(next);
```

`Ctx::follow` 会将完整请求穿过任务队列。若修复此路径，检查 `src/facade.rs` 的 `SpiderNode::parser`、`SpiderNode::generate` 与现有回归测试；只验证 URL 不足以覆盖原先的丢字段问题。翻页必须有终止条件，也要考虑重复 URL 或重复详情页。

`Response::text()` 对非 UTF-8 返回错误，`text_lossy()` 会替换非法字节；`json::<T>()` 反序列化 JSON，`content` 是原始字节。`Response::get_meta` 可读取请求携带的 trait 元数据。为真实站点写 CSS 选择器或 JSON 字段前，先查看实际页面或接口响应；离线样本可用于验证解析逻辑。

## 输出与运行

- `on_item` 适合打印或调用异步出口；`ChannelSink` 适合由调用者收集记录；自定义 `DataSink` 适合按类型写文件或数据库。看 `examples/quotes_scraper.rs` 的 JSONL 实现。
- 默认无配置的 `Mocra::builder().run()` 会自动给每个 spider 注入种子，队列空闲一段时间后退出（当前默认 30 秒）。`.dashboard(port)` 会让无配置单机模式继续运行。
- 本仓库运行 `cargo run --example follow_request` 可离线核对后续请求字段；`cargo run --example quotes_scraper` 需要访问外站并写入 `data/quotes/`。
