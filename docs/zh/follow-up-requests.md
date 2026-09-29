# Spider 后续请求

> **English version:** [Follow-up requests in a Spider](../follow-up-requests.md)

在 `Spider::parse` 中使用 `Ctx` 产出数据或安排后续请求。后续响应会再次进入**同一个** `parse` 方法。可运行 `cargo run --example follow_request` 查看无需网络的完整演示。

## 选择操作

| 操作 | 产生的请求 | 适用场景 |
| --- | --- | --- |
| `seeds.get(url)` | 在 `Spider::start` 中新建 GET | 初始页面 |
| `seeds.add(request)` | 在 `Spider::start` 中加入完整 `Request` | 初始 POST 或自定义请求 |
| `cx.follow_get(url)` | 在 `Spider::parse` 中新建 GET | 简单翻页或详情链接 |
| `cx.follow(request)` | 在 `Spider::parse` 中加入完整 `Request` | POST、自定义请求头、Cookie、元数据、超时或代理 |

例如，解析器可以安排带路由标记的 POST：

```rust
use mocra::prelude::*;

let mut next = Request::new("https://example.org/api/page", RequestMethod::Post)
    .with_body(b"page=2".to_vec())
    .add_meta("page", 2_u32);
next.headers = Headers::new().add("content-type", "application/x-www-form-urlencoded");
next.timeout = 20; // 秒
cx.follow(next);
```

下一个响应可通过 `response.get_meta::<u32>("page")` 读取标记。`Response` 没有独立保存原始 URL 字段；区分页面类型时可以使用元数据或响应内容。`cx.follow_get(url)` 会新建 GET；下一请求不止需要 URL 时，使用 `cx.follow(request)`。

## 保留的字段

`cx.follow(request)` 会将完整请求序列化到解析任务队列，并在下载前还原。请求方法、请求头、Cookie、参数、JSON/form/body、超时、请求元数据、显式代理、下载器名称和优先级都会经过这一步。引擎处理请求时仍会应用正常的任务上下文与模块配置。旧版已入队的“仅 URL”后续任务仍可作为 GET 读取。

新请求**不会自动继承**前一个请求的请求头、body、Cookie 或代理。后续请求需要哪些字段，就显式设置哪些字段。翻页应设终止条件，详情页应避免重复入队。

实现自定义 `Downloader` 时，要把请求的关联字段复制到 `Response`，否则响应可能无法回到正确的 spider 任务。参见[代理与下载器](proxies-and-downloaders.md#自定义下载器)和 [`examples/custom_downloader.rs`](../../examples/custom_downloader.rs) 中的完整构造。

## 验证流程

`cargo run --example follow_request` 使用本地回显下载器：从 GET 开始，安排 POST，最后打印下载器收到的方法、请求头、body、Cookie 与超时；整个流程不访问网络。修改队列交接逻辑时，再运行 `cargo test -p mocra --lib follow_request`，覆盖序列化后的任务元数据和实际下载路径。
