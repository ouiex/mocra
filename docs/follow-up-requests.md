# Follow-up requests in a Spider

> **中文版：** [后续请求指南](zh/follow-up-requests.md)

Use `Ctx` inside `Spider::parse` to emit records and schedule more work. A follow-up response returns to the **same** `parse` method. For an executable, network-free demonstration, run `cargo run --example follow_request`.

## Choose the right operation

| Operation | Request created | Typical use |
| --- | --- | --- |
| `seeds.get(url)` | New GET from `Spider::start` | Initial pages |
| `seeds.add(request)` | Your complete `Request` from `Spider::start` | Initial POST or custom request |
| `cx.follow_get(url)` | New GET from `Spider::parse` | Simple pagination or detail links |
| `cx.follow(request)` | Your complete `Request` from `Spider::parse` | POST, custom headers, cookies, metadata, timeout, or proxy |

For example, a parser can schedule a POST and attach a routing marker:

```rust
use mocra::prelude::*;

let mut next = Request::new("https://example.org/api/page", RequestMethod::Post)
    .with_body(b"page=2".to_vec())
    .add_meta("page", 2_u32);
next.headers = Headers::new().add("content-type", "application/x-www-form-urlencoded");
next.timeout = 20; // seconds
cx.follow(next);
```

The next response can read the marker with `response.get_meta::<u32>("page")`. `Response` does not carry the original URL as a separate field, so use metadata or response content when routing different page types. `cx.follow_get(url)` creates a fresh GET; use `cx.follow(request)` when the next request needs more than a URL.

## What is preserved

`cx.follow(request)` serializes the whole request through the parser task queue and reconstructs it before download. This includes the method, headers, cookies, parameters, JSON/form/body, timeout, request metadata, explicit proxy, downloader name, and priority. The engine still applies its normal task context and module configuration when processing that request. Older queued URL-only follow-up tasks remain readable as GET requests.

The new request does **not** implicitly inherit the preceding request's headers, body, cookies, or proxy. Set every field the follow-up actually needs. Add a stop condition for pagination and avoid scheduling the same detail page repeatedly.

If you implement a custom `Downloader`, copy the request's correlation fields into its `Response`; otherwise the response may not return to the right spider task. See [Proxies and downloaders](proxies-and-downloaders.md#custom-downloaders) and the complete constructor in [`examples/custom_downloader.rs`](../examples/custom_downloader.rs).

## Verify a flow

`cargo run --example follow_request` uses a local echo downloader. It starts with a GET, schedules a POST, and prints the method, header, body, cookie, and timeout received by the downloader. It makes no network request. When changing the queue handoff, also run `cargo test -p mocra --lib follow_request`; the tests cover serialized task metadata and the actual downloader path.
