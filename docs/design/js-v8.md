> 已合并至 `docs/README.md`，本文件保留为历史参考。

# JS-V8 Module Design

## Overview
The `js-v8` module provides a thread-safe, asynchronous interface to execute JavaScript code within the Rust application. It leverages the V8 engine (via `rusty_v8`) to run custom scripts, such as dynamic signature generation or data parsing logic.

## Architecture

### The Concurrency Problem
V8 Isolates are single-threaded and not thread-safe. They cannot be shared across Rust async tasks or threads directly.

### The Solution: Worker Pool
To bridge async Rust and single-threaded V8, this module implements a **Actor-like Worker Pool model**.

1.  **`JsWorker` (The Actor)**
    *   Runs in a dedicated, long-lived OS thread.
    *   Owns the `V8Engine` instance.
    *   Listens on a generic `mpsc` channel for `JsJob`s.
    *   Executes the JS function and sends the result back via a `oneshot` channel.
    *   Catches panics to ensure robustness.

2.  **`JsWorkerPool` (The Manager)**
    *   Manages a collection of `JsWorker` instances.
    *   Implements Round-Robin scheduling to distribute load.
    *   Provides an `async` API (`call_js`) that awaits the `oneshot` reply.

3.  **`V8Engine` (The Core)**
    *   Wraps `rusty_v8` boilerplate: Isolate creation, Context management, HandleScopes.
    *   Compiles and executes the initialization script.
    *   Invokes global functions by name with string arguments.
    *   Converts return values (String, Number, Bool, JSON) to standardized Strings.

## Data Flow
1.  **Init**: `JsWorkerPool::new(source, size)` spawns `size` threads. Each thread initializes V8 and compiles the script.
2.  **Call**: Application calls `pool.call_js("functionName", args)`.
3.  **Dispatch**: Pool selects a worker and sends a `JsJob`.
4.  **Execute**: Worker thread picks up the job, enters the V8 Isolate, runs the function.
5.  **Reply**: Result is sent back to the async task.

## Key Features
*   **Isolation**: Each worker has its own independent V8 Isolate.
*   **Safety**: Rust memory safety rules are maintained; V8 pointers never leak.
*   **Performance**: Overhead of thread switching is mitigated by keeping workers alive (no spawn-per-request).
*   **Flexibility**: Supports loading scripts from file or memory strings.

## Usage
```rust
// Initialize pool with a script
let code = r#"
    function calculate(a, b) {
        return parseInt(a) + parseInt(b);
    }
"#;
let pool = JsWorkerPool::new_with_source(code.to_string(), 4)?;

// Async execution
let result = pool.call_js("calculate", vec!["10".into(), "20".into()]).await?;
assert_eq!(result, "30");
```
