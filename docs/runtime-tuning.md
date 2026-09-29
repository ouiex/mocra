# Runtime tuning and validation

> **中文版：** [运行时调优与验证](zh/runtime-tuning.md)

This guide covers the queue, proxy, and standalone DAG scheduler changes in 0.5.0. Start with the defaults, change one bound at a time, and compare the same workload before and after. The [benchmark index](../benchmarks/README.md) records local methods and limitations.

## Bounded queues and batches

`channel_config.capacity` is a positive capacity for **each** local channel, including the log channel. Batches also respect their configured size, and the batch dispatcher waits for a concurrency permit before accepting more work. Thus a slow processor applies backpressure to the bounded input queue instead of creating an unlimited list of waiting batch tasks.

```toml
[channel_config]
minid_time = 0
capacity = 1000
batch_concurrency = 10
```

This is a fragment of the [full engine config](configuration.md); other required sections still apply. `batch_concurrency` bounds simultaneously running batch flushes, while `capacity` bounds each local channel separately. Monitor queue depth, backpressure retries, accepted/completed work, process RSS, and p95/p99 latency under overload. Raising capacity can absorb bursts but also raises the memory available to queued messages.

## Proxy selection and HTTP clients

Managed proxies use two-candidate selection rather than always taking the highest score. This spreads requests across eligible proxies while still preferring the stronger of the two. It can select a lower-scoring proxy more often than the former highest-score policy; see the [local selection comparison](../benchmarks/proxy-selection.md). Provider `max_size` bounds dynamically fetched IP proxies **per provider**; health probes and proxy-specific HTTP client caching have separate limits.

The default proxy Client cache capacity is 1000. Set `download_config.proxy_client_cache_capacity` to a measured bound, or `0` to disable caching. Compare hit/miss and creation counters together with idle connections and RSS; the [local cache measurement](../benchmarks/proxy-client-cache.md) does not establish a universal best value. See [Proxies and downloaders](proxies-and-downloaders.md) for request-level selection and feedback.

## Standalone DAG scheduler

The `mocra-dag` scheduler waits for node completion events rather than polling. When using its `DagRunStateStore`, `.with_run_state_store(store, run_key)` defaults to a full checkpoint after every 16 successful nodes. `.with_run_state_checkpoint_interval(1)` restores per-node saves; call it **after** `.with_run_state_store`. A process loss between checkpoints may cause up to 15 successful nodes to run again at the default interval, so make those effects safe to repeat or choose a smaller interval. Handled errors save a failure snapshot.

This is a `mocra-dag` scheduler setting, not a `Spider` or engine TOML option. See the [checkpoint measurement](../benchmarks/dag-checkpoints.md) for the local copy-cost comparison and recovery tradeoff. Those microbenchmarks do not measure full crawler throughput.

## Acceptance boundary

The [local full-path acceptance workload](../benchmarks/runtime-acceptance.md) covers bounded queues, batches, the managed proxy path, and retries against local HTTP proxies. It reports throughput, latency, memory, and ACK/NACK accounting. It does **not** include a production or staging canary, real Kafka/NATS pressure, parser work, or a storage service. Before changing production traffic, use the canary and rollback checks in that report against the actual proxy population and retry budget.
