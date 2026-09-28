# Runtime optimization: local stage 5 acceptance

Run the reproducible Linux workload with:

```bash
cargo test -p mocra-core --lib local_full_path -- --ignored --nocapture --test-threads=1
```

The ignored workload uses a local `QueueManager` channel (capacity 32), `Batcher`
(eight-item batches, at most eight running batches), proxy middleware, the
download processor, the default reqwest downloader, and eight local HTTP proxy
servers. It ACKs successful requests and NACKs final failures. Healthy servers
return HTTP 200; four fault servers return HTTP 407. Every request has one retry
available. The fixed-proxy comparison selects a proxy once and keeps it for
retry, while the managed path reports feedback and can rotate. Both paths use
the current power-of-two proxy selection policy. There is no external MQ,
parser, or storage service in this workload.

Each 320-request scenario ran five times in one debug-build process on the
local arm64 host on 2026-09-28. The table reports medians and the observed
range for throughput. Latency is measured from enqueue to final result, so it
includes queueing. RSS is the sampled process peak and includes the local proxy
servers. CPU time uses `/proc/self/stat` with 100 ticks per second and also
includes those servers.

| Proxy faults | Retry target | Success / 320 | Requests/s (range) | p95 / p99 | Peak RSS | CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0/8 | Fixed | 320 | 493.0 (469.0–493.9) | 456.9 / 466.0 ms | 36.7 MiB | 100.1% |
| 0/8 | Managed rotation | 320 | 489.3 (488.7–491.6) | 448.0 / 465.4 ms | 36.8 MiB | 99.8% |
| 4/8 | Fixed | 157 | 427.0 (424.1–429.7) | 455.2 / 483.0 ms | 37.2 MiB | 99.4% |
| 4/8 | Managed rotation | 311 | 453.1 (450.3–464.1) | 453.9 / 481.0 ms | 37.0 MiB | 99.2% |

With four fault proxies, managed rotation raised the median final success rate
from 49.1% to 97.2%; throughput rose 6.1%. With no faults, managed proxy
feedback cost 0.8% of throughput against the fixed-proxy comparison. This is
a local comparison of the new managed path with fixed retries, not a before and
after production throughput measurement.

The selected pool write-lock wait averaged 1.5 µs in the managed fault group
(five-run median; range 1.47–1.59 µs). The per-run maximum had median 27.1 µs
and range 5.0–46.2 µs. The p95 time from the first HTTP 407 to a subsequent
HTTP 200 had median 13.0 ms and range 11.9–162.9 ms; the high rounds warrant
attention in a canary. This lock metric covers proxy selection, not every pool
operation.

Every round reached the configured queue cap of 32 and the batch cap of eight.
The local upper bound is 32 queued messages + 8 running batches × 8 messages
+ one assembled/waiting batch of up to 8 messages = 104 accepted messages.
All rounds satisfied `ACK + NACK = 320`. On the managed path, the number of
started proxy attempts exactly matched local HTTP attempts and also matched
successful plus failed feedback. Rate-limit events were zero because this
workload configured unlimited proxies.

The separate 3,200-request overload run reached 987.9 requests/s, p95/p99
109.2/374.6 ms, 40.1 MiB peak RSS, 32 queued messages, and eight running
batches; 3,097 requests succeeded and 103 failed. Started attempts and
feedback both totaled 3,940. The extra 10× request volume raised peak RSS by
about 3 MiB against the 320-request managed fault group, rather than scaling
with the number of submitted requests. Throughput across the two run lengths
is affected by startup and warmup and should not be read as a scaling ratio.

## Release and rollback gate

Local correctness and resource checks pass. A production or staging canary has
not been run. Before widening deployment, compare a fixed traffic slice against
the previous build with the same proxy population and retry budget. Capture
request success, final ACK/NACK, `mocra_download_attempts_total`,
`mocra_proxy_faults_total`, `mocra_proxy_rotations_total`, proxy selection errors,
proxy Client cache hits/creations, queue depth, p95/p99 end-to-end latency,
process RSS and CPU, and proxy selection wait statistics. Include a controlled
proxy 407/connection-failure injection and a no-fault interval. Keep the old
build available for immediate rollback.

Stop the canary if ACK/NACK accounting breaks, queue/pool capacity exceeds its
configured limit, or normal-load success or latency persistently regresses.
The local no-fault result is a reference only; it cannot certify real MQ,
network, parser, storage, or multi-node behavior. Kafka/NATS service-backed
pressure tests and gray release need the corresponding environment.
