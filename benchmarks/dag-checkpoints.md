# DAG run-state checkpoint comparison

## Event-driven short chains

Run `cargo test -p mocra-dag --lib --release short_chain_workload -- --ignored --nocapture`.
Five release-build runs on the local arm64 host produced the following medians:

| Zero-work chain | Earlier polling probe | Event-driven scheduler |
| ---: | ---: | ---: |
| 1 node | 36 ms | 0.016 ms |
| 10 nodes | 144 ms | 0.058 ms |
| 20 nodes | 262 ms | 0.103 ms |

The earlier values are from a release-build probe. The new ignored test builds the
graph once and times `execute_parallel()`;
the two harnesses are similar zero-work chains, not an identical A/B executable.
These figures characterize the standalone DAG component, not crawler throughput.

## Run-state checkpoints

The public `DagRunStateStore` API provides atomic replacement of a full snapshot via
`save`, but no atomic append operation. Stage 4 therefore uses full checkpoints every
16 successful nodes by default. This keeps existing stores compatible. The interval is
configurable; setting it to 1 restores per-node persistence.

## Copy workload

Run `cargo test -p mocra-dag --lib snapshot_copy_workload -- --ignored --nocapture`.
The test builds payloads in memory, repeats each strategy five times on one machine,
and reports the median. It excludes storage latency and serialization. The incremental
column models copying one completed payload into an append record; it is a candidate,
not an implemented store API.

| Nodes | Output per node | Full save each node | Checkpoint every 16 | Incremental record |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 4 KiB | 10,908 µs | 1,047 µs | 273 µs |
| 64 | 64 KiB | 45,009 µs | 5,004 µs | 1,302 µs |

These figures are debug-build copy timings on the local arm64 host, not end-to-end DAG
throughput. The five samples for each scenario are emitted by the ignored test.

## Storage and recovery model

For 128 successful nodes, 4 KiB outputs, and a 16-node interval, full per-node
snapshots copy about 8,256 output records (32.25 MiB) and make 128 writes. Periodic
checkpoints copy 576 records (2.25 MiB) and make 8 writes. Incremental records copy
128 records (0.5 MiB) and make 128 writes. At an assumed fixed 1 ms store latency,
that is 128 ms, 8 ms, and 128 ms respectively, before transfer or serialization.
At 5 ms per write, the corresponding fixed costs are 640 ms, 40 ms, and 640 ms.

After an abrupt cancellation, periodic checkpoints can require up to 15 successful
nodes to run again, averaging 7.5 when cancellation is uniformly distributed within
an interval. Full per-node and durable incremental records have no such replay gap.
Errors handled by the scheduler save a full failure snapshot, so the replay gap applies
to abrupt cancellation or process loss. A durable incremental design would also need
an atomic append or compare-and-swap operation tied to the run identity and fencing
token; otherwise old workers could contaminate a resumed run.

Larger outputs shift the choice: for 1,024 nodes of 64 KiB each, 16-node checkpoints
copy about 2,080 MiB in aggregate versus 64 MiB for incremental records, while using
64 writes versus 1,024. This is why the interval is configurable and why incremental
storage remains a candidate for stores that can guarantee atomic records.
