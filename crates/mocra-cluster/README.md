# mocra-cluster

Embedded control plane for the [mocra](https://github.com/ouiex/mocra) distributed crawler
framework: a self-organizing **Raft + redb** cluster providing strongly-consistent leader
election, distributed locks (with fencing tokens), KV/CAS, membership, and partition
ownership — **no external coordinator (ZooKeeper / etcd) required**.

## Features

- **openraft 0.9** consensus over a **redb** replicated state machine + persistent log
  (crash-recoverable; snapshot / log compaction).
- HTTP + msgpack inter-node RPC; **any node accepts writes** (auto-forwarded to the leader),
  so "register to any node to join" works.
- Distributed locks with monotonic **fencing tokens**; compare-and-set; KV.
- Membership API + `/cluster/join`; dynamic scale up / down; leader failover; graceful shutdown.
- **Partition ownership** via rendezvous (HRW) hashing (≈1/N keys move on membership change)
  plus Raft-fenced ownership leases.

## Example

```rust,ignore
use mocra_cluster::RaftControlPlane;
use std::time::Duration;

// bootstrap a single-node cluster (or start_cluster_node + init_cluster / join_cluster)
let cp = RaftControlPlane::start_single_node(1, "./data").await?;
cp.wait_leader(Duration::from_secs(10)).await?;

cp.set(b"key", b"value").await?;                         // strongly-consistent write via Raft
let token = cp.acquire_lock("job", "me", 5000).await?;  // fenced distributed lock
let owned = cp.owned_partitions();                      // partitions this node owns
```

Part of the [mocra](https://github.com/ouiex/mocra) workspace.

## License

Licensed under either of MIT or Apache-2.0 at your option.
