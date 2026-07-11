# mocra vs scrapy-redis — throughput benchmark

A local, reproducible benchmark comparing **mocra** and **scrapy-redis**, each running **3 nodes**
against a fast local mock server.

> [!IMPORTANT]
> **This is a CPU / parser-bound benchmark, not a real-world crawl benchmark.** It runs against a
> zero-latency localhost target with tiny pages, so it measures **framework + parser overhead** —
> not production crawl speed. Real crawls are **network-bound** (50–200 ms latency + politeness
> delays per request); there both frameworks sit idle on I/O and the gap collapses. The honest
> takeaway is **efficiency** (mocra does the same work on far fewer cores), not "75× faster crawls".

## What it measures

Each framework crawls **N item pages**, parses every page with CSS selectors (scrapy → lxml,
mocra → [`scraper`](https://crates.io/crates/scraper)), and counts completions. Steady-state
throughput is measured with process startup excluded.

## Results

14-core machine, `N = 30000`, 3 processes each, local mock (measured ceiling **~139k req/s**, so
never the bottleneck):

| | pages/s (steady-state) |
|---|---:|
| **scrapy-redis, 3 nodes** | **~710** |
| **mocra, 3 nodes** | **~53,000** (warm peaks ~87k) |
| **mocra, 1 node** | **~52,000** (warm peaks ~73k) |

**≈ 75× steady-state (up to ~120× warm).** Two findings:

1. **mocra doesn't need multiple processes.** One mocra node (~52k) ≈ three (~54k): a single tokio
   process already saturates all cores. scrapy *must* run 3 processes to escape the GIL and still
   only reaches ~710/s.
2. mocra's facade **distributed mode (`from_toml` + a distributed queue) does not auto-seed** (seeding is gated on
   standalone mode), so the "run N nodes, they share a Redis queue and crawl" model that
   scrapy-redis gives out of the box needs external task injection. Hence mocra's 3 nodes here are
   **partition-parallel** (3 standalone processes over disjoint URL slices), not a shared queue —
   documented so the comparison is honest.

## Prerequisites

- **Go** (mock + load test), **Rust** (mocra bench), **Python 3** + **Redis** (`redis-server`,
  `redis-cli`).

## Run

```bash
cd benchmarks

# 1. clean no-auth Redis for the scrapy-redis queue
redis-server --port 6399 --save "" --appendonly no --protected-mode no --daemonize yes

# 2. the mock target (serves N item pages)
N=5000 go run mock.go &          # (any N works; /item/ID is unbounded)
#    optional: go run loadtest.go   # confirm the mock's ceiling

# 3. mocra side — build then run
(cd mocra_bench && cargo build --release)
N=30000 NODES=3 ./run_mocra.sh
N=30000 NODES=1 ./run_mocra.sh    # one node already saturates the machine

# 4. scrapy-redis side — venv then run
python3 -m venv venv && venv/bin/pip install -r requirements.txt
N=30000 NODES=3 ./run_scrapy.sh   # ~45s at ~710 pages/s
```

## Methodology / fairness notes

- **Both**: retries / cookies / robots.txt / redirects off; no artificial download delay; parse
  work equalized (CSS selectors + text extraction on each page).
- **scrapy**: `CONCURRENT_REQUESTS` and `CONCURRENT_REQUESTS_PER_DOMAIN` = 64/node (the per-domain
  default of 8 would otherwise cap it); Redis-backed shared scheduler + dupefilter; measured
  push→all-done with an 8 s worker warmup excluded.
- **mocra**: default tokio multi-threaded runtime (uses all cores); each node self-times its
  first-emit→last-emit window in-process (startup excluded) and the driver sums the concurrently
  measured per-node rates.
- **Measurement methods differ slightly** (mocra in-process vs scrapy Redis-counter poll); both are
  steady-state and exclude startup. At a ~75× gap the methodological nuance is immaterial.
- **Variance**: mocra 52k–87k across runs (thermal / warmup); scrapy rock-stable ~710.

## Files

| File | Purpose |
|---|---|
| `mock.go` | Fast Go mock crawl target |
| `loadtest.go` | Measures the mock's raw ceiling |
| `mocra_bench/` | mocra benchmark node (standalone crate; `path = "../.."`, own workspace) |
| `run_mocra.sh` | Runs / aggregates N mocra nodes |
| `scrapy_spider.py`, `run_scrapy.sh` | scrapy-redis spider + N-worker driver |
| `requirements.txt` | scrapy / scrapy-redis / redis |
