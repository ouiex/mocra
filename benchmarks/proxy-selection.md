# Proxy selection policy comparison

Run the local workload with:

```bash
cargo test -p mocra-proxy --lib selection_policy_workload -- --ignored --nocapture
```

The debug-build workload selects from 32 static proxies using eight concurrent
tasks and 4000 total selections. Eight proxies have score-derived success rate
0.99 and relative cost 3; 16 have 0.85 and cost 2; eight have 0.60 and cost 1.
No network requests are sent. Each policy was run five times in fresh test
processes on 2026-09-28; the table shows medians.

| Policy | Wall time | Selection p95 | Selection p99 | Proxies used | Max proxy share | Modeled success | Relative cost |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A: highest score, ties rotated | 70 ms | 17 µs | 4501 µs | 8 | 12.5% | 0.990 | 3.000 |
| B: sample two, prefer higher score | 83 ms | 48 µs | 5288 µs | 32 | 6.0% | 0.898 | 2.389 |

The modeled success and cost are calculated from selected tiers; they are not
observed download outcomes. Policy B spreads traffic and lowers modeled proxy
cost at the expense of selection overhead and some lower-quality selections.
The production default is now B. The stage 5 full-path load test must check
whether this tradeoff is acceptable with actual proxy failures and latency.
