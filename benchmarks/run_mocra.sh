#!/usr/bin/env bash
# mocra N-node benchmark (partition-parallel). Each node crawls a disjoint slice of N item URLs
# and self-reports its steady-state rate (startup excluded); aggregate = sum of per-node rates.
#
# Prereq: the mock must be running on :8899, and the bench crate built:
#     (cd mocra_bench && cargo build --release)
# Usage:  N=30000 NODES=3 ./run_mocra.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$HERE/mocra_bench/target/release/mocra_bench"
N="${N:-30000}"; NODES="${NODES:-3}"
[ -x "$BIN" ] || { echo "build first:  (cd '$HERE/mocra_bench' && cargo build --release)"; exit 1; }

per=$(( (N + NODES - 1) / NODES ))
pids=(); logs=()
for k in $(seq 0 $((NODES-1))); do
  off=$(( k * per )); cnt=$per
  [ $(( off + cnt )) -gt "$N" ] && cnt=$(( N - off ))
  [ "$cnt" -le 0 ] && continue
  log="$(mktemp)"
  OFFSET=$off COUNT=$cnt "$BIN" >"$log" 2>&1 &
  pids+=($!); logs+=("$log")
done
wait "${pids[@]}" 2>/dev/null || true

python3 - "$N" "$NODES" "${logs[@]}" <<'PY'
import sys, re
N, NODES, logs = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3:]
rates, secs = [], []
for lg in logs:
    for line in open(lg):
        m = re.search(r'NODE_RESULT count=(\d+) secs=([\d.]+) rate=([\d.]+)', line)
        if m:
            secs.append(float(m.group(2))); rates.append(float(m.group(3)))
if not rates:
    print("no NODE_RESULT — is the mock running on :8899?"); sys.exit(1)
print(f"[mocra  {NODES}-node]  N={N}  ->  slowest-node crawl {max(secs):.3f}s   "
      f"aggregate {sum(rates):.0f} pages/s   (per-node: {[int(r) for r in rates]})")
PY
