#!/usr/bin/env bash
set -euo pipefail

TOTAL=${1:-200000}
CONCURRENCY=${2:-4}
BUFFER=${3:-20000}

echo "Running log stress: total=${TOTAL} concurrency=${CONCURRENCY} buffer=${BUFFER}"
cargo run -p tests --bin log_stress -- --total "${TOTAL}" --concurrency "${CONCURRENCY}" --buffer "${BUFFER}"

echo "Baseline target: >= 20000 logs/sec, drop rate < 0.5% (adjust for hardware)"
