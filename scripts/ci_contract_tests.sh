#!/usr/bin/env bash
set -euo pipefail

echo "Running contract tests"

cargo test -p queue
cargo test -p sync
