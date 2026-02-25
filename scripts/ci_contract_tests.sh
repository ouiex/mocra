#!/usr/bin/env bash
set -euo pipefail

echo "Running contract tests"

cargo test --lib

require_redis_tests="${MOCRA_REQUIRE_REDIS_CONTRACT_TESTS:-0}"

if [[ -n "${REDIS_URL:-}" || -n "${MOCRA_REDIS_TEST_URL:-}" ]]; then
	echo "Running optional Redis integration tests for T09"
	cargo test redis_ -- --nocapture
elif [[ "$require_redis_tests" == "1" ]]; then
	echo "MOCRA_REQUIRE_REDIS_CONTRACT_TESTS=1 but REDIS_URL/MOCRA_REDIS_TEST_URL is not set"
	exit 1
else
	echo "Skipping optional Redis integration tests (set REDIS_URL or MOCRA_REDIS_TEST_URL to enable)"
fi
