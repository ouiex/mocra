#!/usr/bin/env bash
set -euo pipefail

# ── Contract Tests with Evidence Output ───────────────────────────────────
# Runs core test suites and writes structured evidence to
# target/mocra-evidence/{timestamp}/.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$REPO_ROOT/target/mocra-evidence/$TIMESTAMP"
mkdir -p "$EVIDENCE_DIR"

GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")"
GIT_BRANCH="$(git -C "$REPO_ROOT" branch --show-current 2>/dev/null || echo "unknown")"

# ── Collect evidence ──────────────────────────────────────────────────────

collect_test() {
    local name="$1"; shift
    local out="$EVIDENCE_DIR/${name}.txt"
    local rc=0
    echo "Running: $*"
    "$@" > "$out" 2>&1 || rc=$?
    echo "$rc" > "$EVIDENCE_DIR/${name}.exit"
    if [[ "$rc" -eq 0 ]]; then
        echo "  PASS ($name)"
        echo "true" > "$EVIDENCE_DIR/${name}.pass"
    else
        echo "  FAIL ($name) exit=$rc"
        echo "false" > "$EVIDENCE_DIR/${name}.pass"
    fi
    return $rc
}

TOTAL_PASS=true

echo "Running contract tests"
collect_test "test_lib" cargo test --lib || TOTAL_PASS=false
collect_test "raft_rocksdb_backend" cargo test raft_rocksdb --lib -- --nocapture || TOTAL_PASS=false

echo
echo "Running legacy hot-path static check"
collect_test "check_legacy" bash "$REPO_ROOT/scripts/check_legacy_hot_path.sh" || TOTAL_PASS=false

# ── Optional Redis tests ───────────────────────────────────────────────

require_redis_tests="${MOCRA_REQUIRE_REDIS_CONTRACT_TESTS:-0}"
if [[ -n "${REDIS_URL:-}" || -n "${MOCRA_REDIS_TEST_URL:-}" ]]; then
    echo
    echo "Running optional Redis integration tests"
    collect_test "test_redis" cargo test redis_ -- --nocapture || TOTAL_PASS=false
elif [[ "$require_redis_tests" == "1" ]]; then
    echo "MOCRA_REQUIRE_REDIS_CONTRACT_TESTS=1 but REDIS_URL/MOCRA_REDIS_TEST_URL is not set"
    TOTAL_PASS=false
else
    echo "Skipping optional Redis integration tests (set REDIS_URL or MOCRA_REDIS_TEST_URL to enable)"
fi

# ── Write summary ──────────────────────────────────────────────────────

PASS_COUNT=$(grep -rl '^true$' "$EVIDENCE_DIR" --include='*.pass' 2>/dev/null | wc -l)
FAIL_COUNT=$(grep -rl '^false$' "$EVIDENCE_DIR" --include='*.pass' 2>/dev/null | wc -l)

# JSON summary
cat > "$EVIDENCE_DIR/summary.json" << JSONEOF
{
  "timestamp": "$TIMESTAMP",
  "git_sha": "$GIT_SHA",
  "git_branch": "$GIT_BRANCH",
  "passed": $PASS_COUNT,
  "failed": $FAIL_COUNT,
  "overall": "$($TOTAL_PASS && echo 'pass' || echo 'fail')"
}
JSONEOF

# Markdown summary
cat > "$EVIDENCE_DIR/summary.md" << MDEOF
# Mocra Contract Test Evidence

- **Timestamp**: $TIMESTAMP
- **Git**: $GIT_SHA ($GIT_BRANCH)
- **Result**: $($TOTAL_PASS && echo '**PASS**' || echo '**FAIL**')
- **Passed**: $PASS_COUNT
- **Failed**: $FAIL_COUNT

## Commands

| Test | Status |
|------|--------|
MDEOF

for f in "$EVIDENCE_DIR"/*.pass; do
    name="$(basename "$f" .pass)"
    status="$(cat "$f")"
    if [[ "$status" == "true" ]]; then
        echo "| $name | PASS |" >> "$EVIDENCE_DIR/summary.md"
    else
        echo "| $name | FAIL |" >> "$EVIDENCE_DIR/summary.md"
    fi
done

# Latest pointer
echo "$TIMESTAMP" > "$REPO_ROOT/target/mocra-evidence/latest"
cp "$EVIDENCE_DIR/summary.json" "$REPO_ROOT/target/mocra-evidence/latest.json"

echo
echo "Evidence written to $EVIDENCE_DIR"
echo "  summary.json: $(cat "$EVIDENCE_DIR/summary.json")"

if $TOTAL_PASS; then
    echo "All contract tests and checks PASSED."
    exit 0
else
    echo "Contract tests or checks FAILED."
    exit 1
fi
