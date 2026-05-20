#!/usr/bin/env bash
set -euo pipefail

# ── Legacy Hot-Path Static Check ────────────────────────────────────────
# Scans for forbidden legacy patterns in typed main paths.
# Uses allowlists: known-legitimate files are excluded from each check.
# New occurrences outside the allowlist cause a non-zero exit.
#
# Allowlist files are listed relative to the repository root.
# Edit the allowlists below when legacy code is removed from a file.

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FAILED=0

section() { echo; echo "── $1 ──"; }

# ── Check 1: ModuleConfig in chain / interface directories ──────────

section "Check 1: ModuleConfig in chain/interface hot paths"

ALLOWLIST_CHAIN_CONFIG=(
    "src/engine/chain/download_chain.rs"
    "src/engine/chain/parser_chain.rs"
    "src/engine/chain/stream_chain.rs"
    "src/engine/chain/task_model_chain.rs"
    "src/common/interface/middleware.rs"
    "src/common/interface/middleware_manager.rs"
)

MATCHES=$(grep -rn "ModuleConfig" src/engine/chain src/common/interface --include="*.rs" 2>/dev/null || true)
VIOLATIONS=""
while IFS= read -r line; do
    file="${line%%:*}"
    rel="${file#${REPO_ROOT}/}"
    allowed=false
    for a in "${ALLOWLIST_CHAIN_CONFIG[@]}"; do
        [[ "$rel" == "$a" ]] && allowed=true && break
    done
    if ! $allowed; then
        VIOLATIONS+="$line"$'\n'
    fi
done <<< "$MATCHES"

if [[ -n "$VIOLATIONS" ]]; then
    echo "FAIL: ModuleConfig found outside allowlist in chain/interface dirs:"
    echo "$VIOLATIONS"
    FAILED=1
else
    echo "PASS"
fi

# ── Check 2: "legacy.*" schema IDs outside tests ───────────────────────

section 'Check 2: "legacy.*" schema IDs in production code'

ALLOWLIST_LEGACY_SCHEMA=(
    "src/engine/task/module_node_runtime_bridge.rs"
    "src/engine/task/module_processor_with_chain.rs"
    "src/engine/task/parser_error_adapter.rs"
    "src/engine/task/task_dispatch_adapter.rs"
    "src/engine/task/node_context_adapter.rs"
)

MATCHES=$(grep -rn '"legacy\.' src/ --include="*.rs" 2>/dev/null || true)
VIOLATIONS=""
while IFS= read -r line; do
    file="${line%%:*}"
    rel="${file#${REPO_ROOT}/}"
    # Skip test modules
    if echo "$line" | grep -qE '(mod tests|#\[cfg\(test\)\]|_tests::|fn test_)'; then
        continue
    fi
    allowed=false
    for a in "${ALLOWLIST_LEGACY_SCHEMA[@]}"; do
        [[ "$rel" == "$a" ]] && allowed=true && break
    done
    if ! $allowed; then
        VIOLATIONS+="$line"$'\n'
    fi
done <<< "$MATCHES"

if [[ -n "$VIOLATIONS" ]]; then
    echo 'FAIL: "legacy.*" schema IDs found outside allowlist in production code:'
    echo "$VIOLATIONS"
    FAILED=1
else
    echo "PASS"
fi

# ── Check 3: parser_chain.execute / error_chain.execute fallback ────

section "Check 3: parser_chain.execute / error_chain.execute fallback"

ALLOWLIST_CHAIN_FALLBACK=(
    "src/engine/chain/task_model_chain.rs"
)

MATCHES=$(grep -rn 'parser_chain\.execute\|error_chain\.execute' src/ --include="*.rs" 2>/dev/null || true)
VIOLATIONS=""
while IFS= read -r line; do
    file="${line%%:*}"
    rel="${file#${REPO_ROOT}/}"
    # Skip test modules
    if echo "$line" | grep -qE '(mod tests|#\[cfg\(test\)\]|_tests::|fn test_)'; then
        continue
    fi
    allowed=false
    for a in "${ALLOWLIST_CHAIN_FALLBACK[@]}"; do
        [[ "$rel" == "$a" ]] && allowed=true && break
    done
    if ! $allowed; then
        VIOLATIONS+="$line"$'\n'
    fi
done <<< "$MATCHES"

if [[ -n "$VIOLATIONS" ]]; then
    echo "FAIL: chain.execute fallback found outside allowlist:"
    echo "$VIOLATIONS"
    FAILED=1
else
    echo "PASS"
fi

# ── Check 4: build_legacy_* calls outside allowlist ─────────────────

section "Check 4: build_legacy_* calls in production code"

ALLOWLIST_LEGACY_BUILDER=(
    "src/engine/task/module_dag_orchestrator.rs"
    "src/engine/task/module_dag_processor.rs"
    "src/engine/task/module_node_runtime_bridge.rs"
    "src/engine/task/module_processor_with_chain.rs"
    "src/engine/task/node_context_adapter.rs"
    "src/schedule/dag/remote_redis.rs"
)

MATCHES=$(grep -rn 'build_legacy_' src/ --include="*.rs" 2>/dev/null || true)
VIOLATIONS=""
while IFS= read -r line; do
    file="${line%%:*}"
    rel="${file#${REPO_ROOT}/}"
    # Skip test modules
    if echo "$line" | grep -qE '(mod tests|#\[cfg\(test\)\]|_tests::|fn test_)'; then
        continue
    fi
    allowed=false
    for a in "${ALLOWLIST_LEGACY_BUILDER[@]}"; do
        [[ "$rel" == "$a" ]] && allowed=true && break
    done
    if ! $allowed; then
        VIOLATIONS+="$line"$'\n'
    fi
done <<< "$MATCHES"

if [[ -n "$VIOLATIONS" ]]; then
    echo "FAIL: build_legacy_* calls found outside allowlist in production code:"
    echo "$VIOLATIONS"
    FAILED=1
else
    echo "PASS"
fi

# ── Result ──────────────────────────────────────────────────────────

echo
if [[ "$FAILED" -eq 0 ]]; then
    echo "All legacy hot-path checks passed."
    exit 0
else
    echo "Legacy hot-path checks FAILED. See violations above."
    exit 1
fi
