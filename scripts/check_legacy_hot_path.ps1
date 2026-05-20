$ErrorActionPreference = "Stop"

# ── Legacy Hot-Path Static Check (PowerShell) ──────────────────────────
# Scans for forbidden legacy patterns in typed main paths.
# Uses allowlists: known-legitimate files are excluded from each check.
# New occurrences outside the allowlist cause a non-zero exit.

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Failed = 0

function Write-Section($Title) {
    Write-Host ""
    Write-Host "── $Title ──"
}

function Test-Pattern {
    param(
        [string]$SectionTitle,
        [string[]]$SearchPaths,
        [string]$Pattern,
        [string[]]$Allowlist,
        [string]$Description
    )
    Write-Section $SectionTitle

    $matches = @(Select-String -Path $SearchPaths -Pattern $Pattern -SimpleMatch)
    $violations = @()

    foreach ($m in $matches) {
        $rel = $m.Path -replace [regex]::Escape("$RepoRoot\"), ""
        $rel = $rel -replace '\\', '/'

        # Skip test-only lines in the matched content
        if ($m.Line -match '(mod tests|#\[cfg\(test\)\]|_tests::|fn test_)') {
            continue
        }

        $allowed = $false
        foreach ($a in $Allowlist) {
            if ($rel -eq $a) { $allowed = $true; break }
        }
        if (-not $allowed) {
            $violations += "${rel}:$($m.LineNumber): $($m.Line.Trim())"
        }
    }

    if ($violations.Count -gt 0) {
        Write-Host "FAIL: $Description found outside allowlist:"
        foreach ($v in $violations) { Write-Host "  $v" }
        $script:Failed = 1
    } else {
        Write-Host "PASS"
    }
}

# ── Check 1: ModuleConfig in chain/interface directories ──────────────

$allowlistModuleConfig = @(
    "src/engine/chain/download_chain.rs"
    "src/engine/chain/parser_chain.rs"
    "src/engine/chain/stream_chain.rs"
    "src/engine/chain/task_model_chain.rs"
    "src/common/interface/middleware.rs"
    "src/common/interface/middleware_manager.rs"
)

Test-Pattern -SectionTitle "Check 1: ModuleConfig in chain/interface hot paths" `
    -SearchPaths @("src\engine\chain\*.rs", "src\common\interface\*.rs") `
    -Pattern "ModuleConfig" `
    -Allowlist $allowlistModuleConfig `
    -Description "ModuleConfig"

# ── Check 2: "legacy.*" schema IDs outside tests ───────────────────────

$allowlistLegacySchema = @(
    "src/engine/task/module_node_runtime_bridge.rs"
    "src/engine/task/module_processor_with_chain.rs"
    "src/engine/task/parser_error_adapter.rs"
    "src/engine/task/task_dispatch_adapter.rs"
    "src/engine/task/node_context_adapter.rs"
)

Test-Pattern -SectionTitle 'Check 2: "legacy.*" schema IDs in production code' `
    -SearchPaths @("src\**\*.rs") `
    -Pattern '"legacy.' `
    -Allowlist $allowlistLegacySchema `
    -Description '"legacy.*" schema IDs'

# ── Check 3: parser_chain.execute / error_chain.execute fallback ───────

$allowlistChainFallback = @(
    "src/engine/chain/task_model_chain.rs"
)

Test-Pattern -SectionTitle "Check 3: chain.execute fallback calls" `
    -SearchPaths @("src\**\*.rs") `
    -Pattern "parser_chain.execute" `
    -Allowlist $allowlistChainFallback `
    -Description "parser_chain.execute fallback"

Test-Pattern -SectionTitle "Check 3b: error_chain.execute fallback calls" `
    -SearchPaths @("src\**\*.rs") `
    -Pattern "error_chain.execute" `
    -Allowlist $allowlistChainFallback `
    -Description "error_chain.execute fallback"

# ── Check 4: build_legacy_* calls outside allowlist ──────────────────

$allowlistLegacyBuilder = @(
    "src/engine/task/module_dag_orchestrator.rs"
    "src/engine/task/module_dag_processor.rs"
    "src/engine/task/module_node_runtime_bridge.rs"
    "src/engine/task/module_processor_with_chain.rs"
    "src/engine/task/node_context_adapter.rs"
    "src/schedule/dag/remote_redis.rs"
)

Test-Pattern -SectionTitle "Check 4: build_legacy_* calls in production code" `
    -SearchPaths @("src\**\*.rs") `
    -Pattern "build_legacy_" `
    -Allowlist $allowlistLegacyBuilder `
    -Description "build_legacy_* calls"

# ── Result ──────────────────────────────────────────────────────────

Write-Host ""
if ($Failed -eq 0) {
    Write-Host "All legacy hot-path checks passed."
    exit 0
} else {
    Write-Host "Legacy hot-path checks FAILED. See violations above."
    exit 1
}
