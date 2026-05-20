$ErrorActionPreference = "Stop"

# ── Contract Tests with Evidence Output (PowerShell) ──────────────────
# Runs core test suites and writes structured evidence to
# target/mocra-evidence/{timestamp}/.

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$EvidenceDir = Join-Path $RepoRoot "target/mocra-evidence/$Timestamp"
New-Item -ItemType Directory -Force -Path $EvidenceDir | Out-Null

$GitSha = try { git -C $RepoRoot rev-parse --short HEAD 2>$null } catch { "unknown" }
$GitBranch = try { git -C $RepoRoot branch --show-current 2>$null } catch { "unknown" }

# ── Collect evidence ──────────────────────────────────────────────────

function Invoke-EvidenceTest {
    param(
        [string]$Name,
        [scriptblock]$Command
    )
    $outFile = Join-Path $EvidenceDir "$Name.txt"
    $passFile = Join-Path $EvidenceDir "$Name.pass"
    $exitFile = Join-Path $EvidenceDir "$Name.exit"

    Write-Host "Running: $Name"
    $rc = 0
    $savedErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $Command *>&1 | Out-File -FilePath $outFile -Encoding utf8
        $rc = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $savedErrorAction
    }
    "$rc" | Out-File -FilePath $exitFile -Encoding ascii
    if ($rc -eq 0) {
        Write-Host "  PASS ($Name)"
        "true" | Out-File -FilePath $passFile -Encoding ascii
        return $true
    } else {
        Write-Host "  FAIL ($Name) exit=$rc"
        "false" | Out-File -FilePath $passFile -Encoding ascii
        return $false
    }
}

$TotalPass = $true

Write-Host "Running contract tests"
if (-not (Invoke-EvidenceTest "test_lib" { cargo test --lib })) { $TotalPass = $false }
if (-not (Invoke-EvidenceTest "raft_rocksdb_backend" { cargo test raft_rocksdb --lib -- --nocapture })) { $TotalPass = $false }

Write-Host ""
Write-Host "Running legacy hot-path static check"
$legacyCheckScript = Join-Path $RepoRoot "scripts/check_legacy_hot_path.ps1"
if (-not (Invoke-EvidenceTest "check_legacy" { powershell -ExecutionPolicy Bypass -File $legacyCheckScript })) { $TotalPass = $false }

# ── Optional Redis tests ──────────────────────────────────────────────

$requireRedisTests = $env:MOCRA_REQUIRE_REDIS_CONTRACT_TESTS
$redisUrl = if ($env:REDIS_URL) { $env:REDIS_URL } else { $env:MOCRA_REDIS_TEST_URL }

if ($redisUrl) {
    Write-Host ""
    Write-Host "Running optional Redis integration tests"
    if (-not (Invoke-EvidenceTest "test_redis" { cargo test redis_ -- --nocapture })) { $TotalPass = $false }
} elseif ($requireRedisTests -eq "1") {
    Write-Error "MOCRA_REQUIRE_REDIS_CONTRACT_TESTS=1 but REDIS_URL/MOCRA_REDIS_TEST_URL is not set"
    $TotalPass = $false
} else {
    Write-Host "Skipping optional Redis integration tests"
}

# ── Write summary ──────────────────────────────────────────────────────

$passCount = (Get-ChildItem $EvidenceDir -Filter "*.pass" | Where-Object { (Get-Content $_.FullName -Raw).Trim() -eq "true" }).Count
$failCount = (Get-ChildItem $EvidenceDir -Filter "*.pass" | Where-Object { (Get-Content $_.FullName -Raw).Trim() -eq "false" }).Count

$summary = @{
    timestamp = $Timestamp
    git_sha   = $GitSha
    git_branch = $GitBranch
    passed    = $passCount
    failed    = $failCount
    overall   = if ($TotalPass) { "pass" } else { "fail" }
}

$summaryJson = Join-Path $EvidenceDir "summary.json"
$summary | ConvertTo-Json | Out-File -FilePath $summaryJson -Encoding utf8

# Markdown summary
$mdFile = Join-Path $EvidenceDir "summary.md"
$overallMd = if ($TotalPass) { "**PASS**" } else { "**FAIL**" }
@"
# Mocra Contract Test Evidence

- **Timestamp**: $Timestamp
- **Git**: $GitSha ($GitBranch)
- **Result**: $overallMd
- **Passed**: $passCount
- **Failed**: $failCount

## Commands

| Test | Status |
|------|--------|
"@ | Out-File -FilePath $mdFile -Encoding utf8

Get-ChildItem $EvidenceDir -Filter "*.pass" | ForEach-Object {
    $testName = $_.BaseName
    $status = if ((Get-Content $_.FullName -Raw).Trim() -eq "true") { "PASS" } else { "FAIL" }
    "| $testName | $status |" | Out-File -FilePath $mdFile -Encoding utf8 -Append
}

# Latest pointer
$latestDir = Join-Path $RepoRoot "target/mocra-evidence"
Copy-Item $summaryJson (Join-Path $latestDir "latest.json") -Force
"$Timestamp" | Out-File -FilePath (Join-Path $latestDir "latest") -Encoding ascii

Write-Host ""
Write-Host "Evidence written to $EvidenceDir"
Write-Host "  summary.json: $(Get-Content $summaryJson -Raw)"

if ($TotalPass) {
    Write-Host "All contract tests and checks PASSED."
    exit 0
} else {
    Write-Host "Contract tests or checks FAILED."
    exit 1
}
