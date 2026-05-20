param(
    [Parameter(Mandatory = $true)]
    [string]$PromUrl,
    [ValidateSet("dev", "staging", "prod")]
    [string]$Profile = "prod",
    [int]$LookbackHours = 2,
    [int]$StepSeconds = 30,
    [Parameter(Mandatory = $true)]
    [string]$CutInCommand,
    [Parameter(Mandatory = $true)]
    [string]$RollbackCommand,
    [string]$ReleaseTag = "",
    [string]$OutputDir = "docs/dashboards/rehearsal"
)

$ErrorActionPreference = "Stop"

if ($ReleaseTag -eq "") {
    $ReleaseTag = Get-Date -Format "yyyyMMdd_HHmmss"
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null

$gateSummary = Join-Path $OutputDir ("cutover_gate_{0}.json" -f $ReleaseTag)
$report = Join-Path $OutputDir ("cutover_rehearsal_{0}.md" -f $ReleaseTag)

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$CommandText
    )
    Write-Host "==> $Name"
    Write-Host "    $CommandText"
    Invoke-Expression $CommandText
}

$startedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$status = "success"
$failedStep = ""
$errorText = ""

try {
    Invoke-Step -Name "1) Cut in scheduler path" -CommandText $CutInCommand

    Invoke-Step -Name "2) Observe gate metrics" -CommandText (
        "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/dag_alerts_gate.ps1 " +
        "-PromUrl '{0}' -Profile {1} -LookbackHours {2} -StepSeconds {3} -ReleaseTag '{4}' -SummaryJson '{5}'" -f
        $PromUrl, $Profile, $LookbackHours, $StepSeconds, $ReleaseTag, $gateSummary
    )

    Invoke-Step -Name "3) Rollback" -CommandText $RollbackCommand

    Invoke-Step -Name "4) Post-rollback gate check" -CommandText (
        "pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/dag_alerts_gate.ps1 " +
        "-PromUrl '{0}' -Profile {1} -LookbackHours {2} -StepSeconds {3} -ReleaseTag '{4}_rollback' -SummaryJson '{5}'" -f
        $PromUrl, $Profile, $LookbackHours, $StepSeconds, $ReleaseTag, $gateSummary
    )
}
catch {
    $status = "failed"
    $failedStep = "cutover_or_gate"
    $errorText = $_.Exception.Message
    Write-Error "Cutover rehearsal failed: $errorText"
}

$endedAt = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

@"
# Cutover Rehearsal Report ($ReleaseTag)

- started_at: $startedAt
- ended_at: $endedAt
- status: $status
- failed_step: $failedStep
- gate_summary_json: $gateSummary

## Commands

- cut_in: $CutInCommand
- rollback: $RollbackCommand

## Success Criteria

1. Gate check exits with code 0 after cut-in.
2. Gate check exits with code 0 after rollback.
3. No sustained alert for scheduler fallback high ratio or failure gate blocked.

## Failure Handling

1. Execute rollback command immediately.
2. Re-run gate check with the same release tag and archive output.
3. Keep rollout frozen until two consecutive gate runs pass.
4. Investigate summary json and Prometheus alert history before retry.

## Error

$errorText
"@ | Set-Content -Path $report -Encoding UTF8

if ($status -ne "success") {
    exit 1
}

Write-Host "Rehearsal report written to $report"
