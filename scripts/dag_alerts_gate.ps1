param(
    [Parameter(Mandatory = $true)]
    [string]$PromUrl,
    [ValidateSet("dev", "staging", "prod")]
    [string]$Profile = "prod",
    [int]$LookbackHours = 24,
    [int]$StepSeconds = 60,
    [string]$ReleaseTag = "",
    [string]$Output = "docs/dashboards/dag_alerts_baseline_latest.md",
    [string]$SummaryJson = "docs/dashboards/dag_alerts_baseline_latest.json",
    [string]$ArchiveDir = "docs/dashboards/archive"
)

if ($ReleaseTag -eq "") {
    $ReleaseTag = Get-Date -Format "yyyyMMdd_HHmmss"
}

$archivePrefix = "dag_alerts_baseline_{0}" -f $ReleaseTag

Write-Host "Running DAG rollout gate baseline (release_tag=$ReleaseTag, profile=$Profile)"

pwsh -NoProfile -ExecutionPolicy Bypass -File "scripts/dag_alerts_baseline.ps1" `
    -PromUrl $PromUrl `
    -Profile $Profile `
    -LookbackHours $LookbackHours `
    -StepSeconds $StepSeconds `
    -Output $Output `
    -ArchiveDir $ArchiveDir `
    -ArchivePrefix $archivePrefix `
    -SummaryJson $SummaryJson `
    -FailOnGate

exit $LASTEXITCODE
