param(
    [Parameter(Mandatory = $true)]
    [string]$PromUrl,
    [ValidateSet("dev", "staging", "prod")]
    [string]$Profile = "prod",
    [int]$LookbackHours = 24,
    [int]$StepSeconds = 60,
    [string]$Output = "docs/dashboards/dag_alerts_baseline_latest.md",
    [string]$ArchiveDir = "docs/dashboards/archive",
    [string]$ArchivePrefix = "dag_alerts_baseline",
    [string]$SummaryJson = "",
    [switch]$FailOnGate
)

Write-Host "Collecting DAG alert baseline from $PromUrl (profile=$Profile, lookback=${LookbackHours}h, step=${StepSeconds}s)"
$args = @(
    "scripts/dag_alerts_baseline.py",
    "--prom-url", $PromUrl,
    "--profile", $Profile,
    "--lookback-hours", $LookbackHours,
    "--step-seconds", $StepSeconds,
    "--output", $Output,
    "--archive-dir", $ArchiveDir,
    "--archive-prefix", $ArchivePrefix
)

if ($SummaryJson -ne "") {
    $args += @("--summary-json", $SummaryJson)
}
if ($FailOnGate.IsPresent) {
    $args += "--fail-on-gate"
}

python @args
exit $LASTEXITCODE
