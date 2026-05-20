param(
    [string]$BaseUrl = "http://127.0.0.1:8805",
    [string]$ApiKey = "local-dev",
    [string]$Account = "local",
    [string]$Platform = "monitoring",
    [string]$Module = "local_probe_module",
    [int]$MaxPollAttempts = 40
)

function Get-MetricTotal {
    param(
        [string]$Metrics,
        [string]$Pattern
    )

    $total = 0.0
    $matches = [regex]::Matches(
        $Metrics,
        $Pattern,
        [System.Text.RegularExpressions.RegexOptions]::Multiline
    )
    foreach ($match in $matches) {
        $total += [double]$match.Groups[1].Value
    }
    return $total
}

function Get-MetricSnapshot {
    param([string]$Metrics)

    [ordered]@{
        dispatch_success = Get-MetricTotal $Metrics '^mocra_stage_events_total\{[^}]*pipeline="control_plane",stage="dispatch",action="dispatch_task",result="success"[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)$'
        dag_scheduler_success = Get-MetricTotal $Metrics '^mocra_stage_events_total\{[^}]*pipeline="dag",stage="scheduler",action="execute",result="success"[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)$'
        downloader_success = Get-MetricTotal $Metrics '^mocra_stage_events_total\{[^}]*pipeline="engine",stage="downloader",action="http_request",result="success"[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)$'
        scheduler_ingress_total = Get-MetricTotal $Metrics '^mocra_scheduler_ingress_total(?:\{[^}]*\})?\s+([0-9]+(?:\.[0-9]+)?)$'
        stage_errors_total = Get-MetricTotal $Metrics '^mocra_stage_errors_total(?:\{[^}]*\})?\s+([0-9]+(?:\.[0-9]+)?)$'
    }
}

$beforeMetrics = (Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/metrics").Content
$before = Get-MetricSnapshot $beforeMetrics

$payload = @{
    account = $Account
    platform = $Platform
    module = @($Module)
    priority = "normal"
} | ConvertTo-Json -Compress

$dispatch = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$BaseUrl/tasks/dispatch" -Headers @{ "x-api-key" = $ApiKey } -ContentType "application/json" -Body $payload

$after = $before
for ($attempt = 0; $attempt -lt $MaxPollAttempts; $attempt++) {
    $metrics = (Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/metrics").Content
    $after = Get-MetricSnapshot $metrics

    $dispatchDelta = $after.dispatch_success - $before.dispatch_success
    $schedulerDelta = $after.dag_scheduler_success - $before.dag_scheduler_success
    $downloaderDelta = $after.downloader_success - $before.downloader_success
    $schedulerIngressDelta = $after.scheduler_ingress_total - $before.scheduler_ingress_total
    $stageErrorsDelta = $after.stage_errors_total - $before.stage_errors_total

    $blocking = $schedulerIngressDelta -gt 0 -or $stageErrorsDelta -gt 0
    $ready = $dispatchDelta -ge 1 -and $schedulerDelta -ge 1 -and $downloaderDelta -ge 1

    if ($blocking -or $ready) {
        break
    }
}

$dispatchSuccess = ($after.dispatch_success - $before.dispatch_success) -ge 1
$schedulerSuccess = ($after.dag_scheduler_success - $before.dag_scheduler_success) -ge 1
$downloaderSuccess = ($after.downloader_success - $before.downloader_success) -ge 1
$schedulerIngressAbsent = ($after.scheduler_ingress_total - $before.scheduler_ingress_total) -eq 0
$stageErrorsAbsent = ($after.stage_errors_total - $before.stage_errors_total) -eq 0

$result = [ordered]@{
    status = if (
        $dispatch.StatusCode -eq 202 -and
        $dispatchSuccess -and
        $schedulerSuccess -and
        $downloaderSuccess -and
        $schedulerIngressAbsent -and
        $stageErrorsAbsent
    ) {
        "PASS"
    } else {
        "FAIL"
    }
    dispatch_status = [int]$dispatch.StatusCode
    module = $Module
    checks = [ordered]@{
        dispatch_success = $dispatchSuccess
        dag_scheduler_success = $schedulerSuccess
        downloader_success = $downloaderSuccess
        scheduler_ingress_absent = $schedulerIngressAbsent
        stage_errors_absent = $stageErrorsAbsent
    }
}

$result | ConvertTo-Json -Depth 10

if ($result.status -ne "PASS") {
    exit 1
}