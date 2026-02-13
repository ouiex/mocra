$ErrorActionPreference = "Stop"

function Invoke-CargoChecked {
	param(
		[Parameter(Mandatory = $true)]
		[string[]]$Args
	)

	& cargo @Args
	if ($LASTEXITCODE -ne 0) {
		Write-Error "cargo $($Args -join ' ') failed with exit code $LASTEXITCODE"
		exit $LASTEXITCODE
	}
}

Write-Host "Running contract tests"

Invoke-CargoChecked -Args @("test", "-p", "queue")
Invoke-CargoChecked -Args @("test", "-p", "sync")

$requireRedisTests = $env:MOCRA_REQUIRE_REDIS_CONTRACT_TESTS
$redisUrl = $env:REDIS_URL
if (-not $redisUrl) {
	$redisUrl = $env:MOCRA_REDIS_TEST_URL
}

if ($redisUrl) {
	Write-Host "Running optional Redis integration tests for T09"
	Invoke-CargoChecked -Args @("test", "-p", "engine", "redis_", "--", "--nocapture")
} elseif ($requireRedisTests -eq "1") {
	Write-Error "MOCRA_REQUIRE_REDIS_CONTRACT_TESTS=1 but REDIS_URL/MOCRA_REDIS_TEST_URL is not set"
    exit 1
} else {
	Write-Host "Skipping optional Redis integration tests (set REDIS_URL or MOCRA_REDIS_TEST_URL to enable)"
}
