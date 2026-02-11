param(
    [int]$Total = 200000,
    [int]$Concurrency = 4,
    [int]$Buffer = 20000
)

Write-Host "Running log stress: total=$Total concurrency=$Concurrency buffer=$Buffer"
cargo run -p tests --bin log_stress -- --total $Total --concurrency $Concurrency --buffer $Buffer

Write-Host "Baseline target: >= 20000 logs/sec, drop rate < 0.5% (adjust for hardware)"
