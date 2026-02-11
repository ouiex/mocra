$ErrorActionPreference = "Stop"

Write-Host "Running contract tests"

cargo test -p queue
cargo test -p sync
