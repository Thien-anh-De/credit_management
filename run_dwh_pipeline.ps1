# ============================================================
# run_dwh_pipeline.ps1 - Chay toan bo Gold Layer Build Pipeline
# ============================================================
# Cach dung (tu thu muc goc project):
#   .\run_dwh_pipeline.ps1
# ============================================================

$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"
if (-not $env:HADOOP_HOME -and (Test-Path "C:\hadoop")) {
    $env:HADOOP_HOME = "C:\hadoop"
    $env:PATH = "C:\hadoop\bin;$env:PATH"
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  CREDIT MANAGEMENT - DWH Gold Layer Build Pipeline" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$JOBS = @(
    "src/dwh_jobs/01_build_dim_date.py",
    "src/dwh_jobs/02_build_dim_merchant.py",
    "src/dwh_jobs/03_build_dim_users_scd2.py",
    "src/dwh_jobs/04_build_dim_cards_scd2.py",
    "src/dwh_jobs/05_build_fact_transactions.py",
    "src/dwh_jobs/06_build_summary_layers.py"
)

$step = 1
foreach ($job in $JOBS) {
    Write-Host "[$step/6] Running: $job" -ForegroundColor Yellow
    python $job
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED at step $step - $job" -ForegroundColor Red
        exit 1
    }
    Write-Host "Step $step completed successfully" -ForegroundColor Green
    Write-Host ""
    $step++
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Gold Layer BUILD COMPLETE!" -ForegroundColor Green
Write-Host "  Running Data Quality and Benchmark Test..." -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

python src/tests/07_dq_and_benchmark.py

Write-Host ""
Write-Host "  Report saved to: docs/milestone5_dq_benchmark_report.md" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
