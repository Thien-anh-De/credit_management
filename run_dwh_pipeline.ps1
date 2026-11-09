# ============================================================
# run_dwh_pipeline.ps1 - Pipeline xay dung DWH Gold Layer
# ============================================================

$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"
if (-not $env:HADOOP_HOME -and (Test-Path "C:\hadoop")) {
    $env:HADOOP_HOME = "C:\hadoop"
    $env:PATH = "C:\hadoop\bin;$env:PATH"
}

# Kiem tra tang Silver, tu dong tao neu thieu
$silverFact  = "data\silver\fact_transactions"
$silverUsers = "data\silver\dim_users"
$silverCards = "data\silver\dim_cards"

if (-not (Test-Path $silverUsers) -or -not (Test-Path $silverCards) -or -not (Test-Path $silverFact)) {
    Write-Host "[INFO] Tang Silver chua co du lieu. Dang chay batch xu ly..."
    if (-not (Test-Path $silverUsers)) { python src/batch_jobs/process_users.py }
    if (-not (Test-Path $silverCards)) { python src/batch_jobs/process_cards.py }
    if (-not (Test-Path $silverFact))  { python src/batch_jobs/process_transactions.py --limit 20000 }
    Write-Host "[INFO] Da chuan bi xong tang Silver."
}

$JOBS = @(
    "src/dwh_jobs/01_build_dim_date.py",
    "src/dwh_jobs/02_build_dim_merchant.py",
    "src/dwh_jobs/03_build_dim_users_scd2.py",
    "src/dwh_jobs/04_build_dim_cards_scd2.py",
    "src/dwh_jobs/05_build_fact_transactions.py",
    "src/dwh_jobs/06_build_summary_layers.py"
)

Write-Host "[INFO] Bat dau xay dung Gold Layer..."
$step = 1
foreach ($job in $JOBS) {
    Write-Host "[$step/6] Chay $job..."
    python $job
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Loi tai buoc $step: $job" -ForegroundColor Red
        exit 1
    }
    $step++
}

Write-Host "[INFO] Hoan tat xay dung toan bo Gold Layer."
