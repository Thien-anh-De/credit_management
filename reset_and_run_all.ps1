# ====================================================================================================
# SCRIPT RESET VA CHAY LAI TOAN BO HE THONG TU DAU (END-TO-END DEMO PIPELINE)
# Medallion Lakehouse (Bronze -> Silver -> Gold) & Kimball Star Schema Data Warehouse
# ====================================================================================================

$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "[*] BAT DAU RESET TOAN BO DU LIEU SILVER VA GOLD (GIU NGUYEN BRONZE)..." -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan

# 1. Don dep Silver
$silver_items = @(
    "dim_users", "dim_cards", "fact_transactions", "checkpoints", "dim_cards_baseline_backup"
)
foreach ($item in $silver_items) {
    $p = "data\silver\$item"
    if (Test-Path $p) { Remove-Item -Recurse -Force $p }
}
Write-Host "[OK] Da don sach tang Silver." -ForegroundColor Green

# 2. Don dep Gold & Audit
$gold_folders = @("dim_date", "dim_merchant", "dim_users", "dim_cards", "fact_transactions", 
                  "agg_daily_merchant_vol", "agg_monthly_merchant_vol", "agg_quarterly_merchant_vol", "agg_daily_card_fraud", "etl_audit_log")
foreach ($folder in $gold_folders) {
    $path = "data\gold\$folder"
    if (Test-Path $path) { Remove-Item -Recurse -Force $path }
}
if (Test-Path "data\gold\etl_audit_log.json") { Remove-Item -Force "data\gold\etl_audit_log.json" }
Write-Host "[OK] Da don sach tang Gold DWH va Audit log." -ForegroundColor Green

# 3. Don dep Quarantine
if (Test-Path "data\quarantine") {
    Get-ChildItem -Path "data\quarantine" | Where-Object { $_.Name -ne ".gitkeep" } | ForEach-Object {
        Remove-Item -Path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
    }
}
Write-Host "[OK] Da don sach vung cach ly Quarantine." -ForegroundColor Green

# 4. Chay Batch Pipeline du lieu the ngan hang (Bronze -> Silver)
Write-Host "`n[*] BUOC 1: XU LY BATCH DU LIEU NGAN HANG BRONZE -> SILVER..." -ForegroundColor Cyan
python src\batch_jobs\process_users.py
python src\batch_jobs\process_cards.py
python src\batch_jobs\process_transactions.py
Write-Host "[OK] Hoan tat xay dung tang Silver!" -ForegroundColor Green

# 5. Chay DWH Pipeline (Silver -> Gold: Star Schema, SCD2 PIT Join, Inmon Summary Layers)
Write-Host "`n[*] BUOC 2: XAY DUNG DATA WAREHOUSE GOLD LAYER..." -ForegroundColor Cyan
.\run_dwh_pipeline.ps1

# 6. Dong bo vao SQL Server / SSMS (neu service dang chay)
Write-Host "`n[*] BUOC 3: DONG BO DU LIEU VAO SQL SERVER / SSMS..." -ForegroundColor Cyan
try {
    python src\sync_to_sqlserver.py
} catch {
    Write-Host "[!] Bo qua buoc sync SSMS neu dich vu SQL Server chua khoi dong." -ForegroundColor Yellow
}

# 7. Chay kiem thu toan dien Data Quality & Performance Benchmark
Write-Host "`n[*] BUOC 4: CHAY BO KIEM THU DATA QUALITY & QUERY BENCHMARK..." -ForegroundColor Cyan
python src\tests\07_dq_and_benchmark.py

Write-Host "`n================================================================================" -ForegroundColor Green
Write-Host "[SUCCESS] TOAN BO QUY TRINH MEDALLION VA DWH DA DUOC DUNG LAI THANH CONG TU DAU!" -ForegroundColor Green
Write-Host "-> 1. Xem So do Kien truc   : docs/architecture_and_repo.md" -ForegroundColor Yellow
Write-Host "-> 2. Xem Bao cao DQ & Bench: docs/data_quality_benchmark_report.md" -ForegroundColor Yellow
Write-Host "-> 3. Mo Web Dashboard      : streamlit run src/web_dashboard.py" -ForegroundColor Yellow
Write-Host "-> 4. Demo SCD Type 2       : Mo SSMS va chay file docs/demo_scd2.sql" -ForegroundColor Yellow
Write-Host "-> 5. Truy van DWH OLAP     : python src/query_dwh.py" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Green
