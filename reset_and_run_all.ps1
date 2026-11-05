# ====================================================================================================
# SCRIPT RESET VÀ CHẠY LẠI TOÀN BỘ HỆ THỐNG TỪ ĐẦU (END-TO-END PIPELINE)
# ====================================================================================================

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "🧹 BẮT ĐẦU RESET TOÀN BỘ DỮ LIỆU SILVER VÀ GOLD (GIỮ NGUYÊN BRONZE)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan

# 1. Dọn dẹp Silver
if (Test-Path "data\silver\dim_users") { Remove-Item -Recurse -Force "data\silver\dim_users" }
if (Test-Path "data\silver\dim_cards") { Remove-Item -Recurse -Force "data\silver\dim_cards" }
if (Test-Path "data\silver\fact_transactions") { Remove-Item -Recurse -Force "data\silver\fact_transactions" }
if (Test-Path "data\silver\checkpoints") { Remove-Item -Recurse -Force "data\silver\checkpoints" }
Write-Host "✅ Đã dọn sạch tầng Silver." -ForegroundColor Green

# 2. Dọn dẹp Gold
$gold_folders = @("dim_date", "dim_merchant", "dim_users", "dim_cards", "fact_transactions", 
                  "agg_daily_merchant_vol", "agg_monthly_merchant_vol", "agg_quarterly_merchant_vol", "agg_daily_card_fraud")
foreach ($folder in $gold_folders) {
    $path = "data\gold\$folder"
    if (Test-Path $path) { Remove-Item -Recurse -Force $path }
}
Write-Host "✅ Đã dọn sạch tầng Gold DWH." -ForegroundColor Green

# 3. Chạy lại Batch Pipeline (Bronze -> Silver)
Write-Host "`n🚀 BƯỚC 1: XỬ LÝ BATCH BRONZE -> SILVER..." -ForegroundColor Cyan
python src\batch_jobs\process_users.py
python src\batch_jobs\process_cards.py
python src\batch_jobs\process_transactions.py
Write-Host "✅ Hoàn tất xây dựng tầng Silver!" -ForegroundColor Green

# 4. Chạy lại DWH Pipeline (Silver -> Gold)
Write-Host "`n🚀 BƯỚC 2: XÂY DỰNG DATA WAREHOUSE GOLD LAYER..." -ForegroundColor Cyan
.\run_dwh_pipeline.ps1

# 5. Đồng bộ vào SQL Server / SSMS
Write-Host "`n🚀 BƯỚC 3: ĐỒNG BỘ DỮ LIỆU VÀO SQL SERVER / SSMS..." -ForegroundColor Cyan
python src\sync_to_sqlserver.py

Write-Host "`n================================================================================" -ForegroundColor Green
Write-Host "🎉 TOÀN BỘ QUY TRÌNH ĐÃ ĐƯỢC CHẠY LẠI THÀNH CÔNG TỪ ĐẦU!" -ForegroundColor Green
Write-Host "👉 Bạn có thể mở SSMS xem kết quả hoặc chạy 'streamlit run src/web_dashboard.py'." -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Green
